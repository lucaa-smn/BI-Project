from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


RAW_FILENAME = "Cancelled_Diverted_2023.csv"


def get_project_root() -> Path:
    """
    Liefert den Projekt-Root relativ zu diesem File.
    Annahme: dieses Skript liegt in etl/ und data/ liegt direkt im Projektroot.
    """
    return Path(__file__).resolve().parent.parent


def get_raw_path_cancelled() -> Path:
    return get_project_root() / "data" / "raw" / RAW_FILENAME


def get_flights_staging_path() -> Path:
    return get_project_root() / "data" / "staging" / "flights.parquet"


def get_flights_enriched_path() -> Path:
    staging_dir = get_project_root() / "data" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / "flights_enriched.parquet"


def _normalize_key_columns(df: pd.DataFrame, context: str) -> pd.DataFrame:
    """
    Normalisiert die Join-Key-Spalten:
    FlightDate, Airline, Dep_Airport, Tail_Number.

    Erwartete Spalten:
        - FlightDate
        - Airline
        - Dep_Airport
        - Tail_Number
    """

    required = ["FlightDate", "Airline", "Dep_Airport", "Tail_Number"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(
            f"Fehlende Spalten {missing} in DataFrame ({context}). "
            f"Erforderlich für den Join (FlightDate, Airline, Dep_Airport, Tail_Number)."
        )

    # FlightDate als Datum
    df["FlightDate"] = pd.to_datetime(df["FlightDate"], errors="coerce")

    # Codes normalisieren
    df["Airline"] = df["Airline"].astype(str).str.strip()
    df["Dep_Airport"] = df["Dep_Airport"].astype(str).str.strip().str.upper()
    df["Tail_Number"] = df["Tail_Number"].astype(str).str.strip()

    return df


def _to_bool_from_generic(col: pd.Series) -> pd.Series:
    """
    Wandelt eine Spalte mit 0/1, True/False, Y/N etc. in bool um.
    Alles, was nach dem Bereinigen "wahr" aussieht, wird True.
    """
    if col is None:
        return pd.Series([False] * 0)

    s = col.copy()

    numeric = pd.to_numeric(s, errors="coerce")
    if numeric.notna().any():
        s = numeric
        return s.fillna(0) != 0

    s = s.astype(str).str.strip().str.lower()
    true_values = {"1", "y", "yes", "true", "t"}
    return s.isin(true_values)


def ingest_cancelled() -> None:
    """
    Liest Cancelled_Diverted_2023.csv ein, joined es auf flights.parquet
    via (FlightDate, Airline, Dep_Airport, Tail_Number) und setzt
    is_cancelled / is_diverted Flags.

    Ergebnis wird als data/staging/flights_enriched.parquet gespeichert.
    """

    raw_cancelled_path = get_raw_path_cancelled()
    flights_path = get_flights_staging_path()
    enriched_path = get_flights_enriched_path()

    if not flights_path.exists():
        raise FileNotFoundError(
            f"Staging-Datei flights.parquet nicht gefunden: {flights_path}\n"
            f"Bitte zuerst etl/ingest_flights.py ausführen."
        )

    if not raw_cancelled_path.exists():
        raise FileNotFoundError(
            f"Rohdatei für Cancellations/Diverted nicht gefunden: {raw_cancelled_path}\n"
            f"Erwarte Cancelled_Diverted_2023.csv in data/raw/."
        )

    print(f"Lese Flights-Stagingdaten aus: {flights_path}")
    flights_df = pd.read_parquet(flights_path)
    print(f"Flights-Zeilen (vor Join): {len(flights_df)}")

    print(f"Lese Cancellation/Diverted Daten aus: {raw_cancelled_path}")
    canc_df = pd.read_csv(raw_cancelled_path)
    print(f"Cancellation-Zeilen (roh): {len(canc_df)}")

    flights_df = _normalize_key_columns(flights_df, context="flights.parquet")
    canc_df = _normalize_key_columns(canc_df, context="Cancelled_Diverted_2023.csv")

    before = len(canc_df)
    canc_df = canc_df.dropna(
        subset=["FlightDate", "Airline", "Dep_Airport", "Tail_Number"]
    )
    after = len(canc_df)
    print(
        f"[Cancellation] Entfernte Zeilen mit fehlenden Key-Feldern: {before - after}"
    )

    before = len(canc_df)
    canc_df = canc_df.drop_duplicates(
        subset=["FlightDate", "Airline", "Dep_Airport", "Tail_Number"]
    )
    after = len(canc_df)
    print(f"[Cancellation] Entfernte Duplikate auf Join-Key: {before - after}")

    cancel_col: Optional[str] = None
    divert_col: Optional[str] = None

    for candidate in ["Cancelled", "IsCancelled", "is_cancelled"]:
        if candidate in canc_df.columns:
            cancel_col = candidate
            break

    for candidate in ["Diverted", "IsDiverted", "is_diverted"]:
        if candidate in canc_df.columns:
            divert_col = candidate
            break

    if cancel_col is None and divert_col is None:
        print(
            "[Warnung] Keine Spalten für Cancel/Divert Flags im Cancellation-Datensatz gefunden.\n"
            "Erwarte z.B. 'Cancelled' und/oder 'Diverted'.\n"
            "is_cancelled und is_diverted werden auf False gesetzt."
        )
        canc_df["is_cancelled_tmp"] = False
        canc_df["is_diverted_tmp"] = False
    else:
        if cancel_col is not None:
            canc_df["is_cancelled_tmp"] = _to_bool_from_generic(canc_df[cancel_col])
        else:
            canc_df["is_cancelled_tmp"] = False

        if divert_col is not None:
            canc_df["is_diverted_tmp"] = _to_bool_from_generic(canc_df[divert_col])
        else:
            canc_df["is_diverted_tmp"] = False

    join_cols = ["FlightDate", "Airline", "Dep_Airport", "Tail_Number"]
    join_cols_extended = join_cols + ["is_cancelled_tmp", "is_diverted_tmp"]

    canc_join_df = canc_df[join_cols_extended].copy()

    print("Führe Left Join von Flights mit Cancellation/Diverted Daten durch...")

    merged = flights_df.merge(
        canc_join_df,
        how="left",
        on=["FlightDate", "Airline", "Dep_Airport", "Tail_Number"],
        suffixes=("", "_canc"),
    )

    matches = merged["is_cancelled_tmp"].notna().sum()
    print(f"Anzahl Flights mit Cancellation/Diverted-Info (Matches): {matches}")

    merged["is_cancelled"] = merged["is_cancelled_tmp"].fillna(False).astype(bool)
    merged["is_diverted"] = merged["is_diverted_tmp"].fillna(False).astype(bool)

    merged = merged.drop(columns=["is_cancelled_tmp", "is_diverted_tmp"])

    print("Schreibe angereicherte Flights-Daten nach Parquet...")
    merged.to_parquet(enriched_path, index=False)
    print(f"Fertig. Datei erstellt: {enriched_path}")


if __name__ == "__main__":
    ingest_cancelled()
