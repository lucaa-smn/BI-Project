from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


RAW_FILENAME = "US_flights_2023.csv"


def get_project_root() -> Path:
    """
    Liefert den Projekt-Root relativ zu diesem File.
    Annahme: dieses Skript liegt in etl/ und data/ liegt direkt im Projektroot.
    """
    return Path(__file__).resolve().parent.parent


def get_raw_path() -> Path:
    return get_project_root() / "data" / "raw" / RAW_FILENAME


def get_staging_path() -> Path:
    staging_dir = get_project_root() / "data" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / "flights.parquet"


def _normalize_dep_time_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalisiert die Abflugzeit in ein einheitliches Feld 'dep_time_slot'.

    - Wenn eine echte Zeitspalte existiert (z. B. 'DepTime' oder 'CRSDepTime'),
      wird daraus eine Stundenangabe 0–23 berechnet.
    - Sonst wird 'DepTime_label' (falls vorhanden) bereinigt übernommen.
    """

    time_col: Optional[str] = None
    for candidate in ["DepTime", "CRSDepTime"]:
        if candidate in df.columns:
            time_col = candidate
            break

    if time_col is not None:
        df[time_col] = pd.to_numeric(df[time_col], errors="coerce")
        df["dep_hour"] = (df[time_col] // 100).astype("Int64")
        df.loc[(df["dep_hour"] < 0) | (df["dep_hour"] > 23), "dep_hour"] = pd.NA
        df["dep_time_slot"] = df["dep_hour"]
    else:
        if "DepTime_label" in df.columns:
            df["dep_time_slot"] = df["DepTime_label"].astype(str).str.strip()
        else:
            df["dep_time_slot"] = pd.NA

    return df


def _cast_delay_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Castet relevante Delay-Spalten auf numerische Typen.
    Fehlerhafte Werte werden zu NaN.
    Negative Werte (früher Abflug/Ankunft) werden auf 0 gecappt,
    weil wir "Delay" als Verspätung interpretieren.
    """

    delay_cols = [
        "Dep_Delay",
        "Arr_Delay",
        "Delay_Carrier",
        "Delay_Weather",
        "Delay_NAS",
        "Delay_Security",
        "Delay_LastAircraft",
    ]

    for col in delay_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].clip(lower=0)

    return df


def _basic_data_quality(df: pd.DataFrame) -> pd.DataFrame:
    """
    Führt grundlegende Data-Quality-Schritte durch:

    - FlightDate als Datum parsen
    - ungültige / fehlende FlightDate, Dep_Airport, Airline entfernen
    - Dep_Airport / Airline normalisieren (trim, upper)
    - Duplikate entfernen
    """

    if "FlightDate" not in df.columns:
        raise KeyError("Spalte 'FlightDate' fehlt im US_flights_2023.csv")

    df["FlightDate"] = pd.to_datetime(df["FlightDate"], errors="coerce")

    required_cols = ["FlightDate", "Dep_Airport", "Airline"]
    missing_required = [c for c in required_cols if c not in df.columns]
    if missing_required:
        raise KeyError(
            f"Fehlende Pflichtspalten im US_flights_2023.csv: {missing_required}"
        )

    df["Dep_Airport"] = df["Dep_Airport"].astype(str).str.strip().str.upper()

    if "Arr_Airport" in df.columns:
        df["Arr_Airport"] = df["Arr_Airport"].astype(str).str.strip().str.upper()

    df["Airline"] = df["Airline"].astype(str).str.strip()

    before = len(df)
    df = df.dropna(subset=["FlightDate", "Dep_Airport", "Airline"])
    after = len(df)
    print(f"[DQ] Entfernte Zeilen mit fehlenden Pflichtfeldern: {before - after}")

    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"[DQ] Entfernte Duplikate: {before - after}")

    return df


def ingest_flights() -> None:
    """
    Hauptfunktion: liest US_flights_2023.csv, bereinigt & normalisiert
    und schreibt das Ergebnis als Parquet nach data/staging/flights.parquet.
    """

    raw_path = get_raw_path()
    staging_path = get_staging_path()

    print(f"Lese Rohdaten aus: {raw_path}")

    if not raw_path.exists():
        raise FileNotFoundError(
            f"Rohdatei nicht gefunden: {raw_path}. "
            f"Erwarte US_flights_2023.csv in data/raw/"
        )

    df = pd.read_csv(raw_path)

    print(f"Anzahl Zeilen (roh): {len(df)}")

    df = _basic_data_quality(df)
    df = _normalize_dep_time_columns(df)

    df = _cast_delay_columns(df)

    print("Schreibe bereinigte Daten nach Parquet...")
    df.to_parquet(staging_path, index=False)
    print(f"Fertig. Datei erstellt: {staging_path}")


if __name__ == "__main__":
    ingest_flights()
