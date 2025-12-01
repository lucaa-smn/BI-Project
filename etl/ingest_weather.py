from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd


RAW_FILENAME = "weather_meteo_by_airport.csv"


def get_project_root() -> Path:
    """
    Liefert den Projekt-Root relativ zu diesem File.
    Annahme: dieses Skript liegt in etl/ und data/ liegt direkt im Projektroot.
    """
    return Path(__file__).resolve().parent.parent


def get_raw_path_weather() -> Path:
    return get_project_root() / "data" / "raw" / RAW_FILENAME


def get_weather_staging_path() -> Path:
    staging_dir = get_project_root() / "data" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / "dim_weather.parquet"


def _find_first_existing(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Gibt den ersten existierenden Spaltennamen aus candidates zurück oder None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _map_weather_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mappt verschiedene mögliche Spaltennamen der Rohdatei auf ein
    standardisiertes Wetter-Schema:

        airport_id, date, tavg, prcp, wspd, ...

    Ziel ist ein DataFrame mit mindestens:
        airport_id, date
    plus einigen numerischen Wettermetriken.
    """

    col_map_candidates: Dict[str, list[str]] = {
        "airport_id": [
            "airport_id",
            "AIRPORT_ID",
            "IATA_CODE",
            "iata_code",
            "station",
            "STATION",
            "Dep_Airport",
            "origin",
        ],
        "date": ["time", "DATE", "dt", "day"],
        "tavg": ["tavg", "TAVG", "temp_avg", "temperature", "temp"],
        "prcp": ["prcp", "PRCP", "precip", "precipitation", "rain"],
        "wspd": ["wspd", "WSPD", "w_speed", "wind_speed"],
        "tmin": ["tmin", "TMIN", "temp_min"],
        "tmax": ["tmax", "TMAX", "temp_max"],
        "snow": ["snow", "SNOW"],
    }

    mapped_cols: Dict[str, str] = {}

    for target, candidates in col_map_candidates.items():
        src = _find_first_existing(df, candidates)
        if src is None:
            if target in ("airport_id", "date"):
                raise KeyError(
                    f"Keine Spalte für '{target}' gefunden. "
                    f"Prüfe die Spaltennamen in {RAW_FILENAME}. "
                    f"Gesuchte Kandidaten: {candidates}"
                )
            else:
                print(
                    f"[Warnung] Keine Spalte für '{target}' gefunden (Candidates: {candidates})."
                )
        else:
            mapped_cols[target] = src

    result_cols = {}
    for target, src in mapped_cols.items():
        result_cols[target] = df[src]

    result_df = pd.DataFrame(result_cols)

    return result_df


def _basic_data_quality_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    Grundlegende Datenbereinigung für Wetterdaten:

    - airport_id normalisieren (trim, upper)
    - date zu datetime parsen
    - Zeilen mit fehlendem airport_id/date droppen
    - numerische Wetterspalten casten
    - bei mehrfachen Zeilen pro (airport_id, date) aggregieren
    """

    required = ["airport_id", "date"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Fehlende Pflichtspalten in Weather-Staging: {missing}")

    df["airport_id"] = df["airport_id"].astype(str).str.strip().str.upper()

    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["airport_id", "date"])
    after = len(df)
    print(f"[DQ] Entfernte Zeilen mit fehlendem airport_id/date: {before - after}")

    numeric_cols = [c for c in df.columns if c not in ("airport_id", "date")]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    agg_dict: Dict[str, str] = {}
    for col in numeric_cols:
        if col in ("prcp", "snow"):
            agg_dict[col] = "sum"
        else:
            agg_dict[col] = "mean"

    if agg_dict:
        before = len(df)
        df = df.groupby(["airport_id", "date"], as_index=False).agg(agg_dict)
        after = len(df)
        print(f"[Agg] Aggregation auf (airport_id, date): {before} → {after} Zeilen")

    return df


def ingest_weather() -> None:
    """
    Liest weather_meteo_by_airport.csv ein, mappt & bereinigt die Spalten
    und speichert das Ergebnis als data/staging/dim_weather.parquet.
    """

    raw_path = get_raw_path_weather()
    staging_path = get_weather_staging_path()

    if not raw_path.exists():
        raise FileNotFoundError(
            f"Rohdatei für Wetterdaten nicht gefunden: {raw_path}\n"
            f"Erwarte {RAW_FILENAME} in data/raw/."
        )

    print(f"Lese Wetter-Rohdaten aus: {raw_path}")
    df_raw = pd.read_csv(raw_path)
    print(f"Anzahl Zeilen (roh): {len(df_raw)}")
    print(f"Verfügbare Spalten: {list(df_raw.columns)}")

    df = _map_weather_columns(df_raw)

    df = _basic_data_quality_weather(df)

    print("Schreibe dim_weather-Stagingdaten nach Parquet...")
    df.to_parquet(staging_path, index=False)
    print(f"Fertig. Datei erstellt: {staging_path}")


if __name__ == "__main__":
    ingest_weather()
