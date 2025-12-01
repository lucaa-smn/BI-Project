from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import pandas as pd


RAW_FILENAME = "airports_geolocation.csv"


def get_project_root() -> Path:
    """
    Liefert den Projekt-Root relativ zu diesem File.
    Annahme: dieses Skript liegt in etl/ und data/ liegt direkt im Projektroot.
    """
    return Path(__file__).resolve().parent.parent


def get_raw_path_airports() -> Path:
    return get_project_root() / "data" / "raw" / RAW_FILENAME


def get_airports_staging_path() -> Path:
    staging_dir = get_project_root() / "data" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / "dim_airport.parquet"


def _find_first_existing(df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
    """Gibt den ersten existierenden Spaltennamen aus candidates zurück oder None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _map_airport_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mappt verschiedene mögliche Spaltennamen der Rohdatei auf das
    Standard-Schema für dim_airport:

        airport_id, name, city, state, country, latitude, longitude
    """

    col_map_candidates: Dict[str, list[str]] = {
        "airport_id": ["IATA_CODE", "IATA", "airport_code", "code", "iata_code"],
        "name": ["AIRPORT", "name", "airport_name"],
        "city": ["CITY", "city", "metro_area"],
        "state": ["STATE", "state", "region", "province"],
        "country": ["COUNTRY", "country", "country_name", "COUNTRY_NAME"],
        "latitude": ["LATITUDE", "lat", "latitude"],
        "longitude": ["LONGITUDE", "lon", "lng", "longitude"],
    }

    mapped_cols: Dict[str, str] = {}

    for target, candidates in col_map_candidates.items():
        src = _find_first_existing(df, candidates)
        if src is None:
            print(
                f"[Warnung] Keine Spalte für '{target}' gefunden (Candidates: {candidates})."
            )
        else:
            mapped_cols[target] = src

    if "airport_id" not in mapped_cols:
        raise KeyError(
            "Konnte keine Spalte für 'airport_id' finden. "
            "Bitte prüfe die Spaltennamen in airports_geolocation.csv "
            "(z. B. 'IATA_CODE')."
        )

    result_cols = {}
    for target, src in mapped_cols.items():
        result_cols[target] = df[src]

    result_df = pd.DataFrame(result_cols)

    return result_df


def _basic_data_quality_airports(df: pd.DataFrame) -> pd.DataFrame:
    """
    Führt grundlegende Data-Quality-Schritte für die Airports durch:

    - airport_id trim / upper
    - Duplikate auf airport_id entfernen
    - Zeilen mit fehlender airport_id droppen
    """

    if "airport_id" not in df.columns:
        raise KeyError("Spalte 'airport_id' wird für dim_airport benötigt.")

    df["airport_id"] = df["airport_id"].astype(str).str.strip().str.upper()

    before = len(df)
    df = df[df["airport_id"] != ""]
    after = len(df)
    print(f"[DQ] Entfernte Zeilen mit leerem airport_id: {before - after}")

    before = len(df)
    df = df.drop_duplicates(subset=["airport_id"])
    after = len(df)
    print(f"[DQ] Entfernte Duplikate auf airport_id: {before - after}")

    for coord_col in ["latitude", "longitude"]:
        if coord_col in df.columns:
            df[coord_col] = pd.to_numeric(df[coord_col], errors="coerce")

    return df


def ingest_airports() -> None:
    """
    Liest airports_geolocation.csv ein, mappt die Spalten auf das
    dim_airport-Schema und speichert das Ergebnis als
    data/staging/dim_airport.parquet.
    """

    raw_path = get_raw_path_airports()
    staging_path = get_airports_staging_path()

    if not raw_path.exists():
        raise FileNotFoundError(
            f"Rohdatei für Airports nicht gefunden: {raw_path}\n"
            f"Erwarte airports_geolocation.csv in data/raw/."
        )

    print(f"Lese Airport-Rohdaten aus: {raw_path}")
    df_raw = pd.read_csv(raw_path)
    print(f"Anzahl Zeilen (roh): {len(df_raw)}")
    print(f"Verfügbare Spalten: {list(df_raw.columns)}")

    df = _map_airport_columns(df_raw)

    df = _basic_data_quality_airports(df)

    print("Schreibe dim_airport-Stagingdaten nach Parquet...")
    df.to_parquet(staging_path, index=False)
    print(f"Fertig. Datei erstellt: {staging_path}")


if __name__ == "__main__":
    ingest_airports()
