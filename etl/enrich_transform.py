from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd


def get_project_root() -> Path:
    """
    Liefert den Projekt-Root relativ zu diesem File.
    Annahme: dieses Skript liegt in etl/ und data/ liegt direkt im Projektroot.
    """
    return Path(__file__).resolve().parent.parent


def get_flights_enriched_path() -> Path:
    return get_project_root() / "data" / "staging" / "flights_enriched.parquet"


def get_airports_staging_path() -> Path:
    return get_project_root() / "data" / "staging" / "dim_airport.parquet"


def get_weather_staging_path() -> Path:
    return get_project_root() / "data" / "staging" / "dim_weather.parquet"


def get_output_path() -> Path:
    staging_dir = get_project_root() / "data" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / "flights_enriched_weather.parquet"


def _ensure_datetime(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Stellt sicher, dass eine Spalte als datetime64 vorliegt."""
    if col not in df.columns:
        raise KeyError(f"Erwarte Spalte '{col}' im DataFrame.")
    df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _basic_flights_normalization(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basis-Normalisierung für Flights:
    - FlightDate → datetime
    - Dep_Airport / Arr_Airport → trim + upper
    """

    if "FlightDate" not in df.columns:
        raise KeyError("Spalte 'FlightDate' fehlt in flights_enriched.parquet")

    df = _ensure_datetime(df, "FlightDate")

    for col in ["Dep_Airport", "Arr_Airport"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    return df


def _add_date_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Erzeugt eine Spalte date_id (YYYYMMDD als int) aus FlightDate.
    """

    if "FlightDate" not in df.columns:
        raise KeyError("Spalte 'FlightDate' wird für date_id benötigt.")

    date_str = df["FlightDate"].dt.strftime("%Y%m%d")
    df["date_id"] = date_str.astype("int64")

    return df


def _join_weather(flights_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """
    Join Flights mit Wetterdaten:
      Flights:      Dep_Airport, FlightDate
      Wetterdaten:  airport_id, date

    Ergebnis: Wettermetriken (tavg, prcp, wspd, ...) hängen an den Flights.
    """

    if "airport_id" not in weather_df.columns:
        raise KeyError("Spalte 'airport_id' fehlt in dim_weather.parquet.")
    if "date" not in weather_df.columns:
        raise KeyError("Spalte 'date' fehlt in dim_weather.parquet.")

    weather_df = weather_df.copy()
    weather_df["airport_id"] = (
        weather_df["airport_id"].astype(str).str.strip().str.upper()
    )
    weather_df["date"] = pd.to_datetime(weather_df["date"], errors="coerce")

    flights_df = flights_df.copy()

    flights_df["FlightDate_join"] = flights_df["FlightDate"].dt.normalize()
    weather_df["date_join"] = weather_df["date"].dt.normalize()

    weather_value_cols = [
        c for c in weather_df.columns if c not in ("airport_id", "date", "date_join")
    ]

    print("Join Flights ⟵ Weather auf (Dep_Airport, FlightDate)...")

    merged = flights_df.merge(
        weather_df[["airport_id", "date_join"] + weather_value_cols],
        how="left",
        left_on=["Dep_Airport", "FlightDate_join"],
        right_on=["airport_id", "date_join"],
        suffixes=("", "_wx"),
    )

    matches = merged["airport_id"].notna().sum()
    print(f"  Wetter-Matches gefunden: {matches} von {len(merged)} Flights")

    merged = merged.drop(columns=["FlightDate_join", "date_join"])

    merged = merged.rename(columns={"airport_id": "weather_airport_id"})

    return merged


def _join_dep_airport_attributes(
    flights_df: pd.DataFrame, airport_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Optionaler Join: hängt einige Attribute des Abflughafens an die Flights,
    z. B. dep_city, dep_state, dep_country.

    Es bleibt ein reiner Convenience-Join; für das Star-Schema brauchst du
    später nur die Codes.
    """

    if "airport_id" not in airport_df.columns:
        raise KeyError("Spalte 'airport_id' fehlt in dim_airport.parquet.")

    airport_df = airport_df.copy()
    airport_df["airport_id"] = (
        airport_df["airport_id"].astype(str).str.strip().str.upper()
    )

    dep_airport_prefixed = airport_df.rename(
        columns={
            "airport_id": "Dep_Airport",
            "name": "dep_airport_name",
            "city": "dep_city",
            "state": "dep_state",
            "country": "dep_country",
            "latitude": "dep_latitude",
            "longitude": "dep_longitude",
        }
    )

    keep_cols = [
        c
        for c in dep_airport_prefixed.columns
        if c
        in (
            "Dep_Airport",
            "dep_airport_name",
            "dep_city",
            "dep_state",
            "dep_country",
            "dep_latitude",
            "dep_longitude",
        )
    ]

    dep_airport_prefixed = dep_airport_prefixed[keep_cols]

    print("Join Flights ⟵ dep_airport-Attribute auf Dep_Airport...")

    merged = flights_df.merge(dep_airport_prefixed, how="left", on="Dep_Airport")

    dep_matches = merged["dep_airport_name"].notna().sum()
    print(f"  Dep-Airport-Attribute für {dep_matches} Flights gefunden.")

    return merged


def _add_is_delayed_flag(df: pd.DataFrame, threshold_min: float = 15.0) -> pd.DataFrame:
    """
    Fügt eine boolsche Spalte 'is_delayed_15' hinzu, basierend auf Dep_Delay.
    """

    if "Dep_Delay" not in df.columns:
        print(
            "[Warnung] Spalte 'Dep_Delay' nicht gefunden – is_delayed_15 wird False gesetzt."
        )
        df["is_delayed_15"] = False
        return df

    df["Dep_Delay"] = pd.to_numeric(df["Dep_Delay"], errors="coerce")
    df["is_delayed_15"] = (df["Dep_Delay"] >= threshold_min).fillna(False)

    return df


def enrich_transform() -> None:
    """
    Enrichment / Join (Step 2.5):

    - lädt flights_enriched.parquet (Flights + is_cancelled/is_diverted)
    - hängt Wetterdaten aus dim_weather.parquet an
    - optional: hängt Dep-Airport-Attribute aus dim_airport.parquet an
    - erzeugt date_id und is_delayed_15
    - speichert Ergebnis als flights_enriched_weather.parquet
    """

    flights_path = get_flights_enriched_path()
    airports_path = get_airports_staging_path()
    weather_path = get_weather_staging_path()
    output_path = get_output_path()

    if not flights_path.exists():
        raise FileNotFoundError(
            f"Staging-Datei für Flights nicht gefunden: {flights_path}\n"
            f"Bitte zuerst etl/ingest_flights.py und etl/ingest_cancelled.py ausführen."
        )

    if not airports_path.exists():
        raise FileNotFoundError(
            f"Staging-Datei für Airports nicht gefunden: {airports_path}\n"
            f"Bitte zuerst etl/ingest_airports.py ausführen."
        )

    if not weather_path.exists():
        raise FileNotFoundError(
            f"Staging-Datei für Wetterdaten nicht gefunden: {weather_path}\n"
            f"Bitte zuerst etl/ingest_weather.py ausführen."
        )

    print(f"Lese Flights-Stagingdaten aus: {flights_path}")
    flights_df = pd.read_parquet(flights_path)
    print(f"Flights-Zeilen (vor Enrichment): {len(flights_df)}")

    print(f"Lese Airport-Stagingdaten aus: {airports_path}")
    airport_df = pd.read_parquet(airports_path)
    print(f"Airports: {len(airport_df)}")

    print(f"Lese Wetter-Stagingdaten aus: {weather_path}")
    weather_df = pd.read_parquet(weather_path)
    print(f"Wetter-Zeilen: {len(weather_df)}")

    flights_df = _basic_flights_normalization(flights_df)
    flights_df = _add_date_id(flights_df)

    flights_df = _join_weather(flights_df, weather_df)

    flights_df = _join_dep_airport_attributes(flights_df, airport_df)

    flights_df = _add_is_delayed_flag(flights_df, threshold_min=15.0)

    print("Schreibe angereicherte Flights-Daten nach Parquet...")
    flights_df.to_parquet(output_path, index=False)
    print(f"Fertig. Datei erstellt: {output_path}")


if __name__ == "__main__":
    enrich_transform()
