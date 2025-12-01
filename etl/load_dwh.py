from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from config import get_engine


def get_project_root() -> Path:
    """
    Liefert den Projekt-Root relativ zu diesem File.
    Annahme: dieses Skript liegt in etl/ und data/ liegt direkt im Projektroot.
    """
    return Path(__file__).resolve().parent.parent


def get_airports_staging_path() -> Path:
    return get_project_root() / "data" / "staging" / "dim_airport.parquet"


def get_weather_staging_path() -> Path:
    return get_project_root() / "data" / "staging" / "dim_weather.parquet"


def get_flights_enriched_weather_path() -> Path:
    return get_project_root() / "data" / "staging" / "flights_enriched_weather.parquet"


def truncate_dwh_tables(engine: Engine) -> None:
    """
    Leert die DWH-Tabellen, damit der Load idempotent ist.
    Achtung: TRUNCATE CASCADE löscht abhängige Daten mit.
    """

    sql = """
    TRUNCATE TABLE fact_flights RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dim_weather RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dim_airline RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dim_airport RESTART IDENTITY CASCADE;
    TRUNCATE TABLE dim_date RESTART IDENTITY CASCADE;
    """

    with engine.begin() as conn:
        conn.exec_driver_sql(sql)

    print("DWH-Tabellen geleert (TRUNCATE).")


# ---------- dim_date ----------


def create_dim_date_from_flights(engine: Engine, flights_df: pd.DataFrame) -> None:
    """
    Erzeugt dim_date auf Basis der minimalen / maximalen FlightDate aus den Flights.
    date_id = YYYYMMDD als int.
    """

    if "FlightDate" not in flights_df.columns:
        raise KeyError("Spalte 'FlightDate' fehlt in Flights-Stagingdaten.")

    flights_df = flights_df.copy()
    flights_df["FlightDate"] = pd.to_datetime(flights_df["FlightDate"], errors="coerce")
    flights_df = flights_df.dropna(subset=["FlightDate"])

    min_date = flights_df["FlightDate"].dt.date.min()
    max_date = flights_df["FlightDate"].dt.date.max()

    if pd.isna(min_date) or pd.isna(max_date):
        raise ValueError("Konnte min/max FlightDate nicht bestimmen.")

    print(f"Erzeuge dim_date von {min_date} bis {max_date}...")

    date_range = pd.date_range(start=min_date, end=max_date, freq="D")
    dim_date_df = pd.DataFrame({"full_date": date_range})
    dim_date_df["date_id"] = dim_date_df["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date_df["year"] = dim_date_df["full_date"].dt.year.astype(int)
    dim_date_df["month"] = dim_date_df["full_date"].dt.month.astype(int)
    dim_date_df["day"] = dim_date_df["full_date"].dt.day.astype(int)
    dim_date_df["day_of_week"] = dim_date_df["full_date"].dt.dayofweek + 1
    dim_date_df["day_name"] = dim_date_df["full_date"].dt.day_name().str[:3]
    dim_date_df["week_of_year"] = (
        dim_date_df["full_date"].dt.isocalendar().week.astype(int)
    )
    dim_date_df["is_weekend"] = dim_date_df["day_of_week"].isin([6, 7])

    dim_date_df = dim_date_df[
        [
            "date_id",
            "full_date",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "week_of_year",
            "is_weekend",
        ]
    ]

    dim_date_df.to_sql("dim_date", engine, if_exists="append", index=False)
    print(f"dim_date geladen: {len(dim_date_df)} Zeilen.")


# ---------- dim_airport ----------


def load_dim_airport(engine: Engine, airports_path: Path) -> None:
    if not airports_path.exists():
        raise FileNotFoundError(
            f"Staging-Datei für Airports nicht gefunden: {airports_path}"
        )

    df = pd.read_parquet(airports_path)
    print(f"Lade dim_airport ({len(df)} Zeilen)...")

    df["airport_id"] = df["airport_id"].astype(str).str.strip().str.upper()

    df.to_sql("dim_airport", engine, if_exists="append", index=False)
    print("dim_airport geladen.")


# ---------- dim_weather ----------


def load_dim_weather(engine: Engine, weather_path: Path) -> None:
    if not weather_path.exists():
        raise FileNotFoundError(
            f"Staging-Datei für Wetter nicht gefunden: {weather_path}"
        )

    df = pd.read_parquet(weather_path)
    print(f"Lade dim_weather (Input-Zeilen: {len(df)})...")

    df["airport_id"] = df["airport_id"].astype(str).str.strip().str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["airport_id", "date"])

    df["date_id"] = df["date"].dt.strftime("%Y%m%d").astype(int)

    cols = ["airport_id", "date_id"]
    for c in ["tavg", "prcp", "wspd"]:
        if c in df.columns:
            cols.append(c)

    dim_weather_df = df[cols].copy()

    dim_weather_df.to_sql("dim_weather", engine, if_exists="append", index=False)
    print(f"dim_weather geladen: {len(dim_weather_df)} Zeilen.")


# ---------- dim_airline ----------


def load_dim_airline(engine: Engine, flights_df: pd.DataFrame) -> None:
    if "Airline" not in flights_df.columns:
        raise KeyError("Spalte 'Airline' fehlt in Flights-Stagingdaten.")

    airlines = (
        flights_df["Airline"]
        .astype(str)
        .str.strip()
        .dropna()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    dim_airline_df = pd.DataFrame(
        {
            "airline_id": airlines,
            "airline_name": airlines,
        }
    )

    print(f"Lade dim_airline ({len(dim_airline_df)} Zeilen)...")
    dim_airline_df.to_sql("dim_airline", engine, if_exists="append", index=False)
    print("dim_airline geladen.")


# ---------- fact_flights ----------


def load_fact_flights(engine: Engine, flights_df: pd.DataFrame) -> None:
    """
    Mappt das angereicherte Flights-Staging auf das fact_flights-Schema
    und lädt es in die DB.
    """

    df = flights_df.copy()

    required = ["date_id", "FlightDate", "Dep_Airport", "Arr_Airport", "Airline"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(
            f"Fehlende Spalten in Flights-Stagingdaten für fact_flights: {missing}"
        )

    df["Dep_Airport"] = df["Dep_Airport"].astype(str).str.strip().str.upper()
    df["Arr_Airport"] = df["Arr_Airport"].astype(str).str.strip().str.upper()
    df["Airline"] = df["Airline"].astype(str).str.strip()

    for col in [
        "Dep_Delay",
        "Arr_Delay",
        "Delay_Carrier",
        "Delay_Weather",
        "Delay_NAS",
        "Delay_Security",
        "Delay_LastAircraft",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["is_cancelled", "is_diverted", "is_delayed_15"]:
        if col in df.columns:
            df[col] = df[col].astype(bool)
        else:
            df[col] = False

    fact_df = pd.DataFrame(
        {
            "flight_date_id": df["date_id"].astype(int),
            "dep_airport_id": df["Dep_Airport"],
            "arr_airport_id": df["Arr_Airport"],
            "airline_id": df["Airline"],
            "weather_id": None,
            "flight_number": None,
            "tail_number": df.get("Tail_Number"),
            "sched_dep_time": None,
            "sched_arr_time": None,
            "dep_time_label": df.get("DepTime_label"),
            "dep_delay_min": df.get("Dep_Delay"),
            "arr_delay_min": df.get("Arr_Delay"),
            "distance": None,
            "cancelled": df["is_cancelled"],
            "diverted": df["is_diverted"],
            "cancellation_code": None,
            "is_delayed_15": df["is_delayed_15"],
            "carrier_delay_min": df.get("Delay_Carrier"),
            "weather_delay_min": df.get("Delay_Weather"),
            "nas_delay_min": df.get("Delay_NAS"),
            "security_delay_min": df.get("Delay_Security"),
            "late_aircraft_delay_min": df.get("Delay_LastAircraft"),
        }
    )

    print(f"Lade fact_flights ({len(fact_df)} Zeilen)...")
    fact_df.to_sql("fact_flights", engine, if_exists="append", index=False)
    print("fact_flights geladen.")

    print("Aktualisiere fact_flights.weather_id via Join auf dim_weather...")

    update_sql = """
    UPDATE fact_flights ff
    SET weather_id = dw.weather_id
    FROM dim_weather dw
    WHERE dw.airport_id = ff.dep_airport_id
      AND dw.date_id = ff.flight_date_id;
    """

    with engine.begin() as conn:
        conn.exec_driver_sql(update_sql)

    print("weather_id in fact_flights aktualisiert.")


def load_dwh() -> None:
    """
    Gesamter Load-Prozess:

    - Flights-Staging laden
    - DWH-Tabellen truncaten
    - dim_date, dim_airport, dim_weather, dim_airline laden
    - fact_flights laden + weather_id-Update
    """

    engine = get_engine()

    flights_path = get_flights_enriched_weather_path()
    airports_path = get_airports_staging_path()
    weather_path = get_weather_staging_path()

    if not flights_path.exists():
        raise FileNotFoundError(
            f"Staging-Datei flights_enriched_weather.parquet nicht gefunden: {flights_path}\n"
            f"Bitte zuerst ETL-Schritte bis enrich_transform ausführen."
        )

    print(f"Lese Flights-Stagingdaten aus: {flights_path}")
    flights_df = pd.read_parquet(flights_path)
    print(f"Flights-Zeilen (enriched+weather): {len(flights_df)}")

    try:
        truncate_dwh_tables(engine)

        create_dim_date_from_flights(engine, flights_df)
        load_dim_airport(engine, airports_path)
        load_dim_weather(engine, weather_path)
        load_dim_airline(engine, flights_df)

        load_fact_flights(engine, flights_df)

        print("✅ Load ins DWH abgeschlossen.")

    except SQLAlchemyError as e:
        print("❌ Fehler beim Load ins DWH:")
        print(e)
        raise


if __name__ == "__main__":
    load_dwh()
