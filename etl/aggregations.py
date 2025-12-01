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


def get_flights_enriched_weather_path() -> Path:
    return get_project_root() / "data" / "staging" / "flights_enriched_weather.parquet"


def get_delays_agg_path() -> Path:
    staging_dir = get_project_root() / "data" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / "fact_delays_daily.parquet"


def get_cancellations_agg_path() -> Path:
    staging_dir = get_project_root() / "data" / "staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir / "fact_cancellations_daily.parquet"


def _prepare_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stellt sicher, dass wichtige Spalten den richtigen Typ haben.
    """

    if "Dep_Delay" in df.columns:
        df["Dep_Delay"] = pd.to_numeric(df["Dep_Delay"], errors="coerce")

    for col in ["is_delayed_15", "is_cancelled", "is_diverted"]:
        if col in df.columns:
            df[col] = df[col].astype(bool)
        else:
            df[col] = False

    return df


def aggregate_delays_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Durchschnittliche Verspätung pro Tag / Airport / Airline.

    group by: date_id, FlightDate, Dep_Airport, Airline

    Metriken:
      - total_flights
      - avg_dep_delay_min
      - delayed_15_count
      - delayed_15_rate
    """

    required = [
        "date_id",
        "FlightDate",
        "Dep_Airport",
        "Airline",
        "Dep_Delay",
        "is_delayed_15",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Fehlende Spalten für Delay-Aggregation: {missing}")

    group_cols = ["date_id", "FlightDate", "Dep_Airport", "Airline"]

    grouped = df.groupby(group_cols, as_index=False).agg(
        total_flights=("Dep_Delay", "size"),
        avg_dep_delay_min=("Dep_Delay", "mean"),
        delayed_15_count=("is_delayed_15", "sum"),
    )

    grouped["delayed_15_rate"] = grouped["delayed_15_count"] / grouped["total_flights"]

    return grouped


def aggregate_cancellations_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Total Cancellations & Diversions pro Tag / Airport.

    group by: date_id, FlightDate, Dep_Airport

    Metriken:
      - total_flights
      - cancelled_count, cancelled_rate
      - diverted_count, diverted_rate
    """

    required = ["date_id", "FlightDate", "Dep_Airport", "is_cancelled", "is_diverted"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Fehlende Spalten für Cancellation-Aggregation: {missing}")

    group_cols = ["date_id", "FlightDate", "Dep_Airport"]

    grouped = df.groupby(group_cols, as_index=False).agg(
        total_flights=("is_cancelled", "size"),
        cancelled_count=("is_cancelled", "sum"),
        diverted_count=("is_diverted", "sum"),
    )

    grouped["cancelled_rate"] = grouped["cancelled_count"] / grouped["total_flights"]
    grouped["diverted_rate"] = grouped["diverted_count"] / grouped["total_flights"]

    return grouped


def run_aggregations() -> None:
    """
    Liest flights_enriched_weather.parquet ein und erzeugt:

      - fact_delays_daily.parquet
      - fact_cancellations_daily.parquet
    """

    flights_path = get_flights_enriched_weather_path()
    delays_path = get_delays_agg_path()
    canc_path = get_cancellations_agg_path()

    if not flights_path.exists():
        raise FileNotFoundError(
            f"Staging-Datei flights_enriched_weather.parquet nicht gefunden: {flights_path}\n"
            f"Bitte zuerst etl.enrich_transform ausführen."
        )

    print(f"Lese angereicherte Flights-Daten aus: {flights_path}")
    df = pd.read_parquet(flights_path)
    print(f"Flights-Zeilen (für Aggregation): {len(df)}")

    df = _prepare_types(df)

    print("Berechne Delay-Aggregation (pro Tag / Airport / Airline)...")
    delays_daily = aggregate_delays_daily(df)
    print(f"  Aggregierte Zeilen (Delays): {len(delays_daily)}")

    print(f"Schreibe Delay-Aggregation nach: {delays_path}")
    delays_daily.to_parquet(delays_path, index=False)

    print("Berechne Cancellation-/Diversion-Aggregation (pro Tag / Airport)...")
    canc_daily = aggregate_cancellations_daily(df)
    print(f"  Aggregierte Zeilen (Cancellations): {len(canc_daily)}")

    print(f"Schreibe Cancellation-Aggregation nach: {canc_path}")
    canc_daily.to_parquet(canc_path, index=False)

    print("✅ Aggregationen abgeschlossen.")


if __name__ == "__main__":
    run_aggregations()
