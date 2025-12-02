from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from etl.config import get_engine


@dataclass
class AnomalyResult:
    date: pd.Timestamp
    avg_dep_delay: float
    num_flights: int
    z_score: float


def _load_daily_delays_for_airport(
    airport_id: str,
    start_date: str,
    end_date: str,
    extra_history_days: int = 30,
) -> pd.DataFrame:
    """
    Lädt Durchschnittsverspätung pro Tag für einen Airport aus fact_flights.

    Wir holen zusätzlich 'extra_history_days' vor start_date,
    damit das Rolling-Window am Anfang nicht komplett blind ist.
    """

    engine = get_engine()

    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()
    hist_start = start - timedelta(days=extra_history_days)

    sql = """
    SELECT
        dd.full_date::date AS date,
        ff.dep_airport_id,
        AVG(ff.dep_delay_min) AS avg_dep_delay,
        COUNT(*) AS num_flights
    FROM fact_flights ff
    JOIN dim_date dd
      ON dd.date_id = ff.flight_date_id
    WHERE ff.dep_airport_id = %(airport)s
      AND dd.full_date BETWEEN %(hist_start)s AND %(end)s
      AND ff.dep_delay_min IS NOT NULL
    GROUP BY dd.full_date::date, ff.dep_airport_id
    ORDER BY date
    """

    params = {
        "airport": airport_id.strip().upper(),
        "hist_start": hist_start,
        "end": end,
    }

    df = pd.read_sql(sql, con=engine, params=params)

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    return df


def _compute_z_scores(
    df: pd.DataFrame,
    window: int = 30,
    min_periods: int = 10,
) -> pd.DataFrame:
    """
    Fügt Rolling-Mean/-Std und Z-Score pro Tag hinzu.

    Annahme: df ist bereits nach Datum sortiert und enthält:
      - date
      - avg_dep_delay
    """

    if df.empty:
        return df

    df = df.copy().sort_values("date")

    rolling_mean = (
        df["avg_dep_delay"]
        .rolling(
            window=window,
            min_periods=min_periods,
        )
        .mean()
    )

    rolling_std = (
        df["avg_dep_delay"]
        .rolling(
            window=window,
            min_periods=min_periods,
        )
        .std(ddof=0)
    )

    df["rolling_mean"] = rolling_mean
    df["rolling_std"] = rolling_std

    z = (df["avg_dep_delay"] - df["rolling_mean"]) / df["rolling_std"]
    z = z.mask(df["rolling_std"].isna() | (df["rolling_std"] == 0), other=0.0)

    df["z_score"] = z

    return df


def detect_anomalies(
    airport_id: str,
    start_date: str,
    end_date: str,
    threshold: float = 3.0,
    window: int = 30,
    min_periods: int = 10,
    min_flights_per_day: int = 20,
) -> pd.DataFrame:
    """
    Detektiert "Delay-Spikes" für einen Airport im gegebenen Zeitraum.

    Parameter:
        airport_id: IATA-Code des Airports (z. B. "JFK")
        start_date: Startdatum als "YYYY-MM-DD"
        end_date:   Enddatum als "YYYY-MM-DD"
        threshold:  |Z-Score| > threshold => Anomalie (default: 3.0)
        window:     Rolling-Window in Tagen (default: 30)
        min_periods: minimale Anzahl Tage, bevor Rolling-Stats berechnet werden
        min_flights_per_day: minimale Anzahl Flüge, damit ein Tag betrachtet wird

    Rückgabe:
        DataFrame mit Spalten:
          - date
          - avg_dep_delay
          - num_flights
          - z_score
          - is_anomaly (bool)
        (nur Tage im [start_date, end_date]-Bereich)
    """

    airport_id = airport_id.strip().upper()

    df_daily = _load_daily_delays_for_airport(
        airport_id=airport_id,
        start_date=start_date,
        end_date=end_date,
        extra_history_days=window,
    )

    if df_daily.empty:
        return pd.DataFrame(
            columns=["date", "avg_dep_delay", "num_flights", "z_score", "is_anomaly"]
        )

    df_daily = _compute_z_scores(df_daily, window=window, min_periods=min_periods)

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    df_period = df_daily[(df_daily["date"] >= start) & (df_daily["date"] <= end)].copy()

    df_period = df_period[df_period["num_flights"] >= min_flights_per_day]

    df_period["is_anomaly"] = df_period["z_score"].abs() > threshold

    df_period = df_period.sort_values("date").reset_index(drop=True)

    return df_period[["date", "avg_dep_delay", "num_flights", "z_score", "is_anomaly"]]


def detect_anomalies_list(
    airport_id: str,
    start_date: str,
    end_date: str,
    threshold: float = 3.0,
    window: int = 30,
    min_periods: int = 10,
    min_flights_per_day: int = 20,
) -> list[AnomalyResult]:
    """
    Convenience-Funktion:
    gibt nur die Anomalie-Tage als Liste von AnomalyResult zurück.
    """

    df = detect_anomalies(
        airport_id=airport_id,
        start_date=start_date,
        end_date=end_date,
        threshold=threshold,
        window=window,
        min_periods=min_periods,
        min_flights_per_day=min_flights_per_day,
    )

    if df.empty:
        return []

    df_anom = df[df["is_anomaly"]].copy()

    results: list[AnomalyResult] = []
    for _, row in df_anom.iterrows():
        results.append(
            AnomalyResult(
                date=row["date"],
                avg_dep_delay=float(row["avg_dep_delay"]),
                num_flights=int(row["num_flights"]),
                z_score=float(row["z_score"]),
            )
        )

    return results


# ---------------------------------------------------------------------------
# CLI / manuel Testing
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    airport = "JFK"
    start = "2023-01-01"
    end = "2023-12-31"

    print(f"Suche Anomalien für Airport {airport} von {start} bis {end}...")

    df_res = detect_anomalies(airport, start, end, threshold=3.0, window=30)

    print(df_res[df_res["is_anomaly"]].head(20))
    print(f"\nAnzahl Anomalie-Tage: {df_res['is_anomaly'].sum()}")
