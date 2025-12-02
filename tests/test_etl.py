from pathlib import Path

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STAGING_DIR = PROJECT_ROOT / "data" / "staging"


def _load_parquet(filename: str) -> pd.DataFrame:
    path = STAGING_DIR / filename
    if not path.exists():
        pytest.skip(f"Staging-Datei {filename} nicht gefunden unter {path}")
    return pd.read_parquet(path)


def test_flights_staging_has_expected_columns():
    """Prüft, ob flights.parquet die wichtigsten Spalten enthält."""
    df = _load_parquet("flights.parquet")

    required_cols = [
        "FlightDate",
        "Dep_Airport",
        "Arr_Airport",
        "Airline",
        "Dep_Delay",
        "Arr_Delay",
        "DepTime_label",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    assert not missing, f"Fehlende Spalten in flights.parquet: {missing}"


def test_flights_enriched_has_expected_columns():
    """Prüft, ob flights_enriched.parquet die Cxl/Div-Flags enthält."""
    df = _load_parquet("flights_enriched.parquet")

    required_cols = [
        "FlightDate",
        "Dep_Airport",
        "Arr_Airport",
        "Airline",
        "Dep_Delay",
        "Arr_Delay",
        "is_cancelled",
        "is_diverted",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    assert not missing, f"Fehlende Spalten in flights_enriched.parquet: {missing}"


def test_dim_airport_has_expected_columns():
    """Prüft, ob dim_airport-Staging die wichtigsten Spalten enthält."""
    df = _load_parquet("dim_airport.parquet")

    required_cols = [
        "airport_id",
        "name",
        "city",
        "state",
        "country",
        "latitude",
        "longitude",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    assert not missing, f"Fehlende Spalten in dim_airport.parquet: {missing}"


def test_dim_weather_has_expected_columns():
    """Prüft, ob dim_weather-Staging die Key- und Kernmetriken enthält."""
    df = _load_parquet("dim_weather.parquet")

    required_cols = [
        "airport_id",
        "date",
        "tavg",
        "prcp",
        "wspd",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    assert not missing, f"Fehlende Spalten in dim_weather.parquet: {missing}"


@pytest.mark.parametrize("filename", ["flights.parquet", "flights_enriched.parquet"])
def test_flights_key_columns_not_null(filename: str):
    """
    Sicherstellen, dass FlightDate, Dep_Airport, Airline nicht null sind
    (zentrale Business Keys).
    """
    df = _load_parquet(filename)

    for col in ["FlightDate", "Dep_Airport", "Airline"]:
        assert col in df.columns, f"Spalte {col} fehlt in {filename}"
        null_count = df[col].isna().sum()
        assert (
            null_count == 0
        ), f"In {filename} enthält Spalte {col} {null_count} Null-Werte."


@pytest.mark.parametrize("filename", ["flights.parquet", "flights_enriched.parquet"])
def test_airport_codes_are_three_uppercase(filename: str):
    """
    Airport-Codes sollen immer 3 Zeichen und nur Großbuchstaben haben.
    (Annahme: IATA-Codes im Datensatz)
    """
    df = _load_parquet(filename)

    for col in ["Dep_Airport", "Arr_Airport"]:
        if col not in df.columns:
            continue

        series = df[col].astype(str)

        assert not series.isna().any(), f"{filename}: {col} enthält Null-Werte."

        invalid_len = series[series.str.len() != 3]
        assert (
            invalid_len.empty
        ), f"{filename}: {col} enthält Codes mit != 3 Zeichen, Beispiele: {invalid_len.head().tolist()}"

        non_upper = series[~series.str.match(r"^[A-Z]{3}$")]
        assert (
            non_upper.empty
        ), f"{filename}: {col} enthält Nicht-Großbuchstaben, Beispiele: {non_upper.head().tolist()}"


@pytest.mark.parametrize("filename", ["flights.parquet", "flights_enriched.parquet"])
def test_delay_values_non_negative(filename: str):
    """
    Prüft, dass Delay-Spalten keine negativen Werte enthalten.
    Wenn du negative Delays als 'früh' beibehalten willst, musst du
    entweder den ETL anpassen oder diesen Test lockern.
    """
    df = _load_parquet(filename)

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
        if col not in df.columns:
            continue

        s = pd.to_numeric(df[col], errors="coerce")
        negatives = s[s < 0].dropna()

        assert (
            negatives.empty
        ), f"{filename}: Spalte {col} enthält negative Delay-Werte, Beispiele: {negatives.head().tolist()}"


@pytest.mark.parametrize("filename", ["flights_enriched_weather.parquet"])
def test_staging_has_no_all_null_columns(filename: str):
    """
    Stellt sicher, dass im Staging-Dataset (z. B. flights_enriched_weather.parquet)
    keine Spalten existieren, die zu 100 % NULL / NaN sind.

    Idee:
      - Wir laden das Parquet über _load_parquet.
      - Für jede Spalte prüfen wir: gibt es mindestens einen non-null Wert?
      - Falls nicht, schlägt der Test fehl und listet die betroffenen Spalten.
    """
    df = _load_parquet(filename)

    if df.empty:
        pytest.skip(f"{filename} ist leer – Test wird übersprungen.")
        return

    protected_cols = {
        "FlightDate",
        "date_id",
        "Dep_Airport",
        "Arr_Airport",
        "Airline",
        "Dep_Delay",
        "Arr_Delay",
        "is_cancelled",
        "is_diverted",
        "is_delayed_15",
    }

    all_null_cols = []
    for col in df.columns:
        if col in protected_cols:
            continue

        has_non_null = df[col].notna().any()
        if not has_non_null:
            all_null_cols.append(col)

    assert not all_null_cols, (
        f"In {filename} gibt es Spalten, die vollständig NULL sind. "
        "Diese solltest du aus Schema/load_fact_flights entfernen oder bewusst behandeln.\n"
        f"Betroffene Spalten: {all_null_cols}"
    )
