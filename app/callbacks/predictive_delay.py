from dash import Input, Output, State
import pandas as pd

from etl.config import get_engine
from models.delay_logreg import predict_delay_proba


def _load_pred_filters():
    """Airports, Airlines & DepTime-Labels für den Predictive-Tab laden."""
    engine = get_engine()

    airports = pd.read_sql(
        "SELECT airport_id, name FROM dim_airport ORDER BY airport_id",
        con=engine,
    )
    airport_options = [
        {"label": f"{row.airport_id} – {row.name}", "value": row.airport_id}
        for _, row in airports.iterrows()
    ]

    airlines = pd.read_sql(
        "SELECT airline_id, airline_name FROM dim_airline ORDER BY airline_id",
        con=engine,
    )
    airline_options = [
        {"label": row.airline_name, "value": row.airline_id}
        for _, row in airlines.iterrows()
    ]

    dep_labels = pd.read_sql(
        """
        SELECT DISTINCT dep_time_label
        FROM fact_flights
        WHERE dep_time_label IS NOT NULL
        ORDER BY dep_time_label
        """,
        con=engine,
    )
    deptime_options = [
        {"label": lbl, "value": lbl} for lbl in dep_labels["dep_time_label"].tolist()
    ]

    return airport_options, airline_options, deptime_options


def register_callbacks(app):
    @app.callback(
        Output("pred-delay-airport-dropdown", "options"),
        Output("pred-delay-airline-dropdown", "options"),
        Output("pred-delay-deptime-dropdown", "options"),
        Input("main-tabs", "value"),
    )
    def init_predictive_filters(active_tab):
        try:
            airport_options, airline_options, deptime_options = _load_pred_filters()
        except Exception as e:
            print("Fehler beim Laden der Predictive-Delay-Filter:", e)
            empty = []
            return empty, empty, empty

        return airport_options, airline_options, deptime_options

    @app.callback(
        Output("pred-delay-result", "children"),
        Input("pred-delay-submit", "n_clicks"),
        State("pred-delay-airport-dropdown", "value"),
        State("pred-delay-airline-dropdown", "value"),
        State("pred-delay-deptime-dropdown", "value"),
        State("pred-delay-tavg", "value"),
        State("pred-delay-prcp", "value"),
        State("pred-delay-wspd", "value"),
        State("pred-delay-num-deps", "value"),
    )
    def run_predictive_delay(
        n_clicks,
        airport_id,
        airline_id,
        dep_time_label,
        tavg,
        prcp,
        wspd,
        num_deps,
    ):
        if not n_clicks:
            return "Bitte Parameter wählen und 'Vorhersage berechnen' klicken."

        missing = []
        if not airport_id:
            missing.append("Airport")
        if not airline_id:
            missing.append("Airline")
        if not dep_time_label:
            missing.append("Dep Time Label")
        if tavg is None:
            missing.append("tavg")
        if prcp is None:
            missing.append("prcp")
        if wspd is None:
            missing.append("wspd")
        if num_deps is None:
            missing.append("Anzahl Abflüge im Slot")

        if missing:
            return f"Fehlende Eingaben: {', '.join(missing)}"

        try:
            proba = predict_delay_proba(
                airport_id=airport_id,
                airline_id=airline_id,
                dep_time_label=dep_time_label,
                tavg=float(tavg),
                prcp=float(prcp),
                wspd=float(wspd),
                num_departures_same_slot_airport=int(num_deps),
            )
        except FileNotFoundError:
            return (
                "Kein trainiertes Modell gefunden. Bitte zuerst das Modell trainieren "
                "(models/delay_logreg.py ausführen)."
            )
        except Exception as e:
            return f"Fehler bei der Vorhersage: {e}"

        proba_pct = proba * 100.0
        return (
            f"Geschätzte Wahrscheinlichkeit für Delay ≥ 15 Minuten: {proba_pct:.1f} %"
        )
