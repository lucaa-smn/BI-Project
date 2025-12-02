from dash import Input, Output
import pandas as pd
import plotly.express as px

from etl.config import get_engine


def _load_airports_airlines_and_daterange():
    """Lädt Airports, Airlines und globalen Datumsbereich aus dem DWH."""
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

    dates = pd.read_sql(
        "SELECT MIN(full_date) AS min_date, MAX(full_date) AS max_date FROM dim_date",
        con=engine,
    )
    min_date = dates["min_date"].iloc[0]
    max_date = dates["max_date"].iloc[0]
    if pd.isna(min_date) or pd.isna(max_date):
        start_date = end_date = None
    else:
        start_date = pd.to_datetime(min_date).date().isoformat()
        end_date = pd.to_datetime(max_date).date().isoformat()

    return airport_options, airline_options, start_date, end_date


def register_callbacks(app):
    @app.callback(
        Output("cxl-div-airport-dropdown", "options"),
        Output("cxl-div-airline-dropdown", "options"),
        Output("cxl-div-date-range", "start_date"),
        Output("cxl-div-date-range", "end_date"),
        Input("main-tabs", "value"),
    )
    def init_cxl_div_filters(active_tab):
        try:
            airport_options, airline_options, start_date, end_date = (
                _load_airports_airlines_and_daterange()
            )
        except Exception as e:
            print("Fehler beim Laden der Cxl/Div-Filter:", e)
            empty = []
            return empty, empty, None, None

        return airport_options, airline_options, start_date, end_date

    @app.callback(
        Output("cxl-div-rate-by-airport", "figure"),
        Output("cxl-div-rate-by-airline", "figure"),
        Input("cxl-div-airport-dropdown", "value"),
        Input("cxl-div-airline-dropdown", "value"),
        Input("cxl-div-date-range", "start_date"),
        Input("cxl-div-date-range", "end_date"),
    )
    def update_cxl_div_graphs(airport_id, airline_id, start_date, end_date):
        engine = get_engine()

        def _empty_figures(msg_suffix="(no data)"):
            df_empty = pd.DataFrame({"category": [], "rate": [], "metric": []})
            fig_airport = px.bar(
                df_empty,
                x="category",
                y="rate",
                color="metric",
                title=f"Cxl/Div Rate by Airport {msg_suffix}",
            )
            fig_airline = px.bar(
                df_empty,
                x="category",
                y="rate",
                color="metric",
                title=f"Cxl/Div Rate by Airline {msg_suffix}",
            )
            return fig_airport, fig_airline

        if not start_date or not end_date:
            return _empty_figures("(no date range)")

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "airport": airport_id,
            "airline": airline_id,
        }

        sql_airport = """
        SELECT
            ff.dep_airport_id AS category,
            COUNT(*) AS num_flights,
            SUM(CASE WHEN ff.cancelled THEN 1 ELSE 0 END) AS num_cancelled,
            SUM(CASE WHEN ff.diverted THEN 1 ELSE 0 END) AS num_diverted
        FROM fact_flights ff
        JOIN dim_date dd
          ON dd.date_id = ff.flight_date_id
        WHERE dd.full_date BETWEEN %(start_date)s AND %(end_date)s
          AND (%(airline)s IS NULL OR ff.airline_id = %(airline)s)
          AND ff.dep_airport_id IS NOT NULL
        GROUP BY ff.dep_airport_id
        HAVING COUNT(*) >= 50 -- Mindestanzahl Flüge für sinnvolle Rate
        ORDER BY ff.dep_airport_id
        """

        df_airport = pd.read_sql(sql_airport, con=engine, params=params)

        if not df_airport.empty:
            df_airport["cxl_rate"] = (
                df_airport["num_cancelled"] / df_airport["num_flights"]
            )
            df_airport["div_rate"] = (
                df_airport["num_diverted"] / df_airport["num_flights"]
            )

            df_airport_melt = df_airport.melt(
                id_vars="category",
                value_vars=["cxl_rate", "div_rate"],
                var_name="metric",
                value_name="rate",
            )
            metric_labels = {
                "cxl_rate": "Cancellation Rate",
                "div_rate": "Diversion Rate",
            }
            df_airport_melt["metric"] = df_airport_melt["metric"].map(metric_labels)

            fig_airport = px.bar(
                df_airport_melt,
                x="category",
                y="rate",
                color="metric",
                barmode="group",
                title="Cancellation & Diversion Rate by Airport",
                labels={
                    "category": "Departure Airport",
                    "rate": "Rate",
                    "metric": "Metric",
                },
            )
        else:
            fig_airport, _ = _empty_figures("(no airport data)")

        sql_airline = """
        SELECT
            ff.airline_id AS category,
            COUNT(*) AS num_flights,
            SUM(CASE WHEN ff.cancelled THEN 1 ELSE 0 END) AS num_cancelled,
            SUM(CASE WHEN ff.diverted THEN 1 ELSE 0 END) AS num_diverted
        FROM fact_flights ff
        JOIN dim_date dd
          ON dd.date_id = ff.flight_date_id
        WHERE dd.full_date BETWEEN %(start_date)s AND %(end_date)s
          AND (%(airport)s IS NULL OR ff.dep_airport_id = %(airport)s)
          AND ff.airline_id IS NOT NULL
        GROUP BY ff.airline_id
        HAVING COUNT(*) >= 50
        ORDER BY ff.airline_id
        """

        df_airline = pd.read_sql(sql_airline, con=engine, params=params)

        if not df_airline.empty:
            df_airline["cxl_rate"] = (
                df_airline["num_cancelled"] / df_airline["num_flights"]
            )
            df_airline["div_rate"] = (
                df_airline["num_diverted"] / df_airline["num_flights"]
            )

            df_airline_melt = df_airline.melt(
                id_vars="category",
                value_vars=["cxl_rate", "div_rate"],
                var_name="metric",
                value_name="rate",
            )
            metric_labels = {
                "cxl_rate": "Cancellation Rate",
                "div_rate": "Diversion Rate",
            }
            df_airline_melt["metric"] = df_airline_melt["metric"].map(metric_labels)

            fig_airline = px.bar(
                df_airline_melt,
                x="category",
                y="rate",
                color="metric",
                barmode="group",
                title="Cancellation & Diversion Rate by Airline",
                labels={
                    "category": "Airline",
                    "rate": "Rate",
                    "metric": "Metric",
                },
            )
        else:
            _, fig_airline = _empty_figures("(no airline data)")

        return fig_airport, fig_airline
