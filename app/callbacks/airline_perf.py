from dash import Input, Output
import pandas as pd
import plotly.express as px

from etl.config import get_engine


def _load_date_range():
    """Lädt globalen Datumsbereich aus dim_date."""
    engine = get_engine()
    dates = pd.read_sql(
        "SELECT MIN(full_date) AS min_date, MAX(full_date) AS max_date FROM dim_date",
        con=engine,
    )
    min_date = dates["min_date"].iloc[0]
    max_date = dates["max_date"].iloc[0]
    if pd.isna(min_date) or pd.isna(max_date):
        return None, None
    start_date = pd.to_datetime(min_date).date().isoformat()
    end_date = pd.to_datetime(max_date).date().isoformat()
    return start_date, end_date


def register_callbacks(app):
    @app.callback(
        Output("airline-perf-date-range", "start_date"),
        Output("airline-perf-date-range", "end_date"),
        Input("main-tabs", "value"),
    )
    def init_airline_perf_daterange(active_tab):
        try:
            start_date, end_date = _load_date_range()
        except Exception as e:
            print("Fehler beim Laden des Datumsbereichs für Airline Performance:", e)
            return None, None
        return start_date, end_date

    @app.callback(
        Output("airline-perf-bar", "figure"),
        Input("airline-perf-date-range", "start_date"),
        Input("airline-perf-date-range", "end_date"),
        Input("airline-perf-sort-by", "value"),
    )
    def update_airline_perf_bar(start_date, end_date, sort_by):
        engine = get_engine()

        def _empty_fig(msg="(no data)"):
            df_empty = pd.DataFrame({"airline_name": [], "value": []})
            fig = px.bar(
                df_empty,
                x="airline_name",
                y="value",
                title=f"Airline Performance {msg}",
            )
            return fig

        if not start_date or not end_date:
            return _empty_fig("(no date range)")

        params = {
            "start_date": start_date,
            "end_date": end_date,
        }

        sql = """
        SELECT
            ff.airline_id,
            da.airline_name,
            COUNT(*) AS num_flights,
            AVG(ff.dep_delay_min) AS avg_dep_delay,
            SUM(CASE WHEN ff.is_delayed_15 THEN 1 ELSE 0 END) AS delayed_15,
            SUM(CASE WHEN ff.cancelled THEN 1 ELSE 0 END) AS num_cancelled
        FROM fact_flights ff
        JOIN dim_date dd
          ON dd.date_id = ff.flight_date_id
        LEFT JOIN dim_airline da
          ON da.airline_id = ff.airline_id
        WHERE dd.full_date BETWEEN %(start_date)s AND %(end_date)s
          AND ff.airline_id IS NOT NULL
        GROUP BY ff.airline_id, da.airline_name
        HAVING COUNT(*) >= 200  -- Mindestanzahl Flüge pro Airline
        ORDER BY ff.airline_id
        """

        df = pd.read_sql(sql, con=engine, params=params)

        if df.empty:
            return _empty_fig("(no airline data)")

        df["delay_rate"] = df["delayed_15"] / df["num_flights"]
        df["cxl_rate"] = df["num_cancelled"] / df["num_flights"]

        metric_map = {
            "dep_delay": ("avg_dep_delay", "Average Departure Delay (min)"),
            "delay_rate": ("delay_rate", "Delay Rate (≥ 15 min)"),
            "cxl_rate": ("cxl_rate", "Cancellation Rate"),
        }
        metric_col, metric_label = metric_map.get(
            sort_by, ("avg_dep_delay", "Average Departure Delay (min)")
        )

        df_sorted = df.sort_values(metric_col, ascending=False).copy()

        df_sorted["airline_display"] = df_sorted["airline_name"].fillna(
            df_sorted["airline_id"]
        )

        fig = px.bar(
            df_sorted,
            x="airline_display",
            y=metric_col,
            title=f"Airline Performance – sortiert nach {metric_label}",
            labels={
                "airline_display": "Airline",
                metric_col: metric_label,
            },
        )

        fig.update_layout(
            xaxis_tickangle=-45,
            margin=dict(b=120),
        )

        return fig
