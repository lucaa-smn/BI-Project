from dash import Input, Output
import pandas as pd
import plotly.express as px

from etl.config import get_engine


def _load_airport_airline_and_daterange():
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
        Output("delay-overview-airport-dropdown", "options"),
        Output("delay-overview-airline-dropdown", "options"),
        Output("delay-overview-date-range", "start_date"),
        Output("delay-overview-date-range", "end_date"),
        Input("main-tabs", "value"),
    )
    def init_delay_overview_filters(active_tab):
        try:
            airport_options, airline_options, start_date, end_date = (
                _load_airport_airline_and_daterange()
            )
        except Exception as e:
            print("Fehler beim Laden der Delay-Overview-Filter:", e)
            empty = []
            return empty, empty, None, None

        return airport_options, airline_options, start_date, end_date

    @app.callback(
        Output("delay-overview-timeseries", "figure"),
        Output("delay-overview-distribution", "figure"),
        Input("delay-overview-airport-dropdown", "value"),
        Input("delay-overview-airline-dropdown", "value"),
        Input("delay-overview-date-range", "start_date"),
        Input("delay-overview-date-range", "end_date"),
    )
    def update_delay_overview_graphs(airport_id, airline_id, start_date, end_date):
        engine = get_engine()

        if not start_date or not end_date:
            empty_df = pd.DataFrame({"date": [], "avg_dep_delay": []})
            fig_ts = px.line(
                empty_df,
                x="date",
                y="avg_dep_delay",
                title="Average Departure Delay over Time",
            )
            fig_dist = px.histogram(
                empty_df,
                x="avg_dep_delay",
                title="Delay Distribution (Daily Averages)",
            )
            return fig_ts, fig_dist

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "airport": airport_id,
            "airline": airline_id,
        }

        sql = """
        SELECT
            dd.full_date::date AS date,
            AVG(ff.dep_delay_min) AS avg_dep_delay,
            COUNT(*) AS num_flights,
            SUM(CASE WHEN ff.is_delayed_15 THEN 1 ELSE 0 END) AS delayed_15
        FROM fact_flights ff
        JOIN dim_date dd
          ON dd.date_id = ff.flight_date_id
        WHERE dd.full_date BETWEEN %(start_date)s AND %(end_date)s
          AND (%(airport)s IS NULL OR ff.dep_airport_id = %(airport)s)
          AND (%(airline)s IS NULL OR ff.airline_id = %(airline)s)
        GROUP BY dd.full_date::date
        ORDER BY date
        """

        df = pd.read_sql(sql, con=engine, params=params)

        if df.empty:
            empty_df = pd.DataFrame({"date": [], "avg_dep_delay": []})
            fig_ts = px.line(
                empty_df,
                x="date",
                y="avg_dep_delay",
                title="Average Departure Delay over Time (no data)",
            )
            fig_dist = px.histogram(
                empty_df,
                x="avg_dep_delay",
                title="Delay Distribution (no data)",
            )
            return fig_ts, fig_dist

        df["date"] = pd.to_datetime(df["date"])
        df["delay_rate"] = df["delayed_15"] / df["num_flights"]

        fig_ts = px.line(
            df,
            x="date",
            y="avg_dep_delay",
            title="Average Departure Delay over Time",
            labels={"avg_dep_delay": "Avg Dep Delay (min)", "date": "Date"},
        )
        fig_ts.update_traces(mode="lines+markers")
        fig_ts.update_layout(hovermode="x unified")

        fig_dist = px.histogram(
            df,
            x="avg_dep_delay",
            nbins=40,
            title="Distribution of Daily Average Departure Delays",
            labels={"avg_dep_delay": "Avg Dep Delay (min)"},
        )

        return fig_ts, fig_dist
