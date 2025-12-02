from dash import Input, Output
import pandas as pd
import plotly.express as px

from etl.config import get_engine


def _load_airports_and_daterange():
    """Lädt Airports und globalen Datumsbereich aus dem DWH."""
    engine = get_engine()

    airports = pd.read_sql(
        "SELECT airport_id, name FROM dim_airport ORDER BY airport_id",
        con=engine,
    )
    airport_options = [
        {"label": f"{row.airport_id} – {row.name}", "value": row.airport_id}
        for _, row in airports.iterrows()
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

    return airport_options, start_date, end_date


def register_callbacks(app):
    @app.callback(
        Output("weather-impact-airport-dropdown", "options"),
        Output("weather-impact-date-range", "start_date"),
        Output("weather-impact-date-range", "end_date"),
        Input("main-tabs", "value"),
    )
    def init_weather_impact_filters(active_tab):
        try:
            airport_options, start_date, end_date = _load_airports_and_daterange()
        except Exception as e:
            print("Fehler beim Laden der Weather-Impact-Filter:", e)
            empty = []
            return empty, None, None

        return airport_options, start_date, end_date

    @app.callback(
        Output("weather-impact-scatter", "figure"),
        Output("weather-impact-correlation", "figure"),
        Input("weather-impact-airport-dropdown", "value"),
        Input("weather-impact-date-range", "start_date"),
        Input("weather-impact-date-range", "end_date"),
        Input("weather-impact-delay-threshold", "value"),
    )
    def update_weather_impact_graphs(airport_id, start_date, end_date, delay_threshold):
        engine = get_engine()

        def _empty_figures(msg_suffix="(no data)"):
            empty_df = pd.DataFrame({"tavg": [], "avg_dep_delay": []})
            fig_scatter = px.scatter(
                empty_df,
                x="tavg",
                y="avg_dep_delay",
                title=f"Weather vs Delay {msg_suffix}",
            )
            fig_corr = px.imshow(
                [[0]],
                labels=dict(x="Feature", y="Feature", color="Correlation"),
                x=["dummy"],
                y=["dummy"],
                title=f"Correlation Matrix {msg_suffix}",
            )
            return fig_scatter, fig_corr

        if not start_date or not end_date:
            return _empty_figures("(no date range)")

        if delay_threshold is None:
            delay_threshold = 15

        params = {
            "start_date": start_date,
            "end_date": end_date,
            "airport": airport_id,
            "delay_thr": float(delay_threshold),
        }

        sql = """
        SELECT
            dd.full_date::date AS date,
            AVG(ff.dep_delay_min) AS avg_dep_delay,
            COUNT(*) AS num_flights,
            SUM(CASE WHEN ff.dep_delay_min >= %(delay_thr)s THEN 1 ELSE 0 END) AS delayed_thr,
            AVG(dw.tavg) AS tavg,
            AVG(dw.prcp) AS prcp,
            AVG(dw.wspd) AS wspd
        FROM fact_flights ff
        JOIN dim_date dd
          ON dd.date_id = ff.flight_date_id
        LEFT JOIN dim_weather dw
          ON dw.weather_id = ff.weather_id
        WHERE dd.full_date BETWEEN %(start_date)s AND %(end_date)s
          AND (%(airport)s IS NULL OR ff.dep_airport_id = %(airport)s)
          AND ff.dep_delay_min IS NOT NULL
        GROUP BY dd.full_date::date
        HAVING COUNT(*) >= 10  -- min. Anzahl Flüge pro Tag
        ORDER BY date
        """

        df = pd.read_sql(sql, con=engine, params=params)

        if df.empty:
            return _empty_figures("(no data)")

        df["date"] = pd.to_datetime(df["date"])
        df["delay_rate"] = df["delayed_thr"] / df["num_flights"]

        df_plot = df.dropna(
            subset=["tavg", "prcp", "wspd", "delay_rate", "avg_dep_delay"]
        )

        if df_plot.empty:
            return _empty_figures("(no weather data)")

        fig_scatter = px.scatter(
            df_plot,
            x="tavg",
            y="delay_rate",
            color="prcp",
            size="wspd",
            hover_data={
                "date": True,
                "avg_dep_delay": True,
                "num_flights": True,
                "tavg": True,
                "prcp": True,
                "wspd": True,
            },
            labels={
                "tavg": "Avg Temp (°C)",
                "delay_rate": f"Delay Rate (≥ {delay_threshold} min)",
                "prcp": "Precipitation",
                "wspd": "Wind Speed",
            },
            title=f"Weather vs Delay Rate (Threshold ≥ {delay_threshold} min)",
        )

        corr_cols = ["avg_dep_delay", "delay_rate", "tavg", "prcp", "wspd"]
        corr_df = df_plot[corr_cols].corr()

        fig_corr = px.imshow(
            corr_df,
            text_auto=True,
            aspect="auto",
            labels=dict(color="Correlation"),
            x=corr_df.columns,
            y=corr_df.columns,
            title="Correlation Matrix – Delay & Weather Features",
        )

        return fig_scatter, fig_corr
