from dash import dcc, html
import dash_bootstrap_components as dbc


def get_delay_overview_layout():
    """Layout für Tab 'Delay Overview'."""
    return dbc.Container(
        [
            html.H3("Delay Overview"),
            html.P(
                "Overview for the average Delay sorted by Date, Airport and Airline."
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Airport"),
                            dcc.Dropdown(
                                id="delay-overview-airport-dropdown",
                                options=[],
                                placeholder="Chose an Airport...",
                            ),
                        ],
                        md=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Airline"),
                            dcc.Dropdown(
                                id="delay-overview-airline-dropdown",
                                options=[],
                                placeholder="Chose an Airline...",
                            ),
                        ],
                        md=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Period"),
                            dcc.DatePickerRange(
                                id="delay-overview-date-range",
                                display_format="YYYY-MM-DD",
                            ),
                        ],
                        md=4,
                    ),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(id="delay-overview-timeseries"),
                        md=8,
                    ),
                    dbc.Col(
                        dcc.Graph(id="delay-overview-distribution"),
                        md=4,
                    ),
                ]
            ),
        ],
        fluid=True,
    )


def get_weather_impact_layout():
    """Layout für Tab 'Weather Impact'."""
    return dbc.Container(
        [
            html.H3("Weather Impact on Delays"),
            html.P(
                "Analysis of Influences (temperature, precipitation, wind) "
                "on the Delay Probability."
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Airport"),
                            dcc.Dropdown(
                                id="weather-impact-airport-dropdown",
                                options=[],
                                placeholder="Chose an Airport...",
                            ),
                        ],
                        md=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Period"),
                            dcc.DatePickerRange(
                                id="weather-impact-date-range",
                                display_format="YYYY-MM-DD",
                            ),
                        ],
                        md=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Delay-Threshold (min)"),
                            dcc.Input(
                                id="weather-impact-delay-threshold",
                                type="number",
                                value=15,
                                min=0,
                                step=5,
                            ),
                        ],
                        md=4,
                    ),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(id="weather-impact-scatter"),
                        md=6,
                    ),
                    dbc.Col(
                        dcc.Graph(id="weather-impact-correlation"),
                        md=6,
                    ),
                ]
            ),
        ],
        fluid=True,
    )


def get_cancellations_diversions_layout():
    """Layout für Tab 'Cancellations & Diversions'."""
    return dbc.Container(
        [
            html.H3("Cancellations & Diversions"),
            html.P(
                "Overview of cancelled and diverted Flights via Airport, Airline and Period."
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Airport"),
                            dcc.Dropdown(
                                id="cxl-div-airport-dropdown",
                                options=[],
                                placeholder="Chose an Airport...",
                            ),
                        ],
                        md=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Airline"),
                            dcc.Dropdown(
                                id="cxl-div-airline-dropdown",
                                options=[],
                                placeholder="Chose an Airline...",
                            ),
                        ],
                        md=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Period"),
                            dcc.DatePickerRange(
                                id="cxl-div-date-range",
                                display_format="YYYY-MM-DD",
                            ),
                        ],
                        md=4,
                    ),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(id="cxl-div-rate-by-airport"),
                        md=6,
                    ),
                    dbc.Col(
                        dcc.Graph(id="cxl-div-rate-by-airline"),
                        md=6,
                    ),
                ]
            ),
        ],
        fluid=True,
    )


def get_airline_performance_layout():
    """Layout für Tab 'Airline Performance'."""
    return dbc.Container(
        [
            html.H3("Airline Performance"),
            html.P(
                "Comparison of Airlines on Punctuality, Cancellations and Diversions."
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Period"),
                            dcc.DatePickerRange(
                                id="airline-perf-date-range",
                                display_format="YYYY-MM-DD",
                            ),
                        ],
                        md=6,
                    ),
                    dbc.Col(
                        [
                            html.Label("Sort by"),
                            dcc.Dropdown(
                                id="airline-perf-sort-by",
                                options=[
                                    {
                                        "label": "Average Departure Delay",
                                        "value": "dep_delay",
                                    },
                                    {
                                        "label": "Delay Rate (>= 15 min)",
                                        "value": "delay_rate",
                                    },
                                    {"label": "Cancellation Rate", "value": "cxl_rate"},
                                ],
                                value="dep_delay",
                            ),
                        ],
                        md=6,
                    ),
                ],
                className="mb-4",
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(id="airline-perf-bar"),
                        md=12,
                    )
                ]
            ),
        ],
        fluid=True,
    )


def get_predictive_delay_layout():
    """Layout für Tab 'Predictive Delay' (LogReg-Modell)."""
    return dbc.Container(
        [
            html.H3("Predictive Delay (Logistic Regression)"),
            html.P(
                "Estimate the Probability of a Delay (>= 15 min) "
                "on certain Inputs(Airport, Airline, Time and Weather)."
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Airport"),
                            dcc.Dropdown(
                                id="pred-delay-airport-dropdown",
                                options=[],
                                placeholder="Chose an Airport...",
                            ),
                            html.Br(),
                            html.Label("Airline"),
                            dcc.Dropdown(
                                id="pred-delay-airline-dropdown",
                                options=[],
                                placeholder="Chose an Airline...",
                            ),
                            html.Br(),
                            html.Label("Dep Time Label"),
                            dcc.Dropdown(
                                id="pred-delay-deptime-dropdown",
                                options=[],
                                placeholder="Chose a Period...",
                            ),
                        ],
                        md=4,
                    ),
                    dbc.Col(
                        [
                            html.Label("Average temperature (tavg)"),
                            dcc.Input(
                                id="pred-delay-tavg",
                                type="number",
                                step=0.5,
                            ),
                            html.Br(),
                            html.Br(),
                            html.Label("Precipitation (prcp)"),
                            dcc.Input(
                                id="pred-delay-prcp",
                                type="number",
                                step=0.1,
                            ),
                            html.Br(),
                            html.Br(),
                            html.Label("wind speed (wspd)"),
                            dcc.Input(
                                id="pred-delay-wspd",
                                type="number",
                                step=0.5,
                            ),
                            html.Br(),
                            html.Br(),
                            html.Label("Departures in the same period (congestion)"),
                            dcc.Input(
                                id="pred-delay-num-deps",
                                type="number",
                                min=1,
                                step=1,
                                value=10,
                            ),
                        ],
                        md=4,
                    ),
                    dbc.Col(
                        [
                            html.Div(
                                [
                                    html.Button(
                                        "Prediction",
                                        id="pred-delay-submit",
                                        n_clicks=0,
                                        className="btn btn-primary",
                                    ),
                                    html.Br(),
                                    html.Br(),
                                    html.H4("Result"),
                                    html.Div(
                                        id="pred-delay-result",
                                        children="Please chose parameters and click 'Prediction'.",
                                    ),
                                ]
                            )
                        ],
                        md=4,
                    ),
                ],
                className="mb-4",
            ),
        ],
        fluid=True,
    )
