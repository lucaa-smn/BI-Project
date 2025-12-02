from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc

from app.layouts import (
    get_delay_overview_layout,
    get_weather_impact_layout,
    get_cancellations_diversions_layout,
    get_airline_performance_layout,
    get_predictive_delay_layout,
)

from app.callbacks import register_all_callbacks


app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)
app.title = "US Flight Delays 2023"
server = app.server


app.layout = dbc.Container(
    [
        dbc.Row(
            dbc.Col(
                html.H2("US Flight Delays – BI Dashboard", className="mt-3 mb-4"),
                width=12,
            )
        ),
        dbc.Row(
            dbc.Col(
                dcc.Tabs(
                    id="main-tabs",
                    value="tab-delay-overview",
                    children=[
                        dcc.Tab(
                            label="Delay Overview",
                            value="tab-delay-overview",
                        ),
                        dcc.Tab(
                            label="Weather Impact",
                            value="tab-weather-impact",
                        ),
                        dcc.Tab(
                            label="Cancellations & Diversions",
                            value="tab-cancellations-diversions",
                        ),
                        dcc.Tab(
                            label="Airline Performance",
                            value="tab-airline-performance",
                        ),
                        dcc.Tab(
                            label="Predictive Delay",
                            value="tab-predictive-delay",
                        ),
                    ],
                ),
                width=12,
            )
        ),
        html.Hr(),
        dbc.Row(
            dbc.Col(
                html.Div(id="tab-content", className="mt-3"),
                width=12,
            )
        ),
    ],
    fluid=True,
)


@app.callback(
    Output("tab-content", "children"),
    Input("main-tabs", "value"),
)
def render_tab_content(active_tab: str):
    if active_tab == "tab-delay-overview":
        return get_delay_overview_layout()
    elif active_tab == "tab-weather-impact":
        return get_weather_impact_layout()
    elif active_tab == "tab-cancellations-diversions":
        return get_cancellations_diversions_layout()
    elif active_tab == "tab-airline-performance":
        return get_airline_performance_layout()
    elif active_tab == "tab-predictive-delay":
        return get_predictive_delay_layout()
    return html.Div(
        [
            html.H4("Unknown Tab"),
            html.P(f"Aktiver Tab-Wert: {active_tab}"),
        ]
    )


register_all_callbacks(app)


if __name__ == "__main__":
    app.run(debug=True)
