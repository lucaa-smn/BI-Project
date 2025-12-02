from .delay_overview import register_callbacks as register_delay_overview_callbacks
from .predictive_delay import register_callbacks as register_predictive_delay_callbacks
from .weather_impact import register_callbacks as register_weather_impact_callbacks
from .cancellations import register_callbacks as register_cancellations_callbacks
from .airline_perf import register_callbacks as register_airline_perf_callbacks


def register_all_callbacks(app):
    register_delay_overview_callbacks(app)
    register_predictive_delay_callbacks(app)
    register_weather_impact_callbacks(app)
    register_cancellations_callbacks(app)
    register_airline_perf_callbacks(app)
