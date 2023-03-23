from edsteva.probes.biology.viz_configs import normalized_probe_dashboard

from .defaults import (
    chart_style,
    error_line,
    get_horizontal_bar_charts,
    normalized_main_chart,
    normalized_time_line,
    vertical_bar_charts,
)


@normalized_probe_dashboard.register("per_measurement_default")
def get_normalized_probe_dashboard_config(self):
    horizontal_bar_charts = get_horizontal_bar_charts(
        standard_terminologies=self._standard_terminologies.copy()
    )
    normalized_probe_dashboard_config = dict(
        chart_style=chart_style,
        main_chart=normalized_main_chart,
        time_line=normalized_time_line,
        error_line=error_line,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )

    return normalized_probe_dashboard_config
