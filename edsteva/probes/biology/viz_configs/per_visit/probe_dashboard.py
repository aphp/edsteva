from edsteva.probes.biology.viz_configs import probe_dashboard

from .defaults import (
    chart_style,
    get_horizontal_bar_charts,
    main_chart,
    time_line,
    vertical_bar_charts,
)


@probe_dashboard.register("per_measurement_default")
def get_probe_dashboard_config(self):
    horizontal_bar_charts = get_horizontal_bar_charts(
        standard_terminologies=self._standard_terminologies.copy()
    )
    probe_dashboard_config = dict(
        chart_style=chart_style,
        main_chart=main_chart,
        time_line=time_line,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )
    return probe_dashboard_config
