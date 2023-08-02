from .defaults import (
    chart_style,
    horizontal_bar_charts,
    normalized_main_chart,
    normalized_time_line,
    vertical_bar_charts,
)


def get_normalized_probe_dashboard_config(self):
    return dict(
        chart_style=chart_style,
        main_chart=normalized_main_chart,
        time_line=normalized_time_line,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )
