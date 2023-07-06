from .defaults import (
    chart_style,
    error_line,
    horizontal_bar_charts,
    normalized_main_chart,
    normalized_time_line,
    vertical_bar_charts,
)


def get_normalized_probe_dashboard_config(self):
<<<<<<< HEAD
    normalized_probe_dashboard_config = dict(
=======
    return dict(
>>>>>>> main
        chart_style=chart_style,
        main_chart=normalized_main_chart,
        time_line=normalized_time_line,
        error_line=error_line,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )
<<<<<<< HEAD

    return normalized_probe_dashboard_config
=======
>>>>>>> main
