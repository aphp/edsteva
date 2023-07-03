from .defaults import (
    chart_style,
    horizontal_bar_charts,
    main_chart,
    time_line,
    vertical_bar_charts,
)


def get_probe_dashboard_config(self):
    return dict(
        chart_style=chart_style,
        main_chart=main_chart,
        time_line=time_line,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )
