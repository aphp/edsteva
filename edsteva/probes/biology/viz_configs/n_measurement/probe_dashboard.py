from .defaults import (
    chart_style,
    get_horizontal_bar_charts,
    main_chart,
    time_line,
    vertical_bar_charts,
)


def get_probe_dashboard_config(self):
    horizontal_bar_charts = get_horizontal_bar_charts(
        standard_terminologies=self._standard_terminologies.copy()
    )
<<<<<<< HEAD
    probe_dashboard_config = dict(
=======
    return dict(
>>>>>>> main
        chart_style=chart_style,
        main_chart=main_chart,
        time_line=time_line,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )
<<<<<<< HEAD
    return probe_dashboard_config
=======
>>>>>>> main
