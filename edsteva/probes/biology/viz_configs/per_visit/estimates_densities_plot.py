from .defaults import chart_style, get_horizontal_bar_charts, vertical_bar_charts


def get_estimates_densities_plot_config(self):
    horizontal_bar_charts = get_horizontal_bar_charts(
        standard_terminologies=self._standard_terminologies.copy()
    )
<<<<<<< HEAD
    estimates_densities_plot_config = dict(
=======
    return dict(
>>>>>>> main
        chart_style=chart_style,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )
<<<<<<< HEAD

    return estimates_densities_plot_config
=======
>>>>>>> main
