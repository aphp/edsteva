from edsteva.probes.biology.viz_configs import estimates_densities_plot

from .defaults import chart_style, get_horizontal_bar_charts, vertical_bar_charts


@estimates_densities_plot.register("per_visit_default")
def get_estimates_densities_plot_config(self):
    horizontal_bar_charts = get_horizontal_bar_charts(
        standard_terminologies=self._standard_terminologies.copy()
    )
    estimates_densities_plot_config = dict(
        chart_style=chart_style,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )

    return estimates_densities_plot_config
