from edsteva.probes.biology.viz_configs import probe_plot

from .defaults import chart_style, main_chart


@probe_plot.register("per_measurement_default")
def get_probe_plot_config(self):
    probe_plot_config = dict(
        chart_style=chart_style,
        main_chart=main_chart,
    )
    return probe_plot_config
