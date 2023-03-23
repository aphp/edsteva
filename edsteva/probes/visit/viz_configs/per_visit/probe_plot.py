from .defaults import chart_style, main_chart


def get_probe_plot_config(self):
    probe_plot_config = dict(
        chart_style=chart_style,
        main_chart=main_chart,
    )
    return probe_plot_config
