from .defaults import chart_style, main_chart


def get_probe_plot_config(self):
    return dict(
        chart_style=chart_style,
        main_chart=main_chart,
    )
