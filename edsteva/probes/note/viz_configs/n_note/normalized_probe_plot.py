from .defaults import chart_style, normalized_main_chart


def get_normalized_probe_plot_config(self):
    return dict(
        chart_style=chart_style,
        main_chart=normalized_main_chart,
    )
