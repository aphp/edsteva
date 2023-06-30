from .defaults import chart_style, error_line, normalized_main_chart


def get_normalized_probe_plot_config(self):
    normalized_probe_plot_config = dict(
        chart_style=chart_style,
        main_chart=normalized_main_chart,
        error_line=error_line,
    )
    return normalized_probe_plot_config
