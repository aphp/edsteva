from .defaults import chart_style, error_line, normalized_main_chart


def get_normalized_probe_plot_config(self):
    return dict(
        chart_style=chart_style,
        main_chart=normalized_main_chart,
        error_line=error_line,
    )
