from .defaults import chart_style, horizontal_bar_charts, vertical_bar_charts


def get_estimates_densities_plot_config(self):
    return dict(
        chart_style=chart_style,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )
