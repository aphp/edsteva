from .defaults import horizontal_min_c0, model_line, probe_line


def get_probe_dashboard_config(self):
    return dict(
        probe_line=probe_line,
        model_line=model_line,
        extra_horizontal_bar_charts=[horizontal_min_c0],
        extra_vertical_bar_charts=[],
    )
