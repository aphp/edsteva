from .defaults import horizontal_min_c0, model_line, probe_line


def get_probe_dashboard_config(self):
<<<<<<< HEAD
    probe_dashboard_config = dict(
=======
    return dict(
>>>>>>> main
        probe_line=probe_line,
        model_line=model_line,
        extra_horizontal_bar_charts=[horizontal_min_c0],
        extra_vertical_bar_charts=[],
    )
<<<<<<< HEAD
    return probe_dashboard_config
=======
>>>>>>> main
