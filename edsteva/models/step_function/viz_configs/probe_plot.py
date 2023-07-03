from .defaults import model_line, probe_line


def get_probe_plot_config(self):
    return dict(
        probe_line=probe_line,
        model_line=model_line,
    )
