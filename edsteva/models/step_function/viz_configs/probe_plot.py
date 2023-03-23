from .defaults import model_line, probe_line


def get_probe_plot_config(self):
    probe_plot_config = dict(
        probe_line=probe_line,
        model_line=model_line,
    )
    return probe_plot_config
