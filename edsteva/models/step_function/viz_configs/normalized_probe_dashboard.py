from edsteva.utils.typing import DataFrame

from .defaults import (
    error_line,
    get_c_0_min_selection,
    get_error_max_selection,
    get_t_0_selection,
    horizontal_max_error,
    horizontal_min_c0,
    normalized_model_line,
    normalized_probe_line,
)


def get_normalized_probe_dashboard_config(self, predictor: DataFrame):
    c_0_min_selection, c_0_min_filter = get_c_0_min_selection(predictor=predictor)
    t_0_selection, t_0_min_filter = get_t_0_selection(predictor=predictor)
    error_max_selection, error_max_filter = get_error_max_selection(predictor=predictor)
    return dict(
        estimates_selections=[c_0_min_selection, t_0_selection, error_max_selection],
        estimates_filters=[c_0_min_filter, t_0_min_filter, error_max_filter],
        error_line=error_line,
        probe_line=normalized_probe_line,
        model_line=normalized_model_line,
        extra_horizontal_bar_charts=[horizontal_min_c0, horizontal_max_error],
        extra_vertical_bar_charts=[],
    )
