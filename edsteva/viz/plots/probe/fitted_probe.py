from typing import Dict, List

import altair as alt
import pandas as pd

from edsteva.viz.utils import (
    create_groupby_selection,
    generate_main_chart,
    generate_model_line,
    generate_probe_line,
)


def fitted_probe_line(
    predictor: pd.DataFrame,
    probe_config: Dict[str, str],
    model_config: Dict[str, str],
    indexes: List[Dict[str, str]],
    legend_predictor: str = "Predictor c(t)",
    legend_model: str = "Model f(t)",
):
    r"""Script to be used by [``plot_probe()``][edsteva.viz.plots.plot_probe.wrapper]

    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe with its prediction $\hat{c}(t)$
    index : List[str]
        Variable from which data is grouped
    x_axis_title: str, optional,
        Label name for the x axis.
    x_grid: bool, optional,
        If False, remove the grid for the x axis.
    y_axis_title: str, optional,
        Label name for the y axis.
    y_grid: bool, optional,
        If False, remove the grid for the y axis.
    labelAngle: float, optional
        The rotation angle of the label on the x_axis.
    labelFontSize: float, optional
        The font size of the labels (axis and legend).
    """
    predictor["legend_predictor"] = legend_predictor
    predictor["legend_model"] = legend_model
    main_chart_config = probe_config["main_chart"]
    model_line_config = model_config["model_line"]
    probe_line_config = model_config["probe_line"]

    base = alt.Chart(predictor)

    index_selection, index_fields = create_groupby_selection(
        indexes=indexes,
    )
    main_chart = generate_main_chart(
        base=base,
        main_chart_config=main_chart_config,
        index_selection=index_selection,
        index_fields=index_fields,
    )

    probe_line = generate_probe_line(
        main_chart=main_chart, probe_line_config=probe_line_config
    )

    model_line = generate_model_line(
        main_chart=main_chart, model_line_config=model_line_config
    )

    main_chart = probe_line + model_line
    if index_selection:
        main_chart = main_chart.add_selection(index_selection)

    return main_chart
