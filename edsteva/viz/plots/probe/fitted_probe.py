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
    indexes: List[Dict[str, str]],
    legend_predictor: str,
    legend_model: str,
    x_axis_title: str,
    y_axis_title: str,
    main_chart_config: Dict[str, float],
    model_line_config: Dict[str, str],
    probe_line_config: Dict[str, str],
):
    r"""Script to be used by [``plot_probe()``][edsteva.viz.plots.probe.wrapper]

    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe with its prediction $\hat{c}(t)$
    indexes : List[str]
        Variable from which data is grouped
    legend_predictor: str, optional,
        Label name for the predictor legend.
    legend_model: str, optional,
        Label name for the model legend.
    x_axis_title: str, optional,
        Label name for the x axis.
    y_axis_title: str, optional,
        Label name for the y axis.
    main_chart_config: Dict[str, str], optional
        If not None, configuration used to construct the top main chart.
    model_line_config: Dict[str, str], optional
        If not None, configuration used to construct the model line.
    probe_line_config: Dict[str, str], optional
        If not None, configuration used to construct the probe line.
    """
    predictor["legend_predictor"] = legend_predictor
    predictor["legend_model"] = legend_model

    base = alt.Chart(predictor)

    index_selection, index_fields = create_groupby_selection(
        indexes=indexes,
        predictor=predictor,
    )
    main_chart = generate_main_chart(
        base=base,
        main_chart_config=main_chart_config,
        index_selection=index_selection,
        index_fields=index_fields,
        x_axis_title=x_axis_title,
        y_axis_title=y_axis_title,
    )

    probe_line = generate_probe_line(
        main_chart=main_chart, probe_line_config=probe_line_config
    )

    model_line = generate_model_line(
        main_chart=main_chart, model_line_config=model_line_config
    )

    main_chart = probe_line + model_line
    if index_selection:
        main_chart = main_chart.add_params(index_selection)

    return main_chart
