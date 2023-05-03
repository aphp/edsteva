from typing import Dict, List

import altair as alt
import pandas as pd

from edsteva.viz.utils import create_groupby_selection, generate_main_chart


def probe_line(
    predictor: pd.DataFrame,
    indexes: List[Dict[str, str]],
    x_axis_title: str,
    y_axis_title: str,
    main_chart_config: Dict[str, float],
):
    """Script to be used by [``plot_probe()``][edsteva.viz.plots.plot_probe.wrapper]

    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe
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
    """
    base = alt.Chart(predictor)

    index_selection, index_fields = create_groupby_selection(
        indexes=indexes,
    )
    main_chart = generate_main_chart(
        base=base,
        main_chart_config=main_chart_config,
        index_selection=index_selection,
        index_fields=index_fields,
        x_axis_title=x_axis_title,
        y_axis_title=y_axis_title,
    )
    main_chart = main_chart.mark_line()

    if index_selection:
        main_chart = main_chart.add_selection(index_selection)

    return main_chart
