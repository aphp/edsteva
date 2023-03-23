from typing import Dict, List

import altair as alt
import pandas as pd

from edsteva.viz.utils import create_groupby_selection, generate_main_chart


def probe_line(
    predictor: pd.DataFrame,
    probe_config: Dict[str, str],
    indexes: List[Dict[str, str]],
):
    """Script to be used by [``plot_probe()``][edsteva.viz.plots.plot_probe.wrapper]

    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe
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
    show_n_events: bool, optional
        show the number of visit instead of the completeness predictor $c(t)$
    labelAngle: float, optional
        The rotation angle of the label on the x_axis.
    """
    main_chart_config = probe_config["main_chart"]

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
    main_chart = main_chart.mark_line()

    if index_selection:
        main_chart = main_chart.add_selection(index_selection)

    return main_chart
