from typing import List

import altair as alt
import pandas as pd


def probe_line(
    predictor: pd.DataFrame,
    index: List[str],
    x_axis_title: str = "Time (Month Year)",
    y_axis_title: str = "Completeness predictor c(t)",
    x_grid: bool = True,
    y_grid: bool = True,
    show_n_visit: bool = False,
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
    show_n_visit: bool, optional
        show the number of visit instead of the completeness predictor $c(t)$
    """

    chart = (
        alt.Chart(predictor)
        .mark_line()
        .encode(
            x=alt.X(
                f'yearmonth({"date"}):T',
                title=x_axis_title,
                axis=alt.Axis(tickCount="month", labelAngle=-90, grid=x_grid),
            ),
            y=alt.Y(
                "sum(n_visit):Q" if show_n_visit else "mean(c):Q",
                title=y_axis_title,
                axis=alt.Axis(grid=y_grid),
            ),
        )
    ).properties(width=800, height=300)

    if len(index) >= 2:
        index_selection = alt.selection_single(
            fields=["index"],
            bind=alt.binding_radio(
                name="Plot average completeness per: ", options=index
            ),
            init={"index": index[0]},
        )
        chart = (
            chart.transform_fold(index, as_=["index", "value"])
            .encode(
                color=alt.Color(
                    "value:N",
                    sort={"field": "c", "op": "mean", "order": "descending"},
                ),
            )
            .add_selection(index_selection)
            .transform_filter(index_selection)
        ).properties(width=800, height=300)
    elif len(index) == 1:
        chart = (
            chart.encode(
                color=alt.Color(
                    "{}:N".format(index[0]),
                    sort={"field": "c", "op": "mean", "order": "descending"},
                ),
            )
        ).properties(width=800, height=300)

    return chart
