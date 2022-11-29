from typing import List

import altair as alt
import pandas as pd


def fitted_probe_line(
    predictor: pd.DataFrame,
    index: List[str],
    x_axis_title: str = "Time (Month Year)",
    y_axis_title: str = "Completeness predictor c(t)",
    x_grid: bool = True,
    y_grid: bool = True,
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
    """
    predictor["legend_predictor"] = "Predictor c(t)"
    predictor["legend_model"] = "Model f(t)"

    base_chart = (
        alt.Chart(predictor).encode(
            x=alt.X(
                f'yearmonth({"date"}):T',
                title=x_axis_title,
                axis=alt.Axis(tickCount="month", labelAngle=-90, grid=x_grid),
            ),
        )
    ).properties(width=800, height=300)

    probe_line = base_chart.mark_line().encode(
        y=alt.Y(
            "mean(c):Q",
            title=y_axis_title,
            axis=alt.Axis(grid=y_grid),
        ),
        strokeDash=alt.StrokeDash(
            "legend_predictor",
            title="",
            legend=alt.Legend(
                symbolType="stroke",
                symbolStrokeColor="steelblue",
                labelFontSize=11,
                labelFontStyle="bold",
                orient="left",
            ),
        ),
    )
    model_line = base_chart.mark_line(
        interpolate="step-after", strokeDash=[5, 5]
    ).encode(
        y=alt.Y(
            "mean(c_hat):Q",
        ),
        strokeWidth=alt.StrokeWidth(
            "legend_model",
            title="",
            legend=alt.Legend(
                symbolType="stroke",
                symbolStrokeColor="steelblue",
                labelFontSize=11,
                labelFontStyle="bold",
                symbolDash=[2, 2],
                orient="left",
            ),
        ),
    )

    chart = probe_line + model_line

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
                    title=None,
                ),
            )
            .transform_filter(index_selection)
            .add_selection(index_selection)
        )
    elif len(index) == 1:
        chart = chart.encode(
            color=alt.Color(
                "{}:N".format(index[0]),
                sort={"field": "c", "op": "mean", "order": "descending"},
            ),
        )

    return chart
