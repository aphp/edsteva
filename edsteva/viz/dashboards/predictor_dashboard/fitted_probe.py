from functools import reduce
from typing import List

import altair as alt
import pandas as pd


def fitted_probe_dashboard(
    predictor: pd.DataFrame,
    index: List[str],
    x_axis_title: str = "Time (Month Year)",
    y_axis_title: str = "Completeness predictor c(t)",
    x_grid: bool = True,
    y_grid: bool = True,
):
    r"""Script to be used by [``predictor_dashboard()``][edsteva.viz.dashboards.predictor_dashboard.wrapper]

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
    alt.data_transformers.disable_max_rows()

    predictor["legend_predictor"] = "Predictor c(t)"
    predictor["legend_model"] = "Model f(t)"

    time_selection = alt.selection_interval(encodings=["x"])
    time_line = (
        alt.Chart(predictor)
        .mark_line()
        .encode(
            x=alt.X(
                f'yearmonth({"date"}):T',
                title=x_axis_title,
                axis=alt.Axis(tickCount="month", labelAngle=-90),
            ),
            y=alt.Y(
                "mean(c):Q",
                title="c(t)",
            ),
        )
        .add_selection(time_selection)
    ).properties(width=900, height=50)

    care_site_selection = alt.selection_multi(fields=["care_site_short_name"])
    care_site_color = alt.condition(
        care_site_selection,
        alt.Color(
            "care_site_short_name:N",
            legend=None,
            sort={"field": "c", "op": "mean", "order": "descending"},
        ),
        alt.value("lightgray"),
    )
    predictor_hist = (
        alt.Chart(predictor)
        .mark_bar()
        .encode(
            y=alt.Y(
                "care_site_short_name:N",
                title="Care site short name",
                sort="-x",
            ),
            color=care_site_color,
        )
        .transform_filter(time_selection)
        .add_selection(care_site_selection)
    ).properties(width=300)

    care_site_hist = predictor_hist.encode(
        x=alt.X(
            "mean(c):Q",
            title="c",
        ),
        tooltip=alt.Tooltip("mean(c):Q", format=".2"),
    ).properties(title="Average completeness per care site")

    index_variables_hists = []
    index_variables_selections = []
    for index_variable in index:
        index_variable_selection = alt.selection_multi(fields=[index_variable])
        index_variables_selections.append(index_variable_selection)

        index_variable_color = alt.condition(
            index_variable_selection,
            alt.Color(
                "{}:N".format(index_variable),
                legend=None,
                sort={"field": "c", "op": "mean", "order": "descending"},
            ),
            alt.value("lightgray"),
        )
        index_variable_hist = (
            alt.Chart(predictor)
            .mark_bar()
            .encode(
                y=alt.Y(
                    "mean(c):Q",
                    title="c",
                ),
                x=alt.X(
                    "{}:N".format(index_variable),
                    title=index_variable,
                    sort="-y",
                ),
                tooltip=alt.Tooltip("mean(c):Q", format=".2"),
                color=index_variable_color,
            )
            .add_selection(index_variable_selection)
            .transform_filter(time_selection)
            .transform_filter(care_site_selection)
        ).properties(title="Average completeness per {}".format(index_variable))
        index_variables_hists.append(index_variable_hist)

    extra_predictors = predictor.columns.difference(
        index
        + [
            "date",
            "care_site_short_name",
            "care_site_id",
            "c",
            "c_hat",
            "legend_predictor",
            "legend_model",
        ]
    )
    extra_predictors_hists = []

    for extra_predictor in extra_predictors:

        extra_predictor_hist = predictor_hist.encode(
            x=alt.X(
                "sum({}):Q".format(extra_predictor),
                title="{}".format(extra_predictor),
                axis=alt.Axis(format="s"),
            ),
            tooltip=alt.Tooltip("sum({}):Q".format(extra_predictor), format=","),
        ).properties(title="Total {} per care site".format(extra_predictor))

        extra_predictors_hists.append(extra_predictor_hist)

    index.append("care_site_short_name")
    index_selection = alt.selection_single(
        fields=["index"],
        bind=alt.binding_radio(name="Plot average completeness per: ", options=index),
        init={"index": "stay_type"},
    )

    base_line = (
        alt.Chart(predictor)
        .transform_fold(index, as_=["index", "value"])
        .encode(
            x=alt.X(
                f'yearmonth({"date"}):T',
                title=x_axis_title,
                axis=alt.Axis(tickCount="month", labelAngle=-90, grid=x_grid),
            ),
            color=alt.Color(
                "value:N",
                sort={"field": "c", "op": "mean", "order": "descending"},
                title=None,
            ),
        )
        .transform_filter(index_selection)
    ).properties(width=900, height=300)

    probe_line = base_line.mark_line().encode(
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

    model_line = base_line.mark_line(
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

    fitted_probe = (probe_line + model_line).add_selection(index_selection)

    fitted_probe = fitted_probe.transform_filter(care_site_selection)
    fitted_probe = fitted_probe.transform_filter(time_selection)

    for index_variable_selection in index_variables_selections:
        fitted_probe = fitted_probe.transform_filter(index_variable_selection)
        care_site_hist = care_site_hist.transform_filter(index_variable_selection)
        for extra_predictor_hist in extra_predictors_hists:
            extra_predictor_hist = extra_predictor_hist.transform_filter(
                index_variable_selection
            )
        for idx in range(len(index_variables_hists)):
            if idx != index_variables_selections.index(index_variable_selection):
                index_variables_hists[idx] = index_variables_hists[
                    idx
                ].transform_filter(index_variable_selection)

    index_variables_hists = reduce(
        lambda index_variable_hist_1, index_variable_hist_2: index_variable_hist_1
        & index_variable_hist_2,
        index_variables_hists,
    )
    extra_predictors_hists = reduce(
        lambda extra_predictor_hist_1, extra_predictor_hist_2: extra_predictor_hist_1
        & extra_predictor_hist_2,
        extra_predictors_hists,
    )

    chart = (
        alt.vconcat(
            alt.vconcat(fitted_probe, time_line, spacing=130),
            (index_variables_hists | care_site_hist | extra_predictors_hists),
            spacing=10,
        )
    ).resolve_scale(color="independent")

    return chart
