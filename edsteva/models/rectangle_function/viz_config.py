import altair as alt

from edsteva.utils.typing import DataFrame
from edsteva.viz.utils import round_up, scale_it


def get_estimates_dashboard_config(self, predictor: DataFrame):
    c_0_min_slider = alt.binding_range(
        min=0,
        max=round_up(predictor.c_0.max(), 2),
        step=scale_it(predictor.c_0.max()) / 100,
        name="c₀ min: ",
    )
    c_0_min_selection = alt.selection_single(
        name="c_0_min",
        fields=["c_0_min"],
        bind=c_0_min_slider,
        init={"c_0_min": 0},
    )
    c_0_min_filter = alt.datum.c_0 >= c_0_min_selection.c_0_min
    t_0_slider = alt.binding(
        input="t_0",
        name="t₀ max: ",
    )
    t_0_selection = alt.selection_single(
        name="t_0",
        fields=["t_0"],
        bind=t_0_slider,
        init={"t_0": predictor.t_0.max()},
    )
    t_0_min_filter = alt.datum.t_0 <= t_0_selection.t_0
    t_1_slider = alt.binding(
        input="t_1",
        name="t₁ min: ",
    )
    t_1_selection = alt.selection_single(
        name="t_1",
        fields=["t_1"],
        bind=t_1_slider,
        init={"t_1": predictor.t_1.min()},
    )
    t_1_min_filter = alt.datum.t_1 >= t_1_selection.t_1
    error_max_slider = alt.binding_range(
        min=0,
        max=round_up(predictor.error.max(), 2),
        step=scale_it(predictor.error.max()) / 100,
        name="error max: ",
    )
    error_max_selection = alt.selection_single(
        name="error_max",
        fields=["error_max"],
        bind=error_max_slider,
        init={"error_max": round_up(predictor.error.max(), 2)},
    )
    error_max_filter = alt.datum.error <= error_max_selection.error_max
    estimates_dashboard_config = dict(
        estimates_selections=[
            c_0_min_selection,
            t_0_selection,
            t_1_selection,
            error_max_selection,
        ],
        estimates_filters=[
            c_0_min_filter,
            t_0_min_filter,
            t_1_min_filter,
            error_max_filter,
        ],
        probe_line=dict(
            encode=dict(
                strokeDash=alt.StrokeDash(
                    "legend_predictor",
                    title="Predictor line",
                    legend=alt.Legend(
                        symbolType="stroke",
                        symbolStrokeColor="steelblue",
                        labelFontSize=12,
                        labelFontStyle="bold",
                        orient="top",
                    ),
                )
            )
        ),
        model_line=dict(
            mark_line=dict(
                color="black",
                interpolate="step-after",
                strokeDash=[5, 5],
            ),
            encode=dict(
                y="model:Q",
                strokeWidth=alt.StrokeWidth(
                    field="legend_model",
                    title="Model line",
                    legend=alt.Legend(
                        symbolType="stroke",
                        symbolStrokeColor="steelblue",
                        labelFontSize=12,
                        labelFontStyle="bold",
                        symbolDash=[2, 2],
                        orient="top",
                    ),
                ),
                color=alt.Color(),
            ),
        ),
        extra_horizontal_bar_charts=[
            dict(
                x=alt.X(
                    "min(c_0):Q",
                    title="Min(c₀)",
                ),
                tooltip=alt.Tooltip("min(c_0):Q", format=".2"),
                sort={
                    "field": "c_0",
                    "op": "min",
                    "order": "descending",
                },
            ),
            dict(
                x=alt.X(
                    "max(error):Q",
                    title="Max(error)",
                ),
                tooltip=alt.Tooltip("max(error):Q", format=".2"),
                sort={
                    "field": "error",
                    "op": "max",
                    "order": "descending",
                },
            ),
        ],
        extra_vertical_bar_charts=[],
    )
    return estimates_dashboard_config


def get_predictor_dashboard_config(self):
    predictor_dashboard_config = dict(
        probe_line=dict(
            encode=dict(
                strokeDash=alt.StrokeDash(
                    "legend_predictor",
                    title="",
                    legend=alt.Legend(
                        symbolType="stroke",
                        symbolStrokeColor="steelblue",
                        labelFontSize=12,
                        labelFontStyle="bold",
                        orient="left",
                    ),
                )
            )
        ),
        model_line=dict(
            mark_line=dict(
                interpolate="step-after",
                strokeDash=[5, 5],
            ),
            encode=dict(
                y=alt.Y("min(c_hat)"),
                strokeWidth=alt.StrokeWidth(
                    field="legend_model",
                    title="",
                    legend=alt.Legend(
                        symbolType="stroke",
                        symbolStrokeColor="steelblue",
                        labelFontSize=12,
                        labelFontStyle="bold",
                        symbolDash=[2, 2],
                        orient="left",
                    ),
                ),
            ),
        ),
        extra_horizontal_bar_charts=[
            dict(
                x=alt.X(
                    "min(c_0):Q",
                    title="Minimum c₀",
                    axis=alt.Axis(format=".2"),
                ),
                tooltip=alt.Tooltip(
                    "min(c_0):Q",
                    format=".2",
                ),
                sort={
                    "field": "c_0",
                    "op": "min",
                    "order": "descending",
                },
            ),
        ],
        extra_vertical_bar_charts=[],
    )
    return predictor_dashboard_config
