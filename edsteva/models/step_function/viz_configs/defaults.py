import altair as alt

from edsteva.utils.typing import DataFrame
from edsteva.viz.utils import round_up, scale_it


def get_c_0_min_selection(predictor: DataFrame):
    c_0_min_slider = alt.binding_range(
        min=0,
        max=round_up(predictor.c_0.max(), 2),
        step=scale_it(predictor.c_0.max()) / 100,
        name="c₀ min: ",
    )
    c_0_min_selection = alt.selection_point(
        name="c_0_min",
        fields=["c_0_min"],
        bind=c_0_min_slider,
        value=0,
    )
    c_0_min_filter = alt.datum.c_0 >= c_0_min_selection.c_0_min
    return c_0_min_selection, c_0_min_filter


def get_error_max_selection(predictor: DataFrame):
    error_max_slider = alt.binding_range(
        min=0,
        max=round_up(predictor.error.max(), 2),
        step=scale_it(predictor.error.max()) / 100,
        name="error max: ",
    )
    error_max_selection = alt.selection_point(
        name="error_max",
        fields=["error_max"],
        bind=error_max_slider,
        value=round_up(predictor.error.max(), 2),
    )
    error_max_filter = alt.datum.error <= error_max_selection.error_max
    return error_max_selection, error_max_filter


def get_t_0_selection(predictor: DataFrame):
    t_0_slider = alt.binding(
        input="t_0",
        name="t₀ max: ",
    )
    t_0_selection = alt.selection_point(
        name="t_0",
        fields=["t_0"],
        bind=t_0_slider,
        value=predictor.t_0.astype(str).max(),
    )
    t_0_min_filter = alt.datum.t_0 <= t_0_selection.t_0
    return t_0_selection, t_0_min_filter


normalized_probe_line = dict(
    legend_title="Mean",
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
    ),
)

probe_line = dict(
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
)

normalized_model_line = dict(
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
    ),
)

model_line = dict(
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
        tooltip=[
            alt.Tooltip("value:N", title="Index"),
            alt.Tooltip("yearmonth(date):T", title="Date"),
            alt.Tooltip("min(c_hat):Q", title="c₀"),
        ],
    ),
    aggregates=[
        dict(
            max_t0="max(t_0)",
            groupby=["value", "date"],
        ),
    ],
    filters=[dict(filter=alt.datum.t_0 == alt.datum.max_t0)],
)

error_line = dict(
    legend_title="Standard deviation",
    mark_errorband=dict(
        extent="stdev",
    ),
    encode=dict(
        stroke=alt.Stroke(
            "legend_error_band",
            title="Error band",
            legend=alt.Legend(
                symbolType="square",
                orient="top",
                labelFontSize=12,
                labelFontStyle="bold",
            ),
        ),
    ),
)

horizontal_min_c0 = dict(
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
)

horizontal_max_error = dict(
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
)
