import altair as alt

vertical_bar_charts = dict(
    x=[
        {
            "title": "Source system",
            "field": "source_system",
            "type": "nominal",
            "sort": "-y",
        },
        {
            "title": "Condition type",
            "field": "condition_type",
            "type": "nominal",
            "sort": "-y",
        },
        {
            "title": "Diag type",
            "field": "diag_type",
            "type": "nominal",
            "sort": "-y",
        },
        {
            "title": "Stay type",
            "field": "stay_type",
            "type": "nominal",
            "sort": "-y",
        },
        {
            "title": "Length of stay",
            "field": "length_of_stay",
            "type": "nominal",
            "sort": "-y",
        },
    ],
    y=[
        dict(
            y=alt.Y(
                "sum(n_condition):Q",
                title="Number of recorded diagnostics",
                axis=alt.Axis(format="s"),
            ),
            tooltip=alt.Tooltip(
                "sum(n_condition):Q",
                format=",",
            ),
            sort={
                "field": "n_condition",
                "op": "sum",
                "order": "descending",
            },
        ),
    ],
)


horizontal_bar_charts = dict(
    y=[
        {
            "title": "Care site",
            "field": "care_site_short_name",
            "sort": "-x",
        },
        {
            "title": "Care site specialty",
            "field": "care_site_specialty",
            "sort": "-x",
        },
        {
            "title": "Specialties-set",
            "field": "specialties_set",
            "sort": "-x",
        },
        {
            "title": "Care sites-set",
            "field": "care_sites_set",
            "sort": "-x",
        },
    ],
    x=[
        dict(
            x=alt.Y(
                "sum(n_condition):Q",
                title="Number of recorded diagnostics",
                axis=alt.Axis(format="s"),
            ),
            tooltip=alt.Tooltip(
                "sum(n_condition):Q",
                format=",",
            ),
            sort={
                "field": "n_condition",
                "op": "sum",
                "order": "descending",
            },
        ),
    ],
)

normalized_main_chart = dict(
    aggregates=[
        dict(
            sum_condition="sum(n_condition)",
            groupby=["value", "date"],
        ),
        dict(
            max_condition="max(sum_condition)",
            groupby=["value"],
        ),
    ],
    calculates=[
        dict(
            normalized_c=(alt.datum.sum_condition / alt.datum.max_condition)
            / alt.datum.c_0
        )
    ],
    legend_title="Mean",
    encode=dict(
        x=alt.X(
            "normalized_date:Q",
            title="Δt = (t - t₀) months",
            scale=alt.Scale(nice=False),
        ),
        y=alt.Y(
            "mean(normalized_c):Q",
            title="c(Δt) / c₀",
            axis=alt.Axis(grid=True),
        ),
        color=alt.Color(
            "value:N",
            sort={
                "field": "n_condition",
                "op": "sum",
                "order": "descending",
            },
            title=None,
        ),
    ),
    properties=dict(
        height=300,
        width=900,
    ),
)

main_chart = dict(
    encode=dict(
        x=alt.X(
            "yearmonth(date):T",
            title="Time (Month Year)",
            axis=alt.Axis(tickCount="month", labelAngle=0, grid=True),
        ),
        y=alt.Y(
            "sum(n_condition):Q",
            title="Number of recorded diagnostic codes",
            axis=alt.Axis(grid=True),
        ),
        color=alt.Color(
            "value:N",
            sort={
                "field": "n_condition",
                "op": "sum",
                "order": "descending",
            },
            title=None,
        ),
        tooltip=[
            alt.Tooltip("value:N", title="Index"),
            alt.Tooltip("yearmonth(date):T", title="Date"),
            alt.Tooltip(
                "sum(n_condition):Q",
                title="Number of recorded diagnostic codes",
                format=",",
            ),
        ],
    ),
    properties=dict(
        height=300,
        width=900,
    ),
)

normalized_time_line = dict(
    encode=dict(
        x=alt.X(
            "normalized_date:Q",
            title="Δt = (t - t₀) months",
            scale=alt.Scale(nice=False),
        ),
        y=alt.Y(
            "sum(n_condition):Q",
            title="Number of recorded diagnostics",
            axis=alt.Axis(format="s"),
        ),
    ),
    properties=dict(
        height=50,
        width=900,
    ),
)

time_line = dict(
    encode=dict(
        x=alt.X(
            "yearmonth(date):T",
            title="Time (Month Year)",
            axis=alt.Axis(tickCount="month", labelAngle=0, grid=True),
        ),
        y=alt.Y(
            "sum(n_condition):Q",
            title="Number of recorded diagnostics",
            axis=alt.Axis(format="s"),
        ),
    ),
    properties=dict(
        height=50,
        width=900,
    ),
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

chart_style = dict(
    labelFontSize=12,
    titleFontSize=13,
)
