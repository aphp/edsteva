import altair as alt

vertical_bar_charts = dict(
    x=[
        {
            "title": "Note type",
            "field": "note_type",
            "type": "nominal",
            "sort": "-y",
        },
        {
            "title": "Care site level",
            "field": "care_site_level",
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
            "title": "Stay source",
            "field": "stay_source",
            "type": "nominal",
            "sort": "-y",
        },
        {
            "title": "Length of stay",
            "field": "length_of_stay",
            "type": "nominal",
            "sort": "-y",
        },
        {
            "title": "Provenance source",
            "field": "provenance_source",
            "type": "nominal",
            "sort": "-y",
        },
        {
            "title": "Age range",
            "field": "age_range",
            "type": "nominal",
            "sort": "-y",
        },
    ],
    y=[
        dict(
            y=alt.Y(
                "sum(n_note):Q",
                title="Number of discharge summaries",
                axis=alt.Axis(format="s"),
            ),
            tooltip=alt.Tooltip(
                "sum(n_note):Q",
                format=",",
            ),
            sort={
                "field": "n_note",
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
            x=alt.X(
                "sum(n_note):Q",
                title="Number of discharge summaries",
                axis=alt.Axis(format="s"),
            ),
            tooltip=alt.Tooltip(
                "sum(n_note):Q",
                format=",",
            ),
            sort={
                "field": "n_note",
                "op": "sum",
                "order": "descending",
            },
        ),
    ],
)

normalized_main_chart = dict(
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
                "field": "n_note",
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
    aggregates=[
        dict(
            sum_note="sum(n_note)",
            groupby=["value", "date"],
        ),
        dict(
            max_note="max(sum_note)",
            groupby=["value"],
        ),
    ],
    calculates=[
        dict(c=alt.datum.sum_note / alt.datum.max_note),
    ],
    encode=dict(
        x=alt.X(
            "yearmonth(date):T",
            title="Time (Month Year)",
            axis=alt.Axis(tickCount="month", labelAngle=0, grid=True),
        ),
        y=alt.Y(
            "c:Q",
            title="Completeness predictor c(t)",
            axis=alt.Axis(grid=True),
        ),
        color=alt.Color(
            "value:N",
            sort={
                "field": "n_note",
                "op": "sum",
                "order": "descending",
            },
            title=None,
        ),
        tooltip=[
            alt.Tooltip("value:N", title="Index"),
            alt.Tooltip("yearmonth(date):T", title="Date"),
            alt.Tooltip("c:Q", title="c(t)", format=".2f"),
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
            "sum(n_note):Q",
            title="Number of discharge summaries",
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
            "sum(n_note):Q",
            title="Number of discharge summaries",
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
