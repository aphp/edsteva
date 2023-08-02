from typing import List

import altair as alt

vertical_bar_charts = dict(
    x=[
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
                "sum(n_measurement):Q",
                title="Number of measurements",
                axis=alt.Axis(format="s"),
            ),
            tooltip=alt.Tooltip(
                "sum(n_measurement):Q",
                format=",",
            ),
            sort={
                "field": "n_measurement",
                "op": "sum",
                "order": "descending",
            },
        ),
    ],
)


def get_horizontal_bar_charts(standard_terminologies: List[str]):
    return dict(
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
            {"title": "Concepts-set", "field": "concepts_set", "sort": "-x"},
        ]
        + [
            {
                "title": "{} concept".format(terminology),
                "field": "{}_concept_name".format(terminology),
                "sort": "-x",
            }
            for terminology in standard_terminologies
        ],
        x=[
            dict(
                x=alt.X(
                    "sum(n_measurement):Q",
                    title="Number of measurements",
                    axis=alt.Axis(format="s"),
                ),
                tooltip=alt.Tooltip(
                    "sum(n_measurement):Q",
                    format=",",
                ),
                sort={
                    "field": "n_measurement",
                    "op": "sum",
                    "order": "descending",
                },
            ),
        ],
    )


main_chart = dict(
    encode=dict(
        x=alt.X(
            "yearmonth(date):T",
            title="Time (Month Year)",
            axis=alt.Axis(tickCount="month", labelAngle=0, grid=True),
        ),
        y=alt.Y(
            "sum(n_measurement):Q",
            title="Number of measurements",
            axis=alt.Axis(grid=True),
        ),
        color=alt.Color(
            "value:N",
            sort={"field": "n_measurement", "op": "sum", "order": "descending"},
            title=None,
        ),
        tooltip=[
            alt.Tooltip("value:N", title="Index"),
            alt.Tooltip("yearmonth(date):T", title="Date"),
            alt.Tooltip(
                "sum(n_measurement):Q", title="Number of measurements", format=","
            ),
        ],
    ),
    properties=dict(
        height=300,
        width=900,
    ),
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
            sort={"field": "n_measurement", "op": "sum", "order": "descending"},
            title=None,
        ),
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
            "sum(n_measurement):Q",
            title="Number of measurements",
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
            "sum(n_measurement):Q",
            title="Number of measurements",
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
