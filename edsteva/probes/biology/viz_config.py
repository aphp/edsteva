import altair as alt


def get_estimates_dashboard_config(self):
    estimates_dashboard_config = dict(
        chart_style=dict(
            labelFontSize=12,
            titleFontSize=13,
        ),
        main_chart=dict(
            aggregates=[
                dict(
                    sum_measurement="sum(n_measurement)",
                    groupby=["value", "date"],
                ),
                dict(
                    max_measurement="max(sum_measurement)",
                    groupby=["value"],
                ),
            ],
            calculates=[
                dict(
                    normalized_c=(alt.datum.sum_measurement / alt.datum.max_measurement)
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
                    sort={"field": "n_measurement", "op": "sum", "order": "descending"},
                    title=None,
                ),
            ),
            properties=dict(
                height=300,
                width=900,
            ),
        ),
        time_line=dict(
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
        ),
        error_line=dict(
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
        ),
        vertical_bar_charts=dict(
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
        ),
        horizontal_bar_charts=dict(
            y=[
                {
                    "title": "Care site",
                    "field": "care_site_short_name",
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
                for terminology in self._standard_terminologies.copy()
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
        ),
    )

    return estimates_dashboard_config


def get_predictor_dashboard_config(self):
    predictor_dashboard_config = dict(
        chart_style=dict(
            labelFontSize=12,
            titleFontSize=13,
        ),
        main_chart=dict(
            aggregates=[
                dict(
                    sum_measurement="sum(n_measurement)",
                    groupby=["value", "date"],
                ),
                dict(
                    max_measurement="max(sum_measurement)",
                    groupby=["value"],
                ),
            ],
            calculates=[
                dict(
                    completeness=alt.datum.sum_measurement / alt.datum.max_measurement
                ),
            ],
            encode=dict(
                x=alt.X(
                    "yearmonth(date):T",
                    title="Time (Month Year)",
                    axis=alt.Axis(tickCount="month", labelAngle=0, grid=True),
                ),
                y=alt.Y(
                    "completeness:Q",
                    title="Completeness predictor c(t)",
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
        ),
        time_line=dict(
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
        ),
        vertical_bar_charts=dict(
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
        ),
        horizontal_bar_charts=dict(
            y=[
                {
                    "title": "Care site",
                    "field": "care_site_short_name",
                    "type": "nominal",
                    "sort": "-x",
                },
                {
                    "title": "Concepts-set",
                    "field": "concepts_set",
                    "type": "nominal",
                    "sort": "-x",
                },
            ]
            + [
                {
                    "title": "{} concept".format(terminology),
                    "field": "{}_concept_name".format(terminology),
                    "sort": "-x",
                }
                for terminology in self._standard_terminologies.copy()
            ],
            x=[
                dict(
                    x=alt.Y(
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
                )
            ],
        ),
    )
    return predictor_dashboard_config
