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
                    sum_visit="sum(n_visit)",
                    groupby=["value", "date"],
                ),
                dict(
                    sum_visit_with_condition="sum(n_visit_with_condition)",
                    groupby=["value", "date"],
                ),
            ],
            calculates=[
                dict(
                    normalized_c=(
                        alt.datum.sum_visit_with_condition / alt.datum.sum_visit
                    )
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
                        "field": "n_visit_with_condition",
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
        ),
        time_line=dict(
            encode=dict(
                x=alt.X(
                    "normalized_date:Q",
                    title="Δt = (t - t₀) months",
                    scale=alt.Scale(nice=False),
                ),
                y=alt.Y(
                    "sum(n_visit):Q",
                    title="Number of administrative",
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
                        "sum(n_visit_with_condition):Q",
                        title="Number of administrative records with condition",
                        axis=alt.Axis(format="s"),
                    ),
                    tooltip=alt.Tooltip(
                        "sum(n_visit_with_condition):Q",
                        format=",",
                    ),
                    sort={
                        "field": "n_visit_with_condition",
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
            ],
            x=[
                dict(
                    x=alt.Y(
                        "sum(n_visit_with_condition):Q",
                        title="Number of administrative records with condition",
                        axis=alt.Axis(format="s"),
                    ),
                    tooltip=alt.Tooltip(
                        "sum(n_visit_with_condition):Q",
                        format=",",
                    ),
                    sort={
                        "field": "n_visit_with_condition",
                        "op": "sum",
                        "order": "descending",
                    },
                ),
                dict(
                    x=alt.Y(
                        "sum(n_visit):Q",
                        title="Number of administrative",
                        axis=alt.Axis(format="s"),
                    ),
                    tooltip=alt.Tooltip(
                        "sum(n_visit):Q",
                        format=",",
                    ),
                    sort={
                        "field": "n_visit",
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
                    sum_visit="sum(n_visit)",
                    groupby=["value", "date"],
                ),
                dict(
                    sum_visit_with_condition="sum(n_visit_with_condition)",
                    groupby=["value", "date"],
                ),
            ],
            calculates=[
                dict(
                    completeness=alt.datum.sum_visit_with_condition
                    / alt.datum.sum_visit
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
                    sort={
                        "field": "n_visit_with_condition",
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
        ),
        time_line=dict(
            encode=dict(
                x=alt.X(
                    "yearmonth(date):T",
                    title="Time (Month Year)",
                    axis=alt.Axis(tickCount="month", labelAngle=0, grid=True),
                ),
                y=alt.Y(
                    "sum(n_visit):Q",
                    title="Number of administrative",
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
                        "sum(n_visit_with_condition):Q",
                        title="Number of administrative records with condition",
                        axis=alt.Axis(format="s"),
                    ),
                    tooltip=alt.Tooltip(
                        "sum(n_visit_with_condition):Q",
                        format=",",
                    ),
                    sort={
                        "field": "n_visit_with_condition",
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
            ],
            x=[
                dict(
                    x=alt.Y(
                        "sum(n_visit_with_condition):Q",
                        title="Number of administrative records with condition",
                        axis=alt.Axis(format="s"),
                    ),
                    tooltip=alt.Tooltip(
                        "sum(n_visit_with_condition):Q",
                        format=",",
                    ),
                    sort={
                        "field": "n_visit_with_condition",
                        "op": "sum",
                        "order": "descending",
                    },
                ),
                dict(
                    x=alt.Y(
                        "sum(n_visit):Q",
                        title="Number of administrative",
                        axis=alt.Axis(format="s"),
                    ),
                    tooltip=alt.Tooltip(
                        "sum(n_visit):Q",
                        format=",",
                    ),
                    sort={
                        "field": "n_visit",
                        "op": "sum",
                        "order": "descending",
                    },
                ),
            ],
        ),
    )
    return predictor_dashboard_config
