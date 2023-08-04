import altair as alt


def get_normalized_probe_dashboard_config(self, **kwargs):
    vertical_bar_charts, horizontal_bar_charts = self.generate_bar_chart_config(
        **kwargs
    )
    return dict(
        chart_style=chart_style,
        main_chart=normalized_main_chart,
        time_line=normalized_time_line,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )


def get_estimates_densities_plot_config(self, **kwargs):
    vertical_bar_charts, horizontal_bar_charts = self.generate_bar_chart_config(
        **kwargs
    )
    return dict(
        chart_style=chart_style,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )


def get_normalized_probe_plot_config(self, **kwargs):
    return dict(
        chart_style=chart_style,
        main_chart=normalized_main_chart,
    )


def get_probe_dashboard_config(self, **kwargs):
    vertical_bar_charts, horizontal_bar_charts = self.generate_bar_chart_config(
        **kwargs
    )
    return dict(
        chart_style=chart_style,
        main_chart=main_chart,
        time_line=time_line,
        vertical_bar_charts=vertical_bar_charts,
        horizontal_bar_charts=horizontal_bar_charts,
    )


def get_probe_plot_config(self, **kwargs):
    return dict(
        chart_style=chart_style,
        main_chart=main_chart,
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
            "mean(c):Q",
            title="Completeness predictor c(t)",
            axis=alt.Axis(grid=True),
        ),
        color=alt.Color(
            "value:N",
            title=None,
        ),
        tooltip=[
            alt.Tooltip("value:N", title="Index"),
            alt.Tooltip("yearmonth(date):T", title="Date"),
            alt.Tooltip("mean(c):Q", title="c(t)", format=".2f"),
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
            "mean(normalized_c):Q",
            title="c(Δt) / c₀",
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
            "mean(c):Q",
            title="Completeness predictor c(t)",
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

# This is the basic viz configs if not overridden by the probe.
viz_configs = dict(
    normalized_probe_dashboard=get_normalized_probe_dashboard_config,
    probe_dashboard=get_probe_dashboard_config,
    estimates_densities_plot=get_estimates_densities_plot_config,
    normalized_probe_plot=get_normalized_probe_plot_config,
    probe_plot=get_probe_plot_config,
)
