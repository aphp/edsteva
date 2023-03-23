from typing import Dict

import altair as alt
import pandas as pd

from edsteva.viz.utils import (
    add_interactive_selection,
    concatenate_charts,
    configure_style,
    create_groupby_selection,
    generate_horizontal_bar_charts,
    generate_main_chart,
    generate_time_line,
    generate_vertical_bar_charts,
)


def probe_only_dashboard(
    predictor: pd.DataFrame,
    probe_config: Dict[str, str],
):
    """Script to be used by [``predictor_dashboard()``][edsteva.viz.dashboards.predictor_dashboard.wrapper]

    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe.
    index : List[str]
        Variable from which data is grouped.
    x_axis_title: str, optional,
        Label name for the x axis.
    x_grid: bool, optional,
        If False, remove the grid for the x axis.
    y_axis_title: str, optional,
        Label name for the y axis.
    y_grid: bool, optional,
        If False, remove the grid for the y axis.
    show_n_events: bool, optional
        show the number of events instead of the completeness predictor $c(t)$.
    labelAngle: float, optional
        The rotation angle of the label on the x_axis.
    """
    main_chart_config = probe_config["main_chart"]
    time_line_config = probe_config["time_line"]
    vertical_bar_charts_config = probe_config["vertical_bar_charts"]
    horizontal_bar_charts_config = probe_config["horizontal_bar_charts"]
    chart_style = probe_config["chart_style"]

    base = alt.Chart(predictor)
    time_line, time_selection = generate_time_line(
        base=base,
        time_line_config=time_line_config,
    )
    horizontal_bar_charts, y_variables_selections = generate_horizontal_bar_charts(
        base=base,
        horizontal_bar_charts_config=horizontal_bar_charts_config,
    )
    vertical_bar_charts, x_variables_selections = generate_vertical_bar_charts(
        base=base,
        vertical_bar_charts_config=vertical_bar_charts_config,
    )
    selections = dict(
        date=time_selection,
        **y_variables_selections,
        **x_variables_selections,
    )
    selection_charts = dict(
        horizontal_bar_charts,
        **vertical_bar_charts,
    )
    base = add_interactive_selection(
        base=base, selection_charts=selection_charts, selections=selections
    )

    index_selection, index_fields = create_groupby_selection(
        indexes=vertical_bar_charts_config["x"] + horizontal_bar_charts_config["y"],
    )
    main_chart = generate_main_chart(
        base=base,
        main_chart_config=main_chart_config,
        index_selection=index_selection,
        index_fields=index_fields,
    )

    main_chart = main_chart.mark_line()
    if index_selection:
        main_chart = main_chart.add_selection(index_selection)
    chart = concatenate_charts(
        main_chart=main_chart,
        time_line=time_line,
        horizontal_bar_charts=horizontal_bar_charts,
        vertical_bar_charts=vertical_bar_charts,
    )
    chart = configure_style(chart=chart, chart_style=chart_style)

    return chart
