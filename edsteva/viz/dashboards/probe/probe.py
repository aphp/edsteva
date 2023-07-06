from typing import Dict

import altair as alt
import pandas as pd

from edsteva.viz.utils import (
    add_interactive_selection,
    add_selection_on_legend,
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
    x_axis_title: str,
    y_axis_title: str,
    main_chart_config: Dict[str, float],
    vertical_bar_charts_config: Dict[str, str],
    horizontal_bar_charts_config: Dict[str, str],
    time_line_config: Dict[str, str],
    chart_style: Dict[str, float],
):
    """Script to be used by [``predictor_dashboard()``][edsteva.viz.dashboards.probe.wrapper]

    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe.
    x_axis_title: str, optional,
        Label name for the x axis.
    y_axis_title: str, optional,
        Label name for the y axis.
    main_chart_config: Dict[str, str], optional
        If not None, configuration used to construct the top main chart.
    vertical_bar_charts_config: Dict[str, str], optional
        If not None, configuration used to construct the vertical bar charts.
    horizontal_bar_charts_config: Dict[str, str], optional
        If not None, configuration used to construct the horizontal bar charts.
    time_line_config: Dict[str, str], optional
        If not None, configuration used to construct the time line.
    chart_style: Dict[str, float], optional
        If not None, configuration used to configure the chart style.
        **EXAMPLE**: `{"labelFontSize": 13, "titleFontSize": 14}`
    """

    base = alt.Chart(predictor)
    time_line, time_selection = generate_time_line(
        base=base,
        time_line_config=time_line_config,
    )
    horizontal_bar_charts, y_variables_selections = generate_horizontal_bar_charts(
        base=base,
        horizontal_bar_charts_config=horizontal_bar_charts_config,
        predictor=predictor,
    )
    vertical_bar_charts, x_variables_selections = generate_vertical_bar_charts(
        base=base,
        vertical_bar_charts_config=vertical_bar_charts_config,
        predictor=predictor,
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
        predictor=predictor,
    )
    main_chart = generate_main_chart(
        base=base,
        main_chart_config=main_chart_config,
        index_selection=index_selection,
        index_fields=index_fields,
        x_axis_title=x_axis_title,
        y_axis_title=y_axis_title,
    )

    main_chart = main_chart.mark_line()
    main_chart = add_selection_on_legend(main_chart)
    if index_selection:
        main_chart = main_chart.add_params(index_selection)
    chart = concatenate_charts(
        main_chart=main_chart,
        time_line=time_line,
        horizontal_bar_charts=horizontal_bar_charts,
        vertical_bar_charts=vertical_bar_charts,
    )
    return configure_style(chart=chart, chart_style=chart_style)
