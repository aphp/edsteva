import os
from datetime import datetime
from functools import reduce
from math import ceil, floor, log10
from pathlib import Path
from typing import Dict, List, Union

import altair as alt
import pandas as pd
from loguru import logger
from toolz.curried import pipe

from edsteva import CACHE_DIR
from edsteva.probes.utils import CARE_SITE_LEVEL_NAMES, filter_table_by_date


def generate_main_chart(
    base: alt.Chart,
    main_chart_config: Dict[str, str],
    index_selection: alt.Selection = None,
    index_fields: List[str] = None,
):
    if index_fields:
        base = base.transform_fold(index_fields, as_=["index", "value"])
        if "aggregates" in main_chart_config.keys():
            for aggregate in main_chart_config["aggregates"]:
                base = base.transform_joinaggregate(**aggregate)
        if "calculates" in main_chart_config.keys():
            for calculate in main_chart_config["calculates"]:
                base = base.transform_calculate(**calculate)
        if "filters" in main_chart_config.keys():
            for filter in main_chart_config["filters"]:
                base = base.transform_filter(**filter)
        main_chart = base.encode(**main_chart_config["encode"])
        if index_selection:
            main_chart = main_chart.transform_filter(index_selection)
    else:
        main_chart = base.encode(
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
        )
    return main_chart.properties(**main_chart_config["properties"])


def generate_model_line(
    main_chart: alt.Chart,
    model_line_config: Dict[str, str],
):
    model_line = main_chart.mark_line(**model_line_config["mark_line"])
    if "aggregates" in model_line_config.keys():
        for aggregate in model_line_config["aggregates"]:
            model_line = model_line.transform_joinaggregate(**aggregate)
    if "calculates" in model_line_config.keys():
        for calculate in model_line_config["calculates"]:
            model_line = model_line.transform_calculate(**calculate)
    if "filters" in model_line_config.keys():
        for filter in model_line_config["filters"]:
            model_line = model_line.transform_filter(**filter)
    model_line = model_line.encode(**model_line_config["encode"])

    return model_line


def generate_error_line(
    main_chart: alt.Chart,
    error_line_config: Dict[str, str],
):
    error_line = main_chart.mark_errorband(
        **error_line_config["mark_errorband"]
    ).encode(**error_line_config["encode"])
    return error_line


def generate_probe_line(
    main_chart: alt.Chart,
    probe_line_config: Dict[str, str],
):
    probe_line = main_chart.mark_line().encode(**probe_line_config["encode"])

    return probe_line


def generate_time_line(
    base: alt.Chart,
    time_line_config: Dict[str, str],
):
    time_selection = alt.selection_interval(encodings=["x"])
    time_line = (
        base.mark_line()
        .encode(**time_line_config["encode"])
        .add_selection(time_selection)
    ).properties(**time_line_config["properties"])
    return time_line, time_selection


def generate_horizontal_bar_charts(
    base: alt.Chart,
    horizontal_bar_charts_config: Dict[str, str],
):
    horizontal_bar_charts = {}
    y_variables_selections = {}
    for y_variable in horizontal_bar_charts_config["y"]:
        y_variable_bar_charts = []
        y_variable_selection = alt.selection_multi(fields=[y_variable["field"]])
        y_variables_selections[y_variable["field"]] = y_variable_selection
        y_variable_base_chart = (
            base.mark_bar()
            .encode(
                y=alt.Y(**y_variable),
            )
            .add_selection(y_variable_selection)
        )
        for x_variable in horizontal_bar_charts_config["x"]:
            y_index_variable_color = alt.condition(
                y_variables_selections[y_variable["field"]],
                alt.Color(
                    "{}:N".format(y_variable["field"]),
                    legend=None,
                    sort=x_variable["sort"],
                ),
                alt.value("lightgray"),
            )
            y_variable_bar_chart = y_variable_base_chart.encode(
                x=x_variable["x"],
                color=y_index_variable_color,
                tooltip=x_variable["tooltip"],
            )

            y_variable_bar_charts.append(y_variable_bar_chart)
        horizontal_bar_charts[y_variable["field"]] = y_variable_bar_charts
    return horizontal_bar_charts, y_variables_selections


def generate_vertical_bar_charts(
    base: alt.Chart,
    vertical_bar_charts_config: Dict[str, str],
):
    vertical_bar_charts = {}
    x_variables_selections = {}
    for x_variable in vertical_bar_charts_config["x"]:
        x_variable_bar_charts = []
        x_variable_selection = alt.selection_multi(fields=[x_variable["field"]])
        x_variables_selections[x_variable["field"]] = x_variable_selection
        x_variable_base_chart = (
            base.mark_bar()
            .encode(
                x=alt.X(**x_variable),
            )
            .add_selection(x_variable_selection)
        )

        for y_variable in vertical_bar_charts_config["y"]:
            x_index_variable_color = alt.condition(
                x_variables_selections[x_variable["field"]],
                alt.Color(
                    "{}:N".format(x_variable["field"]),
                    legend=None,
                    sort=y_variable["sort"],
                ),
                alt.value("lightgray"),
            )
            x_variable_bar_chart = x_variable_base_chart.encode(
                y=y_variable["y"],
                color=x_index_variable_color,
                tooltip=y_variable["tooltip"],
            )

            x_variable_bar_charts.append(x_variable_bar_chart)
        vertical_bar_charts[x_variable["field"]] = x_variable_bar_charts
    return vertical_bar_charts, x_variables_selections


def add_interactive_selection(
    base: alt.Chart,
    selections: Dict[str, alt.Selection],
    selection_charts: Dict[str, List[alt.Chart]] = {},
):
    for selection_variable, selection in selections.items():
        base = base.transform_filter(selection)
        for chart_variable in selection_charts.keys():
            if chart_variable != selection_variable:
                for i in range(len(selection_charts[chart_variable])):
                    selection_charts[chart_variable][i] = selection_charts[
                        chart_variable
                    ][i].transform_filter(selection)
    return base


def add_estimates_filters(
    base: alt.Chart,
    estimates_filters: Dict[str, alt.Selection],
    selection_charts: Dict[str, List[alt.Chart]] = {},
):
    for estimate_filter in estimates_filters:
        base = base.transform_filter(estimate_filter)
        for chart_variable in selection_charts.keys():
            for i in range(len(selection_charts[chart_variable])):
                selection_charts[chart_variable][i] = selection_charts[chart_variable][
                    i
                ].transform_filter(estimate_filter)

    return base


def create_groupby_selection(
    indexes: List[str],
):
    index_fields = [index["field"] for index in indexes]
    if len(index_fields) >= 2:
        index_labels = [index["title"] for index in indexes]
        index_selection = alt.selection_single(
            fields=["index"],
            bind=alt.binding_radio(
                name="Group by: ", options=index_fields, labels=index_labels
            ),
            init={"index": index_fields[0]},
        )
    else:
        index_selection = None
    return index_selection, index_fields


def configure_style(
    chart: alt.Chart,
    chart_style: Dict[str, float],
):
    return chart.configure_axis(
        labelFontSize=chart_style["labelFontSize"],
        titleFontSize=chart_style["titleFontSize"],
    ).configure_legend(labelFontSize=chart_style["labelFontSize"])


def concatenate_charts(
    main_chart: alt.Chart,
    horizontal_bar_charts: Dict[str, List[alt.Chart]],
    vertical_bar_charts: Dict[str, List[alt.Chart]],
    time_line: alt.Chart = None,
    spacing: float = 10,
):
    bar_charts_rows = []
    for bar_charts_row in horizontal_bar_charts.values():
        bar_charts_row = reduce(
            lambda bar_chart_1, bar_chart_2: (bar_chart_1 | bar_chart_2).resolve_scale(
                color="independent"
            ),
            bar_charts_row,
        )
        bar_charts_rows.append(bar_charts_row)
    horizontal_bar_charts = reduce(
        lambda bar_chart_1, bar_chart_2: (bar_chart_1 & bar_chart_2).resolve_scale(
            color="independent"
        ),
        bar_charts_rows,
    )

    bar_charts_columns = []
    for bar_charts_column in vertical_bar_charts.values():
        bar_charts_column = reduce(
            lambda bar_chart_1, bar_chart_2: (bar_chart_1 | bar_chart_2).resolve_scale(
                color="independent"
            ),
            bar_charts_column,
        )
        bar_charts_columns.append(bar_charts_column)
    vertical_bar_charts = reduce(
        lambda bar_chart_1, bar_chart_2: (bar_chart_1 & bar_chart_2).resolve_scale(
            color="independent"
        ),
        bar_charts_columns,
    )
    if time_line:
        top_chart = alt.vconcat(main_chart, time_line, spacing=100).resolve_scale(
            color="independent"
        )
    else:
        top_chart = main_chart
    concat_chart = alt.vconcat(
        top_chart,
        (vertical_bar_charts | horizontal_bar_charts),
        spacing=spacing,
    )
    return concat_chart


def month_diff(x, y):
    end = x.dt.to_period("M").view(dtype="int64")
    start = y.dt.to_period("M").view(dtype="int64")
    return end - start


def json_dir(data):
    os.makedirs(CACHE_DIR / "data_viz", exist_ok=True)
    return pipe(
        data,
        alt.to_json(
            filename=(CACHE_DIR / "data_viz").__str__() + "/{prefix}-{hash}.{extension}"
        ),
    )


def save_html(obj: alt.Chart, filename: str):
    """Save chart in the specified file

    Parameters
    ----------
    obj : alt.Chart
        Altair chart to be saved
    filename : str
        Folder path where to save the chart in HTML format.

        **EXAMPLE**: `"my_folder/my_file.html"`
    """
    if not isinstance(filename, Path):
        filename = Path(filename)
    os.makedirs(filename.parent, exist_ok=True)
    if hasattr(obj, "save"):
        obj.save(filename)
    else:
        with open(filename, "w") as f:
            f.write(obj)
    logger.info("The chart has been saved in {}", filename)


def round_up(x: float, sig: int):
    if x == 0:
        return 0
    else:
        decimals = sig - floor(log10(abs(x))) - 1
        return ceil(x * 10**decimals) / 10**decimals


def scale_it(x: float):
    if x == 0:
        return 1
    else:
        return 10 ** ceil(log10(x))


def filter_predictor(
    predictor: pd.DataFrame,
    care_site_level: str = None,
    stay_type: List[str] = None,
    care_site_id: List[int] = None,
    care_site_short_name: List[int] = None,
    start_date: Union[datetime, str] = None,
    end_date: Union[datetime, str] = None,
    **kwargs
):
    # Time
    predictor = filter_table_by_date(
        table=predictor,
        table_name="predictor",
        start_date=start_date,
        end_date=end_date,
    )

    # Care site level
    if care_site_level:
        if not isinstance(care_site_level, list):
            care_site_level = [care_site_level]
        care_site_levels = []
        for care_site_lvl in care_site_level:
            if care_site_lvl in CARE_SITE_LEVEL_NAMES.keys():
                care_site_lvl = CARE_SITE_LEVEL_NAMES[care_site_lvl]
            if care_site_lvl not in predictor.care_site_level.unique():
                raise AttributeError(
                    "The selected care site level {} is not part of the computed care site levels {}".format(
                        care_site_level, list(predictor.care_site_level.unique())
                    )
                )
            care_site_levels.append(care_site_lvl)
        predictor = predictor[predictor.care_site_level.isin(care_site_levels)]
        logger.debug(
            "Predictor has been filtered on the selected care site level : {}",
            care_site_levels,
        )

    # Stay type
    if stay_type:
        if not isinstance(stay_type, list):
            stay_type = [stay_type]
        predictor = predictor[predictor.stay_type.isin(stay_type)]
        logger.debug(
            "Predictor has been filtered on the selected stay type : {}",
            stay_type,
        )

    # Care site id
    if care_site_id:
        if not isinstance(care_site_id, list):
            care_site_id = [care_site_id]
        predictor = predictor[predictor.care_site_id.isin(care_site_id)]
        logger.debug(
            "Predictor has been filtered on the selected care site id : {}",
            care_site_id,
        )

    # Care site short name
    if care_site_short_name:
        if not isinstance(care_site_short_name, list):
            care_site_short_name = [care_site_short_name]
        predictor = predictor[predictor.care_site_short_name.isin(care_site_short_name)]
        logger.debug(
            "Predictor has been filtered on the selected care site short name : {}",
            care_site_short_name,
        )

    # Others
    for key, value in kwargs.items():
        if not isinstance(value, list):
            value = [value]
        predictor = predictor[predictor[key].isin(value)]
        logger.debug(
            "Predictor has been filtered on the selected {} : {}",
            key,
            value,
        )

    if predictor.empty:
        raise TypeError("Empty predictor: no data to plot.")
    return predictor
