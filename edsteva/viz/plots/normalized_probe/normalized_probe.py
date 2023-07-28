from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Union

import altair as alt
import pandas as pd

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.viz.utils import (
    add_estimates_filters,
    configure_style,
    create_groupby_selection,
    filter_data,
    generate_error_line,
    generate_main_chart,
    generate_model_line,
    generate_probe_line,
    get_indexes_to_groupby,
    month_diff,
    save_html,
)


def normalized_probe_plot(
    probe: BaseProbe,
    fitted_model: BaseModel,
    care_site_level: str = None,
    stay_type: List[str] = None,
    care_site_id: List[int] = None,
    start_date: Union[datetime, str] = None,
    end_date: Union[datetime, str] = None,
    care_site_short_name: List[int] = None,
    t_min: int = None,
    t_max: int = None,
    save_path: str = None,
    x_axis_title: str = None,
    y_axis_title: str = None,
    main_chart_config: Dict[str, float] = None,
    model_line_config: Dict[str, str] = None,
    probe_line_config: Dict[str, str] = None,
    error_line_config: Dict[str, str] = None,
    estimates_selections: Dict[str, str] = None,
    estimates_filters: Dict[str, str] = None,
    chart_style: Dict[str, float] = None,
    indexes_to_remove: List[str] = ["care_site_id"],
    **kwargs,
):
    r"""Displays a chart with the aggregated normalized completeness predictor $\frac{c(\Delta t)}{c_0}$ over normalized time $\Delta t = t - t_0$. It represents the overall deviation from the Model.

    Is is possible to save the chart in HTML with the "save_path" optional input.

    Parameters
    ----------
    probe : BaseProbe
        Class describing the completeness predictor $c(t)$
    fitted_model : BaseModel
        Model fitted to the probe
    care_site_level : str, optional
        **EXAMPLE**: `"Hospital"`, `"Hôpital"` or `"UF"`
    stay_type : List[str], optional
        **EXAMPLE**: `"All"` or `["All", "Urg"]`
    care_site_id : List[int], optional
        **EXAMPLE**: `[8312056386, 8312027648]`
    start_date : datetime, optional
        **EXAMPLE**: `"2019-05-01"`
    end_date : datetime, optional
        **EXAMPLE**: `"2021-07-01"`
    care_site_short_name : List[int], optional
        **EXAMPLE**: `"HOSPITAL XXXX"`
    t_min : int, optional
        Minimal difference with $t_0$ in month $\Delta t_{min}$
        **EXAMPLE**: `-24`
    t_max : int, optional
        Maximal difference with $t_0$ in month $\Delta t_{max}$
        **EXAMPLE**: `24`
    save_path : str, optional
        Folder path where to save the chart in HTML format.
    x_axis_title: str, optional,
        Label name for the x axis.
    y_axis_title: str, optional,
        Label name for the y axis.
    main_chart_config: Dict[str, str], optional
        If not None, configuration used to construct the top main chart.
    model_line_config: Dict[str, str], optional
        If not None, configuration used to construct the model line.
    error_line_config: Dict[str, str], optional
        If not None, configuration used to construct the error line.
    probe_line_config: Dict[str, str], optional
        If not None, configuration used to construct the probe line.
    estimates_selections: Dict[str, str], optional
        If not None, configuration used to construct the estimates selections.
    estimates_filters: Dict[str, str], optional
        If not None, configuration used to construct the estimates filters.
    chart_style: Dict[str, float], optional
        If not None, configuration used to configure the chart style.
        **EXAMPLE**: `{"labelFontSize": 13, "titleFontSize": 14}`
    indexes_to_remove: List[str], optional
        indexes to remove from the groupby selection.
    """

    alt.data_transformers.disable_max_rows()

    # Pre-processing
    predictor = probe.predictor.copy()
    predictor_metrics = probe._metrics.copy()
    estimates = fitted_model.estimates.copy()
    indexes = get_indexes_to_groupby(
        predictor_columns=predictor.columns,
        predictor_metrics=predictor_metrics,
        indexes_to_remove=indexes_to_remove,
    )
    predictor = predictor.merge(estimates, on=probe._index)
    predictor["normalized_date"] = month_diff(
        predictor["date"], predictor["t_0"]
    ).astype(int)
    for estimate in fitted_model._coefs + fitted_model._metrics:
        if pd.api.types.is_datetime64_any_dtype(predictor[estimate]):
            predictor[estimate] = predictor[estimate].dt.strftime("%Y-%m")
    predictor["normalized_c"] = predictor["c"].where(
        (predictor["normalized_date"] < 0) | (predictor["c_0"] == 0),
        predictor["c"] / predictor["c_0"],
    )
    predictor["model"] = 1
    predictor["model"] = predictor["model"].where(predictor["normalized_date"] >= 0, 0)
    predictor = filter_data(
        data=predictor,
        care_site_level=care_site_level,
        stay_type=stay_type,
        care_site_id=care_site_id,
        care_site_short_name=care_site_short_name,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )
    if t_min:
        predictor = predictor[predictor.normalized_date >= t_min]
    if t_max:
        predictor = predictor[predictor.normalized_date <= t_max]

    # Get viz config
    probe_config = deepcopy(probe.get_viz_config("normalized_probe_plot"))
    model_config = deepcopy(
        fitted_model.get_viz_config("normalized_probe_plot", predictor=predictor)
    )
    if probe_line_config is None:
        probe_line_config = model_config["probe_line"]
    if model_line_config is None:
        model_line_config = model_config["model_line"]
    if error_line_config is None:
        error_line_config = model_config["error_line"]
    if estimates_selections is None:
        estimates_selections = model_config["estimates_selections"]
    if estimates_filters is None:
        estimates_filters = model_config["estimates_filters"]
    if main_chart_config is None:
        main_chart_config = probe_config["main_chart"]
    if chart_style is None:
        chart_style = probe_config["chart_style"]

    # Viz
    predictor["legend_model"] = (
        model_line_config.get("legend_title")
        if model_line_config.get("legend_title")
        else type(fitted_model).__name__
    )
    predictor["legend_predictor"] = probe_line_config["legend_title"]
    predictor["legend_error_band"] = error_line_config["legend_title"]
    index_selection, index_fields = create_groupby_selection(
        indexes=indexes,
        predictor=predictor,
    )
    base = alt.Chart(predictor)
    base = add_estimates_filters(
        base=base,
        estimates_filters=estimates_filters,
    )
    main_chart = generate_main_chart(
        base=base,
        main_chart_config=main_chart_config,
        index_selection=index_selection,
        index_fields=index_fields,
        x_axis_title=x_axis_title,
        y_axis_title=y_axis_title,
    )
    probe_line = generate_probe_line(
        main_chart=main_chart, probe_line_config=probe_line_config
    )
    error_line = generate_error_line(
        main_chart=main_chart, error_line_config=error_line_config
    )
    model_line = generate_model_line(
        main_chart=main_chart, model_line_config=model_line_config
    )
    main_chart = probe_line + error_line + model_line
    if index_selection:
        main_chart = main_chart.add_params(index_selection)

    for estimate_selection in estimates_selections:
        main_chart = main_chart.add_params(estimate_selection)

    main_chart = configure_style(chart=main_chart, chart_style=chart_style)

    if save_path:
        save_html(
            obj=main_chart,
            filename=save_path,
        )

    return main_chart
