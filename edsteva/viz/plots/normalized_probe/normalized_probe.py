from copy import deepcopy
from datetime import datetime
from typing import List, Union

import altair as alt
import pandas as pd

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.viz.utils import (
    add_estimates_filters,
    create_groupby_selection,
    filter_predictor,
    generate_error_line,
    generate_main_chart,
    generate_model_line,
    generate_probe_line,
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
    labelFontSize: float = 12,
    titleFontSize: float = 13,
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
    t_0_max : datetime, optional
        Initial value for the $t_0$ threshold. If None, it will be set as the maximum possible $t_0$ value.

        **EXAMPLE**: `"2022-01"`, `datetime(2020, 2, 1)`
    error_max : float, optional
        Initial value for the $error$ threshold. If None, it will be set as the maximum possible error value.

        **EXAMPLE**: `0.02`, `0.25`
    c_0_min : float, optional
        Initial value for the $c_0$ threshold. If None, it will be set as 0.

        **EXAMPLE**: `0.1`, `0.8`
    stay_type : List[str], optional
        **EXAMPLE**: `"All"` or `["All", "Urg"]`
    care_site_id : List[int], optional
        **EXAMPLE**: `[8312056386, 8312027648]`
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
    save_path : str, optional
        Folder path where to save the chart in HTML format.

        **EXAMPLE**: `"my_folder/my_file.html"`
    x_axis_title: str, optional,
        Label name for the x axis
    y_axis_title: str, optional,
        Label name for the y axis
    show_per_care_site: bool, optional
        If True, the average completeness predictor $c(t)$ is computed for each care site independently. If False, it is computed over all care sites.
    labelFontSize: float, optional
        The font size of the labels (axis and legend).
    titleFontSize: float, optional
        The font size of the titles.
    """

    predictor = probe.predictor.copy()
    estimates = fitted_model.estimates.copy()

    indexes = list(set(predictor.columns).difference(["date"] + probe._metrics))
    predictor = predictor.merge(estimates, on=probe._index)

    probe_config = deepcopy(probe.get_viz_config("normalized_probe_plot"))
    main_chart_config = probe_config["main_chart"]
    error_line_config = probe_config["error_line"]

    predictor["normalized_date"] = month_diff(
        predictor["date"], predictor["t_0"]
    ).astype(int)
    predictor["legend_predictor"] = "Mean"
    predictor["legend_error_band"] = "Standard deviation"
    predictor["legend_model"] = type(fitted_model).__name__

    predictor["model"] = 1
    predictor["model"] = predictor["model"].where(predictor["normalized_date"] >= 0, 0)

    predictor = filter_predictor(
        predictor=predictor,
        care_site_level=care_site_level,
        stay_type=stay_type,
        care_site_id=care_site_id,
        care_site_short_name=care_site_short_name,
        start_date=start_date,
        end_date=end_date,
        **kwargs,
    )
    for estimate in fitted_model._coefs + fitted_model._metrics:
        if pd.api.types.is_datetime64_any_dtype(predictor[estimate]):
            predictor[estimate] = predictor[estimate].dt.strftime("%Y-%m")
    model_config = deepcopy(
        deepcopy(
            fitted_model.get_viz_config(
                "normalized_probe_dashboard", predictor=predictor
            )
        )
    )
    probe_line_config = model_config["probe_line"]
    model_line_config = model_config["model_line"]
    estimates_selections = model_config["estimates_selections"]
    estimates_filters = model_config["estimates_filters"]

    if t_min:
        predictor = predictor[predictor.normalized_date >= t_min]
    if t_max:
        predictor = predictor[predictor.normalized_date <= t_max]

    indexes = [
        {"field": variable, "title": variable.replace("_", " ").capitalize()}
        for variable in indexes
        if len(predictor[variable].unique()) >= 2
    ]

    index_selection, index_fields = create_groupby_selection(
        indexes=indexes,
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
        main_chart = main_chart.add_selection(index_selection)

    for estimate_selection in estimates_selections:
        main_chart = main_chart.add_selection(estimate_selection)

    if save_path:
        save_html(
            obj=main_chart.configure_axis(
                labelFontSize=labelFontSize, titleFontSize=titleFontSize
            ).configure_legend(labelFontSize=labelFontSize),
            filename=save_path,
        )

    return main_chart
