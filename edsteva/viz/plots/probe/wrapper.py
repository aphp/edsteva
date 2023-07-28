from copy import deepcopy
from datetime import datetime
from typing import Dict, List

import altair as alt

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.viz.plots.probe.fitted_probe import fitted_probe_line
from edsteva.viz.plots.probe.probe import probe_line
from edsteva.viz.utils import configure_style, filter_data, save_html


def probe_plot(
    probe: BaseProbe,
    fitted_model: BaseModel = None,
    care_site_level: str = None,
    stay_type: List[str] = None,
    care_site_id: List[int] = None,
    start_date: datetime = None,
    end_date: datetime = None,
    care_site_short_name: List[int] = None,
    save_path: str = None,
    legend_predictor: str = "Predictor c(t)",
    legend_model: str = "Model f(t)",
    x_axis_title: str = None,
    y_axis_title: str = None,
    main_chart_config: Dict[str, float] = None,
    model_line_config: Dict[str, str] = None,
    probe_line_config: Dict[str, str] = None,
    chart_style: Dict[str, float] = None,
    indexes_to_remove: List[str] = ["care_site_id"],
    **kwargs,
):
    r"""
    Displays a chart with the average completeness predictor $c(t)$ over time $t$ with the fitted model $\hat{c}(t)$ if specified.
    The chart is exportable in png or svg format and easy to integrate into a report. Is also possible to save the chart in HTML with the "save_path" optional input.

    Parameters
    ----------
    probe : BaseProbe
        Class describing the completeness predictor $c(t)$.
    fitted_model : BaseModel, optional
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
    save_path : str, optional
        Folder path where to save the chart in HTML format.
        **EXAMPLE**: `"my_folder/my_file.html"`
    legend_predictor: str, optional,
        Label name for the predictor legend.
    legend_model: str, optional,
        Label name for the model legend.
    x_axis_title: str, optional,
        Label name for the x axis.
    y_axis_title: str, optional,
        Label name for the y axis.
    main_chart_config: Dict[str, str], optional
        If not None, configuration used to construct the top main chart.
    model_line_config: Dict[str, str], optional
        If not None, configuration used to construct the model line.
    probe_line_config: Dict[str, str], optional
        If not None, configuration used to construct the probe line.
    chart_style: Dict[str, float], optional
        If not None, configuration used to configure the chart style.
        **EXAMPLE**: `{"labelFontSize": 13, "titleFontSize": 14}`
    indexes_to_remove: List[str], optional
        indexes to remove from the groupby selection.
    """
    alt.data_transformers.enable("default")
    alt.data_transformers.disable_max_rows()

    probe_config = deepcopy(probe.get_viz_config("probe_plot"))
    if main_chart_config is None:
        main_chart_config = probe_config["main_chart"]
    if chart_style is None:
        chart_style = probe_config["chart_style"]
    predictor = probe.predictor.copy()
    cols_to_remove = ["date", *probe._metrics]
    if indexes_to_remove:
        cols_to_remove.extend(indexes_to_remove)
    indexes = list(set(predictor.columns).difference(cols_to_remove))

    if fitted_model:
        predictor = fitted_model.predict(probe).copy()
    else:
        predictor = probe.predictor.copy()

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

    indexes = [
        {"field": variable, "title": variable.replace("_", " ").capitalize()}
        for variable in indexes
        if variable in predictor.columns and len(predictor[variable].unique()) >= 2
    ]

    if fitted_model:
        model_config = deepcopy(fitted_model.get_viz_config("probe_plot"))
        if model_line_config is None:
            model_line_config = model_config["model_line"]
        if probe_line_config is None:
            probe_line_config = model_config["probe_line"]
        chart = fitted_probe_line(
            predictor=predictor,
            indexes=indexes,
            legend_predictor=legend_predictor,
            legend_model=legend_model,
            x_axis_title=x_axis_title,
            y_axis_title=y_axis_title,
            main_chart_config=main_chart_config,
            model_line_config=model_line_config,
            probe_line_config=probe_line_config,
        )
    else:
        chart = probe_line(
            predictor=predictor,
            indexes=indexes,
            x_axis_title=x_axis_title,
            y_axis_title=y_axis_title,
            main_chart_config=main_chart_config,
        )

    if save_path:
        save_html(
            obj=configure_style(chart=chart, chart_style=chart_style),
            filename=save_path,
        )

    return chart
