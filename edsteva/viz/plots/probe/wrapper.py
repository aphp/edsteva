from copy import deepcopy
from datetime import datetime
from typing import List

import altair as alt

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.viz.plots.probe.fitted_probe import fitted_probe_line
from edsteva.viz.plots.probe.probe import probe_line
from edsteva.viz.utils import configure_style, filter_predictor, save_html


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
    x_axis_title: str, optional,
        Label name for the x axis.
    x_grid: bool, optional,
        If False, remove the grid for the x axis.
    y_axis_title: str, optional,
        Label name for the y axis.
    y_grid: bool, optional,
        If False, remove the grid for the y axis.
    show_n_events: bool, optional
        If True, compute the sum of the number of visit instead of the mean of the completeness predictor $c(t)$.
    show_per_care_site: bool, optional
        If True, the average completeness predictor $c(t)$ is computed for each care site independently. If False, it is computed over all care sites.
    labelAngle: float, optional
        The rotation angle of the label on the x_axis.
    labelFontSize: float, optional
        The font size of the labels (axis and legend).
    titleFontSize: float, optional
        The font size of the titles.
    """
    alt.data_transformers.enable("default")
    alt.data_transformers.disable_max_rows()

    probe_config = deepcopy(probe.get_viz_config("probe_plot"))
    chart_style = probe_config["chart_style"]
    predictor = probe.predictor.copy()
    indexes = list(set(predictor.columns).difference(["date"] + probe._metrics))

    if fitted_model:
        predictor = fitted_model.predict(probe).copy()
    else:
        predictor = probe.predictor.copy()

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

    indexes = [
        {"field": variable, "title": variable.replace("_", " ").capitalize()}
        for variable in indexes
        if len(predictor[variable].unique()) >= 2
    ]

    if fitted_model:
        model_config = deepcopy(fitted_model.get_viz_config("probe_plot"))
        chart = fitted_probe_line(
            predictor=predictor,
            probe_config=probe_config,
            model_config=model_config,
            indexes=indexes,
            legend_predictor=legend_predictor,
            legend_model=legend_model,
        )
    else:
        chart = probe_line(
            predictor=predictor,
            probe_config=probe_config,
            indexes=indexes,
        )

    if save_path:
        save_html(
            obj=configure_style(chart=chart, chart_style=chart_style),
            filename=save_path,
        )

    return chart
