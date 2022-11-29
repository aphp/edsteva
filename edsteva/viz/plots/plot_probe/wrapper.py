from datetime import datetime
from typing import List

import altair as alt

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.viz.plots.plot_probe.fitted_probe import fitted_probe_line
from edsteva.viz.plots.plot_probe.probe import probe_line
from edsteva.viz.utils import filter_predictor, save_html


def plot_probe(
    probe: BaseProbe,
    fitted_model: BaseModel = None,
    care_site_level: str = None,
    stay_type: List[str] = None,
    care_site_id: List[int] = None,
    start_date: datetime = None,
    end_date: datetime = None,
    care_site_short_name: List[int] = None,
    save_path: str = None,
    x_axis_title: str = "Time (Month Year)",
    x_grid: bool = True,
    y_axis_title: str = "Completeness predictor c(t)",
    y_grid: bool = True,
    show_n_visit: bool = False,
    show_per_care_site: bool = True,
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
    show_n_visit: bool, optional
        If True, compute the sum of the number of visit instead of the mean of the completeness predictor $c(t)$.
    show_per_care_site: bool, optional
        If True, the average completeness predictor $c(t)$ is computed for each care site independently. If False, it is computed over all care sites.
    """

    index = list(set(probe._index).difference(["care_site_level", "care_site_id"]))
    if show_per_care_site:
        index = index + ["care_site_short_name"]

    if fitted_model:
        predictor = fitted_model.predict(probe)
    else:
        predictor = probe.predictor

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

    index = [variable for variable in index if len(predictor[variable].unique()) >= 2]

    alt.data_transformers.disable_max_rows()

    if fitted_model:
        chart = fitted_probe_line(
            predictor=predictor,
            index=index,
            x_axis_title=x_axis_title,
            y_axis_title=y_axis_title,
            x_grid=x_grid,
            y_grid=y_grid,
        )
    else:
        chart = probe_line(
            predictor=predictor,
            index=index,
            x_axis_title=x_axis_title,
            y_axis_title=y_axis_title,
            x_grid=x_grid,
            y_grid=y_grid,
            show_n_visit=show_n_visit,
        )

    if save_path:
        save_html(
            obj=chart.configure_axis(
                labelFontSize=11, titleFontSize=12
            ).configure_legend(labelFontSize=11),
            filename=save_path,
        )

    return chart
