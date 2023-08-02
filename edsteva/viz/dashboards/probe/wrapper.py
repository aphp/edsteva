import uuid
from copy import deepcopy
from typing import Dict

import altair as alt
from IPython.display import HTML, display

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.viz.dashboards.probe.fitted_probe import fitted_probe_dashboard
from edsteva.viz.dashboards.probe.probe import probe_only_dashboard
from edsteva.viz.utils import filter_data, save_html


def probe_dashboard(
    probe: BaseProbe,
    fitted_model: BaseModel = None,
    care_site_level: str = None,
    save_path: str = None,
    legend_predictor: str = "Predictor c(t)",
    legend_model: str = "Model f(t)",
    x_axis_title: str = None,
    y_axis_title: str = None,
    main_chart_config: Dict[str, float] = None,
    model_line_config: Dict[str, str] = None,
    probe_line_config: Dict[str, str] = None,
    vertical_bar_charts_config: Dict[str, str] = None,
    horizontal_bar_charts_config: Dict[str, str] = None,
    time_line_config: Dict[str, str] = None,
    chart_style: Dict[str, float] = None,
    **kwargs,
):
    r"""Displays an interactive chart with:

    - On the top, the aggregated average completeness predictor $c(t)$ over time $t$ with the fitted model $\hat{c}(t)$ if specified.
    - On the bottom, interactive filters including all the concepts in the [Probe][probe] (such as time, care site, number of visits...etc.)

    Is is possible to save the chart in HTML with the "save_path" optional input.

    Parameters
    ----------
    probe : BaseProbe
        Class describing the completeness predictor $c(t)$.
    fitted_model : BaseModel, optional
        Model fitted to the probe.
    care_site_level : str, optional
        **EXAMPLE**: `"Hospital"`, `"Hôpital"` or `"UF"`
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

    alt.data_transformers.enable("default")
    alt.data_transformers.disable_max_rows()

    probe_config = deepcopy(probe.get_viz_config("probe_dashboard"))
    if fitted_model:
        model_config = deepcopy(fitted_model.get_viz_config("probe_dashboard"))
        if model_line_config is None:
            model_line_config = model_config["model_line"]
        if probe_line_config is None:
            probe_line_config = model_config["probe_line"]
    if main_chart_config is None:
        main_chart_config = probe_config["main_chart"]
    if time_line_config is None:
        time_line_config = probe_config["time_line"]
    if vertical_bar_charts_config is None:
        vertical_bar_charts_config = probe_config["vertical_bar_charts"]
        if fitted_model:
            vertical_bar_charts_config["y"] = (
                vertical_bar_charts_config["y"]
                + model_config["extra_vertical_bar_charts"]
            )
    if horizontal_bar_charts_config is None:
        horizontal_bar_charts_config = probe_config["horizontal_bar_charts"]
        if fitted_model:
            horizontal_bar_charts_config["x"] = (
                horizontal_bar_charts_config["x"]
                + model_config["extra_horizontal_bar_charts"]
            )
    if chart_style is None:
        chart_style = probe_config["chart_style"]

    predictor = fitted_model.predict(probe) if fitted_model else probe.predictor.copy()
    predictor = filter_data(
        data=predictor,
        care_site_level=care_site_level,
        **kwargs,
    )

    if fitted_model:
        chart = fitted_probe_dashboard(
            predictor=predictor,
            legend_predictor=legend_predictor,
            legend_model=legend_model,
            x_axis_title=x_axis_title,
            y_axis_title=y_axis_title,
            main_chart_config=main_chart_config,
            model_line_config=model_line_config,
            probe_line_config=probe_line_config,
            vertical_bar_charts_config=vertical_bar_charts_config,
            horizontal_bar_charts_config=horizontal_bar_charts_config,
            time_line_config=time_line_config,
            chart_style=chart_style,
        )
    else:
        chart = probe_only_dashboard(
            predictor=predictor,
            x_axis_title=x_axis_title,
            y_axis_title=y_axis_title,
            main_chart_config=main_chart_config,
            vertical_bar_charts_config=vertical_bar_charts_config,
            horizontal_bar_charts_config=horizontal_bar_charts_config,
            time_line_config=time_line_config,
            chart_style=chart_style,
        )

    vis_probe = "id" + uuid.uuid4().hex
    new_index_probe_id = "id" + uuid.uuid4().hex
    old_index_probe_id = "id" + uuid.uuid4().hex
    left_shift = "145px" if fitted_model else "45px"
    html_chart = f"""
        <!DOCTYPE html>
        <html>
        <head>
          <script src="https://cdn.jsdelivr.net/npm/vega@{alt.VEGA_VERSION}"></script>
          <script src="https://cdn.jsdelivr.net/npm/vega-lite@{alt.VEGALITE_VERSION}"></script>
          <script src="https://cdn.jsdelivr.net/npm/vega-embed@{alt.VEGAEMBED_VERSION}"></script>
        </head>
        <body>

        <div class="container">
          <div class="row">
            <div>
            <div id={vis_probe}></div>
            </div>
            <div style="position:absolute;left:{left_shift};top:380px;width: -webkit-fill-available;">
            <div id={new_index_probe_id}>
              <div id={old_index_probe_id}></div>
            </div>
            <hr/>
            <h1 style="text-align:center"> Interactive filters </h1>
            </div>
          </div>
        </div>

        <script type="text/javascript">
        vegaEmbed('#{vis_probe}', {chart.to_json(indent=None)}).then(function(result) {{
            const sliders = document.getElementsByClassName('vega-bindings');
            const newparent = document.getElementById('{new_index_probe_id}');
            const oldchild = document.getElementById('{old_index_probe_id}');
            for (var i = 0; i < sliders.length; i++) {{
                if (sliders[i].parentElement.parentElement.id == '{vis_probe}') {{
                    var index_slider = sliders[i]
                    }}
                }}
            newparent.replaceChild(index_slider, oldchild);
            }}).catch(console.error);
        </script>
        </body>
        </html>
        """
    if save_path:
        save_html(
            obj=html_chart,
            filename=save_path,
        )
    else:
        display(HTML(html_chart))
