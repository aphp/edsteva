import uuid

import altair as alt
from IPython.display import HTML, display

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.viz.dashboards.predictor_dashboard.fitted_probe import (
    fitted_probe_dashboard,
)
from edsteva.viz.dashboards.predictor_dashboard.probe import probe_dashboard
from edsteva.viz.utils import filter_predictor, save_html


def predictor_dashboard(
    probe: BaseProbe,
    fitted_model: BaseModel = None,
    care_site_level: str = None,
    save_path: str = None,
    x_axis_title: str = "Time (Month Year)",
    x_grid: bool = True,
    y_axis_title: str = "Completeness predictor c(t)",
    y_grid: bool = True,
    show_n_visit: bool = False,
):
    r"""Displays an interactive chart with:

    - On the top, the aggregated average completeness predictor $c(t)$ over time $t$ with the fitted model $\hat{c}(t)$ if specified.
    - On the bottom, interactive filters including all the concepts in the [Probe][probe] (such as time, care site, number of visits...etc.)

    Is is possible to save the chart in HTML with the "save_path" optional input.

    Parameters
    ----------
    probe : BaseProbe
        Class describing the completeness predictor $c(t)$
    fitted_model : BaseModel, optional
        Model fitted to the probe
    care_site_level : str, optional
        **EXAMPLE**: `"Hospital"`, `"Hôpital"` or `"UF"`
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
        show the number of visit instead of the completeness predictor $c(t)$
    """

    index = list(set(probe._index).difference(["care_site_level", "care_site_id"]))

    if fitted_model:
        predictor = fitted_model.predict(probe)
    else:
        predictor = probe.predictor

    predictor = filter_predictor(
        predictor=predictor,
        care_site_level=care_site_level,
    )

    if fitted_model:
        chart = fitted_probe_dashboard(
            predictor=predictor,
            index=index,
            x_axis_title=x_axis_title,
            y_axis_title=y_axis_title,
            x_grid=x_grid,
            y_grid=y_grid,
        )
    else:
        chart = probe_dashboard(
            predictor=predictor,
            index=index,
            x_axis_title=x_axis_title,
            y_axis_title=y_axis_title,
            x_grid=x_grid,
            y_grid=y_grid,
            show_n_visit=show_n_visit,
        )

    chart = chart.configure_axis(labelFontSize=11, titleFontSize=12).configure_legend(
        labelFontSize=11
    )
    vis_probe = "id" + uuid.uuid4().hex
    new_index_probe_id = "id" + uuid.uuid4().hex
    old_index_probe_id = "id" + uuid.uuid4().hex

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
            <div style="position:absolute;left:45px;top:380px;width: -webkit-fill-available;">
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
    display(HTML(html_chart))
    if save_path:
        save_html(
            obj=html_chart,
            filename=save_path,
        )
