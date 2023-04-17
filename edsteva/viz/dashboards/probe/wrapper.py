import uuid
from copy import deepcopy

import altair as alt
from IPython.display import HTML, display

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.viz.dashboards.probe.fitted_probe import fitted_probe_dashboard
from edsteva.viz.dashboards.probe.probe import probe_only_dashboard
from edsteva.viz.utils import filter_predictor, save_html


def probe_dashboard(
    probe: BaseProbe,
    fitted_model: BaseModel = None,
    care_site_level: str = None,
    save_path: str = None,
    legend_predictor: str = "Predictor c(t)",
    legend_model: str = "Model f(t)",
    remove_singleton_bar_chart: bool = True,
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
    labelFontSize: float, optional
        The font size of the labels (axis and legend).
    titleFontSize: float, optional
        The font size of the titles.
    """

    alt.data_transformers.enable("default")
    alt.data_transformers.disable_max_rows()

    probe_config = deepcopy(probe.get_viz_config("probe_dashboard"))
    if fitted_model:
        predictor = fitted_model.predict(probe)
    else:
        predictor = probe.predictor.copy()
    predictor = filter_predictor(
        predictor=predictor,
        care_site_level=care_site_level,
        **kwargs,
    )

    if fitted_model:
        model_config = deepcopy(fitted_model.get_viz_config("probe_dashboard"))
        chart = fitted_probe_dashboard(
            predictor=predictor,
            probe_config=probe_config,
            model_config=model_config,
            legend_predictor=legend_predictor,
            legend_model=legend_model,
            remove_singleton_bar_chart=remove_singleton_bar_chart,
        )
    else:
        chart = probe_only_dashboard(
            predictor=predictor,
            probe_config=probe_config,
            remove_singleton_bar_chart=remove_singleton_bar_chart,
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