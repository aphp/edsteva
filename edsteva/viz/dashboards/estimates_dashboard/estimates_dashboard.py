import uuid
from copy import deepcopy

import altair as alt
import pandas as pd
from IPython.display import HTML, display

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.probes.utils import CARE_SITE_LEVEL_NAMES
from edsteva.viz.utils import (
    add_estimates_filters,
    add_interactive_selection,
    concatenate_charts,
    configure_style,
    create_groupby_selection,
    filter_predictor,
    generate_error_line,
    generate_horizontal_bar_charts,
    generate_main_chart,
    generate_model_line,
    generate_probe_line,
    generate_time_line,
    generate_vertical_bar_charts,
    month_diff,
    save_html,
)


def estimates_dashboard(
    probe: BaseProbe,
    fitted_model: BaseModel,
    care_site_level: str = CARE_SITE_LEVEL_NAMES["Hospital"],
    save_path: str = None,
    **kwargs,
):
    r"""Displays an interactive chart with:

    - On the top, the aggregated normalized completeness predictor $\frac{c(\Delta t)}{c_0}$ over normalized time $\Delta t = t - t_0$. It represents the overall deviation from the Model.
    - On the bottom, interactive filters including all the columns in the [Probe][probe] (such as time, care site, number of visits...etc.) and all the estimates (coefficients and metrics) in the [Model][model].

    Is is possible to save the chart in HTML with the "save_path" optional input.

    Parameters
    ----------
    probe : BaseProbe
        Class describing the completeness predictor $c(t)$
    fitted_model : BaseModel
        Model fitted to the probe
    care_site_level : str, optional
        **EXAMPLE**: `"Hospital"`, `"Hôpital"` or `"UF"`
    save_path : str, optional
        Folder path where to save the chart in HTML format.

        **EXAMPLE**: `"my_folder/my_file.html"`
    labelFontSize: float, optional
        The font size of the labels (axis and legend).
    titleFontSize: float, optional
        The font size of the titles.
    """
    alt.data_transformers.disable_max_rows()

    predictor = probe.predictor.copy()
    estimates = fitted_model.estimates.copy()
    predictor = predictor.merge(estimates, on=probe._index)

    predictor["normalized_date"] = month_diff(
        predictor["date"], predictor["t_0"]
    ).astype(int)
    predictor["normalized_c_0"] = predictor["c_0"].mask(
        (predictor["normalized_date"] < 0) | (predictor["c_0"] == 0), 1
    )

    predictor["model"] = 1
    predictor["model"] = predictor["model"].where(predictor["normalized_date"] >= 0, 0)

    predictor.t_0 = predictor.t_0.dt.strftime("%Y-%m")
    probe_config = deepcopy(probe.get_estimates_dashboard_config())
    main_chart_config = probe_config["main_chart"]
    time_line_config = probe_config["time_line"]
    error_line_config = probe_config["error_line"]
    vertical_bar_charts_config = probe_config["vertical_bar_charts"]
    horizontal_bar_charts_config = probe_config["horizontal_bar_charts"]
    chart_style = probe_config["chart_style"]

    predictor["legend_predictor"] = main_chart_config["legend_title"]
    predictor["legend_error_band"] = error_line_config["legend_title"]
    predictor["legend_model"] = type(fitted_model).__name__

    predictor = filter_predictor(
        predictor=predictor, care_site_level=care_site_level, **kwargs
    )
    predictor = probe.add_names_columns(predictor)
    for estimate in fitted_model._coefs + fitted_model._metrics:
        if pd.api.types.is_datetime64_any_dtype(predictor[estimate]):
            predictor[estimate] = predictor[estimate].dt.strftime("%Y-%m")
    model_config = deepcopy(
        fitted_model.get_estimates_dashboard_config(predictor=predictor)
    )
    probe_line_config = model_config["probe_line"]
    model_line_config = model_config["model_line"]
    estimates_selections = model_config["estimates_selections"]
    estimates_filters = model_config["estimates_filters"]
    horizontal_bar_charts_config["x"] = (
        horizontal_bar_charts_config["x"] + model_config["extra_horizontal_bar_charts"]
    )
    vertical_bar_charts_config["y"] = (
        vertical_bar_charts_config["y"] + model_config["extra_vertical_bar_charts"]
    )

    base = alt.Chart(predictor)
    time_line, time_selection = generate_time_line(
        base=base,
        time_line_config=time_line_config,
    )

    horizontal_bar_charts, y_variables_selections = generate_horizontal_bar_charts(
        base=base,
        horizontal_bar_charts_config=horizontal_bar_charts_config,
    )
    vertical_bar_charts, x_variables_selections = generate_vertical_bar_charts(
        base=base,
        vertical_bar_charts_config=vertical_bar_charts_config,
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
        base=base,
        selection_charts=selection_charts,
        selections=selections,
    )
    base = add_estimates_filters(
        base=base,
        selection_charts=selection_charts,
        estimates_filters=estimates_filters,
    )
    index_selection, index_fields = create_groupby_selection(
        indexes=vertical_bar_charts_config["x"] + horizontal_bar_charts_config["y"],
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
    chart = concatenate_charts(
        main_chart=main_chart,
        time_line=time_line,
        horizontal_bar_charts=horizontal_bar_charts,
        vertical_bar_charts=vertical_bar_charts,
        spacing=0,
    )
    chart = configure_style(chart=chart, chart_style=chart_style)
    for estimate_selection in estimates_selections:
        chart = chart.add_selection(estimate_selection)

    vis_threshold = "id" + uuid.uuid4().hex
    new_sliders_threshold_id = "id" + uuid.uuid4().hex
    old_sliders_threshold_id = "id" + uuid.uuid4().hex
    new_index_threshold_id = "id" + uuid.uuid4().hex
    old_index_threshold_id = "id" + uuid.uuid4().hex
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
            <div id={vis_threshold}></div>
            </div>
            <div style="position:absolute;left:1000px;top:540px;width: -webkit-fill-available;">
            <div id={new_sliders_threshold_id}>
              <div id={old_sliders_threshold_id}></div>
            </div>
            </div>
            <div style="position:absolute;left:45px;top:410px;width: -webkit-fill-available;">
            <div id={new_index_threshold_id}>
              <div id={old_index_threshold_id}></div>
            </div>
            <hr/>
            <h1 style="text-align:center"> Interactive filters </h1>
            </div>
          </div>
        </div>

        <script type="text/javascript">
        vegaEmbed('#{vis_threshold}', {chart.to_json(indent=None)}).then(function(result) {{
            const sliders = document.getElementsByClassName('vega-bindings');
            const newestimate = document.getElementById('{new_sliders_threshold_id}');
            const oldestimate = document.getElementById('{old_sliders_threshold_id}');
            const newparent = document.getElementById('{new_index_threshold_id}');
            const oldchild = document.getElementById('{old_index_threshold_id}');
            for (var i = 0; i < sliders.length; i++) {{
                if (sliders[i].parentElement.parentElement.id == '{vis_threshold}') {{
                    var estimate_slider = sliders[i]
                    var index_slider = estimate_slider.querySelectorAll(".vega-bind")
                    }}
                }}
            newestimate.replaceChild(estimate_slider, oldestimate);
            for (var i = 0; i < index_slider.length; i++) {{
                if (index_slider[i].firstChild.innerHTML == "Group by: ") {{
                    var index_color = index_slider[i]}}
                }}
            newparent.replaceChild(index_color, oldchild);
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
