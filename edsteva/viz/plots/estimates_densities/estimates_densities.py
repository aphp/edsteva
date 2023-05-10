import uuid
from copy import deepcopy
from datetime import datetime
from functools import reduce
from typing import Dict, List, Union

import altair as alt

from edsteva.models import BaseModel
from edsteva.probes import BaseProbe
from edsteva.viz.utils import (
    add_interactive_selection,
    concatenate_charts,
    configure_style,
    filter_predictor,
    generate_horizontal_bar_charts,
    generate_vertical_bar_charts,
    save_html,
)


def estimates_densities_plot(
    probe: BaseProbe,
    fitted_model: BaseModel,
    care_site_level: str = None,
    stay_type: List[str] = None,
    care_site_id: List[int] = None,
    start_date: Union[datetime, str] = None,
    end_date: Union[datetime, str] = None,
    care_site_short_name: List[int] = None,
    save_path: str = None,
    remove_singleton_bar_chart: bool = True,
    vertical_bar_charts_config: Dict[str, str] = None,
    horizontal_bar_charts_config: Dict[str, str] = None,
    chart_style: Dict[str, float] = None,
    y_axis_title: str = None,
    **kwargs,
):
    r"""Displays the density plot with the associated box plot of each estimate and metric computed in the input model. It can help you to set the thresholds.


    Parameters
    ----------
    probe : BaseProbe
        Class describing the completeness predictor $c(t)$
    fitted_model : BaseModel
        Model with estimates of interest
        **EXAMPLE**: StepFunction Model with $(\hat{t_0}, \hat{c_0})$
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
    remove_singleton_bar_chart : bool, optional
        If set to True, remove the bar charts with only one element
        **EXAMPLE**: `True`
    vertical_bar_charts_config: Dict[str, str], optional
        Configuration used to construct the vertical bar charts.
    horizontal_bar_charts_config: Dict[str, str], optional
        Configuration used to construct the horizontal bar charts.
    chart_style: Dict[str, float], optional
        Configuration used to configure the chart style.
        **EXAMPLE**: `{"labelFontSize": 13, "titleFontSize": 14}`
    y_axis_title: str, optional,
        Label name for the y axis.
    """
    alt.data_transformers.disable_max_rows()

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
    estimates = fitted_model.estimates.copy()
    predictor = filter_predictor(
        predictor=predictor,
        care_site_level=care_site_level,
        stay_type=stay_type,
        care_site_id=care_site_id,
        care_site_short_name=care_site_short_name,
        start_date=None,
        end_date=None,
        **kwargs,
    )
    estimates = estimates.merge(
        predictor[probe._index + ["care_site_short_name"]].drop_duplicates(),
        on=probe._index,
    )
    probe_config = deepcopy(probe.get_viz_config("estimates_densities_plot"))
    if not vertical_bar_charts_config:
        vertical_bar_charts_config = probe_config["vertical_bar_charts"]
    if not horizontal_bar_charts_config:
        horizontal_bar_charts_config = probe_config["horizontal_bar_charts"]
    if not chart_style:
        chart_style = probe_config["chart_style"]

    quantitative_estimates = []
    time_estimates = []

    for estimate in fitted_model._coefs + fitted_model._metrics:
        if estimates[estimate].dtype == float or estimates[estimate].dtype == int:
            max_value = estimates[estimate].max()
            min_value = estimates[estimate].min()
            estimates[estimate] = round(estimates[estimate], 3)

            estimate_density = (
                alt.vconcat(
                    (
                        (
                            alt.Chart(estimates)
                            .transform_density(
                                estimate,
                                as_=[estimate, "Density"],
                                extent=[min_value, max_value],
                            )
                            .mark_area()
                            .encode(
                                x=alt.X("{}:Q".format(estimate), title=None),
                                y=alt.Y("Density:Q", title=y_axis_title),
                            )
                        )
                        + alt.Chart(estimates)
                        .mark_rule(color="red")
                        .encode(
                            x="median({}):Q".format(estimate),
                            tooltip=alt.Tooltip("median({}):Q".format(estimate)),
                        )
                    ).properties(width=800, height=300),
                    (
                        alt.Chart(estimates)
                        .mark_tick()
                        .encode(x=alt.X("{}:Q".format(estimate), axis=None))
                    ),
                    spacing=0,
                )
                & (
                    alt.Chart(estimates)
                    .mark_boxplot()
                    .encode(
                        x="{}:Q".format(estimate),
                    )
                )
            ).resolve_scale(x="shared")
            quantitative_estimates.append(estimate_density)

        else:
            estimates[estimate] = estimates[estimate].astype("datetime64[ns]")
            estimate_density = (
                (
                    alt.Chart(estimates)
                    .transform_timeunit(estimate="yearmonth({})".format(estimate))
                    .mark_bar(size=10)
                    .encode(
                        x=alt.X(
                            "{}:T".format(estimate),
                            axis=alt.Axis(
                                tickCount="month",
                                format="%Y, %b",
                                labelAngle=-90,
                            ),
                            title=estimate,
                        ),
                        y=alt.Y(
                            "count({}):Q".format(estimate),
                            axis=alt.Axis(tickMinStep=1),
                            title=y_axis_title,
                        ),
                    )
                )
                + alt.Chart(estimates)
                .mark_rule(color="red")
                .encode(
                    x="median({}):T".format(estimate),
                    tooltip=alt.Tooltip("median({}):T".format(estimate)),
                )
            ).properties(width=800, height=300)
            time_estimates.append(estimate_density)

    estimates_densities = time_estimates + quantitative_estimates
    care_site_level_selection = alt.selection_single(
        fields=["care_site_level"],
        bind=alt.binding_select(
            name="Care site level : ", options=estimates["care_site_level"].unique()
        ),
        init={"care_site_level": estimates["care_site_level"].unique()[0]},
    )
    main_chart = reduce(
        lambda estimate_density_1, estimate_density_2: estimate_density_1
        & estimate_density_2,
        estimates_densities,
    ).add_selection(care_site_level_selection)

    base = alt.Chart(predictor)

    horizontal_bar_charts, y_variables_selections = generate_horizontal_bar_charts(
        base=base,
        horizontal_bar_charts_config=horizontal_bar_charts_config,
        predictor=predictor,
        remove_singleton_bar_chart=remove_singleton_bar_chart,
    )
    vertical_bar_charts, x_variables_selections = generate_vertical_bar_charts(
        base=base,
        vertical_bar_charts_config=vertical_bar_charts_config,
        predictor=predictor,
        remove_singleton_bar_chart=remove_singleton_bar_chart,
    )

    selections = dict(
        y_variables_selections,
        **x_variables_selections,
        **dict(cares_site_level=care_site_level_selection),
    )
    selection_charts = dict(
        horizontal_bar_charts,
        **vertical_bar_charts,
    )
    main_chart = add_interactive_selection(
        base=main_chart, selection_charts=selection_charts, selections=selections
    )

    chart = concatenate_charts(
        main_chart=main_chart,
        horizontal_bar_charts=horizontal_bar_charts,
        vertical_bar_charts=vertical_bar_charts,
        spacing=0,
    )
    chart = configure_style(chart=chart, chart_style=chart_style)

    vis_threshold = "id" + uuid.uuid4().hex
    new_sliders_threshold_id = "id" + uuid.uuid4().hex
    old_sliders_threshold_id = "id" + uuid.uuid4().hex
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
            <div style="width: -webkit-fill-available;">
            <div id={new_sliders_threshold_id}>
              <div id={old_sliders_threshold_id}></div>
            </div>
            </div>
            <div>
            <div id={vis_threshold}></div>
            </div>

          </div>
        </div>

        <script type="text/javascript">
        vegaEmbed('#{vis_threshold}', {chart.to_json(indent=None)}).then(function(result) {{
            const sliders = document.getElementsByClassName('vega-bindings');
            const newestimate = document.getElementById('{new_sliders_threshold_id}');
            const oldestimate = document.getElementById('{old_sliders_threshold_id}');
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

    return html_chart
