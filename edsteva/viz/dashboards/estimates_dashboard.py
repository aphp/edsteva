import uuid
from functools import reduce

import altair as alt
from IPython.display import HTML, display

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.probes.utils import CARE_SITE_LEVEL_NAMES
from edsteva.viz.utils import filter_predictor, round_it, save_html, scale_it


def estimates_dashboard(
    probe: BaseProbe,
    fitted_model: BaseModel,
    care_site_level: str = CARE_SITE_LEVEL_NAMES["Hospital"],
    save_path: str = None,
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
    """
    alt.data_transformers.disable_max_rows()

    predictor = probe.predictor.copy()
    estimates = fitted_model.estimates.copy()
    predictor = predictor.merge(estimates, on=probe._index)

    def month_diff(x, y):
        end = x.dt.to_period("M").view(dtype="int64")
        start = y.dt.to_period("M").view(dtype="int64")
        return end - start

    predictor["date"] = predictor["date"].astype("datetime64[ns]")
    predictor["t_0"] = predictor["t_0"].astype("datetime64[ns]")
    predictor["normalized_date"] = month_diff(predictor["date"], predictor["t_0"])
    predictor["t_0"] = predictor["t_0"].astype(str)
    predictor["normalized_date"] = predictor["normalized_date"].astype(int)

    predictor["normalized_c"] = predictor["c"].mask(
        (predictor["normalized_date"] >= 0) & (predictor["c_0"] == 0), 1
    )
    predictor["normalized_c"] = predictor["normalized_c"].mask(
        (predictor["normalized_date"] >= 0) & (predictor["c_0"] > 0),
        predictor["c"] / predictor["c_0"],
    )

    predictor["legend_error_band"] = "Standard deviation"
    predictor["legend_model"] = type(fitted_model).__name__

    predictor["model"] = predictor["c_0"].where(predictor["normalized_date"] < 0, 1)
    predictor["model"] = predictor["model"].where(predictor["normalized_date"] >= 0, 0)

    predictor = filter_predictor(
        predictor=predictor,
        care_site_level=care_site_level,
    )

    # RectangleModel
    if fitted_model.name == "RectangleFunction":
        predictor = predictor[predictor.date < predictor.t_1]

    index = list(set(probe._index).difference(["care_site_level", "care_site_id"]))
    time = "date"
    _predictor = "c"

    c_0_min_slider = alt.binding_range(
        min=0,
        max=round_it(predictor.c_0.max(), 2),
        step=scale_it(predictor.c_0.max()) / 100,
        name="c₀ min: ",
    )
    c_0_min_selection = alt.selection_single(
        name="c_0_min",
        fields=["c_0_min"],
        bind=c_0_min_slider,
        init={"c_0_min": 0},
    )
    t_0_slider = alt.binding(
        input="t_0",
        name="t₀ max: ",
    )
    t_0_selection = alt.selection_single(
        name="t_0",
        fields=["t_0"],
        bind=t_0_slider,
        init={"t_0": predictor.t_0.astype(str).max()},
    )
    if fitted_model.name == "RectangleFunction":
        t_1_slider = alt.binding(
            input="t_1",
            name="t₁ min: ",
        )
        t_1_selection = alt.selection_single(
            name="t_1",
            fields=["t_1"],
            bind=t_1_slider,
            init={"t_1": predictor.t_1.astype(str).min()},
        )

    error_max_slider = alt.binding_range(
        min=0,
        max=round_it(predictor.error.max(), 2),
        step=scale_it(predictor.error.max()) / 100,
        name="error max: ",
    )
    error_max_selection = alt.selection_single(
        name="error_max",
        fields=["error_max"],
        bind=error_max_slider,
        init={"error_max": round_it(predictor.error.max(), 2)},
    )

    care_site_selection = alt.selection_multi(fields=["care_site_short_name"])
    care_site_color = alt.condition(
        care_site_selection,
        alt.Color(
            "care_site_short_name:N",
            legend=None,
            sort={"field": "c_0", "op": "min", "order": "descending"},
        ),
        alt.value("lightgray"),
    )

    base_chart = alt.Chart(predictor).transform_joinaggregate(
        mean_c_0="mean(c_0)",
        mean_error="mean(error)",
        groupby=["care_site_short_name"] + index,
    )

    time_selection = alt.selection_interval(encodings=["x"])
    time_line = (
        base_chart.mark_line()
        .encode(
            x=alt.X(
                "normalized_date:Q",
                title="Δt = (t - t₀) months",
                scale=alt.Scale(nice=False),
            ),
            y=alt.Y(
                "mean(c):Q",
                title="c(Δt)",
            ),
        )
        .add_selection(time_selection)
    ).properties(width=800, height=50)

    predictor_hist = (
        base_chart.mark_bar()
        .encode(
            y=alt.Y(
                "care_site_short_name:N",
                title="Care site short name",
                sort="-x",
            ),
            color=care_site_color,
        )
        .add_selection(care_site_selection)
        .transform_filter(alt.datum.mean_c_0 >= c_0_min_selection.c_0_min)
        .transform_filter(alt.datum.mean_error <= error_max_selection.error_max)
        .transform_filter(alt.datum.t_0 <= t_0_selection.t_0)
    ).properties(width=300)

    # RectangleModel
    if fitted_model.name == "RectangleFunction":
        predictor_hist = predictor_hist.transform_filter(
            alt.datum.t_1 >= t_1_selection.t_1
        )

    c_0_hist = predictor_hist.encode(
        x=alt.X(
            "min(mean_c_0):Q",
            title="Min(c₀)",
        ),
        tooltip=alt.Tooltip("min(mean_c_0):Q", format=".2"),
    )
    error_hist = predictor_hist.encode(
        x=alt.X(
            "max(mean_error):Q",
            title="Max(error)",
        ),
        tooltip=alt.Tooltip("max(mean_error):Q", format=".2"),
    )

    index_variables_hists = []
    index_variables_selections = []
    for index_variable in index:
        index_variable_selection = alt.selection_multi(fields=[index_variable])
        index_variables_selections.append(index_variable_selection)

        index_variable_color = alt.condition(
            index_variable_selection,
            alt.Color(
                "{}:N".format(index_variable),
                legend=None,
                sort={"field": "c_0", "op": "min", "order": "descending"},
            ),
            alt.value("lightgray"),
        )
        index_variable_hist = (
            base_chart.mark_bar()
            .encode(
                y=alt.Y(
                    "mean(c):Q",
                    title="c",
                ),
                x=alt.X(
                    "{}:N".format(index_variable),
                    title=index_variable,
                    sort="-y",
                ),
                tooltip=alt.Tooltip("mean(c):Q", format=".2"),
                color=index_variable_color,
            )
            .add_selection(index_variable_selection)
            .transform_filter(care_site_selection)
            .transform_filter(alt.datum.mean_c_0 >= c_0_min_selection.c_0_min)
            .transform_filter(alt.datum.t_0 <= t_0_selection.t_0)
            .transform_filter(alt.datum.mean_error <= error_max_selection.error_max)
        ).properties(title="Average completeness per {}".format(index_variable))

        # RectangleModel
        if fitted_model.name == "RectangleFunction":
            index_variable_hist = index_variable_hist.transform_filter(
                alt.datum.t_1 >= t_1_selection.t_1
            )

        index_variables_hists.append(index_variable_hist)

    extra_predictors = predictor.columns.difference(
        index
        + [time]
        + [_predictor]
        + [
            "care_site_short_name",
            "care_site_id",
            "model",
            "legend_error_band",
            "legend_model",
            "normalized_date",
            "normalized_c",
            "c_0",
            "t_0",
            "t_1",
            "error",
        ]
    )
    extra_predictors_hists = []

    for extra_predictor in extra_predictors:

        extra_predictor_hist = predictor_hist.encode(
            x=alt.X(
                "sum({}):Q".format(extra_predictor),
                title="{}".format(extra_predictor),
                axis=alt.Axis(format="s"),
            ),
            tooltip=alt.Tooltip("sum({}):Q".format(extra_predictor), format=","),
        ).properties(title="Total {} per care site".format(extra_predictor))
        extra_predictors_hists.append(extra_predictor_hist)

    index.append("care_site_short_name")
    index_selection = alt.selection_single(
        fields=["index"],
        bind=alt.binding_radio(name="Plot average completeness per: ", options=index),
        init={"index": "stay_type"},
    )

    probe_line = (
        base_chart.transform_fold(index, as_=["index", "value"])
        .encode(
            x=alt.X(
                "normalized_date:Q",
                title="Δt = (t - t₀) months",
                scale=alt.Scale(nice=False),
            ),
            color=alt.Color(
                "value:N",
                sort={"field": "c_0", "op": "min", "order": "descending"},
                title=None,
            ),
        )
        .transform_filter(alt.datum.mean_c_0 >= c_0_min_selection.c_0_min)
        .transform_filter(alt.datum.mean_error <= error_max_selection.error_max)
        .transform_filter(alt.datum.t_0 <= t_0_selection.t_0)
        .transform_filter(care_site_selection)
        .transform_filter(index_selection)
    ).properties(width=900, height=300)
    # RectangleModel
    if fitted_model.name == "RectangleFunction":
        probe_line = probe_line.transform_filter(alt.datum.t_1 >= t_1_selection.t_1)

    mean_line = probe_line.mark_line().encode(
        y=alt.Y(
            "mean(normalized_c):Q",
        )
    )
    error_line = probe_line.mark_errorband(extent="stdev").encode(
        y=alt.Y(
            "normalized_c:Q",
            title="c(Δt) / c₀",
        ),
        stroke=alt.Stroke(
            "legend_error_band",
            title="Error band",
            legend=alt.Legend(symbolType="square", orient="top"),
        ),
    )
    model_line = (
        alt.Chart(predictor)
        .mark_line(color="black", interpolate="step-after", strokeDash=[5, 5])
        .encode(
            x=alt.X(
                "normalized_date:Q",
                scale=alt.Scale(nice=False),
            ),
            y="model:Q",
            strokeWidth=alt.StrokeWidth(
                "legend_model",
                title="Model line",
                legend=alt.Legend(orient="top", symbolDash=[2, 2]),
            ),
        )
    )

    top_chart = mean_line + error_line + model_line

    top_chart = top_chart.add_selection(index_selection).transform_filter(
        time_selection
    )

    for index_variable_selection in index_variables_selections:
        top_chart = top_chart.transform_filter(index_variable_selection)
        c_0_hist = c_0_hist.transform_filter(index_variable_selection)
        error_hist = error_hist.transform_filter(index_variable_selection)
        for idx in range(len(extra_predictors_hists)):
            extra_predictors_hists[idx] = extra_predictors_hists[idx].transform_filter(
                index_variable_selection
            )
        for idx in range(len(index_variables_hists)):
            if idx != index_variables_selections.index(index_variable_selection):
                index_variables_hists[idx] = index_variables_hists[
                    idx
                ].transform_filter(index_variable_selection)

    index_variables_hists = reduce(
        lambda index_variable_hist_1, index_variable_hist_2: index_variable_hist_1
        & index_variable_hist_2,
        index_variables_hists,
    )
    extra_predictors_hists = reduce(
        lambda extra_predictor_hist_1, extra_predictor_hist_2: extra_predictor_hist_1
        & extra_predictor_hist_2,
        extra_predictors_hists,
    )

    chart = (
        (
            (
                alt.vconcat(
                    alt.vconcat(top_chart, time_line, spacing=130),
                    (
                        index_variables_hists
                        | c_0_hist
                        | error_hist & extra_predictors_hists
                    ),
                    spacing=10,
                )
            )
            .resolve_scale(color="independent")
            .add_selection(error_max_selection)
            .add_selection(c_0_min_selection)
            .add_selection(t_0_selection)
        )
        .configure_axis(labelFontSize=11, titleFontSize=12)
        .configure_legend(labelFontSize=11)
    )
    if fitted_model.name == "RectangleFunction":
        chart = chart.add_selection(t_1_selection)

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
            <div style="position:absolute;left:920px;top:520px;width: -webkit-fill-available;">
            <div id={new_sliders_threshold_id}>
              <div id={old_sliders_threshold_id}></div>
            </div>
            </div>
            <div style="position:absolute;left:45px;top:390px;width: -webkit-fill-available;">
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
                if (index_slider[i].firstChild.innerHTML == "Plot average completeness per: ") {{
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
