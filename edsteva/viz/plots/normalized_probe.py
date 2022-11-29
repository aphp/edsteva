from datetime import datetime
from typing import List, Union

import altair as alt
import pandas as pd

from edsteva.models.base import BaseModel
from edsteva.probes.base import BaseProbe
from edsteva.viz.utils import filter_predictor, round_it, save_html, scale_it


def plot_normalized_probe(
    probe: BaseProbe,
    fitted_model: BaseModel,
    t_0_max: datetime = None,
    t_1_min: datetime = None,
    error_max: float = None,
    c_0_min: float = None,
    care_site_level: str = None,
    stay_type: List[str] = None,
    care_site_id: List[int] = None,
    start_date: Union[datetime, str] = None,
    end_date: Union[datetime, str] = None,
    care_site_short_name: List[int] = None,
    t_min: int = None,
    t_max: int = None,
    save_path: str = None,
    x_axis_title: str = "Δt = (t - t₀) months",
    y_axis_title: str = "c(Δt) / c₀",
    show_per_care_site: bool = False,
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
    """

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
    if t_min:
        predictor = predictor[predictor.normalized_date >= t_min]
    if t_max:
        predictor = predictor[predictor.normalized_date <= t_max]
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

    # RectangleModel
    if fitted_model.name == "RectangleFunction":
        predictor = predictor[predictor.date < predictor.t_1]

    index = list(set(probe._index).difference(["care_site_level", "care_site_id"]))
    if show_per_care_site:
        index = index + ["care_site_short_name"]

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

    if c_0_min:
        c_0_min = round_it(float(c_0_min), 2)
        if c_0_min > round_it(predictor.c_0.max(), 2):
            c_0_min = round_it(predictor.c_0.max(), 2)
        elif c_0_min < round_it(predictor.c_0.min(), 2):
            c_0_min = round_it(predictor.c_0.min(), 2)
    else:
        c_0_min = round_it(predictor.c_0.min(), 2)

    if error_max:
        error_max = round_it(float(error_max), 2)
        if error_max > predictor.error.max():
            error_max = round_it(predictor.error.max(), 2)
        elif error_max < round_it(predictor.error.min(), 2):
            error_max = round_it(predictor.error.min(), 2)
    else:
        error_max = round_it(predictor.error.max(), 2)

    t_0_max = (
        str(pd.to_datetime(t_0_max)) if t_0_max else predictor.t_0.astype(str).max()
    )

    c_0_min_slider = alt.binding_range(
        min=0,
        max=round_it(predictor.c_0.max(), 2),
        step=1 / 100,
        name="c₀ min: ",
    )
    c_0_min_selection = alt.selection_single(
        name="c_0_min",
        fields=["c_0_min"],
        bind=c_0_min_slider,
        init={"c_0_min": c_0_min},
    )
    t_0_slider = alt.binding(
        input="t_0",
        name="t₀ max: ",
    )
    t_0_selection = alt.selection_single(
        name="t_0",
        fields=["t_0"],
        bind=t_0_slider,
        init={"t_0": t_0_max},
    )
    if fitted_model.name == "RectangleFunction":
        t_1_min = (
            str(pd.to_datetime(t_1_min)) if t_1_min else predictor.t_1.astype(str).min()
        )
        t_1_slider = alt.binding(
            input="t_1",
            name="t₁ min: ",
        )
        t_1_selection = alt.selection_single(
            name="t_1",
            fields=["t_1"],
            bind=t_1_slider,
            init={"t_1": t_1_min},
        )
    error_max_slider = alt.binding_range(
        min=round_it(predictor.error.min(), 2),
        max=round_it(predictor.error.max(), 2),
        step=scale_it(predictor.error.max()) / 100,
        name="error max: ",
    )
    error_max_selection = alt.selection_single(
        name="error_max",
        fields=["error_max"],
        bind=error_max_slider,
        init={"error_max": error_max},
    )

    alt.data_transformers.disable_max_rows()

    base_chart = (
        alt.Chart(predictor).encode(
            x=alt.X(
                "normalized_date:Q",
                title=x_axis_title,
                scale=alt.Scale(nice=False),
            ),
        )
    ).properties(width=900, height=300)

    transform_chart = (
        base_chart.transform_joinaggregate(
            mean_c_0="mean(c_0)",
            mean_error="mean(error)",
            groupby=["care_site_short_name"] + index,
        )
        .transform_filter(alt.datum.mean_c_0 >= c_0_min_selection.c_0_min)
        .transform_filter(alt.datum.mean_error <= error_max_selection.error_max)
        .transform_filter(alt.datum.t_0 <= t_0_selection.t_0)
    )
    # RectangleModel
    if fitted_model.name == "RectangleFunction":
        transform_chart = transform_chart.transform_filter(
            alt.datum.t_1 >= t_1_selection.t_1
        )

    mean_line = transform_chart.mark_line().encode(
        y=alt.Y(
            "mean(normalized_c):Q",
        )
    )

    error_line = transform_chart.mark_errorband(extent="stdev").encode(
        y=alt.Y(
            "normalized_c:Q",
            title=y_axis_title,
        ),
        stroke=alt.Stroke(
            "legend_error_band",
            title="Error band",
            legend=alt.Legend(symbolType="square", orient="top"),
        ),
    )

    model_line = base_chart.mark_line(
        color="black", interpolate="step-after", strokeDash=[5, 5]
    ).encode(
        y="model:Q",
        strokeWidth=alt.StrokeWidth(
            "legend_model",
            title="Model line",
            legend=alt.Legend(orient="top", symbolDash=[2, 2]),
        ),
    )

    chart = mean_line + error_line + model_line

    if len(index) >= 2:
        index_selection = alt.selection_single(
            fields=["index"],
            bind=alt.binding_radio(
                name="Plot average completeness per: ", options=index
            ),
            init={"index": index[0]},
        )
        chart = (
            chart.transform_fold(index, as_=["index", "value"])
            .encode(
                color=alt.Color(
                    "value:N",
                    sort={"field": "c_0", "op": "min", "order": "descending"},
                    title=None,
                ),
            )
            .transform_filter(index_selection)
            .add_selection(index_selection)
        )

    elif len(index) == 1:
        chart = chart.encode(
            color=alt.Color(
                "{}:N".format(index[0]),
                sort={"field": "c_0", "op": "min", "order": "descending"},
            ),
        )

    chart = (
        chart.add_selection(error_max_selection)
        .add_selection(c_0_min_selection)
        .add_selection(t_0_selection)
    )
    # RectangleModel
    if fitted_model.name == "RectangleFunction":
        chart = chart.add_selection(t_1_selection)

    if save_path:
        save_html(
            obj=chart.configure_axis(
                labelFontSize=11, titleFontSize=12
            ).configure_legend(labelFontSize=11),
            filename=save_path,
        )

    return chart
