from functools import reduce

import altair as alt

from edsteva.models import BaseModel
from edsteva.viz.utils import save_html


def plot_estimates_densities(
    fitted_model: BaseModel,
    save_path: str = None,
    **kwargs,
):
    r"""Displays the density plot with the associated box plot of each estimate and metric computed in the input model. It can help you to set the thresholds.


    Parameters
    ----------
    fitted_model : BaseModel
        Model with estimates of interest
        **EXAMPLE**: StepFunction Model with $(\hat{t_0}, \hat{c_0})$
    save_path : str, optional
        Folder path where to save the chart in HTML format.

        **EXAMPLE**: `"my_folder/my_file.html"`
    """
    alt.data_transformers.disable_max_rows()

    estimates = fitted_model.estimates.copy()

    indexes = list(
        estimates.columns.difference(fitted_model._coefs + fitted_model._metrics)
    )
    indexes.remove("care_site_id")

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
                                groupby=indexes,
                                extent=[min_value, max_value],
                                **kwargs,
                            )
                            .mark_area()
                            .encode(
                                x=alt.X(
                                    "{}:Q".format(estimate),
                                    title=None,
                                ),
                                y="Density:Q",
                            )
                        )
                        + alt.Chart(estimates)
                        .mark_rule(color="red")
                        .encode(x="median({}):Q".format(estimate))
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
            estimates[estimate] = estimates[estimate]

            estimates[estimate] = estimates[estimate].astype("datetime64[s]")
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
                        ),
                    )
                )
                + alt.Chart(estimates)
                .mark_rule(color="red")
                .encode(x="median({}):T".format(estimate))
            ).properties(width=800, height=300)
            time_estimates.append(estimate_density)

    estimates_densities = time_estimates + quantitative_estimates
    estimates_densities = reduce(
        lambda estimate_density_1, estimate_density_2: estimate_density_1
        & estimate_density_2,
        estimates_densities,
    )

    for index in indexes:
        index_selection = alt.selection_single(
            fields=[index],
            bind=alt.binding_select(name=index, options=estimates[index].unique()),
            init={index: estimates[index].unique()[0]},
        )

        estimates_densities = estimates_densities.add_selection(index_selection)
        estimates_densities = estimates_densities.transform_filter(index_selection)

    if save_path:
        save_html(
            obj=estimates_densities.configure_axis(
                labelFontSize=11, titleFontSize=12
            ).configure_legend(labelFontSize=11),
            filename=save_path,
        )

    return estimates_densities
