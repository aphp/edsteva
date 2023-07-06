# Visualization

The fourth (and last) step in the [EDS-TeVa usage workflow][4-set-the-thresholds-to-fix-the-deployment-bias] is [setting][available-visualizations] the thresholds associated with the coefficients and the metrics of the [Model][model] fitted on the [Probes][probe].

## Definition

The EDS-TeVa library provides dashboards and plots to visualize the temporal evolution of [Probes][probe] along with fitted [Models][model]. Visualization functionalities can be used to explore the database and set thresholds relative to selection criteria.

<figure markdown>
  ![Image title](../assets/uml_class/visualization.svg)
  <figcaption>Visualization diagram</figcaption>
</figure>

## Dashboard

A **Dashboard** is an interactive [Altair](https://altair-viz.github.io/) chart that lets you visualize variables aggregated by any combination of columns included in the [Probe][probe]. In the library, the dashboards are divided into two parts:

- On the top, there is the plot of the aggregated variable of interest.
- On the bottom, there are interactive filters to set. Only the selected data is aggregated to produce the plot on the top.
## Plot

A **Plot** is exportable in png or svg format and easy to integrate into a report. However, as it is less interactive it is preferred to specify the filters in the inputs of the functions.

## Available Visualizations

=== "Dashboard"

    The library provides interactive dashboards that let you set any combination of care sites, stay types and other columns if included in the Probe. You can only export a dashboard in HTML format.

    === "probe_dashboard()"

        The [``probe_dashboard()``][edsteva.viz.dashboards.probe.wrapper] returns:

        - On the top, the aggregated variable is the average completeness predictor $c(t)$ over time $t$ with the prediction $\hat{c}(t)$ if the [fitted Model][model] is specified.
        - On the bottom, the interactive filters are all the columns included in the [Probe][probe] (such as time, care site, number of visits...etc.).

        ```python
        from edsteva.viz.dashboards import probe_dashboard

        probe_dashboard(
            probe=probe,
            fitted_model=step_function_model,
            care_site_level=care_site_level,
        )
        ```
        An example is available [here](../assets/charts/interactive_fitted_visit.html).

    === "normalized_probe_dashboard()"

        The [``normalized_probe_dashboard()``][edsteva.viz.dashboards.normalized_probe.normalized_probe] returns a representation of the overall deviation from the [Model][model]:

        - On the top, the aggregated variable is a normalized completeness predictor $\frac{c(t)}{c_0}$ over normalized time $t - t_0$.
        - On the bottom, the interactive filters are all the columns included in the [Probe][probe] (such as time, care site, number of visits...etc.) with all the [Model coefficients][model-coefficients] and [metrics][metrics] included in the [Model][model].

        ```python
        from edsteva.viz.dashboards import normalized_probe_dashboard

        normalized_probe_dashboard(
            probe=probe,
            fitted_model=step_function_model,
            care_site_level=care_site_level,
        )
        ```

        An example is available [here](../assets/charts/normalized_probe_dashboard.html).

=== "Plot"

    The library provides static plots that you can export in png or svg. As it is less interactive, you may specify the filters in the inputs of the functions.

    === "probe_plot()"

        The [``probe_plot()``][edsteva.viz.plots.probe.wrapper] returns the top plot of the [``probe_dashboard()``][edsteva.viz.dashboards.probe.wrapper]: the normalized completeness predictor $\frac{c(t)}{c_0}$ over normalized time $t - t_0$.

        ```python
        from edsteva.viz.plots import probe_plot

        probe_plot(
            probe=probe,
            fitted_model=step_function_model,
            care_site_level=care_site_level,
            stay_type=stay_type,
            save_path=plot_path,
        )
        ```

        ```vegalite
        {
        "schema-url": "../../assets/charts/fitted_visit.json"
        }
        ```

    === "normalized_probe_plot()"

        The [``normalized_probe_plot()``][edsteva.viz.plots.normalized_probe] returns the top plot of the [``normalized_probe_dashboard()``][edsteva.viz.dashboards.normalized_probe.normalized_probe]. Consequently, you have to specify the filters in the inputs of the function.

        ```python
        from edsteva.viz.plots import normalized_probe_plot

        normalized_probe_plot(
            probe=probe,
            fitted_model=step_function_model,
            t_min=-15,
            t_max=15,
            save_path=plot_path,
        )
        ```
        ```vegalite
        {
        "schema-url": "../../assets/charts/normalized_probe.json"
        }
        ```


    === "estimates_densities_plot()"

        The [``estimates_densities_plot()``][edsteva.viz.plots.estimates_densities] returns the density plot and the median of each estimate. It can help you to set the thresholds.

        ```python
        from edsteva.viz.plots import estimates_densities_plot

        estimates_densities_plot(
            fitted_model=step_function_model,
        )
        ```
        ```vegalite
        {
        "schema-url": "../../assets/charts/estimates_densities.json"
        }
        ```
