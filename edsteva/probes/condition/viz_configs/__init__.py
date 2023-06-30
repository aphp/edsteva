import catalogue

from edsteva.probes.condition.viz_configs import n_condition, per_condition, per_visit

normalized_probe_dashboard = catalogue.create(
    "edsteva.probes.condition.viz_configs", "normalized_probe_dashboard"
)
normalized_probe_dashboard.register(
    "per_visit_default", func=per_visit.get_normalized_probe_dashboard_config
)

normalized_probe_dashboard.register(
    "per_condition_default", func=per_condition.get_normalized_probe_dashboard_config
)

normalized_probe_dashboard.register(
    "n_condition", func=n_condition.get_normalized_probe_dashboard_config
)
probe_dashboard = catalogue.create(
    "edsteva.probes.condition.viz_configs", "probe_dashboard"
)
probe_dashboard.register("per_visit_default", func=per_visit.get_probe_dashboard_config)
probe_dashboard.register(
    "per_condition_default", func=per_condition.get_probe_dashboard_config
)
probe_dashboard.register("n_condition", func=n_condition.get_probe_dashboard_config)

estimates_densities_plot = catalogue.create(
    "edsteva.probes.condition.viz_configs", "estimates_densities_plot"
)
estimates_densities_plot.register(
    "per_visit_default", func=per_visit.get_estimates_densities_plot_config
)
estimates_densities_plot.register(
    "per_condition_default", func=per_condition.get_estimates_densities_plot_config
)
estimates_densities_plot.register(
    "n_condition", func=n_condition.get_estimates_densities_plot_config
)

normalized_probe_plot = catalogue.create(
    "edsteva.probes.condition.viz_configs", "normalized_probe_plot"
)
normalized_probe_plot.register(
    "per_visit_default", func=per_visit.get_normalized_probe_plot_config
)
normalized_probe_plot.register(
    "per_condition_default", func=per_condition.get_normalized_probe_plot_config
)
normalized_probe_plot.register(
    "n_condition", func=n_condition.get_normalized_probe_plot_config
)
probe_plot = catalogue.create("edsteva.probes.condition.viz_configs", "probe_plot")
probe_plot.register("per_visit_default", func=per_visit.get_probe_plot_config)
probe_plot.register("per_condition_default", func=per_condition.get_probe_plot_config)
probe_plot.register("n_condition", func=n_condition.get_probe_plot_config)

viz_configs = dict(
    normalized_probe_dashboard=normalized_probe_dashboard,
    probe_dashboard=probe_dashboard,
    estimates_densities_plot=estimates_densities_plot,
    normalized_probe_plot=normalized_probe_plot,
    probe_plot=probe_plot,
)
