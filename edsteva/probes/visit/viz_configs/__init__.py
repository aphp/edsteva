import catalogue

from edsteva.probes.visit.viz_configs.per_visit.estimates_densities_plot import (
    get_estimates_densities_plot_config,
)
from edsteva.probes.visit.viz_configs.per_visit.normalized_probe_dashboard import (
    get_normalized_probe_dashboard_config,
)
from edsteva.probes.visit.viz_configs.per_visit.normalized_probe_plot import (
    get_normalized_probe_plot_config,
)
from edsteva.probes.visit.viz_configs.per_visit.probe_dashboard import (
    get_probe_dashboard_config,
)
from edsteva.probes.visit.viz_configs.per_visit.probe_plot import get_probe_plot_config

normalized_probe_dashboard = catalogue.create(
    "edsteva.probes.visit.viz_configs", "normalized_probe_dashboard"
)
normalized_probe_dashboard.register(
    "per_visit_default", func=get_normalized_probe_dashboard_config
)


probe_dashboard = catalogue.create(
    "edsteva.probes.visit.viz_configs", "probe_dashboard"
)
probe_dashboard.register("per_visit_default", func=get_probe_dashboard_config)

estimates_densities_plot = catalogue.create(
    "edsteva.probes.visit.viz_configs", "estimates_densities_plot"
)
estimates_densities_plot.register(
    "per_visit_default", func=get_estimates_densities_plot_config
)

normalized_probe_plot = catalogue.create(
    "edsteva.probes.visit.viz_configs", "normalized_probe_plot"
)
normalized_probe_plot.register(
    "per_visit_default", func=get_normalized_probe_plot_config
)

probe_plot = catalogue.create("edsteva.probes.visit.viz_configs", "probe_plot")
probe_plot.register("per_visit_default", func=get_probe_plot_config)

viz_configs = dict(
    normalized_probe_dashboard=normalized_probe_dashboard,
    probe_dashboard=probe_dashboard,
    estimates_densities_plot=estimates_densities_plot,
    normalized_probe_plot=normalized_probe_plot,
    probe_plot=probe_plot,
)
