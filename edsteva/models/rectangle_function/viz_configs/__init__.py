import catalogue

from edsteva.models.rectangle_function.viz_configs.normalized_probe_dashboard import (
    get_normalized_probe_dashboard_config,
)
from edsteva.models.rectangle_function.viz_configs.normalized_probe_plot import (
    get_normalized_probe_plot_config,
)
from edsteva.models.rectangle_function.viz_configs.probe_dashboard import (
    get_probe_dashboard_config,
)
from edsteva.models.rectangle_function.viz_configs.probe_plot import (
    get_probe_plot_config,
)

normalized_probe_dashboard = catalogue.create(
    "edsteva.models.rectangle_function.viz_configs", "normalized_probe_dashboard"
)
normalized_probe_dashboard.register(
    "default", func=get_normalized_probe_dashboard_config
)


probe_dashboard = catalogue.create(
    "edsteva.models.rectangle_function.viz_configs", "probe_dashboard"
)
probe_dashboard.register("default", func=get_probe_dashboard_config)


normalized_probe_plot = catalogue.create(
    "edsteva.models.rectangle_function.viz_configs", "normalized_probe_plot"
)
normalized_probe_plot.register("default", func=get_normalized_probe_plot_config)


probe_plot = catalogue.create(
    "edsteva.models.rectangle_function.viz_configs", "probe_plot"
)
probe_plot.register("default", func=get_probe_plot_config)

viz_configs = dict(
    normalized_probe_dashboard=normalized_probe_dashboard,
    probe_dashboard=probe_dashboard,
    normalized_probe_plot=normalized_probe_plot,
    probe_plot=probe_plot,
)
