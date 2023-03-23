import catalogue

from edsteva.metrics.error import error
from edsteva.metrics.error_after_t0 import error_after_t0
from edsteva.metrics.error_between_t0_t1 import error_between_t0_t1

metrics = catalogue.create("edsteva", "metrics")

metrics.register("error", func=error)
metrics.register("error_after_t0", func=error_after_t0)
metrics.register("error_between_t0_t1", func=error_between_t0_t1)
