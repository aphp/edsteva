import catalogue

from .per_condition import compute_completeness_predictor_per_condition
from .per_visit import compute_completeness_predictor_per_visit

completeness_predictors = catalogue.create(
    "edsteva.probes.condition", "completeness_predictors"
)

completeness_predictors.register(
    "per_visit_default", func=compute_completeness_predictor_per_visit
)

completeness_predictors.register(
    "per_condition_default", func=compute_completeness_predictor_per_condition
)
