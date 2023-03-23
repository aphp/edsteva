import catalogue

from .per_measurement import compute_completeness_predictor_per_measurement

completeness_predictors = catalogue.create(
    "edsteva.probes.biology", "completeness_predictors"
)

completeness_predictors.register(
    "per_measurement_default", func=compute_completeness_predictor_per_measurement
)
