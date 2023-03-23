import catalogue

from .per_note import compute_completeness_predictor_per_note
from .per_visit import compute_completeness_predictor_per_visit

completeness_predictors = catalogue.create(
    "edsteva.probes.note", "completeness_predictors"
)

completeness_predictors.register(
    "per_visit_default", func=compute_completeness_predictor_per_visit
)
completeness_predictors.register(
    "per_note_default", func=compute_completeness_predictor_per_note
)
