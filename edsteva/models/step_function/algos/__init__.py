import catalogue

from edsteva.models.step_function.algos.loss_minimization import loss_minimization
from edsteva.models.step_function.algos.quantile import quantile

algos = catalogue.create("edsteva.models.step_function", "algos")

algos.register("loss_minimization", func=loss_minimization)
algos.register("quantile", func=quantile)
