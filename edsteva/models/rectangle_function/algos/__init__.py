import catalogue

from edsteva.models.rectangle_function.algos.loss_minimization import loss_minimization

algos = catalogue.create("edsteva.models.rectangle_function", "algos")

algos.register("loss_minimization", func=loss_minimization)
