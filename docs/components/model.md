# Model

[Choosing][available-models] or [customizing][defining-a-custom-model] a Model is the third step in the [EDS-TeVa usage workflow][3-choose-a-model-or-create-a-new-model].

## Definition

A **Model** is a python class designed to characterize the temporal variability of data availability. It estimates the coefficients $\Theta$ and the metrics from a [Probe][probe].

<figure markdown>
  ![Image title](../assets/uml_class/model_class.svg)
  <figcaption>Model class diagram</figcaption>
</figure>

### Input

The Model class is expecting a [**``Probe``**][probe] object in order to estimate the Model coefficients $\Theta$ and some metrics if desired.
### Attributes

- [**`estimates`**][edsteva.models.base.BaseModel] is a [`Pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/version/1.3/reference/api/pandas.DataFrame.html) computed by the [`fit()`][edsteva.models.base.BaseModel.fit] method. It contains the estimated coefficients $\Theta$ and metrics for each column given by the [`Probe._index`][edsteva.probes.visit.VisitProbe] (e.g. care site, stay type, etc.).
- [**`_coefs`**][edsteva.models.step_function.step_function.StepFunction] is the list of the Model coefficients $\Theta$ that are estimated by the [`fit()`][edsteva.models.base.BaseModel.fit] method.

### Methods

- [**`fit()`**][edsteva.models.base.BaseModel.fit] method calls the [`fit_process()`][edsteva.models.step_function.step_function.StepFunction.fit_process] method to compute the estimated coefficients $\Theta$ and metrics and store them in the [`estimates`][edsteva.models.base.BaseModel] attribute.
- [**`fit_process()`**][edsteva.models.step_function.step_function.StepFunction.fit_process] method computes the estimated coefficients $\Theta$ and metrics from a [`Probe.predictor`][edsteva.probes.base.BaseProbe] DataFrame.
- [**`predict()`**][edsteva.models.base.BaseModel.predict] method applies the [`predict_process()`][edsteva.models.step_function.step_function.StepFunction.predict_process] on a [`Probe.predictor`][edsteva.probes.base.BaseProbe] DataFrame and returns a [`Pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/version/1.3/reference/api/pandas.DataFrame.html) of the estimated prediction $\hat{c}(t)$ for each columns given by [`Probe._index`][edsteva.probes.visit.VisitProbe].
- [**`predict_process()`**][edsteva.models.step_function.step_function.StepFunction.predict_process] method computes the estimated completeness predictor $\hat{c}(t)$ for each column given by [`Probe._index`][edsteva.probes.visit.VisitProbe].
- [**`save()`**][edsteva.models.base.BaseModel.save] method saves the [`Model`][edsteva.models.base.BaseModel] in the desired path. By default it is saved in the cache directory (~/.cache/edsteva/models).
- [**`load()`**][edsteva.models.base.BaseModel.load] method loads the [`Model`][edsteva.models.base.BaseModel] from the desired path. By default it is loaded from the cache directory (~/.cache/edsteva/models).

!!! warning "Prediction"
    [**`predict()`**][edsteva.models.base.BaseModel.predict] method must be called on a fitted Model.
## Estimates schema

Data stored in the `estimates` attribute follows a specific schema:

### Indexes

The estimates are computed for each column given by the [`Probe._index`][edsteva.probes.visit.VisitProbe]. For example, if you fit your Model on the [``VisitProbe``][edsteva.probes.visit.VisitProbe], the estimates will be computed for each:

- **`care_site_level`**: care site hierarchic level (`uf`, `pole`, ``hospital``).
- **`care_site_id`**: care site unique identifier.
- **`stay_type`**: type of stay (``hospitalisés``, ``urgence``, ``hospitalisation incomplète``, ``consultation externe``).
### Model coefficients

It depends on the Model used, for instance the step function Model has 2 coefficients:

- **$t_0$** the characteristic time that estimates the time the after which the data is available.
- **$c_0$** the characteristic completeness that estimates the stabilized routine completeness after $t_0$.

### Metrics

It depends on the metrics you specify in the [`fit()`][edsteva.models.base.BaseModel.fit] method. For instance, you can specify an $error$ metric:

$$
error = \frac{\sum_{t_0 \leq  t \leq t_{max}} \epsilon(t)^2}{t_{max} - t_0}
$$

- **$error$** estimates the stability of the data after $t_0$.

### Example

When considering the [`StepFunction.estimates`][edsteva.models.step_function.step_function.StepFunction] fitted on a [``VisitProbe``][edsteva.probes.visit.VisitProbe], it may for instance look like this:

| care_site_level          | care_site_id | stay_type | t_0        | c_0   | error |
| :----------------------- | :----------- | :-------- | :--------- | :---- | :---- |
| Unité Fonctionnelle (UF) | 8312056386   | 'Urg'     | 2019-05-01 | 0.397 | 0.040 |
| Pôle/DMU                 | 8653815660   | 'All'     | 2011-04-01 | 0.583 | 0.028 |
| Unité Fonctionnelle (UF) | 8312027648   | 'Hospit'  | 2021-03-01 | 0.677 | 0.022 |
| Unité Fonctionnelle (UF) | 8312056379   | 'All'     | 2018-08-01 | 0.764 | 0.014 |
| Hôpital                  | 8312022130   | 'Hospit'  | 2022-02-01 | 0.652 | 0.027 |

## Saving and loading a fitted Model

In order to ease the future loading of a Model that has been fitted with the [`fit()`][edsteva.models.base.BaseModel.fit] method, one can pickle it using the [`save()`][edsteva.models.base.BaseModel.save] method. This enables a rapid loading of the Model from local disk using the [`load()`][edsteva.models.base.BaseModel.load] method.

```python
from edsteva.models import StepFunction

model = StepFunction()

model.fit(probe)  # (1)
model.save()  # (2)

model_2 = StepFunction()
model_2.load()  # (3)
```

1. Computation of the estimates (long).
2. Saving of the fitted Model on the local disk.
3. Rapid loading of the fitted Model fom the local disk.

## Defining a custom Model

If none of the available Models meets your requirements, you may want to create your own. To define a custom Model class ``CustomModel`` that inherits from the abstract class [``BaseModel``][edsteva.models.base.BaseModel] you'll have to implement the [`fit_process()`][edsteva.models.step_function.step_function.StepFunction.fit_process] and [`predict_process()`][edsteva.models.step_function.step_function.StepFunction.predict_process] methods (these methods are respectively called by the [`fit()`][edsteva.models.base.BaseModel.fit] method and the [`predict()`][edsteva.models.base.BaseModel.predict] method inherited by the [``BaseModel``][edsteva.models.base.BaseModel]  class). You'll also have to define the [``_coefs``][edsteva.models.step_function.step_function.StepFunction] attribute which is the list of the Model coefficients.

```python
from edsteva.models import BaseModel
from edsteva.probes import BaseProbe


# Definition of a new Model class
class CustomProbe(BaseModel):
    _coefs = ["my_model_coefficient_1", "my_model_coefficient_2"]

    def fit_process(self, probe: BaseProbe):
        # fit process
        return custom_predictor

    def predict_process(self, probe: BaseProbe):
        # predict process
        return custom_predictor
```
[`fit_process()`][edsteva.models.step_function.step_function.StepFunction.fit_process] and [`predict_process()`][edsteva.models.step_function.step_function.StepFunction.predict_process] methods take a Probe as the first argument. All other parameters must be keyword arguments. For a detailed example of the implementation of a Model, please have a look on the implemented [``StepFunction``][edsteva.models.step_function.step_function.StepFunction] Model.

!!!success "Contributions"
    If you managed to create your own Model do not hesitate to share it with the community by following the [contribution guidelines][contributing]. Contributions are welcome, and they are greatly appreciated! Every little bit helps, and credit will always be given.
## Available Models

We detail hereafter the step function Model that has already been implemented in the library.

=== "StepFunction"

    === "Coefficients"

        The [``StepFunction``][edsteva.models.step_function.step_function.StepFunction] fits a step function $f_{t_0, c_0}(t)$ with coefficients $\Theta = (t_0, c_0)$ on a completeness predictor $c(t)$:

        $$
        \begin{aligned}
        f_{t_0, c_0}(t) & = c_0 \ \mathbb{1}_{t \geq t_0}(t) \\
        c(t) & = f_{t_0, c_0}(t) + \epsilon(t)
        \end{aligned}
        $$

        - the characteristic time $t_0$ estimates the time after which the data is available.
        - the characteristic value $c_0$ estimates the stabilized routine completeness.


    === "Metrics"

        The default metric computed is the mean squared error after $t_0$:

        $$
        error = \frac{\sum_{t_0 \leq  t \leq t_{max}} \epsilon(t)^2}{t_{max} - t_0}
        $$

        - $error$ estimates the stability of the data after $t_0$.

        !!! info "Custom metric"
                You can define your own metric if this one doesn't meet your requirements.

    === "Algos"

        The available algorithms used to fit the step function are listed below:

        !!! info "Custom algo"
                You can define your own algo if they don't meet your requirements.

        === "Loss minimization"

            This algorithm computes the estimated coefficients $\hat{t_0}$ and $\hat{c_0}$ by minimizing the loss function $\mathcal{L}(t_0, c_0)$:

            $$
            \begin{aligned}
            \mathcal{L}(t_0, c_0) & = \frac{\sum_{t = t_{min}}^{t_{max}} \mathcal{l}(c(t), f_{t_0, c_0}(t))}{t_{max} - t_{min}} \\
            (\hat{t_0}, \hat{c_0}) & = \underset{t_0, c_0}{\mathrm{argmin}}(\mathcal{L}(t_0, c_0)) \\
            \end{aligned}
            $$

            !!! info "Default loss function $\mathcal{l}$"
                The loss function is $l_2$ by default:
                $$
                \mathcal{l}(c(t), f_{t_0, c_0}(t)) = |c(t) - f_{t_0, c_0}(t)|^2
                $$

            !!! danger "Optimal estimates"
                For complexity purposes, this algorithm has been implemented to compute the optimal estimates only with the $l_2$ loss function. For more informations, you can have a look on the [source code][edsteva.models.step_function.algos.loss_minimization.loss_minimization].

        === "Quantile"

            In this algorithm, $\hat{c_0}$ is directly estimated as the $x^{th}$ quantile of the completeness predictor $c(t)$, where $x$ is a number between 0 and 1. Then, $\hat{t_0}$ is the first time $c(t)$ reaches $\hat{c_0}$.

            $$
            \begin{aligned}
            \hat{c_0} & = x^{th} \text{ quantile of } c(t) \\
            \hat{t_0} & = \underset{t}{\mathrm{argmin}}(c(t) \geq \hat{c_0})
            \end{aligned}
            $$

            !!! info "Default quantile $x$"
                The default quantile is $x = 0.8$.

    === "Example"

        ```python
        from edsteva.models.step_function import StepFunction

        step_function_model = StepFunction()
        step_function_model.fit(probe)
        step_function_model.estimates.head()
        ```

        | care_site_level          | care_site_id | stay_type | t_0        | c_0   | error |
        | :----------------------- | :----------- | :-------- | :--------- | :---- | :---- |
        | Unité Fonctionnelle (UF) | 8312056386   | 'Urg'     | 2019-05-01 | 0.397 | 0.040 |
        | Unité Fonctionnelle (UF) | 8312056386   | 'All'     | 2011-04-01 | 0.583 | 0.028 |
        | Pôle/DMU                 | 8312027648   | 'Hospit'  | 2021-03-01 | 0.677 | 0.022 |
        | Pôle/DMU                 | 8312027648   | 'All'     | 2018-08-01 | 0.764 | 0.014 |
        | Hôpital                  | 8312022130   | 'Hospit'  | 2022-02-01 | 0.652 | 0.027 |

=== "RectangleFunction"

    === "Coefficients"

        The [``RectangleFunction``][edsteva.models.rectangle_function.rectangle_function.RectangleFunction] fits a step function $f_{t_0, c_0, t_1}(t)$ with coefficients $\Theta = (t_0, c_0, t_1)$ on a completeness predictor $c(t)$:

        $$
        \begin{aligned}
        f_{t_0, c_0, t_1}(t) & = c_0 \ \mathbb{1}_{t_0 \leq t \leq t_1}(t) \\
        c(t) & = f_{t_0, c_0, t_1}(t) + \epsilon(t)
        \end{aligned}
        $$

        - the characteristic time $t_0$ estimates the time after which the data is available.
        - the characteristic time $t_1$ estimates the time after which the data is not available anymore.
        - the characteristic value $c_0$ estimates the completeness between $t_0$ and $t_1$.


    === "Metrics"

        The default metric computed is the mean squared error between $t_0$ and $t_1$:

        $$
        error = \frac{\sum_{t_0 \leq  t \leq t_1} \epsilon(t)^2}{t_1 - t_0}
        $$

        - $error$ estimates the stability of the data between $t_0$ and $t_1$.

        !!! info "Custom metric"
                You can define your own metric if this one doesn't meet your requirements.

    === "Algos"

        The available algorithms used to fit the step function are listed below:

        !!! info "Custom algo"
                You can define your own algorithm if they don't meet your requirements.

        === "Loss minimization"

            This algorithm computes the estimated coefficients $\hat{t_0}$, $\hat{c_0}$ and $\hat{t_1}$ by minimizing the loss function $\mathcal{L}(t_0, c_0, t_1)$:

            $$
            \begin{aligned}
            \mathcal{L}(t_0, c_0, t_1) & = \frac{\sum_{t = t_{min}}^{t_{max}} \mathcal{l}(c(t), f_{t_0, c_0, t_1}(t))}{t_{max} - t_{min}} \\
            (\hat{t_0}, \hat{t_1}, \hat{c_0}) & = \underset{t_0, c_0, t_1}{\mathrm{argmin}}(\mathcal{L}(t_0, c_0, t_1)) \\
            \end{aligned}
            $$

            !!! info "Default loss function $\mathcal{l}$"
                The loss function is $l_2$ by default:
                $$
                \mathcal{l}(c(t), f_{t_0, c_0, t_1}(t)) = |c(t) - f_{t_0, c_0, t_1}(t)|^2
                $$

            !!! danger "Optimal estimates"
                For complexity purposes, this algorithm has been implemented with a dependency relation between $c_0$ and $t_0$ derived from the optimal estimates using the $l_2$ loss function. For more informations, you can have a look on the [source code][edsteva.models.step_function.algos.loss_minimization.loss_minimization].

    === "Example"

        ```python
        from edsteva.models.rectangle_function import RectangleFunction

        rectangle_function_model = RectangleFunction()
        rectangle_function_model.fit(probe)
        rectangle_function_model.estimates.head()
        ```

        | care_site_level          | care_site_id | stay_type | t_0        | c_0   | t_1        | error |
        | :----------------------- | :----------- | :-------- | :--------- | :---- | :--------- | :---- |
        | Unité Fonctionnelle (UF) | 8312056386   | 'Urg'     | 2019-05-01 | 0.397 | 2020-05-01 | 0.040 |
        | Unité Fonctionnelle (UF) | 8312056386   | 'All'     | 2011-04-01 | 0.583 | 2013-04-01 | 0.028 |
        | Pôle/DMU                 | 8312027648   | 'Hospit'  | 2021-03-01 | 0.677 | 2022-03-01 | 0.022 |
        | Pôle/DMU                 | 8312027648   | 'All'     | 2018-08-01 | 0.764 | 2019-08-01 | 0.014 |
        | Hôpital                  | 8312022130   | 'Hospit'  | 2022-02-01 | 0.652 | 2022-08-01 | 0.027 |
