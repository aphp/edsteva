<!-- ---
ᴴₒᴴₒᴴₒ: true
---

!!! Warning "DISCLAIMER"
    EDS-TeVa is intended to be a module of [EDS-Scikit](https://github.com/aphp/EDS-Scikit) -->

<p align="center">
  <a href="https://aphp.github.io/edsteva/latest/"><img src="https://aphp.github.io/edsteva/latest/assets/logo/edsteva_logo_small.svg" alt="EDS-TeVa"></a>
</p>
<p align="center" style="font-size:35px">
    EDS-TeVa <a href="https://mybinder.org/v2/gh/aphp/edsteva/HEAD?labpath=notebooks%2Fsynthetic_data.ipynb" target="_blank">
    <img src="https://mybinder.org/badge_logo.svg" alt="Documentation">
</a>
</p>
<p align="center">
<a href="https://aphp.github.io/edsteva/latest/" target="_blank">
    <img src="https://img.shields.io/github/actions/workflow/status/aphp/edsteva/documentation.yaml?branch=main&label=docs&style=flat" alt="Documentation">
</a>
<a href="https://pypi.org/project/edsteva/" target="_blank">
    <img src="https://img.shields.io/pypi/v/edsteva?color=blue&style=flat" alt="PyPI">
</a>
<a href="https://codecov.io/github/aphp/edsteva?branch=main" target="_blank">
    <img src="https://codecov.io/github/aphp/edsteva/coverage.svg?branch=main" alt="Codecov">
</a>
<a href="https://github.com/psf/black" target="_blank">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg" alt="Black">
</a>
<a href="https://python-poetry.org/" target="_blank">
    <img src="https://img.shields.io/badge/reproducibility-poetry-blue" alt="Poetry">
</a>
<a href="https://www.python.org/" target="_blank">
    <img src="https://img.shields.io/badge/python-~3.7.1-brightgreen" alt="Supported Python versions">
</a>
<a href="https://github.com/astral-sh/ruff" target="_blank">
    <img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json" alt="Ruff">
</a>
</p>

---

**Documentation**: <a href="https://aphp.github.io/edsteva/latest/" target="_blank">https://aphp.github.io/edsteva/latest/</a>

**Source Code**: <a href="https://github.com/aphp/edsteva" target="_blank">https://github.com/aphp/edsteva</a>

---

# Getting Started

EDS-TeVa provides a set of tools to characterize the temporal variability of data induced by the dynamics of the clinical IT system.
## Context

Real world data is subject to important temporal drifts that may be caused by a variety of factors[@finlayson2021clinician]. In particular, data availability fluctuates with the deployment of clinical softwares and their clinical use. The dynamics of software deployment and adoption is not trivial as it depends on the care site and on the category of data that are considered.

## Installation

!!! Warning "Requirements"
    EDS-TeVa stands on the shoulders of [Spark 2.4](https://spark.apache.org/docs/2.4.8/index.html) which runs on [Java 8](https://www.oracle.com/java/technologies/java8.html) and [Python](https://www.python.org/) ~3.7.1, it is essential to:

    - Install a version of Python $\geq 3.7.1$ and $< 3.8$.
    - Install [OpenJDK 8](https://openjdk.org/projects/jdk8/), an open-source reference implementation of Java 8 wit the following command lines:

        === "Linux (Debian, Ubunutu, etc.)"
            <div class="termy">
            ```console
            $ sudo apt-get update
            $ sudo apt-get install openjdk-8-jdk
            ---> 100%
            ```
            </div>

            For more details, check this [installation guide](https://www.geofis.org/en/install/install-on-linux/install-openjdk-8-on-ubuntu-trusty/)

        === "Mac"
            <div class="termy">
            ```console
            $ brew tap AdoptOpenJDK/openjdk
            $ brew install --cask adoptopenjdk8
            ---> 100%
            ```
            </div>

            For more details, check this [installation guide](https://installvirtual.com/install-openjdk-8-on-mac-using-brew-adoptopenjdk/)

        === "Windows"

            Follow this [installation guide](https://techoral.com/blog/java/openjdk-install-windows.html)

You can install EDS-TeVa through `pip`:

<div class="termy">

```console
$ pip install edsteva
---> 100%
color:green Successfully installed edsteva
```

</div>

We recommend pinning the library version in your projects, or use a strict package manager like [Poetry](https://python-poetry.org/).

```
pip install edsteva==0.2.2
```
## Working example: administrative records relative to visits

Let's consider a basic category of data: administrative records relative to visits. Visits are characterized by a stay type (full hospitalisation, emergency, consultation, etc.). In this example, the objective is to estimate the availability of visits records with respect to time, care site and stay type.

### 1. Load your [data][loading-data]

As detailled in [the dedicated section][loading-data], EDS-TeVa is expecting to work with [Pandas](https://pandas.pydata.org/) or [Koalas](https://koalas.readthedocs.io/en/latest/) DataFrames.  We provide various connectors to facilitate data fetching, namely a [Hive][loading-from-hive-hivedata] connector, a [Postgres][loading-from-postgres-postgresdata] connector and a [LocalData][persistingreading-a-sample-tofrom-disk-localdata].


=== "Using a Hive DataBase"

    ```python
    from edsteva.io import HiveData

    db_name = "my_db"
    tables_to_load = [
        "visit_occurrence",
        "visit_detail",
        "care_site",
        "fact_relationship",
    ]
    data = HiveData(db_name, tables_to_load=tables_to_load)
    data.visit_occurrence  # (1)
    ```

    1. With this connector, `visit_occurrence` will be a *Koalas* DataFrame

=== "Using a Postgres DataBase"

    ```python
    from edsteva.io import PostgresData

    db_name = "my_db"
    schema = "my_schema"
    user = "my_username"
    data = PostgresData(db_name, schema=schema, user=user)  # (1)
    data.visit_occurrence  # (2)
    ```

    1. This connector expects a `.pgpass` file storing the connection parameters
    2. With this connector, `visit_occurrence` will be a *Pandas* DataFrame

=== "Using a Local DataBase"

    ```python
    import os
    from edsteva.io import LocalData

    folder = os.path.abspath(MY_FOLDER_PATH)

    data = LocalData(folder)  # (1)
    data.visit_occurrence  # (2)
    ```

    1. This connector expects a `folder` with a file per table to load.
    2. With this connector, `visit_occurrence` will be a *Pandas* DataFrame

### 2. Choose a [Probe][probe] or [create a new Probe][defining-a-custom-probe]
!!! info "Probe"
    A [Probe][probe] is a python class designed to compute a completeness predictor $c(t)$ that characterizes data availability of a target variable over time $t$.

In this example, $c(t)$ predicts the availability of administrative records relative to visits. It is defined for each care site and stay type as the number of visits $n_{visit}(t)$ per month $t$, normalized by the maximum number of records per month $n_{max} = \max_{t}(n_{visit}(t))$ computed over the entire study period:

$$
c(t) = \frac{n_{visit}(t)}{n_{max}}
$$


!!!info ""
    If the maximum number of records per month $n_{max}$ is equal to 0, we consider that the completeness predictor $c(t)$ is also equal to 0.

The [VisitProbe][available-probes] is already available by default in the library:

#### 2.1 Compute your Probe

The [``compute()``][edsteva.probes.base.BaseProbe.compute] method takes a [Data][loading-data] object as input and stores the computed completeness predictor $c(t)$ in the [``predictor``][predictor-schema] attribute of a [``Probe``][probe]:

```python
from edsteva.probes import VisitProbe

probe_path = "my_path/visit.pkl"

visit = VisitProbe()
visit.compute(
    data,
    stay_types={
        "All": ".*",
        "Urg_Hospit": "urgence|hospitalisés",  # (1)
    },
    care_site_levels=["Hospital", "Pole", "UF"],  # (2)
)
visit.save(path=probe_path)  # (3)
visit.predictor.head()
```
1. The stay_types argument expects a python dictionary with labels as keys and regex as values.
2. The care sites are articulated into levels (cf. [AP-HP's reference structure](https://doc-new.eds.aphp.fr/donnees_dispo/donnees_par_domaine/R%C3%A9f%C3%A9rentielsStructures)).
3. Saving the Probe after computation saves you from having to compute it again. You just use `VisitProbe.load(path=probe_path)`.

``Saved to /my_path/visit.pkl``

| care_site_level          | care_site_id | care_site_short_name | stay_type    | date       | n_visit | c     |
| :----------------------- | :----------- | :------------------- | :----------- | :--------- | :------ | :---- |
| Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'Urg_Hospit' | 2019-05-01 | 233.0   | 0.841 |
| Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'All'        | 2021-04-01 | 393.0   | 0.640 |
| Pôle/DMU                 | 8312027648   | Care site 2          | 'Urg_Hospit' | 2017-03-01 | 204.0   | 0.497 |
| Pôle/DMU                 | 8312027648   | Care site 2          | 'All'        | 2018-08-01 | 22.0    | 0.274 |
| Hôpital                  | 8312022130   | Care site 3          | 'Urg_Hospit' | 2022-02-01 | 9746.0  | 0.769 |

#### 2.2 Filter your Probe

In this example, we consider the poles of three hospitals. We consequently filter data before any further analysis.

```python
from edsteva.probes import VisitProbe

care_site_short_name = ["Hôpital-1", "Hôpital-2", "Hôpital-3"]

filtered_visit = VisitProbe()
filtered_visit.load(path=probe_path)
filtered_visit.filter_care_site(care_site_short_names=care_site_short_name)  # (1)
```

1. To filter care sites there is a dedicated method that also includes all upper and lower levels care sites related to the selected care sites.

#### 2.3 Visualize your Probe
##### Interactive dashboard

Interactive dashboards can be used to visualize the average completeness predictor $c(t)$ of the selected care sites and stay types.

```python
from edsteva.viz.dashboards import probe_dashboard

probe_dashboard(
    probe=filtered_visit,
    care_site_level="Pole",
)
```
Interactive dashboard is available [here](assets/charts/interactive_visit.html)
##### Static plot

If you need a static plot for a report, a paper or anything else, you can use the [`probe_plot()`][edsteva.viz.plots.probe.wrapper] function. It returns the top plot of the dashboard without the interactive filters. Consequently, you have to specify the filters in the inputs of the function.

```python
from edsteva.viz.plots import probe_plot

plot_path = "my_path/visit.html"
stay_type = "All"

probe_plot(
    probe=filtered_visit,
    care_site_level="Hospital",
    stay_type=stay_type,
    save_path=plot_path,  # (1)
)
```

1. If a `save_path` is specified, it'll save your plot in the specified path.

```vegalite
{
  "schema-url": "assets/charts/visit.json"
}
```

### 3. Choose a [Model][available-models] or [create a new Model][defining-a-custom-model]

!!! info "[Model][model]"
    A [Model][model] is a python class designed to fit a function $f_\Theta(t)$ to each completeness predictor $c(t)$ of a [Probe][probe]. The fit process estimates the coefficients $\Theta$ with metrics to characterize the temporal variability of data availability.

In this example, the model fits a step function $f_{t_0, c_0}(t)$ to the completeness predictor $c(t)$ with coefficients $\Theta = (t_0, c_0)$:

$$
f_{t_0, c_0}(t) = c_0 \ \mathbb{1}_{t \geq t_0}(t)
$$

- the characteristic time $t_0$ estimates the time after which the data is available.
- the characteristic value $c_0$ estimates the stabilized routine completeness.

It also computes the following $error$ metric that estimates the stability of the data after $t_0$:

$$
\begin{aligned}
error & = \frac{\sum_{t_0 \leq  t \leq t_{max}} \epsilon(t)^2}{t_{max} - t_0} \\
\epsilon(t) & = f_{t_0, c_0}(t) - c(t)
\end{aligned}
$$

This [step function Model][available-models] is available in the library.
#### 3.1 Fit your [Model](components/model)

The ``fit`` method takes a [Probe][probe] as input, it estimates the coefficients, for example by minimizing a quadratic loss function and computes the metrics. Finally, it stores the estimated coefficients and the computed metrics in the [``estimates``][estimates-schema] attribute of the ``Model``.

```python
from edsteva.models.step_function import StepFunction

model_path = "my_path/fitted_visit.pkl"

step_function_model = StepFunction()
step_function_model.fit(probe=filtered_visit)
step_function_model.save(model_path)  # (1)
step_function_model.estimates.head()
```

1. Saving the Model after fitting saves you from having to fit it again. You just use `StepFunction.load(path=model_path)`.

``Saved to /my_path/fitted_visit.pkl``

| care_site_level | care_site_id | stay_type    | t_0        | c_0   | error |
| :-------------- | :----------- | :----------- | :--------- | :---- | :---- |
| Pôle/DMU        | 8312056386   | 'Urg_Hospit' | 2019-05-01 | 0.397 | 0.040 |
| Pôle/DMU        | 8312056386   | 'All'        | 2017-04-01 | 0.583 | 0.028 |
| Pôle/DMU        | 8312027648   | 'Urg_Hospit' | 2021-03-01 | 0.677 | 0.022 |
| Pôle/DMU        | 8312027648   | 'All'        | 2018-08-01 | 0.764 | 0.014 |
| Pôle/DMU        | 8312022130   | 'Urg_Hospit' | 2022-02-01 | 0.652 | 0.027 |

#### 3.2 Visualize your fitted Probe

##### Interactive dashboard

Interactive dashboards can be used to visualize the average completeness predictor $c(t)$ along with the fitted step function of the selected care sites and stay types.

```python
from edsteva.viz.dashboards import probe_dashboard

probe_dashboard(
    probe=filtered_visit,
    fitted_model=step_function_model,
    care_site_level="Pole",
)
```
Interactive dashboard is available [here](assets/charts/interactive_fitted_visit.html).
##### Static plot

If you need a static plot for a report, a paper or anything else, you can use the [`probe_plot()`][edsteva.viz.plots.probe.wrapper] function. It returns the top plot of the dashboard without the interactive filters. Consequently, you have to specify the filters in the inputs of the function.

```python
from edsteva.viz.plots import probe_plot

plot_path = "my_path/fitted_visit.html"
stay_type = "All"

probe_plot(
    probe=filtered_visit,
    fitted_model=step_function_model,
    care_site_level="Hospital",
    stay_type=stay_type,
    save_path=plot_path,  # (1)
)
```
1. If a `save_path` is specified, it'll save your plot in the specified path.

```vegalite
{
  "schema-url": "assets/charts/fitted_visit.json"
}
```
### 4. Set the thresholds to fix the deployment bias

Now, that we have estimated $t_0$, $c_0$ and $error$ for each care site and each stay type, one can set a threshold for each estimate in order to select only the care sites where the visits are available over the period of interest.

#### 4.1 Visualize estimates distributions

Visualizing the density plots and the medians of the estimates can help you setting the thresholds' values.

```python
from edsteva.viz.plots import estimates_densities_plot

estimates_densities_plot(
    probe=filtered_visit,
    fitted_model=step_function_model,
)
```
```vegalite
{
  "schema-url": "assets/charts/estimates_densities.json"
}
```
#### 4.2 Set the thresholds

The estimates dashboard provides a representation of the overall deviation from the Model on the top and interactive sliders on the bottom that allows you to vary the thresholds. The idea is to set the thresholds that keep the most care sites while having an acceptable overall deviation.

```python
from edsteva.viz.dashboards import estimates_dashboard

estimates_dashboard(
    probe=filtered_visit,
    fitted_model=step_function_model,
    care_site_level="Pole",
)
```


The threshold dashboard is available [here](assets/charts/normalized_probe_dashboard.html).
#### 4.3 Fix the deployment bias

Once you set the thresholds, you can extract for each stay type the care sites for which data availability is estimated to be stable over the entire study period.

```python
t_0_max = "2020-01-01"  # (1)
c_0_min = 0.6  # (2)
error_max = 0.05  # (3)

estimates = step_function_model.estimates
selected_care_site = estimates[
    (estimates["t_0"] <= t_0_max)
    & (estimates["c_0"] >= c_0_min)
    & (estimates["error"] <= error_max)
]
print(selected_care_site["care_site_id"].unique())
```

1. In this example the study period starts on January 1, 2020.
2. The characteristic value $c_0$ estimates the stabilized routine completeness. As we want the selected care sites to have a good completeness after $t_0$, one can for example set the threshold around the median (cf. [distribution][41-visualize-estimates-distributions]) to keep half of the care sites with the highest completeness after $t_0$.
3. $error$ estimates the stability of the data after $t_0$. As we want the selected care sites to be stable after $t_0$, one can set the threshold around the median (cf. [distribution][41-visualize-estimates-distributions]) to keep half of the care sites with the lowest error after $t_0$.

```
[8312056386, 8457691845, 8745619784, 8314578956, 8314548764, 8542137845]
```

In this example, $c_0$ and $error$ thresholds have been set around the median (cf. [distribution][41-visualize-estimates-distributions]). However, this method is arbitrary and you have to find the appropriate method for your study with the help of the [estimate dashboard](assets/charts/normalized_probe_dashboard.html).

!!!danger "Limitations"
    EDS-TeVa provides modelling tools to characterize the temporal variability of your data, it does not intend to provide direct methods to fix the deployment bias. As an open-source library, EDS-TeVa is also here to host a discussion in order to facilitate collective methodological convergence on flexible solutions. The default methods proposed in this example is intended to be reviewed and challenged by the user community.

## Make it your own

The working example above describes the canonical usage workflow. However, you would probably need different Probes, Models, Visualizations and methods to set the thresholds for your projects. The components already available in the library are listed below but if it doesn't meet your requirements, you are encouraged to create your own.

!!!success "Contribution"
    If you managed to implement your own component, or even if you just thought about a new component do not hesitate to share it with the community by following the [contribution guidelines][contributing]. Contributions are welcome, and they are greatly appreciated! Every little bit helps, and credit will always be given.
### Available components

=== "Probe"

    === "VisitProbe"

        The [``VisitProbe``][edsteva.probes.visit.visit.VisitProbe] computes $c_{visit}(t)$ the availability of administrative stays:

        === "per_visit_default"

            $$
            c(t) = \frac{n_{visit}(t)}{n_{max}}
            $$

            Where $n_{visit}(t)$ is the number of administrative stays, $t$ is the month and $n_{max} = \max_{t}(n_{visit}(t))$.

            !!!info ""
                If the maximum number of records per month $n_{max}$ is equal to 0, we consider that the completeness predictor $c(t)$ is also equal to 0.

            ```python
            from edsteva.probes import VisitProbe

            visit = VisitProbe()
            visit.compute(
                data,
                stay_types={
                    "Urg": "urgence",
                    "Hospit": "hospitalisés",
                    "Urg_Hospit": "urgence|hospitalisés",
                },
            )
            visit.predictor.head()
            ```

            | care_site_level          | care_site_id | care_site_short_name | stay_type    | date       | n_visit | c     |
            | :----------------------- | :----------- | :------------------- | :----------- | :--------- | :------ | :---- |
            | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'Urg'        | 2019-05-01 | 233.0   | 0.841 |
            | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'Urg'        | 2021-04-01 | 393.0   | 0.640 |
            | Pôle/DMU                 | 8312027648   | Care site 2          | 'Hospit'     | 2017-03-01 | 204.0   | 0.497 |
            | Pôle/DMU                 | 8312027648   | Care site 2          | 'Urg'        | 2018-08-01 | 22.0    | 0.274 |
            | Hôpital                  | 8312022130   | Care site 3          | 'Urg_Hospit' | 2022-02-01 | 9746.0  | 0.769 |

    === "NoteProbe"

        The [``NoteProbe``][edsteva.probes.note.note.NoteProbe] computes $c_{note}(t)$ the availability of clinical documents:

        === "per_visit_default"

            The [``per_visit_default``][edsteva.probes.note.completeness_predictors.per_visit] algorithm computes $c_(t)$ the availability of clinical documents linked to patients' administrative stays:

            $$
            c(t) = \frac{n_{with\,doc}(t)}{n_{visit}(t)}
            $$

            Where $n_{visit}(t)$ is the number of administrative stays, $n_{with\,doc}$ the number of visits having at least one document and $t$ is the month.

            !!!info ""
                If the number of visits $n_{visit}(t)$ is equal to 0, we consider that the completeness predictor $c(t)$ is also equal to 0.

            ```python
            from edsteva.probes import NoteProbe

            note = Note(completeness_predictor="per_visit_default")
            note.compute(
                data,
                stay_types={
                    "Urg": "urgence",
                    "Hospit": "hospitalisés",
                    "Urg_Hospit": "urgence|hospitalisés",
                },
                note_types={
                    "All": ".*",
                    "CRH": "crh",
                    "Ordonnance": "ordo",
                    "CR Passage Urgences": "urge",
                },
            )
            note.predictor.head()
            ```

            | care_site_level          | care_site_id | care_site_short_name | stay_type    | note_type             | date       | n_visit | n_visit_with_note | c     |
            | :----------------------- | :----------- | :------------------- | :----------- | :-------------------- | :--------- | :------ | :---------------- | :---- |
            | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'Urg'        | 'All'                 | 2019-05-01 | 233.0   | 196.0             | 0.841 |
            | Unité Fonctionnelle (UF) | 8653815660   | Care site 1          | 'Hospit'     | 'CRH'                 | 2017-04-01 | 393.0   | 252.0             | 0.640 |
            | Pôle/DMU                 | 8312027648   | Care site 2          | 'Hospit'     | 'CRH'                 | 2021-03-01 | 204.0   | 101.0             | 0.497 |
            | Pôle/DMU                 | 8312056379   | Care site 2          | 'Urg'        | 'Ordonnance'          | 2018-08-01 | 22.0    | 6.0               | 0.274 |
            | Hôpital                  | 8312022130   | Care site 3          | 'Urg_Hospit' | 'CR Passage Urgences' | 2022-02-01 | 9746.0  | 7495.0            | 0.769 |

        === "per_note_default"

            The [``per_note_default``][edsteva.probes.note.completeness_predictors.per_note] algorithm computes $c_(t)$ the availability of clinical documents as follow:

            $$
            c(t) = \frac{n_{note}(t)}{n_{max}}
            $$

            Where $n_{note}(t)$ is the number of clinical documents, $t$ is the month and $n_{max} = \max_{t}(n_{note}(t))$.

            !!!info ""
                If the maximum number of recorded notes per month $n_{max}$ is equal to 0, we consider that the completeness predictor $c(t)$ is also equal to 0.

            ```python
            from edsteva.probes import NoteProbe

            note = Note(completeness_predictor="per_note_default")
            note.compute(
                data,
                stay_types={
                    "Urg": "urgence",
                    "Hospit": "hospitalisés",
                    "Urg_Hospit": "urgence|hospitalisés",
                },
                note_types={
                    "All": ".*",
                    "CRH": "crh",
                    "Ordonnance": "ordo",
                    "CR Passage Urgences": "urge",
                },
            )
            note.predictor.head()
            ```

            | care_site_level          | care_site_id | care_site_short_name | stay_type    | note_type             | date       | n_note | c     |
            | :----------------------- | :----------- | :------------------- | :----------- | :-------------------- | :--------- | :----- | :---- |
            | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'Urg'        | 'All'                 | 2019-05-01 | 233.0  | 0.841 |
            | Unité Fonctionnelle (UF) | 8653815660   | Care site 1          | 'Hospit'     | 'CRH'                 | 2017-04-01 | 393.0  | 0.640 |
            | Pôle/DMU                 | 8312027648   | Care site 2          | 'Hospit'     | 'CRH'                 | 2021-03-01 | 204.0  | 0.497 |
            | Pôle/DMU                 | 8312056379   | Care site 2          | 'Urg'        | 'Ordonnance'          | 2018-08-01 | 22.0   | 0.274 |
            | Hôpital                  | 8312022130   | Care site 3          | 'Urg_Hospit' | 'CR Passage Urgences' | 2022-02-01 | 9746.0 | 0.769 |

    === "ConditionProbe"

        The [``ConditionProbe``][edsteva.probes.condition.condition.ConditionProbe] computes $c_{condition}(t)$ the availability of claim data:

        === "per_visit_default"

            The [``per_visit_default``][edsteva.probes.condition.completeness_predictors.per_visit] algorithm computes $c_(t)$ the availability of claim data linked to patients' administrative stays:

            $$
            c(t) = \frac{n_{with\,condition}(t)}{n_{visit}(t)}
            $$

            Where $n_{visit}(t)$ is the number of administrative stays, $n_{with\,condition}$ the number of stays having at least one claim code (e.g. ICD-10) recorded and $t$ is the month.

            !!!info ""
                If the number of visits $n_{visit}(t)$ is equal to 0, we consider that the completeness predictor $c(t)$ is also equal to 0.

            !!!Warning "Care site level"
                AREM claim data are only available at hospital level.

            ```python
            from edsteva.probes import ConditionProbe

            condition = ConditionProbe(completeness_predictor="per_visit_default")
            condition.compute(
                data,
                stay_types={
                    "Hospit": "hospitalisés",
                },
                diag_types={
                    "All": ".*",
                    "DP/DR": "DP|DR",
                },
                condition_types={
                    "All": ".*",
                    "Pulmonary_embolism": "I26",
                },
                source_systems=["AREM", "ORBIS"],
            )
            condition.predictor.head()
            ```

            | care_site_level          | care_site_id | care_site_short_name | stay_type | diag_type | condition_type       | source_systems | date       | n_visit | n_visit_with_condition | c     |
            | :----------------------- | :----------- | :------------------- | :-------- | :-------- | :------------------- | :------------- | :--------- | :------ | :--------------------- | :---- |
            | Hôpital                  | 8312057527   | Care site 1          | 'Hospit'  | 'All'     | 'Pulmonary_embolism' | AREM           | 2019-05-01 | 233.0   | 196.0                  | 0.841 |
            | Hôpital                  | 8312057527   | Care site 1          | 'Hospit'  | 'DP/DR'   | 'Pulmonary_embolism' | AREM           | 2021-04-01 | 393.0   | 252.0                  | 0.640 |
            | Hôpital                  | 8312027648   | Care site 2          | 'Hospit'  | 'All'     | 'Pulmonary_embolism' | AREM           | 2017-03-01 | 204.0   | 101.0                  | 0.497 |
            | Unité Fonctionnelle (UF) | 8312027648   | Care site 2          | 'Hospit'  | 'All'     | 'All'                | ORBIS          | 2018-08-01 | 22.0    | 6.0                    | 0.274 |
            | Pôle/DMU                 | 8312022130   | Care site 3          | 'Hospit'  | 'DP/DR'   | 'Pulmonary_embolism' | ORBIS          | 2022-02-01 | 9746.0  | 7495.0                 | 0.769 |

        === "per_condition_default"

            The [``per_condition_default``][edsteva.probes.condition.completeness_predictors.per_condition] algorithm computes $c_(t)$ the availability of claim data as follow:

            $$
            c(t) = \frac{n_{condition}(t)}{n_{max}}
            $$

            Where $n_{condition}(t)$ is the number of claim codes (e.g. ICD-10) recorded, $t$ is the month and $n_{max} = \max_{t}(n_{condition}(t))$.

            !!!info ""
                If the maximum number of recorded diagnosis per month $n_{max}$ is equal to 0, we consider that the completeness predictor $c(t)$ is also equal to 0.

            ```python
            from edsteva.probes import ConditionProbe

            condition = ConditionProbe(completeness_predictor="per_condition_default")
            condition.compute(
                data,
                stay_types={
                    "All": ".*",
                    "Hospit": "hospitalisés",
                },
                diag_types={
                    "All": ".*",
                    "DP/DR": "DP|DR",
                },
                condition_types={
                    "All": ".*",
                    "Pulmonary_embolism": "I26",
                },
                source_systems=["AREM", "ORBIS"],
            )
            condition.predictor.head()
            ```

            | care_site_level          | care_site_id | care_site_short_name | stay_type | diag_type | condition_type       | source_systems | date       | n_condition | c     |
            | :----------------------- | :----------- | :------------------- | :-------- | :-------- | :------------------- | :------------- | :--------- | :---------- | :---- |
            | Hôpital                  | 8312057527   | Care site 1          | 'Hospit'  | 'All'     | 'Pulmonary_embolism' | AREM           | 2019-05-01 | 233.0       | 0.841 |
            | Hôpital                  | 8312057527   | Care site 1          | 'Hospit'  | 'DP/DR'   | 'Pulmonary_embolism' | AREM           | 2021-04-01 | 393.0       | 0.640 |
            | Hôpital                  | 8312027648   | Care site 2          | 'Hospit'  | 'All'     | 'Pulmonary_embolism' | AREM           | 2017-03-01 | 204.0       | 0.497 |
            | Unité Fonctionnelle (UF) | 8312027648   | Care site 2          | 'Hospit'  | 'All'     | 'All'                | ORBIS          | 2018-08-01 | 22.0        | 0.274 |
            | Pôle/DMU                 | 8312022130   | Care site 3          | 'Hospit'  | 'DP/DR'   | 'Pulmonary_embolism' | ORBIS          | 2022-02-01 | 9746.0      | 0.769 |

    === "BiologyProbe"

        The [``BiologyProbe``][edsteva.probes.biology.biology.BiologyProbe] computes $c_(t)$ the availability of laboratory data:

        === "per_visit_default"

            The [``per_visit_default``][edsteva.probes.biology.completeness_predictors.per_visit] algorithm computes $c_(t)$ the availability of laboratory data linked to patients' administrative stays:

            $$
            c(t) = \frac{n_{with\,biology}(t)}{n_{visit}(t)}
            $$

            Where $n_{visit}(t)$ is the number of administrative stays, $n_{with\,biology}$ the number of stays having at least one biological measurement recorded and $t$ is the month.

            !!!info ""
                If the number of visits $n_{visit}(t)$ is equal to 0, we consider that the completeness predictor $c(t)$ is also equal to 0.

            !!!Warning "Care site level"
                Laboratory data are only available at hospital level.

            ```python
            from edsteva.probes import BiologyProbe

            biology = BiologyProbe(completeness_predictor="per_visit_default")
            biology.compute(
                data,
                stay_types={
                    "Hospit": "hospitalisés",
                },
                concepts_sets={
                    "Créatinine": "E3180|G1974|J1002|A7813|A0094|G1975|J1172|G7834|F9409|F9410|C0697|H4038|F2621",
                    "Leucocytes": "A0174|K3232|H6740|E4358|C9784|C8824|E6953",
                },
            )
            biology.predictor.head()
            ```

            | care_site_level | care_site_id | care_site_short_name | stay_type | concepts_sets | date       | n_visit | n_visit_with_measurement | c     |
            | :-------------- | :----------- | :------------------- | :-------- | :------------ | :--------- | :------ | :----------------------- | :---- |
            | Hôpital         | 8312057527   | Care site 1          | 'Hospit'  | 'Créatinine'  | 2019-05-01 | 233.0   | 196.0                    | 0.841 |
            | Hôpital         | 8312057527   | Care site 1          | 'Hospit'  | 'Leucocytes'  | 2021-04-01 | 393.0   | 252.0                    | 0.640 |
            | Hôpital         | 8312027648   | Care site 2          | 'Hospit'  | 'Créatinine'  | 2017-03-01 | 204.0   | 101.0                    | 0.497 |
            | Hôpital         | 8312027648   | Care site 2          | 'Hospit'  | 'Leucocytes'  | 2018-08-01 | 22.0    | 6.0                      | 0.274 |
            | Hôpital         | 8312022130   | Care site 3          | 'Hospit'  | 'Leucocytes'  | 2022-02-01 | 9746.0  | 7495.0                   | 0.769 |

        === "per_measurement_default"

            The [``per_measurement_default``][edsteva.probes.biology.completeness_predictors.per_measurement] algorithm computes $c_(t)$ the availability of biological measurements:

            $$
            c(t) = \frac{n_{biology}(t)}{n_{max}}
            $$

            Where $n_{biology}(t)$ is the number of biological measurements, $t$ is the month and $n_{max} = \max_{t}(n_{biology}(t))$.

            !!!info ""
                If the maximum number of recorded biological measurements per month $n_{max}$ is equal to 0, we consider that the completeness predictor $c(t)$ is also equal to 0.

            !!!Warning "Care site level"
                Laboratory data are only available at hospital level.

            ```python
            from edsteva.probes import BiologyProbe

            biology = BiologyProbe(completeness_predictor="per_measurement_default")
            biology.compute(
                data,
                stay_types={
                    "Hospit": "hospitalisés",
                },
                concepts_sets={
                    "Créatinine": "E3180|G1974|J1002|A7813|A0094|G1975|J1172|G7834|F9409|F9410|C0697|H4038|F2621",
                    "Leucocytes": "A0174|K3232|H6740|E4358|C9784|C8824|E6953",
                },
            )
            biology.predictor.head()
            ```

            | care_site_level          | care_site_id | care_site_short_name | stay_type | concepts_sets | date       | n_measurement | c     |
            | :----------------------- | :----------- | :------------------- | :-------- | :------------ | :--------- | :------------ | :---- |
            | Hôpital                  | 8312057527   | Care site 1          | 'Hospit'  | 'Créatinine'  | 2019-05-01 | 233.0         | 0.841 |
            | Hôpital                  | 8312057527   | Care site 1          | 'Hospit'  | 'Leucocytes'  | 2021-04-01 | 393.0         | 0.640 |
            | Hôpital                  | 8312027648   | Care site 2          | 'Hospit'  | 'Créatinine'  | 2017-03-01 | 204.0         | 0.497 |
            | Unité Fonctionnelle (UF) | 8312027648   | Care site 2          | 'Hospit'  | 'Leucocytes'  | 2018-08-01 | 22.0          | 0.274 |
            | Pôle/DMU                 | 8312022130   | Care site 3          | 'Hospit'  | 'Leucocytes'  | 2022-02-01 | 9746.0        | 0.769 |

=== "Model"

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
                    You can define your own algorithm if they don't meet your requirements.

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
                    For complexity purposes, this algorithm has been implemented with a dependency relation between $c_0$ and $t_0$ derived from the optimal estimates using the $l_2$ loss function. For more informations, you can have a look on the [source code][edsteva.models.step_function.algos.loss_minimization.loss_minimization].

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
            | Unité Fonctionnelle (UF) | 8312056386   | 'All'     | 2017-04-01 | 0.583 | 0.028 |
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
            | Unité Fonctionnelle (UF) | 8312056386   | 'All'     | 2017-04-01 | 0.583 | 2013-04-01 | 0.028 |
            | Pôle/DMU                 | 8312027648   | 'Hospit'  | 2021-03-01 | 0.677 | 2022-03-01 | 0.022 |
            | Pôle/DMU                 | 8312027648   | 'All'     | 2018-08-01 | 0.764 | 2019-08-01 | 0.014 |
            | Hôpital                  | 8312022130   | 'Hospit'  | 2022-02-01 | 0.652 | 2022-08-01 | 0.027 |

=== "Visualization"

    === "Dashboard"

        The library provides interactive dashboards that let you set any combination of care sites, stay types and other columns if included in the Probe. You can only export a dashboard in HTML format.

        === "probe_dashboard()"

            The [``probe_dashboard()``][edsteva.viz.dashboards.probe.wrapper] returns:

            - On the top, the aggregated variable is the average completeness predictor $c(t)$ over time $t$ with the prediction $\hat{c}(t)$ if the [fitted Model][model] is specified.
            - On the bottom, the interactive filters are all the columns included in the [Probe][probe] (such as time, care site, number of visits...etc.).

            ```python
            from edsteva.viz.dashboards import probe_dashboard

            probe_dashboard(
                probe=probe,
                fitted_model=step_function_model,
                care_site_level=care_site_level,
            )
            ```
            An example is available [here](assets/charts/interactive_fitted_visit.html).

        === "normalized_probe_dashboard()"

            The [``normalized_probe_dashboard()``][edsteva.viz.dashboards.normalized_probe.normalized_probe] returns a representation of the overall deviation from the [Model][model]:

            - On the top, the aggregated variable is a normalized completeness predictor $\frac{c(t)}{c_0}$ over normalized time $t - t_0$.
            - On the bottom, the interactive filters are all the columns included in the [Probe][probe] (such as time, care site, number of visits...etc.) with all the [Model coefficients][model-coefficients] and [metrics][metrics] included in the [Model][model].

            ```python
            from edsteva.viz.dashboards import normalized_probe_dashboard

            normalized_probe_dashboard(
                probe=probe,
                fitted_model=step_function_model,
                care_site_level=care_site_level,
            )
            ```

            An example is available [here](assets/charts/normalized_probe_dashboard.html).

    === "Plot"

        The library provides static plots that you can export in png or svg. As it is less interactive, you may specify the filters in the inputs of the functions.

        === "probe_plot()"

            The [``probe_plot()``][edsteva.viz.plots.probe.wrapper] returns the top plot of the [``probe_dashboard()``][edsteva.viz.dashboards.probe.wrapper]: the normalized completeness predictor $\frac{c(t)}{c_0}$ over normalized time $t - t_0$.

            ```python
            from edsteva.viz.plots import probe_plot

            probe_plot(
                probe=probe,
                fitted_model=step_function_model,
                care_site_level=care_site_level,
                stay_type=stay_type,
                save_path=plot_path,
            )
            ```

            ```vegalite
            {
            "schema-url": "assets/charts/fitted_visit.json"
            }
            ```

        === "normalized_probe_plot()"

            The [``normalized_probe_plot()``][edsteva.viz.plots.normalized_probe] returns the top plot of the [``normalized_probe_dashboard()``][edsteva.viz.dashboards.normalized_probe.normalized_probe]. Consequently, you have to specify the filters in the inputs of the function.

            ```python
            from edsteva.viz.plots import normalized_probe_plot

            normalized_probe_plot(
                probe=probe,
                fitted_model=step_function_model,
                care_site_level=care_site_level,
                stay_type=stay_type,
                save_path=plot_path,
            )
            ```
            ```vegalite
            {
            "schema-url": "assets/charts/normalized_probe.json"
            }
            ```


        === "estimates_densities_plot()"

            The [``estimates_densities_plot()``][edsteva.viz.plots.estimates_densities] returns the density plot and the median of each estimate. It can help you to set the thresholds.

            ```python
            from edsteva.viz.plots import estimates_densities_plot

            estimates_densities_plot(
                fitted_model=step_function_model,
            )
            ```
            ```vegalite
            {
            "schema-url": "assets/charts/estimates_densities.json"
            }
            ```
