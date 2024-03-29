# Probe

[Choosing][available-probes] or [customizing][defining-a-custom-probe] a Probe is the second step in the [EDS-TeVa usage workflow][2-choose-a-probe-or-create-a-new-probe].
## Definition

A **Probe** is a python class designed to characterize data availability of a target variable over time $t$. It aggregates the [loaded data][loading-data] to obtain a completeness predictor $c(t)$.

<figure markdown>
  ![Image title](../assets/uml_class/probe_class.svg)
  <figcaption>Probe class diagram</figcaption>
</figure>

### Input

As detailled in [the dedicated section][loading-data], the Probe class is expecting a [**``Data``**][edsteva.io] object with [Pandas](https://pandas.pydata.org/) or [Koalas](https://koalas.readthedocs.io/en/latest/) DataFrames.  We provide various connectors to facilitate data fetching, namely a [Hive][loading-from-hive-hivedata] connector, a [Postgres][loading-from-postgres-postgresdata] connector and a [LocalData][persistingreading-a-sample-tofrom-disk-localdata].

### Attributes

- [**``predictor``**][edsteva.probes.base.BaseProbe] is a [`Pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/version/1.3/reference/api/pandas.DataFrame.html) computed by the [`compute()`][edsteva.probes.base.BaseProbe.compute] method. It contains the desired completeness predictor $c(t)$ for each column in the [`_index`][edsteva.probes.visit.visit.VisitProbe] attribute (care site, stay type and any other needed column).
- [**`_index`**][edsteva.probes.visit.visit.VisitProbe] is the list of columns that are used to aggregate the data in the [`compute()`][edsteva.probes.base.BaseProbe.compute] method.

### Methods

- [**`compute()`**][edsteva.probes.base.BaseProbe.compute] method calls the [`compute_process()`][edsteva.probes.visit.visit.VisitProbe.compute_process] method to compute the completeness predictors $c(t)$ and store them in the [`predictor`][edsteva.probes.base.BaseProbe] attribute.
- [**`compute_process()`**][edsteva.probes.visit.visit.VisitProbe.compute_process]  method aggregates the input data to compute the completeness predictors $c(t)$.
- [**`filter_care_site()`**][edsteva.probes.base.BaseProbe.filter_care_site] method filters [`predictor`][edsteva.probes.base.BaseProbe] attribute on the selected care sites including upper and lower levels care sites.
- [**`save()`**][edsteva.probes.base.BaseProbe.save] method saves the [`Probe`][edsteva.probes.base.BaseProbe] in the desired path. By default it is saved in the the cache directory (~/.cache/edsteva/probes).
- [**`load()`**][edsteva.probes.base.BaseProbe.load] method loads the [`Probe`][edsteva.probes.base.BaseProbe] from the desired path.  By default it is loaded from the the cache directory (~/.cache/edsteva/probes).


## Predictor schema

Data stored in ``predictor`` attribute follows a specific schema:

### Predictors

It must include a completeness predictor $c(t)$:

- **`c`**: value of the completeness predictor $c(t)$.

Then, it can have any other extra predictor you find useful such as:

- **`n_visit`**: the number of visits.

!!! warning "Extra predictor"
    The extra predictors must be additive to be aggregated properly in the dashboards. For instance, the number of visits is additive but the $99^{th}$ percentile is not.

### Indexes

It must include one and only one time related column:

- **`date`**: date of the event associated with the target variable (by default, the dates are truncated to the month in which the event occurs).

Then, it can have any other string type column such as:

- **`care_site_level`**: care site hierarchic level (`uf`, `pole`, ``hospital``).
- **`care_site_id`**: care site unique identifier.
- **`stay_type`**: type of stay (``hospitalisés``, ``urgence``, ``hospitalisation incomplète``, ``consultation externe``).
- **`note_type`**: type of note (``CRH``, ``Ordonnance``, ``CR Passage Urgences``).

### Example

When considering the availability of clinical notes, a [``NoteProbe.predictor``][edsteva.probes.note.note.NoteProbe] may for instance look like this:

| care_site_level          | care_site_id | care_site_short_name | stay_type    | note_type             | date       | n_visit | c      |
| :----------------------- | :----------- | :------------------- | :----------- | :-------------------- | :--------- | :------ | :----- |
| Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'Urg_Hospit' | 'All'                 | 2019-05-01 | 233.0   | '0.841 |
| Unité Fonctionnelle (UF) | 8653815660   | Care site 1          | 'All'        | 'CRH'                 | 2011-04-01 | 393.0   | 0.640  |
| Pôle/DMU                 | 8312027648   | Care site 2          | 'Urg_Hospit' | 'CRH'                 | 2021-03-01 | 204.0   | 0.497  |
| Pôle/DMU                 | 8312056379   | Care site 2          | 'All'        | 'Ordonnance'          | 2018-08-01 | 22.0    | 0.274  |
| Hôpital                  | 8312022130   | Care site 3          | 'Urg_Hospit' | 'CR Passage Urgences' | 2022-02-01 | 9746.0  | 0.769  |

## Saving and loading a computed Probe

In order to ease the future loading of a Probe that has been computed with the [`compute()`][edsteva.probes.base.BaseProbe.compute] method, one can pickle it using the [``save()``][edsteva.probes.base.BaseProbe.save] method. This enables a rapid loading of the Probe from local disk using the [``load()``][edsteva.probes.base.BaseProbe.load] method.

```python
from edsteva.probes import NoteProbe

note = NoteProbe()

note.compute(data)  # (1)
note.save()  # (2)

note_2 = NoteProbe()
note_2.load()  # (3)
```

1. Computation of the Probe querying the database (long).
2. Saving of the Probe on the local disk.
3. Rapid loading of the Probe fom the local disk.

## Defining a custom Probe

If none of the available Probes meets your requirements, you may want to create your own. To define a custom Probe class ``CustomProbe`` that inherits from the abstract class [``BaseProbe``][edsteva.probes.base.BaseProbe] you'll have to implement the [`compute_process()`][edsteva.probes.visit.visit.VisitProbe.compute_process] method (this method is natively called by the [`compute()`][edsteva.probes.base.BaseProbe.compute] method inherited by the [``BaseProbe``][edsteva.probes.base.BaseProbe] class). You'll also have to define the [``_index``][edsteva.probes.visit.visit.VisitProbe] attribute which is the list of columns that are used to aggregate the data in the [`compute_process()`][edsteva.probes.visit.visit.VisitProbe.compute_process] method.

```python
from edsteva.probes import BaseProbe


# Definition of a new Probe class
class CustomProbe(BaseProbe):
    def __init__(
        self,
    ):
        self._index = ["my_custom_column_1", "my_custom_column_2"]
        super().__init__(
            index=self._index,
        )

    def compute_process(
        self,
        data: Data,
        **kwargs,
    ):
        # query using Pandas API
        return custom_predictor
```

[`compute_process()`][edsteva.probes.visit.visit.VisitProbe.compute_process] can take as much as argument as you need but it must include a [``data``][edsteva.io] argument and must return a [`Pandas.DataFrame`](https://pandas.pydata.org/pandas-docs/version/1.3/reference/api/pandas.DataFrame.html) which contains at least the columns of the [standard schema of a predictor](#predictor-schema). For a detailed example of the implementation of a Probe, please have a look on the implemented Probes such as [``VisitProbe``][edsteva.probes.visit.visit.VisitProbe] or [``NoteProbe``][edsteva.probes.note.note.NoteProbe].

!!!success "Contributions"
    If you managed to create your own Probe do not hesitate to share it with the community by following the [contribution guidelines][contributing]. Contributions are welcome, and they are greatly appreciated! Every little bit helps, and credit will always be given.

## Available Probes

We list hereafter the Probes that have already been implemented in the library.

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
        | Pôle/DMU                 | 8312027648   | Care site 2          | 'Hospit'     | 2011-03-01 | 204.0   | 0.497 |
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
        | Unité Fonctionnelle (UF) | 8653815660   | Care site 1          | 'Hospit'     | 'CRH'                 | 2011-04-01 | 393.0   | 252.0             | 0.640 |
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
        | Unité Fonctionnelle (UF) | 8653815660   | Care site 1          | 'Hospit'     | 'CRH'                 | 2011-04-01 | 393.0  | 0.640 |
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
        | Hôpital                  | 8312027648   | Care site 2          | 'Hospit'  | 'All'     | 'Pulmonary_embolism' | AREM           | 2011-03-01 | 204.0   | 101.0                  | 0.497 |
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
        | Hôpital                  | 8312027648   | Care site 2          | 'Hospit'  | 'All'     | 'Pulmonary_embolism' | AREM           | 2011-03-01 | 204.0       | 0.497 |
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
        | Hôpital         | 8312027648   | Care site 2          | 'Hospit'  | 'Créatinine'  | 2011-03-01 | 204.0   | 101.0                    | 0.497 |
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
        | Hôpital                  | 8312027648   | Care site 2          | 'Hospit'  | 'Créatinine'  | 2011-03-01 | 204.0         | 0.497 |
        | Unité Fonctionnelle (UF) | 8312027648   | Care site 2          | 'Hospit'  | 'Leucocytes'  | 2018-08-01 | 22.0          | 0.274 |
        | Pôle/DMU                 | 8312022130   | Care site 3          | 'Hospit'  | 'Leucocytes'  | 2022-02-01 | 9746.0        | 0.769 |
