from typing import Callable, List

import pandas as pd

from edsteva.metrics import error_after_t0
from edsteva.models import BaseModel
from edsteva.models.step_function import algos


class StepFunction(BaseModel):
    r"""It models the completeness predictor $c(t)$ as a step function $f_{t_0, c_0}(t)$ as follow:

    $$
    f_{t_0, c_0}(t) = c_0 \ \mathbb{1}_{t \geq t_0}(t)
    $$

    It computes the following estimates $(t_0, c_0)$:

    - the characteristic time $t_0$ estimates the time after which the data is available
    - the characteristic value $c_0$ estimates the stabilized routine completeness

    Attributes
    ----------
    _coefs: List[str]
        Model coefficients

        **VALUE**: ``["t_0", "c_0"]``

    Example
    ----------

    ```python
    from edsteva.models.step_function import StepFunction

    step_function_model = StepFunction()
    step_function_model.fit(probe)
    step_function_model.estimates.head()
    ```

    | care_site_level          | care_site_id | stay_type    | t_0        | c_0   |
    | :----------------------- | :----------- | :----------- | :--------- | :---- |
    | Unité Fonctionnelle (UF) | 8312056386   | 'Urg_Hospit' | 2019-05-01 | 0.397 |
    | Unité Fonctionnelle (UF) | 8312056386   | 'All'        | 2011-04-01 | 0.583 |
    | Pôle/DMU                 | 8312027648   | 'Urg_Hospit' | 2021-03-01 | 0.677 |
    | Pôle/DMU                 | 8312027648   | 'All'        | 2018-08-01 | 0.764 |
    | Hôpital                  | 8312022130   | 'Urg_Hospit' | 2022-02-01 | 0.652 |
    """

    _coefs = ["t_0", "c_0"]

    def fit_process(
        self,
        predictor: pd.DataFrame,
        index: List[str] = None,
        algo: Callable = algos.loss_minimization,
        **kwargs
    ) -> None:
        """Script to be used by [``fit()``][edsteva.models.base.BaseModel.fit]

        Parameters
        ----------
        predictor : pd.DataFrame
            Target variable to be fitted
        index : List[str], optional
            Variable from which data is grouped

            **EXAMPLE**: `["care_site_level", "stay_type", "note_type", "care_site_id"]`
        algo : Callable, optional
            Algorithm used for the coefficients estimation ($t_0$ and $c_0$)
        """

        estimates = algo(predictor=predictor, index=index, **kwargs)

        return estimates[index + self._coefs]

    def predict_process(
        self,
        predictor: pd.DataFrame,
        index: List[str],
    ) -> pd.DataFrame:
        """Script to be used by [``predict()``][edsteva.models.base.BaseModel.predict]

        Parameters
        ----------
        predictor : pd.DataFrame
            Target DataFrame to be predicted
        index : List[str]
            List of the columns given by Probe._index

        Returns
        -------
        pd.DataFrame
            Prediction

        Raises
        ------
        Exception
            Some indexes have no associated estimates, the model must be fitted on an adequate probe

        Examples
        --------
        | care_site_level          | care_site_id | care_site_short_name | stay_type    | date       | n_visit | c     | c_fit |
        | :----------------------- | :----------- | :------------------- | :----------- | :--------- | :------ | :---- | :---- |
        | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'Urg_Hospit' | 2019-05-01 | 233.0   | 0.841 | 0.758 |
        | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'All'        | 2021-04-01 | 393.0   | 0.640 | 0.758 |
        | Pôle/DMU                 | 8312027648   | Care site 2          | 'Urg_Hospit' | 2011-03-01 | 204.0   | 0.497 | 0     |
        | Pôle/DMU                 | 8312027648   | Care site 2          | 'All'        | 2018-08-01 | 22.0    | 0.784 | 0.874 |
        | Hôpital                  | 8312022130   | Care site 3          | 'Urg_Hospit' | 2022-02-01 | 9746.0  | 0.974 | 0.912 |
        """

        prediction = self.is_predictable_probe(predictor=predictor, index=index)

        prediction["c_hat"] = prediction["c_0"].where(
            prediction["date"] >= prediction["t_0"], 0
        )
        return prediction.drop(columns=self._coefs + self._metrics)

    def default_metrics(
        self,
        predictor: pd.DataFrame,
        estimates: pd.DataFrame,
        index: List[str],
    ):
        r"""Default metrics used if metric_functions is set to None. Here the default metric is the mean squared error computed after $t_0$

        Parameters
        ----------
        predictor : pd.DataFrame
            Target DataFrame describing the completeness predictor $c(t)$
        estimates : pd.DataFrame
            Target DataFrame describing the estimates $(\hat{t_0}, \hat{c_0})$
        index : List[str]
            Variable from which data is grouped

            **EXAMPLE**: `["care_site_level", "stay_type", "note_type", "care_site_id"]`
        """
        return error_after_t0(predictor=predictor, estimates=estimates, index=index)
