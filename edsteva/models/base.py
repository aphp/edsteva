from abc import ABCMeta, abstractmethod
from functools import reduce
from typing import Callable, List

import pandas as pd

from edsteva import CACHE_DIR
from edsteva.probes.base import BaseProbe
from edsteva.probes.utils import (
    delete_object,
    filter_table_by_date,
    load_object,
    save_object,
)


class BaseModel(metaclass=ABCMeta):

    """Base class for Models

    Attributes
    ----------
     _coefs: List[str]
        The list of the Model coefficients
    estimates: pd.DataFrame
        Available with the [``fit()``][edsteva.models.base.BaseModel.fit] method
    _metrics: List[str]
        Available with the [``fit()``][edsteva.models.base.BaseModel.fit] method

        The list of computed metrics if any
    params: List[str]
        Available with the [``fit()``][edsteva.models.base.BaseModel.fit] method

        Ths list of extra keyword parameters used.
    """

    def __init__(self):
        self.is_valid_model()
        self.name = type(self).__name__

    def is_valid_model(self) -> None:
        """Raises an error if the instantiated Model is not valid"""
        if not hasattr(self, "_coefs"):
            raise Exception(
                "Model must have _coefs attribute. Please review the code of your model"
            )

    def is_computed_estimates(self) -> None:
        """Raises an error if the Probe has not been fitted properly"""
        if hasattr(self, "estimates"):
            if isinstance(self.estimates, pd.DataFrame):
                if len(self.estimates) == 0:
                    raise Exception(
                        "Estimates are empty, please review the process method or your arguments"
                    )
            else:
                raise Exception(
                    "The fit process must return a Pandas Dataframe and not {}".format(
                        type(self.estimates)
                    )
                )

        else:
            raise Exception(
                "Model has not been fitted, please use the fit method as follow: Model.fit()"
            )

    @abstractmethod
    def fit_process(
        self,
        predictor: pd.DataFrame,
        index: List[str] = None,
        **kwargs,
    ):
        """Fit the Probe in order to obtain estimates"""

    @abstractmethod
    def predict_process(
        self,
        prediction: pd.DataFrame,
        **kwargs,
    ):
        """Compute the predicted Probe"""

    def fit(
        self,
        probe: BaseProbe,
        metric_functions: List[Callable] = None,
        start_date: str = None,
        end_date: str = None,
        **kwargs,
    ) -> None:
        """Fit the model to the probe instance

        Parameters
        ----------
        probe : BaseProbe
            Target variable to be fitted
        metric_functions : List[Callable], optional
            Metrics to apply on the fitted Probe. By default it will apply the default metric specified in the model.

            **EXAMPLE**: `[error, error_after_t0]`
        start_date : str, optional
            **EXAMPLE**: `"2019-05-01"`
        end_date : str, optional
            **EXAMPLE**: `"2021-07-01"`

        Examples
        --------
        ```python
        from edsteva.models.step_function import StepFunction

        step_function_model = StepFunction()
        step_function_model.fit(probe)
        step_function_model.estimates.head()
        ```

        | care_site_level          | care_site_id | stay_type    | t_0        | c_0   | error |
        | :----------------------- | :----------- | :----------- | :--------- | :---- | :---- |
        | Unité Fonctionnelle (UF) | 8312056386   | 'Urg_Hospit' | 2019-05-01 | 0.397 | 0.040 |
        | Unité Fonctionnelle (UF) | 8312056386   | 'All'        | 2011-04-01 | 0.583 | 0.028 |
        | Pôle/DMU                 | 8312027648   | 'Urg_Hospit' | 2021-03-01 | 0.677 | 0.022 |
        | Pôle/DMU                 | 8312027648   | 'All'        | 2018-08-01 | 0.764 | 0.014 |
        | Hôpital                  | 8312022130   | 'Urg_Hospit' | 2022-02-01 | 0.652 | 0.027 |
        """
        if isinstance(probe, BaseProbe):
            probe.is_computed_probe()
        else:
            raise TypeError("Unsupported type {} for probe.".format(type(probe)))

        predictor = filter_table_by_date(
            table=probe.predictor,
            table_name="predictor",
            start_date=start_date,
            end_date=end_date,
        )
        index = probe._index

        estimates = self.fit_process(
            predictor=predictor,
            index=index,
            **kwargs,
        )

        metrics = self._compute_metrics(
            predictor=predictor,
            estimates=estimates,
            index=index,
            metric_functions=metric_functions,
        )

        if metrics is not None:
            self._metrics = list(metrics.columns.difference(index))
            self.estimates = estimates.merge(metrics, on=index)

        else:
            self.estimates = estimates

        self.is_computed_estimates()
        self.params = kwargs

    def predict(
        self,
        probe: BaseProbe,
    ) -> pd.DataFrame:
        """Computes the predicted probe by using the estimates

        Parameters
        ----------
        probe : BaseProbe
            Target variable to be predicted

        Examples
        --------
        ```python
        from edsteva.models.step_function import StepFunction

        step_function_model.predict(visit).head()
        ```

        | care_site_level          | care_site_id | care_site_short_name | stay_type    | date       | n_visit | c     | c_fit |
        | :----------------------- | :----------- | :------------------- | :----------- | :--------- | :------ | :---- | :---- |
        | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'Urg_Hospit' | 2019-05-01 | 233.0   | 0.841 | 0.758 |
        | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'All'        | 2021-04-01 | 393.0   | 0.640 | 0.758 |
        | Pôle/DMU                 | 8312027648   | Care site 2          | 'Urg_Hospit' | 2011-03-01 | 204.0   | 0.497 | 0     |
        | Pôle/DMU                 | 8312027648   | Care site 2          | 'All'        | 2018-08-01 | 22.0    | 0.784 | 0.874 |
        | Hôpital                  | 8312022130   | Care site 3          | 'Urg_Hospit' | 2022-02-01 | 9746.0  | 0.974 | 0.912 |

        """

        predictor = probe.predictor
        index = probe._index

        prediction = self.predict_process(predictor=predictor, index=index)
        return prediction

    def load(self, path=None) -> None:
        """Loads a Model from local

        Parameters
        ----------
        path : str, optional
            **EXAMPLE**: `"my_folder/my_file.html"`

        Examples
        -------
        ```python
        from edsteva.probes import VisitProbe

        probe_path = "my_path/visit.pkl"

        visit = VisitProbe()
        visit.load(path=probe_path)
        ```

        """

        if not path:
            path = CACHE_DIR / "edsteva" / "models" / f"{self.name.lower()}.pickle"

        loaded_model = load_object(path)
        self.__dict__ = loaded_model.__dict__.copy()
        self.path = path

    def save(self, path: str = None, name: str = None) -> bool:
        """Saves computed Model instance

        Parameters
        ----------
        path : str, optional
            **EXAMPLE**: `"my_folder/my_file.html"`
        name : str, optional
            **EXAMPLE**: `"fitted_visit"`

        Examples
        -------
        ```python
        from edsteva.probes import VisitProbe

        probe_path = "my_path/visit.pkl"

        visit = VisitProbe()
        visit.compute(data)
        visit.save(path=probe_path)
        ```

        """

        self.is_computed_estimates()

        if not path:
            if name:
                self.name = name
            path = CACHE_DIR / "edsteva" / "models" / f"{self.name.lower()}.pickle"

        self.path = path
        save_object(self, path)

    def delete(self, path: str = None) -> bool:
        """Delete the saved Model instance

        Parameters
        ----------
        path : str, optional
            **EXAMPLE**: `"my_folder/my_file.html"`
        """

        if not path:
            if hasattr(self, "path"):
                path = self.path
            else:
                path = CACHE_DIR / "edsteva" / "models" / f"{self.name.lower()}.pickle"

        delete_object(self, path)

    def _compute_metrics(
        self,
        predictor: pd.DataFrame,
        estimates: pd.DataFrame,
        index: List[str],
        metric_functions: List[Callable] = None,
    ):
        if metric_functions:
            if callable(metric_functions):
                metrics = metric_functions(
                    predictor=predictor, estimates=estimates, index=index
                )
            elif isinstance(metric_functions, list):
                metrics = []
                for metric_function in metric_functions:
                    if callable(metric_function):
                        metrics.append(
                            metric_function(
                                predictor=predictor, estimates=estimates, index=index
                            )
                        )
                    else:
                        raise TypeError(
                            "{} is not callable. The metrics input must be a list of callable functions".format(
                                type(metric_function)
                            )
                        )
                metrics = reduce(
                    lambda left, right: pd.merge(left, right, on=index), metrics
                )
            else:
                raise TypeError(
                    "{} is not callable. The metrics input must be a callable function".format(
                        type(metrics)
                    )
                )

        elif hasattr(self, "default_metrics"):
            metrics = self.default_metrics(
                predictor=predictor, estimates=estimates, index=index
            )
        else:
            metrics = None

        return metrics

    def is_predictable_probe(
        self,
        predictor: pd.DataFrame,
        index: List[str],
    ) -> pd.DataFrame:
        """Raises an error if the model has not been fitted on the input predictor.

        Parameters
        ----------
        predictor : pd.DataFrame
            Target DataFrame to be predicted
        index : List[str]
            List of the columns given by Probe._index

        Returns
        -------
        pd.DataFrame
            Predictor along with the fitted estimates

        Raises
        ------
        Exception
            Some indexes have no associated estimates, the model must be fitted on an adequate probe
        """
        prediction = predictor.merge(
            self.estimates, on=index, how="left", validate="many_to_one", indicator=True
        )
        if (prediction["_merge"] == "both").all():
            prediction = prediction.drop(columns="_merge")
            return prediction

        else:
            raise Exception(
                "Some indexes have no associated estimates, the model must be fitted on an adequate probe"
            )
