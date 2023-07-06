from abc import ABCMeta, abstractmethod
from functools import reduce
from typing import List

import pandas as pd
from loguru import logger

from edsteva import CACHE_DIR
from edsteva.metrics import metrics
from edsteva.probes.base import BaseProbe
from edsteva.probes.utils.filter_df import filter_table_by_date
from edsteva.utils.file_management import delete_object, load_object, save_object


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

    def __init__(
        self,
        algo: str,
        coefs: List[str],
        default_metrics: List[str],
    ):
        self._algo = algo
        self._coefs = coefs
        self._default_metrics = default_metrics
        self._viz_config = {}

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
                        type(self.estimates).__name__
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
        metric_functions: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        with_cache: bool = True,
        **kwargs,
    ) -> None:
        """Fit the model to the probe instance

        Parameters
        ----------
        probe : BaseProbe
            Target variable to be fitted
        metric_functions : List[str], optional
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
            raise TypeError(
                "Unsupported type {} for probe.".format(type(probe).__name__)
            )

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

        metrics_df = self._compute_metrics(
            predictor=predictor,
            estimates=estimates,
            index=index,
            metric_functions=metric_functions,
        )

        if metrics_df is not None:
            self._metrics = list(metrics_df.columns.difference(index))
            self.estimates = estimates.merge(metrics_df, on=index)

        else:
            self.estimates = estimates

        self.is_computed_estimates()
        self.params = kwargs
        if with_cache:
            self.cache_estimates()

    def reset_estimates(
        self,
    ) -> None:
        """Reset the estimates to its initial state"""
        self.estimates = self._cache_estimates.copy()

    def cache_estimates(
        self,
    ) -> None:
        """Cache the predictor"""
        self._cache_estimates = self.estimates.copy()
        logger.info(
            "Cache the estimates, you can reset the estimates to this state with the method reset_estimates"
        )

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

        | care_site_level          | care_site_id | stay_type    | date       | n_visit | c     | c_fit |
        | :----------------------- | :----------- | :----------- | :--------- | :------ | :---- | :---- |
        | Unité Fonctionnelle (UF) | 8312056386   | 'Urg_Hospit' | 2019-05-01 | 233.0   | 0.841 | 0.758 |
        | Unité Fonctionnelle (UF) | 8312056386   | 'All'        | 2021-04-01 | 393.0   | 0.640 | 0.758 |
        | Pôle/DMU                 | 8312027648   | 'Urg_Hospit' | 2011-03-01 | 204.0   | 0.497 | 0     |
        | Pôle/DMU                 | 8312027648   | 'All'        | 2018-08-01 | 22.0    | 0.784 | 0.874 |
        | Hôpital                  | 8312022130   | 'Urg_Hospit' | 2022-02-01 | 9746.0  | 0.974 | 0.912 |

        """

        predictor = probe.predictor
        index = probe._index

        return self.predict_process(predictor=predictor, index=index)

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

        path = path or self._get_path()
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

        if name:
            self.name = name
        if not path:
            path = self._get_path()

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
            path = self.path

        delete_object(self, path)

    def _get_path(self):
        base_path = CACHE_DIR / "edsteva" / "models"
        if hasattr(self, "name"):
            filename = f"{self.name.lower()}.pickle"
        else:
            filename = f"{type(self).__name__.lower()}.pickle"
        return base_path / filename

    def _compute_metrics(
        self,
        predictor: pd.DataFrame,
        estimates: pd.DataFrame,
        index: List[str],
        metric_functions: List[str] = None,
    ):
        if metric_functions is None:
            if hasattr(self, "_default_metrics") and self._default_metrics:
                metric_functions = self._default_metrics
            else:
                return None
        if isinstance(metric_functions, str):
            metric_functions = [metric_functions]
        metrics_df = []
        for metric_function in metric_functions:
            metrics_df.append(
                metrics.get(metric_function)(
                    predictor=predictor, estimates=estimates, index=index
                )
            )
        return reduce(lambda left, right: left.merge(right, on=index), metrics_df)

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
            return prediction.drop(columns="_merge")

        raise Exception(
            "Some indexes have no associated estimates, the model must be fitted on an adequate probe"
        )
