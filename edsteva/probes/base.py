import datetime
from abc import ABCMeta, abstractmethod
from typing import ClassVar, Dict, List, Union

import pandas as pd
from loguru import logger

from edsteva import CACHE_DIR
from edsteva.probes.utils.filter_df import filter_table_by_care_site
from edsteva.probes.utils.prepare_df import prepare_care_site_relationship
from edsteva.utils.checks import check_columns, check_tables
from edsteva.utils.file_management import delete_object, load_object, save_object
from edsteva.utils.typing import Data, DataFrame


class BaseProbe(metaclass=ABCMeta):
    """Base class for Probes

    Attributes
    ----------
    _schema: List[str]
        The columns a predictor must have

        **VALUE**: ``["care_site_id", "care_site_level", "stay_type", "date", "c"]``
    predictor: pd.DataFrame
        Available with the [``compute()``][edsteva.probes.base.BaseProbe.compute] method
    _cache_predictor: pd.DataFrame
        Available with the [``compute()``][edsteva.probes.base.BaseProbe.compute] method

        It is a copy of the predictor DataFrame used to [``reset_predictor()``][edsteva.probes.base.BaseProbe.reset_predictor]
    care_site_relationship: pd.DataFrame
        Available with the [``compute()``][edsteva.probes.base.BaseProbe.compute] method

        It describes the care site structure (cf. [``prepare_care_site_relationship()``][edsteva.probes.utils.prepare_df.prepare_care_site_relationship])
    """

    _schema: ClassVar[List[str]] = ["care_site_level", "care_site_id", "date", "c"]

    def __init__(
        self,
        completeness_predictor: str,
        index: List[str],
    ):
        self._completeness_predictor = completeness_predictor
        self._cache_index = index.copy()
        self._viz_config = {}

    def validate_input_data(self, data: Data) -> None:
        """Raises an error if the input data is not valid

        Parameters
        ----------
        data: Data
            Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]
        """

        if not isinstance(data, Data.__args__):
            raise TypeError("Unsupported type {} for data".format(type(data).__name__))

        check_tables(
            data=data,
            required_tables=[
                "visit_occurrence",
                "care_site",
                "fact_relationship",
            ],
        )

    def is_computed_probe(self) -> None:
        """Raises an error if the Probe has not been computed properly"""
        if hasattr(self, "predictor"):
            if not isinstance(self.predictor, pd.DataFrame):
                raise TypeError(
                    "Predictor must be a Pandas DataFrame and not a {}, please review the process method or your arguments".format(
                        type(self.predictor).__name__
                    )
                )
            if self.predictor.empty:
                raise Exception(
                    "Predictor is empty, please review the process method or your arguments"
                )
            check_columns(
                self.predictor,
                required_columns=self._schema,
            )
            if self.predictor.dtypes["date"] != "datetime64[ns]":
                try:
                    self.predictor["date"] = self.predictor["date"].astype(
                        "datetime64[ns]"
                    )
                except Exception as e:
                    raise TypeError(
                        "Predictor column 'date' type is {} and cannot convert to datetime and return the following error: {}. Please review the process method or your arguments".format(
                            self.predictor.dtypes["date"], e
                        )
                    ) from e
        else:
            raise Exception(
                "Predictor has not been computed, please use the compute method as follow: Predictor.compute()"
            )

    def filter_date_per_care_site(self, target_column: str):
        filtered_predictor = self.predictor.copy()
        predictor_activity = self.predictor[self.predictor[target_column] > 0].copy()
        predictor_activity = (
            predictor_activity.groupby("care_site_id")
            .agg({"date": ["min", "max"]})
            .droplevel(axis="columns", level=0)
            .reset_index()
        )
        filtered_predictor = filtered_predictor.merge(
            predictor_activity, on="care_site_id"
        )
        filtered_predictor = filtered_predictor[
            (filtered_predictor["date"] >= filtered_predictor["min"])
            & (filtered_predictor["date"] <= filtered_predictor["max"])
        ].drop(columns=["min", "max"])
        self.predictor = filtered_predictor

    @abstractmethod
    def compute_process(
        self,
        data: Data,
        care_site_relationship: pd.DataFrame,
        start_date: datetime,
        end_date: datetime,
        care_site_levels: List[str],
        stay_types: Union[str, Dict[str, str]],
        care_site_ids: List[int],
        **kwargs,
    ) -> pd.DataFrame:
        """Process the data in order to obtain a predictor table"""

    def compute(
        self,
        data: Data,
        start_date: datetime = None,
        end_date: datetime = None,
        care_site_levels: List[str] = None,
        stay_types: Union[str, Dict[str, str]] = None,
        care_site_ids: List[int] = None,
        with_cache: bool = True,
        **kwargs,
    ) -> None:
        """Calls [``compute_process()``][edsteva.probes.base.BaseProbe.compute_process]


        Here are the following computation steps:

        - check if input data is valid with [``validate_input_data()``][edsteva.probes.base.BaseProbe.validate_input_data] method
        - query care site relationship table with [``prepare_care_site_relationship()``][edsteva.probes.utils.prepare_df.prepare_care_site_relationship]
        - compute predictor with [``compute_process()``][edsteva.probes.base.BaseProbe.compute_process] method
        - check if predictor is valid with [``is_computed_probe()``][edsteva.probes.base.BaseProbe.is_computed_probe] method



        Parameters
        ----------
        data : Data
            Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]
        start_date : datetime, optional
            **EXAMPLE**: `"2019-05-01"`
        end_date : datetime, optional
            **EXAMPLE**: `"2021-07-01"`
        care_site_levels : List[str], optional
            **EXAMPLE**: `["Hospital", "Pole", "UF"]`
        stay_types : Union[str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "Urg_and_consult": "urgences|consultation"}` or `"hospitalisés`
        care_site_ids : List[int], optional
            **EXAMPLE**: `[8312056386, 8312027648]`

        Attributes
        ----------
        Add to the Probe th following attributes:

        - predictor is the target DataFrame
        - _cache_predictor is a copy of the target DataFrame (used to [``reset_predictor()``][edsteva.probes.base.BaseProbe.reset_predictor])
        - care_site_relationship is a DataFrame with the hierarchy of the care site structure

        Examples
        -------

        ```python
        from edsteva.probes import VisitProbe

        visit = VisitProbe()
        visit.compute(
            data,
            stay_types={"All": ".*", "Urg_and_consult": "urgences|consultation"},
            care_site_levels=["Hospital", "Pole", "UF"],
        )
        visit.predictor.head()
        ```

        | care_site_level          | care_site_id | care_site_short_name | stay_type       | date       | n_visit | c     |
        | :----------------------- | :----------- | :------------------- | :-------------- | :--------- | :------ | :---- |
        | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | Urg_and_consult | 2019-05-01 | 233.0   | 0.841 |
        | Unité Fonctionnelle (UF) | 8312056386   | Care site 1          | 'All'           | 2021-04-01 | 393.0   | 0.640 |
        | Pôle/DMU                 | 8312027648   | Care site 2          | Urg_and_consult | 2011-03-01 | 204.0   | 0.497 |
        | Pôle/DMU                 | 8312027648   | Care site 2          | 'All'           | 2018-08-01 | 22.0    | 0.274 |
        | Hôpital                  | 8312022130   | Care site 3          | Urg_and_consult | 2022-02-01 | 9746.0  | 0.769 |


        """
        self.validate_input_data(data=data)
        self._reset_index()
        care_site_relationship = prepare_care_site_relationship(data=data)
        self.start_date = pd.to_datetime(start_date) if start_date else None
        self.end_date = pd.to_datetime(end_date) if end_date else None
        self.predictor = self.compute_process(
            data=data,
            care_site_relationship=care_site_relationship,
            start_date=start_date,
            end_date=end_date,
            care_site_levels=care_site_levels,
            stay_types=stay_types,
            care_site_ids=care_site_ids,
            **kwargs,
        )
        self.is_computed_probe()
        self.care_site_relationship = care_site_relationship
        self.predictor = self.add_names_columns(self.predictor)
        if with_cache:
            self.cache_predictor()

    def reset_predictor(
        self,
    ) -> None:
        """Reset the predictor to its initial state"""
        self.predictor = self._cache_predictor.copy()

    def cache_predictor(
        self,
    ) -> None:
        """Cache the predictor"""
        self._cache_predictor = self.predictor.copy()
        logger.info(
            "Cache the predictor, you can reset the predictor to this state with the method reset_predictor"
        )

    def filter_care_site(
        self,
        care_site_ids: Union[int, List[int]] = None,
        care_site_short_names: Union[str, List[str]] = None,
        care_site_specialties: Union[str, List[str]] = None,
    ) -> None:
        """Filters all the care sites related to the selected care sites.

        Parameters
        ----------
        care_site_ids : Union[int, List[int]], optional
            **EXAMPLE**: `[8312056386, 8312027648]`
        care_site_short_names : Union[str, List[str]], optional
            **EXAMPLE**: `["HOSPITAL 1", "HOSPITAL 2"]`
        """
        self.predictor = filter_table_by_care_site(
            table_to_filter=self.predictor,
            care_site_relationship=self.care_site_relationship,
            care_site_ids=care_site_ids,
            care_site_short_names=care_site_short_names,
            care_site_specialties=care_site_specialties,
        )
        logger.info("Use probe.reset_predictor() to get back the initial predictor")

    def add_names_columns(self, df: DataFrame):
        if hasattr(self, "care_site_relationship") and "care_site_id" in df.columns:
            df = df.merge(
                self.care_site_relationship[
                    ["care_site_id", "care_site_short_name"]
                ].drop_duplicates(),
                on="care_site_id",
                how="left",
            )
        if hasattr(self, "biology_relationship"):
            concept_codes = [
                "{}_concept_code".format(terminology)
                for terminology in self._standard_terminologies
            ]
            concept_names = [
                "{}_concept_name".format(terminology)
                for terminology in self._standard_terminologies
            ]
            if set(concept_codes).issubset(df.columns):
                df = df.merge(
                    self.biology_relationship[
                        concept_codes + concept_names
                    ].drop_duplicates(),
                    on=concept_codes,
                    how="left",
                )
        return df.reset_index(drop=True)

    def load(self, path=None) -> None:
        """Loads a Probe from local

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

        loaded_probe = load_object(path)
        self.__dict__ = loaded_probe.__dict__.copy()
        self.path = path

    def save(self, path: str = None, name: str = None) -> bool:
        """Saves computed Model instance

        Parameters
        ----------
        path : str, optional
            **EXAMPLE**: `"my_folder/my_file.html"`
        name : str, optional
            **EXAMPLE**: `"visit_from_BCT"`

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

        self.is_computed_probe()

        if name:
            self.name = name
        if not path:
            path = self._get_path()

        self.path = path
        save_object(self, path)

    def delete(self, path: str = None):
        """Delete the saved Probe instance

        Parameters
        ----------
        path : str, optional
            **EXAMPLE**: `"my_folder/my_file.html"`
        """

        if not path:
            path = self.path

        delete_object(self, path)

    def _get_path(self):
        base_path = CACHE_DIR / "edsteva" / "probes"
        if hasattr(self, "name"):
            filename = f"{self.name.lower()}.pickle"
        else:
            filename = f"{type(self).__name__.lower()}.pickle"
        return base_path / filename

    def _reset_index(
        self,
    ) -> None:
        """Reset the index to its initial state"""
        self._index = self._cache_index.copy()
