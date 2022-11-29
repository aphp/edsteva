import datetime
from abc import ABCMeta, abstractmethod
from typing import Dict, List, Union

import pandas as pd
from loguru import logger

from edsteva import CACHE_DIR
from edsteva.probes.utils import (
    delete_object,
    filter_table_by_care_site,
    get_care_site_relationship,
    load_object,
    save_object,
)
from edsteva.utils.checks import check_columns, check_tables
from edsteva.utils.typing import Data


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

        It describes the care site structure (cf. [``get_care_site_relationship()``][edsteva.probes.utils.get_care_site_relationship])
    """

    def __init__(self):
        self.is_valid_probe()
        self.name = self._get_name()

    _schema = ["care_site_id", "care_site_level", "stay_type", "date", "c"]

    def validate_input_data(self, data: Data) -> None:
        """Raises an error if the input data is not valid

        Parameters
        ----------
        data: Data
            Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]
        """

        if not isinstance(data, Data.__args__):
            raise TypeError("Unsupported type {} for data".format(type(data)))

        check_tables(
            data=data,
            required_tables=[
                "visit_occurrence",
                "care_site",
                "fact_relationship",
            ],
        )

    def is_valid_probe(self) -> None:
        """Raises an error if the instantiated Probe is not valid"""
        if not hasattr(self, "_index"):
            raise Exception(
                "Probe must have _index attribute. Please review the code of your probe"
            )

    def is_computed_probe(self) -> None:
        """Raises an error if the Probe has not been computed properly"""
        if hasattr(self, "predictor"):
            if not isinstance(self.predictor, pd.DataFrame):
                raise TypeError(
                    "Predictor must be a Pandas DataFrame and not a {}, please review the process method or your arguments".format(
                        type(self.predictor)
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
            if not self.predictor.dtypes["date"] == "datetime64[ns]":
                try:
                    self.predictor["date"] = self.predictor["date"].astype(
                        "datetime64[ns]"
                    )
                except Exception as e:
                    raise TypeError(
                        "Predictor column 'date' type is {} and cannot convert to datetime and return the following error: {}. Please review the process method or your arguments".format(
                            self.predictor.dtypes["date"], e
                        )
                    )
        else:
            raise Exception(
                "Predictor has not been computed, please use the compute method as follow: Predictor.compute()"
            )

    def impute_missing_date(
        self,
        only_impute_per_care_site: bool = False,
    ) -> pd.DataFrame:
        """Impute missing date with 0 on the predictor of a probe.

        Parameters
        ----------
        only_impute_per_care_site : bool, optional
            If True it will only impute missing date between the first and the last observation of each care site.
            If False it will impute missing data on the entire study period whatever the care site
        """
        # Check if probe has been computed.
        self.is_computed_probe()

        # Set start_date to the beginning of the month.
        date_index = pd.date_range(
            start=self.start_date,
            end=self.end_date,
            freq="MS",
            closed="left",
        )
        date_index = pd.DataFrame({"date": date_index})

        # Precompute the mapping:
        # {'Hôpital-1': {'min': Timestamp('2010-06-01'), 'max': Timestamp('2019-11-01')}
        if only_impute_per_care_site:
            site_to_min_max_ds = (
                self.predictor.groupby(["care_site_short_name"])["date"]
                .agg([min, max])
                .to_dict("index")
            )

        partition_cols = self._index + ["care_site_short_name"]
        groups = []
        for partition, group in self.predictor.groupby(partition_cols):
            group = date_index.merge(group, on="date", how="left")

            # Filter on each care site timeframe.
            if only_impute_per_care_site:
                care_site_short_name = partition[-1]
                ds_min = site_to_min_max_ds[care_site_short_name]["min"]
                ds_max = site_to_min_max_ds[care_site_short_name]["max"]
                group = group.loc[(group["date"] >= ds_min) & (group["date"] <= ds_max)]

            # Fill specific partition values.
            for key, val in zip(partition_cols, partition):
                group[key] = val
            # Fill remaining NaN from counts values with 0.
            group.fillna(0, inplace=True)
            groups.append(group)

        self.predictor = pd.concat(groups)

    @abstractmethod
    def compute_process(
        self,
        data: Data,
        care_site_relationship: pd.DataFrame,
        start_date: datetime = None,
        end_date: datetime = None,
        care_site_levels: List[str] = None,
        stay_types: Union[str, Dict[str, str]] = None,
        care_site_ids: List[int] = None,
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
        impute_missing_dates: bool = True,
        only_impute_per_care_site: bool = False,
        **kwargs,
    ) -> None:
        """Calls [``compute_process()``][edsteva.probes.base.BaseProbe.compute_process]


        Here are the following computation steps:

        - check if input data is valid with [``validate_input_data()``][edsteva.probes.base.BaseProbe.validate_input_data] method
        - query care site relationship table with [``get_care_site_relationship()``][edsteva.probes.utils.get_care_site_relationship]
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
        care_site_relationship = get_care_site_relationship(data=data)

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

        self.start_date = (
            pd.to_datetime(start_date) if start_date else self.predictor["date"].min()
        )
        self.end_date = (
            pd.to_datetime(end_date) if end_date else self.predictor["date"].max()
        )

        if impute_missing_dates:
            self.impute_missing_date(
                only_impute_per_care_site=only_impute_per_care_site,
            )
        self.cache_predictor()
        self.care_site_relationship = care_site_relationship

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
            table_name="{} predictor".format(type(self).__name__.lower()),
            care_site_relationship=self.care_site_relationship,
            care_site_ids=care_site_ids,
            care_site_short_names=care_site_short_names,
        )
        logger.info("Use probe.reset_predictor() to get back the initial predictor")

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

        if not path:
            if name:
                self.name = name
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
            if hasattr(self, "path"):
                path = self.path
            else:
                path = self._get_path()

        delete_object(self, path)

    def _get_path(self):
        base_path = CACHE_DIR / "edsteva" / "probes"
        filename = f"{self.name.lower()}.pickle"
        return base_path / filename

    def _get_name(self):
        return type(self).__name__
