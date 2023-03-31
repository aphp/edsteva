from datetime import datetime
from typing import Dict, List, Union

import pandas as pd

from edsteva.probes.base import BaseProbe
from edsteva.probes.visit.completeness_predictors import completeness_predictors
from edsteva.probes.visit.viz_configs import viz_configs
from edsteva.utils.typing import Data


class VisitProbe(BaseProbe):
    r"""
    The ``VisitProbe`` computes $c_(t)$ the availability of administrative data related to visits for each care site according to time:

    $$
    c(t) = \frac{n_{visit}(t)}{n_{99}}
    $$

    Where $n_{visit}(t)$ is the number of visits, $n_{99}$ is the $99^{th}$ percentile of visits and $t$ is the month.

    Attributes
    ----------
    _index: List[str]
        Variable from which data is grouped

        **VALUE**: ``["care_site_level", "stay_type", "length_of_stay", "care_site_id"]``
    """

    def __init__(
        self,
        completeness_predictor: str = "per_visit_default",
        _viz_config: Dict[str, str] = None,
    ):
        self._completeness_predictor = completeness_predictor
        self._index = [
            "care_site_level",
            "stay_type",
            "length_of_stay",
            "care_site_id",
            "care_site_specialty",
            "specialties_set",
        ]
        if _viz_config is None:
            self._viz_config = {}

    def compute_process(
        self,
        data: Data,
        care_site_relationship: pd.DataFrame,
        start_date: datetime,
        end_date: datetime,
        care_site_levels: List[str],
        stay_types: Union[str, Dict[str, str]],
        care_site_ids: List[int],
        care_site_short_names: List[str] = None,
        care_site_specialties: List[str] = None,
        specialties_sets: Union[str, Dict[str, str]] = None,
        stay_durations: List[float] = None,
        hdfs_user_path: str = None,
        **kwargs,
    ):
        """Script to be used by [``compute()``][edsteva.probes.base.BaseProbe.compute]

        Parameters
        ----------
        data : Data
            Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]
        care_site_relationship : pd.DataFrame
            DataFrame computed in the [``compute()``][edsteva.probes.base.BaseProbe.compute] that gives the hierarchy of the care site structure.
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
        care_site_short_names : List[str], optional
            **EXAMPLE**: `["HOSPITAL 1", "HOSPITAL 2"]`
        """
        if specialties_sets is None:
            self._index.remove("specialties_set")
        return completeness_predictors.get(self._completeness_predictor)(
            self,
            data=data,
            care_site_relationship=care_site_relationship,
            start_date=start_date,
            end_date=end_date,
            care_site_levels=care_site_levels,
            stay_types=stay_types,
            care_site_ids=care_site_ids,
            care_site_short_names=care_site_short_names,
            care_site_specialties=care_site_specialties,
            specialties_sets=specialties_sets,
            stay_durations=stay_durations,
            hdfs_user_path=hdfs_user_path,
            **kwargs,
        )

    def get_viz_config(self, viz_type: str, **kwargs):
        if viz_type in viz_configs.keys():
            _viz_config = self._viz_config.get(viz_type)
            if _viz_config is None:
                _viz_config = self._completeness_predictor
        else:
            raise ValueError(f"edsteva has no {viz_type} registry !")
        return viz_configs[viz_type].get(_viz_config)(self, **kwargs)
