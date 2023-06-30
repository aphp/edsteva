from datetime import datetime
from typing import Dict, List, Union

import pandas as pd

from edsteva.probes.base import BaseProbe
from edsteva.probes.visit.completeness_predictors import completeness_predictors
from edsteva.probes.visit.viz_configs import viz_configs
from edsteva.utils.typing import Data


class VisitProbe(BaseProbe):
    r"""
    The ``VisitProbe`` computes $c_(t)$ the availability of administrative data according to time:

    Parameters
    ----------
    completeness_predictor: str
        Algorithm used to compute the completeness predictor
        **EXAMPLE**: ``"per_visit_default"``

    Attributes
    ----------
    _completeness_predictor: str
        Algorithm used to compute the completeness predictor
        **VALUE**: ``"per_visit_default"``
    _index: List[str]
        Variable from which data is grouped
        **VALUE**: ``["care_site_level", "stay_type", "length_of_stay", "care_site_id"]``
    _viz_config: List[str]
        Dictionary of configuration for visualization purpose.
        **VALUE**: ``{}``
    """

    def __init__(
        self,
        completeness_predictor: str = "per_visit_default",
    ):
        self._index = [
            "care_site_level",
            "stay_type",
            "length_of_stay",
            "care_site_id",
            "care_site_specialty",
            "care_sites_set",
            "specialties_set",
        ]
        super().__init__(
            completeness_predictor=completeness_predictor,
            index=self._index,
        )

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
        care_sites_sets: Union[str, Dict[str, str]] = None,
        specialties_sets: Union[str, Dict[str, str]] = None,
        length_of_stays: List[float] = None,
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
            **EXAMPLE**: `["Hospital", "Pole", "UF", "UC", "UH"]`
        stay_types : Union[str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "Urg_and_consult": "urgences|consultation"}` or `"hospitalisés`
        care_site_ids : List[int], optional
            **EXAMPLE**: `[8312056386, 8312027648]`
        care_site_short_names : List[str], optional
            **EXAMPLE**: `["HOSPITAL 1", "HOSPITAL 2"]`
        care_site_specialties : List[str], optional
            **EXAMPLE**: `["CARDIOLOGIE", "CHIRURGIE"]`
        care_sites_sets : Union[str, Dict[str, str]], optional
            **EXAMPLE**: `{"All AP-HP": ".*"}` or `{"All AP-HP": ".*", "Pediatrics": r"debre|trousseau|necker"}`
        specialties_sets : Union[str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "ICU": r"REA\s|USI\s|SC\s"}`
        length_of_stays : List[float], optional
            **EXAMPLE**: `[1, 30]`
        """
        if specialties_sets is None and "specialties_set" in self._index:
            self._index.remove("specialties_set")
        if length_of_stays is None and "length_of_stay" in self._index:
            self._index.remove("length_of_stay")
        if care_sites_sets is None and "care_sites_set" in self._index:
            self._index.remove("care_sites_set")
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
            care_sites_sets=care_sites_sets,
            specialties_sets=specialties_sets,
            length_of_stays=length_of_stays,
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

    def available_completeness_predictors(self):
        return list(completeness_predictors.get_all().keys())
