from datetime import datetime
from typing import Dict, List, Union

from edsteva.probes.base import BaseProbe
from edsteva.probes.note.completeness_predictors import completeness_predictors
from edsteva.probes.note.viz_configs import viz_configs
from edsteva.utils.typing import Data


class NoteProbe(BaseProbe):
    r"""
    The ``NoteProbe`` computes $c(t)$ the availability of clinical documents

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

        **VALUE**: ["care_site_level", "stay_type", "length_of_stay", "note_type", "care_site_id", "care_site_specialty", "specialties_set"]``
    _viz_config: List[str]
        Dictionary of configuration for visualization purpose.

        **VALUE**: ``{}``
    care_site_relationship: pd.DataFrame

        It describes the care site structure and gives the hierarchy of the different care site levels. (cf. [``prepare_care_site_relationship()``][edsteva.probes.utils.prepare_df.prepare_care_site_relationship])
    """

    def __init__(
        self,
        completeness_predictor: str = "per_visit_default",
    ):
        self._index = [
            "note_type",
            "care_site_id",
            "care_site_level",
            "care_sites_set",
            "care_site_specialty",
            "specialties_set",
            "stay_type",
            "stay_source",
            "length_of_stay",
            "provenance_source",
            "age_range",
            "drg_source",
            "condition_type",
        ]
        super().__init__(
            completeness_predictor=completeness_predictor,
            index=self._index,
        )

    def compute_process(
        self,
        data: Data,
        start_date: datetime,
        end_date: datetime,
        note_types: Union[bool, str, Dict[str, str]] = {
            "Urgence": "urge",
            "Ordonnance": "ordo",
            "CRH": "crh",
        },
        extra_data: Data = None,
        care_site_ids: List[int] = None,
        care_site_short_names: List[str] = None,
        care_site_levels: Union[bool, str, List[str]] = True,
        care_sites_sets: Union[str, Dict[str, str]] = None,
        care_site_specialties: Union[bool, List[str]] = None,
        specialties_sets: Union[str, Dict[str, str]] = None,
        stay_types: Union[bool, str, Dict[str, str]] = True,
        stay_sources: Union[bool, str, Dict[str, str]] = None,
        length_of_stays: List[float] = None,
        provenance_sources: Union[bool, str, Dict[str, str]] = None,
        condition_types: Union[bool, str, Dict[str, str]] = None,
        drg_sources: Union[bool, str, Dict[str, str]] = None,
        age_ranges: List[int] = None,
        **kwargs,
    ):
        """Script to be used by [``compute()``][edsteva.probes.base.BaseProbe.compute]

        Parameters
        ----------
        data : Data
            Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]
        start_date : datetime, optional
            **EXAMPLE**: `"2019-05-01"`
        end_date : datetime, optional
            **EXAMPLE**: `"2021-07-01"`
        note_types: Union[bool, str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}` or `{"CRH": "crh", "Urgence": "urge"}`
        extra_data : Data, optional
            Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]. This is not OMOP-standardized data but data needed to associate note with UF and Pole. If not provided, it will only compute the predictor for hospitals.
        care_site_ids : List[int], optional
            **EXAMPLE**: `[8312056386, 8312027648]`
        care_site_short_names : List[str], optional
            **EXAMPLE**: `["HOSPITAL 1", "HOSPITAL 2"]`
        care_site_levels : Union[bool, str, List[str]], optional
            **EXAMPLE**: `["Hospital", "Pole", "UF", "UC", "UH"]`
        care_sites_sets: Union[str, Dict[str, str]], optional
            **EXAMPLE**: `{"All AP-HP": ".*"}` or `{"All AP-HP": ".*", "Pediatrics": r"debre|trousseau|necker"}`
        care_site_specialties: Union[bool, List[str]], optional
            **EXAMPLE**: `["CARDIOLOGIE", "CHIRURGIE"]`
        specialties_sets: Union[str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "ICU": r"REA\s|USI\s|SC\s"}`
        stay_types: Union[bool, str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "Urg_and_consult": "urgences|consultation"}` or `"hospitalisés`
        stay_sources: Union[bool, str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}, {"MCO" : "MCO", "MCO_PSY_SSR" : "MCO|Psychiatrie|SSR"}`
        length_of_stays: List[float], optional
            **EXAMPLE**: `[1, 30]`
        provenance_sources:  Union[bool, str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}, {"urgence" : "service d'urgence"}`
        condition_types :  Union[bool, str, Dict[str, str]], optional
            **EXAMPLE**: `{"Pulmonary_infection": "J22|J15|J13|J958|..."}`
        drg_sources :  Union[bool, str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}, {"medical" : ".{2}M"}`
        age_ranges: List[int], optional
            **EXAMPLE**: `[18, 64]`
        """
        if not note_types and "note_type" in self._index:
            self._index.remove("note_type")
        if not care_site_levels and "care_site_level" in self._index:
            self._index.remove("care_site_level")
        if not care_sites_sets and "care_sites_set" in self._index:
            self._index.remove("care_sites_set")
        if not care_site_specialties and "care_site_specialty" in self._index:
            self._index.remove("care_site_specialty")
        if not specialties_sets and "specialties_set" in self._index:
            self._index.remove("specialties_set")
        if not stay_types and "stay_type" in self._index:
            self._index.remove("stay_type")
        if not stay_sources and "stay_source" in self._index:
            self._index.remove("stay_source")
        if not length_of_stays and "length_of_stay" in self._index:
            self._index.remove("length_of_stay")
        if not provenance_sources and "provenance_source" in self._index:
            self._index.remove("provenance_source")
        if not age_ranges and "age_range" in self._index:
            self._index.remove("age_range")
        if condition_types is None and "condition_type" in self._index:
            self._index.remove("condition_type")
        if not drg_sources and "drg_source" in self._index:
            self._index.remove("drg_source")
        return completeness_predictors.get(self._completeness_predictor)(
            self,
            data=data,
            start_date=start_date,
            end_date=end_date,
            care_site_levels=care_site_levels,
            stay_types=stay_types,
            care_site_ids=care_site_ids,
            extra_data=extra_data,
            care_site_short_names=care_site_short_names,
            care_site_specialties=care_site_specialties,
            care_sites_sets=care_sites_sets,
            specialties_sets=specialties_sets,
            note_types=note_types,
            length_of_stays=length_of_stays,
            provenance_sources=provenance_sources,
            stay_sources=stay_sources,
            age_ranges=age_ranges,
            drg_sources=drg_sources,
            condition_types=condition_types,
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
