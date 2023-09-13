from datetime import datetime
from typing import Dict, List, Tuple, Union

from edsteva.probes.base import BaseProbe
from edsteva.probes.biology.completeness_predictors import completeness_predictors
from edsteva.probes.biology.viz_configs import viz_configs
from edsteva.utils.typing import Data


class BiologyProbe(BaseProbe):
    r"""
    The ``BiologyProbe`` computes $c_(t)$ the availability of laboratory data related to biological measurements for each biological code and each care site according to time:

    $$
    c(t) = \frac{n_{biology}(t)}{n_{99}}
    $$

    Where $n_{biology}(t)$ is the number of biological measurements, $n_{99}$ is the $99^{th}$ percentile and $t$ is the month.

    Parameters
    ----------
    completeness_predictor: str
        Algorithm used to compute the completeness predictor

        **EXAMPLE**: ``"per_visit_default"``
    standard_terminologies: List[str]
        List of standards terminologies to consider

        **EXAMPLE**: ``["LOINC", "ANABIO"]``

    Attributes
    ----------
    _completeness_predictor: str
        Algorithm used to compute the completeness predictor

        **VALUE**: ``"per_visit_default"``
    _standard_terminologies: List[str]
        List of standards terminologies to consider

        **VALUE**: ``["LOINC", "ANABIO"]``
    _index: List[str]
        Variable from which data is grouped

        **VALUE**: ``["care_site_level", "concepts_set", "stay_type", "length_of_stay", "care_site_id", "care_site_specialty", "specialties_set", "<std_terminology>_concept_code"]``
    _viz_config: List[str]
        Dictionary of configuration for visualization purpose.

        **VALUE**: ``{}``
    care_site_relationship: pd.DataFrame

        It describes the care site structure and gives the hierarchy of the different care site levels. (cf. [``prepare_care_site_relationship()``][edsteva.probes.utils.prepare_df.prepare_care_site_relationship])
    biology_relationship: pd.DataFrame

        It provides the link between the different terminology (ex: LOINC, ANABIO, etc.) (cf. [``prepare_biology_relationship()``][edsteva.probes.utils.prepare_df.prepare_biology_relationship])
    """

    def __init__(
        self,
        completeness_predictor: str = "per_measurement_default",
        standard_terminologies: List[str] = ["ANABIO", "LOINC"],
    ):
        self._standard_terminologies = standard_terminologies
        self._index = [
            "{}_concept_code".format(terminology)
            for terminology in standard_terminologies
        ] + [
            "concepts_set",
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
        source_terminologies: Dict[str, str] = {
            "ANALYSES_LABORATOIRE": r"Analyses Laboratoire",
            "GLIMS_ANABIO": r"GLIMS.{0,20}Anabio",
            "GLIMS_LOINC": r"GLIMS.{0,20}LOINC",
            "ANABIO_ITM": r"ITM - ANABIO",
            "LOINC_ITM": r"ITM - LOINC",
        },
        mapping: List[Tuple[str, str, str]] = [
            ("ANALYSES_LABORATOIRE", "GLIMS_ANABIO", "Maps to"),
            ("ANALYSES_LABORATOIRE", "GLIMS_LOINC", "Maps to"),
            ("GLIMS_ANABIO", "ANABIO_ITM", "Mapped from"),
            ("ANABIO_ITM", "LOINC_ITM", "Maps to"),
        ],
        concept_codes: Union[bool, List[str]] = None,
        concepts_sets: Union[str, Dict[str, str]] = {
            "Leucocytes": "A0174|K3232|H6740|E4358|C9784|C8824|E6953",
            "Plaquettes": "E4812|C0326|A1636|A0230|H6751|A1598|G7728|G7727|G7833|A2538|A2539|J4463",
            "Créatinine": "E3180|G1974|J1002|A7813|A0094|G1975|J1172|G7834|F9409|F9410|C0697|H4038|F2621",
            "Potassium": "A1656|C8757|C8758|A2380|E2073|L5014|F2618|E2337|J1178|A3819|J1181",
            "Sodium": "A1772|C8759|C8760|A0262|J1177|F8162|L5013|F2617|K9086|J1180",
            "Chlorure": "B5597|F2359|A0079|J1179|F2619|J1182|F2358|A0079|J1179|F2619|J1182",
            "Glucose": "A1245|E7961|C8796|H7753|A8029|H7749|A0141|H7323|J7401|F2622|B9553|C7236|E7312|G9557|A7338|H7324|C0565|E9889|A8424|F6235|F5659|F2406",
            "Bicarbonate": "A0422|H9622|C6408|F4161",
        },
        condition_types: Union[bool, str, Dict[str, str]] = None,
        drg_sources: Union[bool, str, Dict[str, str]] = None,
        care_site_ids: List[int] = None,
        care_site_short_names: List[str] = None,
        care_site_levels: Union[bool, str, List[str]] = True,
        care_sites_sets: Union[str, Dict[str, str]] = None,
        care_site_specialties: Union[bool, List[str]] = None,
        specialties_sets: Union[str, Dict[str, str]] = None,
        stay_types: Union[bool, str, Dict[str, str]] = True,
        stay_sources: Union[bool, str, Dict[str, str]] = None,
        length_of_stays: List[float] = [1],
        provenance_sources: Union[bool, str, Dict[str, str]] = None,
        age_ranges: List[int] = None,
        **kwargs,
    ):
        """Script to be used by [``compute()``][edsteva.probes.base.BaseProbe.compute]

        Parameters
        ----------
        data : Data
            Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]
        start_date: datetime, optional
            **EXAMPLE**: `"2019-05-01"`
        end_date: datetime, optional
            **EXAMPLE**: `"2021-07-01"`
        source_terminologies: Dict[str, str], optional
            Dictionary of regex used to detect terminology in the column `vocabulary_id`.

            **EXAMPLE**: `{"GLIMS_LOINC": r"GLIMS.{0,20}LOINC"}`
        mapping: List[Tuple[str, str, str]], optional
            List of values to filter in the column `relationship_id` in order to map between 2 terminologies.

            **EXAMPLE**: `[("ANALYSES_LABORATOIRE", "GLIMS_ANABIO", "Maps to")]`
        concepts_sets: Union[str, Dict[str, str]] , optional
            **EXAMPLE**: `{"Créatinine": "E3180|G1974|J1002|A7813|A0094|G1975|J1172|G7834|F9409|F9410|C0697|H4038|F2621", "Leucocytes": r"A0174|K3232|H6740|E4358|C9784|C8824|E6953"}`
        concept_codes: Union[bool, List[str]], optional
            **EXAMPLE**: ['E3180', 'G1974', 'J1002', 'A7813', 'A0094', 'G1975', 'J1172', 'G7834', 'F9409', 'F9410', 'C0697', 'H4038']`
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
        provenance_sources: Union[bool, str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}, {"urgence" : "service d'urgence"}`
        condition_types : Union[bool, str, Dict[str, str]], optional
            **EXAMPLE**: `{"Pulmonary_infection": "J22|J15|J13|J958|..."}`
        drg_sources : Union[bool, str, Dict[str, str]], optional
            **EXAMPLE**: `{"All": ".*"}, {"medical" : ".{2}M"}`
        age_ranges: List[int], optional
            **EXAMPLE**: `[18, 64]`
        """
        if not concepts_sets and "concepts_set" in self._index:
            self._index.remove("concepts_set")
        if not concept_codes:
            for terminology in self._standard_terminologies:
                if "{}_concept_code".format(terminology) in self._index:
                    self._index.remove("{}_concept_code".format(terminology))
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
        if not condition_types and "condition_type" in self._index:
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
            care_site_short_names=care_site_short_names,
            care_site_specialties=care_site_specialties,
            concept_codes=concept_codes,
            care_sites_sets=care_sites_sets,
            specialties_sets=specialties_sets,
            concepts_sets=concepts_sets,
            condition_types=condition_types,
            length_of_stays=length_of_stays,
            source_terminologies=source_terminologies,
            mapping=mapping,
            provenance_sources=provenance_sources,
            stay_sources=stay_sources,
            drg_sources=drg_sources,
            age_ranges=age_ranges,
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
