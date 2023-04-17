from datetime import datetime
from typing import Dict, List, Tuple, Union

import pandas as pd

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

    Attributes
    ----------
    _index: List[str]
        Variable from which data is grouped

        **VALUE**: ``["care_site_level", "concepts_set", "stay_type", "length_of_stay", "care_site_id"]``
    _index: List[str]
        Variable from which data is grouped

        **VALUE**: ``["care_site_level", "concepts_set", "stay_type", "length_of_stay", "care_site_id"]``
    _index: List[str]
        Variable from which data is grouped

        **VALUE**: ``["care_site_level", "concepts_set", "stay_type", "length_of_stay", "care_site_id"]``
    _extra_predictor: str
        Variable from which data is grouped

        **VALUE**: ``["care_site_level", "concepts_set", "stay_type", "length_of_stay", "care_site_id"]``
    """

    def __init__(
        self,
        completeness_predictor: str = "per_measurement_default",
        standard_terminologies: List[str] = ["ANABIO", "LOINC"],
        _viz_config: Dict[str, str] = None,
    ):
        self._completeness_predictor = completeness_predictor
        self._standard_terminologies = standard_terminologies
        self._index = [
            "care_site_level",
            "concepts_set",
            "stay_type",
            "length_of_stay",
            "care_site_id",
            "care_site_specialty",
            "specialties_set",
        ] + [
            "{}_concept_code".format(terminology)
            for terminology in standard_terminologies
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
        concepts_sets: Union[str, Dict[str, str]] = {
            "Leucocytes": "A0174|K3232|H6740|E4358|C9784|C8824|E6953",
            "Plaquettes": "E4812|C0326|A1636|A0230|H6751|A1598|G7728|G7727|G7833|A2538|A2539|J4463",
            "Créatinine": "E3180|G1974|J1002|A7813|A0094|G1975|J1172|G7834|F9409|F9410|C0697|H4038|F2621",
            "Potassium": "A1656|C8757|C8758|A2380|E2073|L5014|F2618|E2337|J1178|A3819|J1181",
            "Sodium": "A1772|C8759|C8760|A0262|J1177|F8162|L5013|F2617|K9086|J1180 ",
            "Chlorure": "B5597|F2359|A0079|J1179|F2619|J1182|F2358|A0079|J1179|F2619|J1182",
            "Glucose": "A1245|E7961|C8796|H7753|A8029|H7749|A0141|H7323|J7401|F2622|B9553|C7236|E7312|G9557|A7338|H7324|C0565|E9889|A8424|F6235|F5659|F2406",
            "Bicarbonate": "A0422|H9622|C6408|F4161",
        },
        stay_durations: List[float] = [1],
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
        if concepts_sets is None:
            self._index.remove("concepts_set")
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
            concepts_sets=concepts_sets,
            stay_durations=stay_durations,
            source_terminologies=source_terminologies,
            mapping=mapping,
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