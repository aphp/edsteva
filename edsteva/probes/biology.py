from datetime import datetime
from typing import Dict, List, Tuple, Union

import pandas as pd
from loguru import logger

from edsteva.probes.base import BaseProbe
from edsteva.probes.utils import (
    CARE_SITE_LEVEL_NAMES,
    concatenate_predictor_by_level,
    get_biology_relationship,
    hospital_only,
    prepare_care_site,
    prepare_measurement,
    prepare_visit_occurrence,
)
from edsteva.utils.checks import check_tables
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data


def compute_completeness(biology_predictor, standard_terminologies, root_terminology):
    partition_cols = (
        [
            "care_site_level",
            "care_site_id",
            "care_site_short_name",
            "stay_type",
            "concepts_set",
            "length_of_stay",
            "date",
        ]
        + [
            "{}_concept_code".format(terminology)
            for terminology in standard_terminologies
        ]
        + [
            "{}_concept_name".format(terminology)
            for terminology in standard_terminologies
        ]
        + [
            "{}_vocabulary".format(terminology)
            for terminology in standard_terminologies
        ]
    )
    n_measurement = (
        biology_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"measurement_id": "nunique"})
        .rename(columns={"measurement_id": "n_measurement"})
    )

    n_measurement = to("pandas", n_measurement)

    partition_cols = list(set(partition_cols) - {"date"})
    q_99_measurement = (
        n_measurement.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )[["n_measurement"]]
        .quantile(q=0.99)
        .rename(columns={"n_measurement": "q_99_measurement"})
    )

    biology_predictor = n_measurement.merge(
        q_99_measurement,
        on=partition_cols,
    )

    biology_predictor["c"] = biology_predictor["q_99_measurement"].where(
        biology_predictor["q_99_measurement"] == 0,
        biology_predictor["n_measurement"] / biology_predictor["q_99_measurement"],
    )
    biology_predictor = biology_predictor.drop(columns="q_99_measurement")

    return biology_predictor


def get_hospital_measurements(measurement, visit_occurrence, care_site):
    hospital_measurement = measurement.merge(
        visit_occurrence.drop(columns="date"), on="visit_occurrence_id"
    )
    hospital_measurement = hospital_measurement.merge(care_site, on="care_site_id")

    if is_koalas(hospital_measurement):
        hospital_measurement.spark.cache()

    return hospital_measurement


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
    """

    _index = [
        "care_site_level",
        "concepts_set",
        "stay_type",
        "length_of_stay",
        "care_site_id",
    ]

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
        standard_terminologies: List[str] = ["ANABIO", "LOINC"],
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
        check_tables(
            data=data,
            required_tables=["measurement", "concept", "concept_relationship"],
        )

        biology_relationship = get_biology_relationship(
            data=data,
            standard_terminologies=standard_terminologies,
            source_terminologies=source_terminologies,
            mapping=mapping,
        )

        self.biology_relationship = biology_relationship
        root_terminology = mapping[0][0]
        self._index.extend(
            [
                "{}_concept_code".format(terminology)
                for terminology in standard_terminologies
            ]
            + [
                "{}_concept_name".format(terminology)
                for terminology in standard_terminologies
            ]
            + [
                "{}_vocabulary".format(terminology)
                for terminology in standard_terminologies
            ]
        )

        measurement = prepare_measurement(
            data=data,
            biology_relationship=biology_relationship,
            concepts_sets=concepts_sets,
            start_date=start_date,
            end_date=end_date,
            root_terminology=root_terminology,
            standard_terminologies=standard_terminologies,
            per_visit=False,
        )

        visit_occurrence = prepare_visit_occurrence(
            data=data,
            start_date=None,
            end_date=None,
            stay_types=stay_types,
            stay_durations=stay_durations,
        )

        care_site = prepare_care_site(
            data=data,
            care_site_ids=care_site_ids,
            care_site_short_names=care_site_short_names,
            care_site_relationship=care_site_relationship,
        )

        hospital_visit = get_hospital_measurements(
            measurement=measurement,
            visit_occurrence=visit_occurrence,
            care_site=care_site,
        )
        hospital_name = CARE_SITE_LEVEL_NAMES["Hospital"]
        biology_predictor_by_level = {hospital_name: hospital_visit}

        if care_site_levels and not hospital_only(care_site_levels=care_site_levels):
            logger.info(
                "Biological measurements are only available at hospital level for now"
            )
            care_site_levels = "Hospital"

        biology_predictor = concatenate_predictor_by_level(
            predictor_by_level=biology_predictor_by_level,
            care_site_levels=care_site_levels,
        )

        return compute_completeness(
            biology_predictor, standard_terminologies, root_terminology
        )
