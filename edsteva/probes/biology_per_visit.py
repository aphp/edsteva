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


def compute_completeness(biology_predictor):
    partition_cols = [
        "care_site_level",
        "care_site_id",
        "care_site_short_name",
        "stay_type",
        "concepts_set",
        "length_of_stay",
        "date",
    ]
    n_visit_with_bio = (
        biology_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"has_bio": "count"})
        .rename(columns={"has_bio": "n_visit_with_bio"})
    )

    partition_cols = list(set(partition_cols) - {"concepts_set"})
    n_visit = (
        biology_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"visit_id": "nunique"})
        .rename(columns={"visit_id": "n_visit"})
    )

    biology_predictor = n_visit_with_bio.merge(
        n_visit,
        on=partition_cols,
    )

    biology_predictor = to("pandas", biology_predictor)

    biology_predictor["c"] = biology_predictor["n_visit"].where(
        biology_predictor["n_visit"] == 0,
        biology_predictor["n_visit_with_bio"] / biology_predictor["n_visit"],
    )
    biology_predictor = biology_predictor.drop(columns=["n_visit_with_bio"])

    return biology_predictor


def get_hospital_visit(measurement, visit_occurrence, care_site):
    measurement_hospital = measurement[
        ["visit_occurrence_id", "concepts_set"]
    ].drop_duplicates()
    measurement_hospital["has_bio"] = True
    hospital_visit = visit_occurrence.merge(
        measurement_hospital, on="visit_occurrence_id", how="left"
    )
    hospital_visit = hospital_visit.rename(columns={"visit_occurrence_id": "visit_id"})
    hospital_visit = hospital_visit.merge(care_site, on="care_site_id")
    if is_koalas(hospital_visit):
        hospital_visit.spark.cache()

    return hospital_visit


def get_hospital_measurements(measurement, visit_occurrence, care_site):
    hospital_measurement = measurement.merge(visit_occurrence, on="visit_occurrence_id")
    hospital_measurement = hospital_measurement.merge(care_site, on="care_site_id")

    if is_koalas(hospital_measurement):
        hospital_measurement.spark.cache()

    return hospital_measurement


class BiologyPerVisitProbe(BaseProbe):
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

        **VALUE**: ``["care_site_level", "concept_code", "stay_type", "care_site_id"]``
    """

    _index = ["care_site_level", "concept_code", "stay_type", "care_site_id"]

    def compute_process(
        self,
        data: Data,
        care_site_relationship: pd.DataFrame,
        start_date: datetime = None,
        end_date: datetime = None,
        care_site_levels: List[str] = None,
        stay_types: Union[str, Dict[str, str]] = None,
        concepts_sets: Union[str, Dict[str, str]] = None,
        care_site_ids: List[int] = None,
        care_site_short_names: List[str] = None,
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

        measurement = prepare_measurement(
            data=data,
            biology_relationship=biology_relationship,
            concepts_sets=concepts_sets,
            start_date=start_date,
            end_date=end_date,
            mapping=mapping,
            standard_terminologies=standard_terminologies,
            per_visit=True,
        )

        visit_occurrence = prepare_visit_occurrence(
            data=data,
            start_date=start_date,
            end_date=end_date,
            stay_types=stay_types,
            stay_durations=stay_durations,
        )

        care_site = prepare_care_site(
            data,
            care_site_ids,
            care_site_short_names,
            care_site_relationship,
        )

        hospital_visit = get_hospital_visit(
            measurement,
            visit_occurrence,
            care_site,
        )
        hospital_name = CARE_SITE_LEVEL_NAMES["Hospital"]
        biology_predictor_by_level = {hospital_name: hospital_visit}

        if not hospital_only(care_site_levels=care_site_levels):
            logger.info(
                "Biological measurements are only available at hospital level for now"
            )
            care_site_levels = "Hospital"

        biology_predictor = concatenate_predictor_by_level(
            predictor_by_level=biology_predictor_by_level,
            care_site_levels=care_site_levels,
        )

        return compute_completeness(biology_predictor)
