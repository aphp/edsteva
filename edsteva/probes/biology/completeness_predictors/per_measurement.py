from datetime import datetime
from typing import Dict, List, Tuple, Union

import pandas as pd
from loguru import logger

from edsteva.probes.utils.prepare_df import (
    prepare_biology_relationship,
    prepare_care_site,
    prepare_measurement,
    prepare_visit_occurrence,
)
from edsteva.probes.utils.utils import (
    CARE_SITE_LEVEL_NAMES,
    concatenate_predictor_by_level,
    hospital_only,
)
from edsteva.utils.checks import check_tables
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data


def compute_completeness_predictor_per_measurement(
    self,
    data: Data,
    care_site_relationship: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    care_site_levels: List[str],
    stay_types: Union[str, Dict[str, str]],
    care_site_ids: List[int],
    care_site_short_names: List[str],
    care_site_specialties: List[str],
    specialties_sets: Union[str, Dict[str, str]],
    concepts_sets: Union[str, Dict[str, str]],
    stay_durations: List[float],
    source_terminologies: Dict[str, str],
    mapping: List[Tuple[str, str, str]],
    hdfs_user_path: str,
):
    """Script to be used by [``compute()``][edsteva.probes.base.BaseProbe.compute]"""
    self._metrics = ["c", "n_measurement"]
    check_tables(
        data=data,
        required_tables=["measurement", "concept", "concept_relationship"],
    )
    standard_terminologies = self._standard_terminologies
    biology_relationship = prepare_biology_relationship(
        data=data,
        standard_terminologies=standard_terminologies,
        source_terminologies=source_terminologies,
        mapping=mapping,
    )

    self.biology_relationship = biology_relationship
    root_terminology = mapping[0][0]

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
        care_site_specialties=care_site_specialties,
        specialties_sets=specialties_sets,
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

    return compute_completeness(self, biology_predictor, hdfs_user_path)


def compute_completeness(self, biology_predictor, hdfs_user_path: str = None):
    partition_cols = self._index.copy() + ["date"]
    n_measurement = (
        biology_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"measurement_id": "nunique"})
        .rename(columns={"measurement_id": "n_measurement"})
    )

    n_measurement = to("pandas", n_measurement, hdfs_user_path=hdfs_user_path)
    partition_cols = list(set(partition_cols) - {"date"})
    q_99_measurement = (
        n_measurement.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )[["n_measurement"]]
        .agg({"n_measurement": "max"})
        .rename(columns={"n_measurement": "max_n_measurement"})
    )

    biology_predictor = n_measurement.merge(
        q_99_measurement,
        on=partition_cols,
    )

    biology_predictor["c"] = biology_predictor["max_n_measurement"].where(
        biology_predictor["max_n_measurement"] == 0,
        biology_predictor["n_measurement"] / biology_predictor["max_n_measurement"],
    )
    biology_predictor = biology_predictor.drop(columns="max_n_measurement")

    return biology_predictor


def get_hospital_measurements(measurement, visit_occurrence, care_site):
    hospital_measurement = measurement.merge(
        visit_occurrence.drop(columns="date"), on="visit_occurrence_id"
    )
    hospital_measurement = hospital_measurement.merge(care_site, on="care_site_id")

    if is_koalas(hospital_measurement):
        hospital_measurement = hospital_measurement.spark.cache()

    return hospital_measurement
