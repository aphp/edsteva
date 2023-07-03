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
    impute_missing_dates,
)
from edsteva.utils.checks import check_tables
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data, DataFrame


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
    concept_codes: List[str],
    care_sites_sets: Union[str, Dict[str, str]],
    specialties_sets: Union[str, Dict[str, str]],
    concepts_sets: Union[str, Dict[str, str]],
    length_of_stays: List[float],
    source_terminologies: Dict[str, str],
    mapping: List[Tuple[str, str, str]],
    **kwargs
):
    r"""Script to be used by [``compute()``][edsteva.probes.base.BaseProbe.compute]

    The ``per_measurement`` algorithm computes $c_(t)$ the availability of biological measurements:

    $$
    c(t) = \frac{n_{biology}(t)}{n_{max}}
    $$

    Where $n_{biology}(t)$ is the number of biological measurements, $t$ is the month and $n_{max} = \max_{t}(n_{biology}(t))$.
    """
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
        concept_codes=concept_codes,
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
        length_of_stays=length_of_stays,
    ).drop(columns=["visit_occurrence_source_value", "date"])

    care_site = prepare_care_site(
        data=data,
        care_site_ids=care_site_ids,
        care_site_short_names=care_site_short_names,
        care_site_specialties=care_site_specialties,
        care_sites_sets=care_sites_sets,
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

    return compute_completeness(self, biology_predictor)


def compute_completeness(
    self,
    biology_predictor: DataFrame,
):
    partition_cols = [*self._index.copy(), "date"]
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
    n_measurement = impute_missing_dates(
        start_date=self.start_date,
        end_date=self.end_date,
        predictor=n_measurement,
        partition_cols=partition_cols,
    )

    partition_cols = list(set(partition_cols) - {"date"})
    max_measurement = (
        n_measurement.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )[["n_measurement"]]
        .agg({"n_measurement": "max"})
        .rename(columns={"n_measurement": "max_n_measurement"})
    )

    biology_predictor = n_measurement.merge(
        max_measurement,
        on=partition_cols,
    )

    biology_predictor["c"] = biology_predictor["max_n_measurement"].where(
        biology_predictor["max_n_measurement"] == 0,
        biology_predictor["n_measurement"] / biology_predictor["max_n_measurement"],
    )
    return biology_predictor.drop(columns="max_n_measurement")


def get_hospital_measurements(
    measurement: DataFrame,
    visit_occurrence: DataFrame,
    care_site: DataFrame,
):
    hospital_measurement = measurement.merge(
        visit_occurrence,
        on="visit_occurrence_id",
    )
    hospital_measurement = hospital_measurement.merge(care_site, on="care_site_id")

    if is_koalas(hospital_measurement):
        hospital_measurement = hospital_measurement.spark.cache()

    return hospital_measurement
