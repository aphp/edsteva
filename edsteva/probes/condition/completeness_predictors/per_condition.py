from datetime import datetime
from typing import Dict, List, Union

import pandas as pd
from loguru import logger

from edsteva.probes.utils.filter_df import convert_uf_to_pole
from edsteva.probes.utils.prepare_df import (
    prepare_care_site,
    prepare_condition_occurrence,
    prepare_visit_detail,
    prepare_visit_occurrence,
)
from edsteva.probes.utils.utils import (
    CARE_SITE_LEVEL_NAMES,
    concatenate_predictor_by_level,
    hospital_only,
)
from edsteva.utils.checks import check_conditon_source_systems, check_tables
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data


def compute_completeness_predictor_per_condition(
    self,
    data: Data,
    care_site_relationship: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    care_site_levels: List[str],
    stay_types: Union[str, Dict[str, str]],
    care_site_ids: List[int],
    extra_data: Data,
    care_site_short_names: List[str],
    care_site_specialties: List[str],
    specialties_sets: Union[str, Dict[str, str]],
    diag_types: Union[str, Dict[str, str]],
    condition_types: Union[str, Dict[str, str]],
    source_systems: List[str],
    stay_durations: List[float],
    hdfs_user_path: str,
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
        **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "Urg_and_consult": "urgences|consultation"}` or `"hospitalisés"`
    diag_types : Union[str, Dict[str, str]], optional
        **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "DP\DR": "DP|DR"}` or `"DP"`
    condition_types : Union[str, Dict[str, str]], optional
        **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "Pulmonary_embolism": "I26"}`
    source_systems : List[str], optional
        **EXAMPLE**: `["AREM", "ORBIS"]`
    care_site_ids : List[int], optional
        **EXAMPLE**: `[8312056386, 8312027648]`
    care_site_short_names : List[str], optional
        **EXAMPLE**: `["HOSPITAL 1", "HOSPITAL 2"]`
    """

    self._metrics = ["c", "n_condition"]
    check_tables(data=data, required_tables=["condition_occurrence"])
    check_conditon_source_systems(source_systems=source_systems)
    if "AREM" in source_systems and not hospital_only(
        care_site_levels=care_site_levels
    ):
        logger.info("AREM claim data are only available at hospital level")

    visit_occurrence = prepare_visit_occurrence(
        data=data,
        stay_types=stay_types,
        stay_durations=stay_durations,
    ).drop(columns="date")

    condition_occurrence = prepare_condition_occurrence(
        data=data,
        extra_data=extra_data,
        visit_occurrence=visit_occurrence,
        source_systems=source_systems,
        diag_types=diag_types,
        condition_types=condition_types,
        start_date=start_date,
        end_date=end_date,
    )

    care_site = prepare_care_site(
        data=data,
        care_site_ids=care_site_ids,
        care_site_short_names=care_site_short_names,
        care_site_relationship=care_site_relationship,
        care_site_specialties=care_site_specialties,
        specialties_sets=specialties_sets,
    )

    hospital_visit = get_hospital_condition(
        condition_occurrence,
        visit_occurrence,
        care_site,
    )
    hospital_name = CARE_SITE_LEVEL_NAMES["Hospital"]
    condition_predictor_by_level = {hospital_name: hospital_visit}

    # UF selection
    if not hospital_only(care_site_levels=care_site_levels):
        visit_detail = prepare_visit_detail(
            data=data,
            start_date=start_date,
            end_date=end_date,
        )

        uf_condition = get_uf_condition(
            condition_occurrence=condition_occurrence,
            visit_occurrence=visit_occurrence,
            visit_detail=visit_detail,
            care_site=care_site,
        )
        uf_name = CARE_SITE_LEVEL_NAMES["UF"]
        condition_predictor_by_level[uf_name] = uf_condition

        pole_condition = get_pole_condition(
            uf_condition, care_site, care_site_relationship
        )
        pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
        condition_predictor_by_level[pole_name] = pole_condition

    condition_predictor = concatenate_predictor_by_level(
        predictor_by_level=condition_predictor_by_level,
        care_site_levels=care_site_levels,
    )

    return compute_completeness(
        self, condition_predictor, hdfs_user_path=hdfs_user_path
    )


def compute_completeness(self, condition_predictor, hdfs_user_path: str):
    partition_cols = self._index.copy() + ["date"]

    n_condition = (
        condition_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"condition_occurrence_id": "nunique"})
        .rename(columns={"condition_occurrence_id": "n_condition"})
    )
    n_condition = to("pandas", n_condition, hdfs_user_path=hdfs_user_path)

    partition_cols = list(set(partition_cols) - {"date"})
    max_n_condition = (
        n_condition.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"n_condition": "max"})
        .rename(columns={"n_condition": "max_n_condition"})
    )

    condition_predictor = n_condition.merge(
        max_n_condition,
        on=partition_cols,
    )

    condition_predictor = to("pandas", condition_predictor)
    condition_predictor["c"] = condition_predictor["max_n_condition"].where(
        condition_predictor["max_n_condition"] == 0,
        condition_predictor["n_condition"] / condition_predictor["max_n_condition"],
    )
    condition_predictor = condition_predictor.drop(columns="max_n_condition")

    return condition_predictor


def get_hospital_condition(condition_occurrence, visit_occurrence, care_site):
    hospital_condition = condition_occurrence.merge(
        visit_occurrence,
        on="visit_occurrence_id",
        how="left",
    )
    hospital_condition = hospital_condition.drop(columns="visit_occurrence_id")
    hospital_condition = hospital_condition.merge(care_site, on="care_site_id")

    if is_koalas(hospital_condition):
        hospital_condition = hospital_condition.spark.cache()

    return hospital_condition


def get_uf_condition(
    condition_occurrence,
    visit_occurrence,
    visit_detail,
    care_site,
):  # pragma: no cover
    # Add visit information
    visit_detail = visit_detail[visit_detail.visit_detail_type == "RUM"]
    uf_condition = condition_occurrence.merge(
        visit_occurrence.drop(columns="care_site_id"),
        on="visit_occurrence_id",
        how="left",
    ).rename(columns={"visit_detail_id": "visit_id"})

    # Add care_site information
    uf_condition = uf_condition.merge(
        visit_detail[["visit_id", "care_site_id"]],
        on="visit_id",
        how="left",
    )
    uf_condition = uf_condition.merge(care_site, on="care_site_id")

    uf_name = CARE_SITE_LEVEL_NAMES["UF"]
    uf_condition = uf_condition[uf_condition["care_site_level"] == uf_name]

    if is_koalas(uf_condition):
        uf_condition = uf_condition.spark.cache()

    return uf_condition


def get_pole_condition(
    uf_condition, care_site, care_site_relationship
):  # pragma: no cover
    pole_condition = convert_uf_to_pole(
        table=uf_condition.drop(
            columns=["care_site_short_name", "care_site_level", "care_site_specialty"]
        ),
        table_name="uf_condition",
        care_site_relationship=care_site_relationship,
    )

    pole_condition = pole_condition.merge(care_site, on="care_site_id")

    pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
    pole_condition = pole_condition[pole_condition["care_site_level"] == pole_name]

    if is_koalas(pole_condition):
        pole_condition = pole_condition.spark.cache()

    return pole_condition