from datetime import datetime
from typing import Dict, List, Union

import pandas as pd
from loguru import logger

from edsteva.probes.base import BaseProbe
from edsteva.probes.utils import (
    CARE_SITE_LEVEL_NAMES,
    concatenate_predictor_by_level,
    convert_table_to_pole,
    convert_table_to_uf,
    hospital_only,
    prepare_care_site,
    prepare_condition_occurrence,
    prepare_visit_detail,
    prepare_visit_occurrence,
)
from edsteva.utils.checks import check_conditon_source_systems, check_tables
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data


def compute_completeness(condition_predictor):
    partition_cols = [
        "care_site_level",
        "care_site_id",
        "care_site_short_name",
        "stay_type",
        "diag_type",
        "condition_type",
        "source_system",
        "date",
    ]
    n_visit_with_condition = (
        condition_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"has_condition": "count"})
        .rename(columns={"has_condition": "n_visit_with_condition"})
    )
    partition_cols = list(
        set(partition_cols) - {"diag_type", "condition_type", "source_system"}
    )

    n_visit = (
        condition_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"visit_id": "nunique"})
        .rename(columns={"visit_id": "n_visit"})
    )

    condition_predictor = n_visit_with_condition.merge(
        n_visit,
        on=partition_cols,
    )

    condition_predictor = to("pandas", condition_predictor)

    condition_predictor["c"] = condition_predictor["n_visit"].where(
        condition_predictor["n_visit"] == 0,
        condition_predictor["n_visit_with_condition"] / condition_predictor["n_visit"],
    )
    condition_predictor = condition_predictor.drop(columns=["n_visit_with_condition"])

    return condition_predictor


def get_hospital_visit(condition_occurrence, visit_occurrence, care_site):
    condition_hospital = condition_occurrence.drop_duplicates(
        ["visit_occurrence_id", "diag_type", "condition_type", "source_system"]
    )
    condition_hospital["has_condition"] = True
    hospital_visit = visit_occurrence.merge(
        condition_hospital,
        on="visit_occurrence_id",
        how="left",
    )
    hospital_visit = hospital_visit.rename(columns={"visit_occurrence_id": "visit_id"})
    hospital_visit = hospital_visit.merge(care_site, on="care_site_id")

    if is_koalas(hospital_visit):
        hospital_visit.spark.cache()

    return hospital_visit


def get_uf_visit(
    condition_occurrence,
    visit_occurrence,
    visit_detail,
    care_site,
    care_site_relationship,
):  # pragma: no cover
    condition_uf = (
        condition_occurrence[
            [
                "visit_detail_id",
                "diag_type",
                "condition_type",
                "source_system",
            ]
        ]
        .drop_duplicates()
        .rename(columns={"visit_detail_id": "visit_id"})
    )
    condition_uf["has_condition"] = True

    visit_detail = visit_detail.merge(
        visit_occurrence[["visit_occurrence_id", "stay_type"]],
        on="visit_occurrence_id",
    )
    visit_detail = visit_detail.merge(
        condition_uf,
        on="visit_id",
        how="left",
    )
    visit_detail = visit_detail.drop(columns=["visit_occurrence_id"])

    uf_visit = convert_table_to_uf(
        table=visit_detail,
        table_name="visit_detail",
        care_site_relationship=care_site_relationship,
    )
    uf_visit = uf_visit.merge(care_site, on="care_site_id")

    uf_name = CARE_SITE_LEVEL_NAMES["UF"]
    uf_visit = uf_visit[uf_visit["care_site_level"] == uf_name]

    if is_koalas(uf_visit):
        uf_visit.spark.cache()

    return uf_visit


def get_pole_visit(uf_visit, care_site, care_site_relationship):  # pragma: no cover
    pole_visit = convert_table_to_pole(
        table=uf_visit.drop(columns=["care_site_short_name", "care_site_level"]),
        table_name="uf_visit",
        care_site_relationship=care_site_relationship,
    )

    pole_visit = pole_visit.merge(care_site, on="care_site_id")

    pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
    pole_visit = pole_visit[pole_visit["care_site_level"] == pole_name]

    if is_koalas(pole_visit):
        pole_visit.spark.cache()

    return pole_visit


class ConditionProbe(BaseProbe):
    r"""
    The [``ConditionProbe``][edsteva.probes.condition.ConditionProbe] computes $c_{condition}(t)$ the availability of claim data in patients' administrative stay:

    $$
    c_{condition}(t) = \frac{n_{with\,condition}(t)}{n_{visit}(t)}
    $$

    Where $n_{visit}(t)$ is the number of administrative stays, $n_{with\,condition}$ the number of stays having at least one claim code (e.g. ICD-10) recorded and $t$ is the month.

    Attributes
    ----------
    _index: List[str]
        Variable from which data is grouped

        **VALUE**: ``["care_site_level", "stay_type", "diag_type", "condition_type", "source_system", "care_site_id"]``
    """

    _index = [
        "care_site_level",
        "stay_type",
        "diag_type",
        "condition_type",
        "source_system",
        "care_site_id",
    ]

    def compute_process(
        self,
        data: Data,
        care_site_relationship: pd.DataFrame,
        extra_data: Data = None,
        start_date: datetime = None,
        end_date: datetime = None,
        care_site_levels: List[str] = None,
        stay_types: Union[str, Dict[str, str]] = None,
        diag_types: Union[str, Dict[str, str]] = None,
        condition_types: Union[str, Dict[str, str]] = {
            "All": ".*",
            "Cancer": "C",
        },
        source_systems: List[str] = ["ORBIS"],
        care_site_ids: List[int] = None,
        care_site_short_names: List[str] = None,
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

        check_tables(data=data, required_tables=["condition_occurrence"])
        check_conditon_source_systems(source_systems=source_systems)
        if "AREM" in source_systems and not hospital_only(
            care_site_levels=care_site_levels
        ):
            logger.info("AREM claim data are only available at hospital level")

        visit_occurrence = prepare_visit_occurrence(
            data=data,
            start_date=start_date,
            end_date=end_date,
            stay_types=stay_types,
        )

        condition_occurrence = prepare_condition_occurrence(
            data=data,
            extra_data=extra_data,
            visit_occurrence=visit_occurrence,
            source_systems=source_systems,
            diag_types=diag_types,
            condition_types=condition_types,
        )

        care_site = prepare_care_site(
            data=data,
            care_site_ids=care_site_ids,
            care_site_short_names=care_site_short_names,
            care_site_relationship=care_site_relationship,
        )

        hospital_visit = get_hospital_visit(
            condition_occurrence,
            visit_occurrence,
            care_site,
        )
        hospital_name = CARE_SITE_LEVEL_NAMES["Hospital"]
        condition_predictor_by_level = {hospital_name: hospital_visit}

        # UF selection
        if not hospital_only(care_site_levels=care_site_levels):
            visit_detail = prepare_visit_detail(
                data, start_date, end_date, visit_detail_type="RUM"
            )

            uf_visit = get_uf_visit(
                condition_occurrence,
                visit_occurrence,
                visit_detail,
                care_site,
                care_site_relationship,
            )
            uf_name = CARE_SITE_LEVEL_NAMES["UF"]
            condition_predictor_by_level[uf_name] = uf_visit

            pole_visit = get_pole_visit(uf_visit, care_site, care_site_relationship)
            pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
            condition_predictor_by_level[pole_name] = pole_visit

        condition_predictor = concatenate_predictor_by_level(
            predictor_by_level=condition_predictor_by_level,
            care_site_levels=care_site_levels,
        )

        return compute_completeness(condition_predictor)
