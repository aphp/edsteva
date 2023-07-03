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
    impute_missing_dates,
)
from edsteva.utils.checks import check_condition_source_systems, check_tables
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data, DataFrame


def compute_completeness_predictor_per_visit(
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
    care_sites_sets: Union[str, Dict[str, str]],
    specialties_sets: Union[str, Dict[str, str]],
    diag_types: Union[str, Dict[str, str]],
    condition_types: Union[str, Dict[str, str]],
    source_systems: List[str],
    length_of_stays: List[float],
    **kwargs
):
    r"""Script to be used by [``compute()``][edsteva.probes.base.BaseProbe.compute]

    The ``per_visit`` algorithm computes $c_(t)$ the availability of claim data linked to patients' administrative stays:

    $$
    c(t) = \frac{n_{with\,condition}(t)}{n_{visit}(t)}
    $$

    Where $n_{visit}(t)$ is the number of administrative stays, $n_{with\,condition}$ the number of stays having at least one claim code (e.g. ICD-10) recorded and $t$ is the month.
    """

    self._metrics = ["c", "n_visit", "n_visit_with_condition"]
    check_tables(data=data, required_tables=["condition_occurrence"])
    check_condition_source_systems(source_systems=source_systems)
    if "AREM" in source_systems and not hospital_only(
        care_site_levels=care_site_levels
    ):  # pragma: no cover
        logger.info("AREM claim data are only available at hospital level")

    visit_occurrence = prepare_visit_occurrence(
        data=data,
        start_date=start_date,
        end_date=end_date,
        stay_types=stay_types,
        length_of_stays=length_of_stays,
    )

    condition_occurrence = prepare_condition_occurrence(
        data=data,
        extra_data=extra_data,
        visit_occurrence=visit_occurrence,
        source_systems=source_systems,
        diag_types=diag_types,
        condition_types=condition_types,
    ).drop(columns=["condition_occurrence_id", "date"])

    care_site = prepare_care_site(
        data=data,
        care_site_ids=care_site_ids,
        care_site_short_names=care_site_short_names,
        care_site_relationship=care_site_relationship,
        care_site_specialties=care_site_specialties,
        care_sites_sets=care_sites_sets,
        specialties_sets=specialties_sets,
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
        visit_detail = prepare_visit_detail(data, start_date, end_date)

        uf_visit = get_uf_visit(
            condition_occurrence=condition_occurrence,
            visit_occurrence=visit_occurrence,
            visit_detail=visit_detail,
            care_site=care_site,
        )
        uf_name = CARE_SITE_LEVEL_NAMES["UF"]
        condition_predictor_by_level[uf_name] = uf_visit

        pole_visit = get_pole_visit(
            uf_visit=uf_visit,
            care_site=care_site,
            care_site_relationship=care_site_relationship,
        )
        pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
        condition_predictor_by_level[pole_name] = pole_visit

    condition_predictor = concatenate_predictor_by_level(
        predictor_by_level=condition_predictor_by_level,
        care_site_levels=care_site_levels,
    )

    return compute_completeness(self, condition_predictor)


def compute_completeness(
    self,
    condition_predictor: DataFrame,
):
    # Visit with diagnosis
    partition_cols = [*self._index.copy(), "date"]
    n_visit_with_condition = (
        condition_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"has_condition": "count"})
        .rename(columns={"has_condition": "n_visit_with_condition"})
    )
    n_visit_with_condition = to("pandas", n_visit_with_condition)
    n_visit_with_condition = n_visit_with_condition[
        n_visit_with_condition.n_visit_with_condition > 0
    ]
    n_visit_with_condition = impute_missing_dates(
        start_date=self.start_date,
        end_date=self.end_date,
        predictor=n_visit_with_condition,
        partition_cols=partition_cols,
    )

    # Visit total
    condition_columns = ["diag_type", "condition_type", "source_system"]
    partition_cols = list(set(partition_cols) - set(condition_columns))
    n_visit = (
        condition_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"visit_id": "nunique"})
        .rename(columns={"visit_id": "n_visit"})
    )
    n_visit = to("pandas", n_visit)
    n_visit = impute_missing_dates(
        start_date=self.start_date,
        end_date=self.end_date,
        predictor=n_visit,
        partition_cols=partition_cols,
    )

    condition_predictor = n_visit_with_condition.merge(
        n_visit,
        on=partition_cols,
    )

    condition_predictor["c"] = condition_predictor["n_visit"].where(
        condition_predictor["n_visit"] == 0,
        condition_predictor["n_visit_with_condition"] / condition_predictor["n_visit"],
    )

    return condition_predictor


def get_hospital_visit(
    condition_occurrence: DataFrame,
    visit_occurrence: DataFrame,
    care_site: DataFrame,
):
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
        hospital_visit = hospital_visit.spark.cache()

    return hospital_visit


def get_uf_visit(
    condition_occurrence: DataFrame,
    visit_occurrence: DataFrame,
    visit_detail: DataFrame,
    care_site: DataFrame,
):  # pragma: no cover
    visit_detail = visit_detail[visit_detail.visit_detail_type == "RUM"]
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
        visit_occurrence[
            visit_occurrence.columns.intersection(
                set(["visit_occurrence_id", "length_of_stay", "stay_type"])
            )
        ],
        on="visit_occurrence_id",
    )
    uf_visit = visit_detail.merge(
        condition_uf,
        on="visit_id",
        how="left",
    ).drop(columns=["visit_occurrence_id"])

    uf_visit = uf_visit.merge(care_site, on="care_site_id")

    uf_name = CARE_SITE_LEVEL_NAMES["UF"]
    uf_visit = uf_visit[uf_visit["care_site_level"] == uf_name]

    if is_koalas(uf_visit):
        uf_visit = uf_visit.spark.cache()

    return uf_visit


def get_pole_visit(
    uf_visit: DataFrame,
    care_site: DataFrame,
    care_site_relationship: DataFrame,
):  # pragma: no cover
    care_site_cols = list(
        set(
            [
                "care_site_short_name",
                "care_site_level",
                "care_site_specialty",
                "specialties_set",
                "care_sites_set",
            ]
        ).intersection(uf_visit.columns)
    )
    pole_visit = convert_uf_to_pole(
        table=uf_visit.drop(columns=care_site_cols),
        table_name="uf_visit",
        care_site_relationship=care_site_relationship,
    )

    pole_visit = pole_visit.merge(care_site, on="care_site_id")
    pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
    pole_visit = pole_visit[pole_visit["care_site_level"] == pole_name]
    if is_koalas(pole_visit):
        pole_visit = pole_visit.spark.cache()

    return pole_visit
