from datetime import datetime
from typing import Dict, List, Union

import pandas as pd

from edsteva.probes.utils.filter_df import convert_uf_to_pole
from edsteva.probes.utils.prepare_df import (
    prepare_care_site,
    prepare_visit_detail,
    prepare_visit_occurrence,
)
from edsteva.probes.utils.utils import (
    CARE_SITE_LEVEL_NAMES,
    concatenate_predictor_by_level,
    hospital_only,
)
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data


def compute_completeness_predictor_per_visit(
    self,
    data: Data,
    care_site_relationship: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    care_site_levels: List[str],
    stay_types: Union[str, Dict[str, str]],
    care_site_ids: List[int],
    care_site_short_names: List[str] = None,
    stay_durations: List[float] = None,
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
    self._metrics = ["c", "n_visit"]
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
        visit_occurrence,
        care_site,
    )
    hospital_name = CARE_SITE_LEVEL_NAMES["Hospital"]
    visit_predictor_by_level = {hospital_name: hospital_visit}

    if not hospital_only(care_site_levels=care_site_levels):
        visit_detail = prepare_visit_detail(data, start_date, end_date)

        uf_name = CARE_SITE_LEVEL_NAMES["UF"]
        uf_visit = get_uf_visit(
            visit_occurrence,
            visit_detail,
            care_site,
        )
        visit_predictor_by_level[uf_name] = uf_visit

        pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
        pole_visit = get_pole_visit(
            uf_visit,
            care_site,
            care_site_relationship,
        )
        visit_predictor_by_level[pole_name] = pole_visit

    visit_predictor = concatenate_predictor_by_level(
        predictor_by_level=visit_predictor_by_level,
        care_site_levels=care_site_levels,
    )

    return compute_completeness(self, visit_predictor)


def compute_completeness(self, visit_predictor):
    partition_cols = self._index.copy() + ["date"]

    n_visit = (
        visit_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"visit_id": "nunique"})
        .rename(columns={"visit_id": "n_visit"})
    )

    n_visit = to("pandas", n_visit)

    partition_cols = list(set(partition_cols) - {"date"})
    q_99_visit = (
        n_visit.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"n_visit": "max"})
        .rename(columns={"n_visit": "max_n_visit"})
    )

    visit_predictor = n_visit.merge(
        q_99_visit,
        on=partition_cols,
    )

    visit_predictor["c"] = visit_predictor["max_n_visit"].where(
        visit_predictor["max_n_visit"] == 0,
        visit_predictor["n_visit"] / visit_predictor["max_n_visit"],
    )
    visit_predictor = visit_predictor.drop(columns="max_n_visit")

    return visit_predictor


def get_hospital_visit(visit_occurrence, care_site):
    hospital_visit = visit_occurrence.rename(
        columns={"visit_occurrence_id": "visit_id"}
    )

    hospital_visit = hospital_visit.merge(care_site, on="care_site_id")
    hospital_visit = hospital_visit[
        hospital_visit["care_site_level"] == CARE_SITE_LEVEL_NAMES["Hospital"]
    ]
    if is_koalas(hospital_visit):
        hospital_visit.spark.cache()

    return hospital_visit


def get_uf_visit(visit_occurrence, visit_detail, care_site):
    visit_detail = visit_detail.merge(
        visit_occurrence[["visit_occurrence_id", "length_of_stay", "stay_type"]],
        on="visit_occurrence_id",
    ).drop(columns="visit_occurrence_id")

    uf_visit = visit_detail.merge(care_site, on="care_site_id")
    uf_visit = uf_visit[uf_visit["care_site_level"] == CARE_SITE_LEVEL_NAMES["UF"]]
    if is_koalas(uf_visit):
        uf_visit.spark.cache()

    return uf_visit


def get_pole_visit(uf_visit, care_site, care_site_relationship):
    pole_visit = convert_uf_to_pole(
        table=uf_visit.drop(columns=["care_site_short_name", "care_site_level"]),
        table_name="uf_visit",
        care_site_relationship=care_site_relationship,
    )

    pole_visit = pole_visit.merge(care_site, on="care_site_id")
    pole_visit = pole_visit[
        pole_visit["care_site_level"] == CARE_SITE_LEVEL_NAMES["Pole"]
    ]
    if is_koalas(pole_visit):
        pole_visit.spark.cache()

    return pole_visit
