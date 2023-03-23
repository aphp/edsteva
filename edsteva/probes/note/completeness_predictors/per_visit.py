from datetime import datetime
from typing import Dict, List, Union

import pandas as pd
from loguru import logger

from edsteva.probes.utils.filter_df import convert_uf_to_pole
from edsteva.probes.utils.prepare_df import (
    prepare_care_site,
    prepare_note,
    prepare_note_care_site,
    prepare_visit_detail,
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


def compute_completeness_predictor_per_visit(
    self,
    data: Data,
    care_site_relationship: pd.DataFrame,
    start_date: datetime,
    end_date: datetime,
    care_site_levels: List[str],
    stay_types: Union[str, Dict[str, str]],
    care_site_ids: List[int],
    care_site_short_names: List[str],
    extra_data: Data,
    stay_durations: List[float],
    note_types: Union[str, Dict[str, str]],
    hdfs_user_path: str,
):
    """Script to be used by [``compute()``][edsteva.probes.base.BaseProbe.compute]

    Parameters
    ----------
    data : Data
        Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]
    care_site_relationship : pd.DataFrame
        DataFrame computed in the [``compute()``][edsteva.probes.base.BaseProbe.compute] that gives the hierarchy of the care site structure.
    extra_data : Data, optional
        Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]. This is not OMOP-standardized data but data needed to associate note with UF and Pole. If not provided, it will only compute the predictor for hospitals.
    start_date : datetime, optional
        **EXAMPLE**: `"2019-05-01"`
    end_date : datetime, optional
        **EXAMPLE**: `"2021-07-01"`
    care_site_levels : List[str], optional
        **EXAMPLE**: `["Hospital", "Pole", "UF"]`
    care_site_short_names : List[str], optional
        **EXAMPLE**: `["HOSPITAL 1", "HOSPITAL 2"]`
    stay_types : Union[str, Dict[str, str]], optional
        **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "Urg_and_consult": "urgences|consultation"}` or `"hospitalisés`
    note_types : Union[str, Dict[str, str]], optional
    care_site_ids : List[int], optional
        **EXAMPLE**: `[8312056386, 8312027648]`
    """
    self._metrics = ["c", "n_visit", "n_visit_with_note"]
    check_tables(data=data, required_tables=["note"])

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

    note = prepare_note(data, note_types)

    hospital_visit = get_hospital_visit(note, visit_occurrence, care_site)
    hospital_name = CARE_SITE_LEVEL_NAMES["Hospital"]
    note_predictor_by_level = {hospital_name: hospital_visit}

    # UF selection
    if not hospital_only(care_site_levels=care_site_levels):
        if extra_data:
            visit_detail = prepare_visit_detail(data, start_date, end_date)

            uf_visit = get_uf_visit(
                extra_data,
                note,
                visit_occurrence,
                visit_detail,
                care_site,
            )
            uf_name = CARE_SITE_LEVEL_NAMES["UF"]
            note_predictor_by_level[uf_name] = uf_visit

            pole_visit = get_pole_visit(uf_visit, care_site, care_site_relationship)
            pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
            note_predictor_by_level[pole_name] = pole_visit
        else:
            logger.info("Note data are only available at hospital level")
            care_site_levels = ["Hospital"]

    # Concatenate all predictors
    note_predictor = concatenate_predictor_by_level(
        predictor_by_level=note_predictor_by_level,
        care_site_levels=care_site_levels,
    )

    if is_koalas(note_predictor):
        note_predictor.spark.cache()

    return compute_completeness(self, note_predictor, hdfs_user_path=hdfs_user_path)


def compute_completeness(self, note_predictor, hdfs_user_path: str = None):
    partition_cols = self._index.copy() + ["date"]

    n_visit_with_note = (
        note_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"has_note": "count"})
        .rename(columns={"has_note": "n_visit_with_note"})
    )

    partition_cols = list(set(partition_cols) - {"note_type"})
    n_visit = (
        note_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"visit_id": "nunique"})
        .rename(columns={"visit_id": "n_visit"})
    )

    note_predictor = n_visit_with_note.merge(
        n_visit,
        on=partition_cols,
    )

    note_predictor = to("pandas", note_predictor, hdfs_user_path=hdfs_user_path)

    note_predictor["c"] = note_predictor["n_visit"].where(
        note_predictor["n_visit"] == 0,
        note_predictor["n_visit_with_note"] / note_predictor["n_visit"],
    )

    return note_predictor


def get_hospital_visit(note, visit_occurrence, care_site):
    note_hospital = note[["visit_occurrence_id", "note_type"]].drop_duplicates()
    note_hospital["has_note"] = True
    hospital_visit = visit_occurrence.merge(
        note_hospital, on="visit_occurrence_id", how="left"
    )
    hospital_visit = hospital_visit.rename(columns={"visit_occurrence_id": "visit_id"})
    hospital_visit = hospital_visit.merge(care_site, on="care_site_id")
    if is_koalas(hospital_visit):
        hospital_visit.spark.cache()

    return hospital_visit


def get_uf_visit(
    extra_data,
    note,
    visit_occurrence,
    visit_detail,
    care_site,
):  # pragma: no cover
    note = prepare_note_care_site(extra_data=extra_data, note=note)
    note_uf = note[
        ["visit_occurrence_id", "note_type", "care_site_id"]
    ].drop_duplicates()
    note_uf["has_note"] = True

    visit_detail = visit_detail.merge(
        visit_occurrence[["visit_occurrence_id", "stay_type"]],
        on="visit_occurrence_id",
    )
    visit_detail = visit_detail.merge(
        note_uf,
        on=["visit_occurrence_id", "care_site_id"],
        how="left",
    ).drop(columns="visit_occurrence_id")

    uf_visit = visit_detail.merge(care_site, on="care_site_id")

    uf_name = CARE_SITE_LEVEL_NAMES["UF"]
    uf_visit = uf_visit[uf_visit["care_site_level"] == uf_name]

    if is_koalas(uf_visit):
        uf_visit.spark.cache()

    return uf_visit


def get_pole_visit(uf_visit, care_site, care_site_relationship):  # pragma: no cover
    pole_visit = convert_uf_to_pole(
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
