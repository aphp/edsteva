from datetime import datetime
from typing import Dict, List, Union

import pandas as pd
from loguru import logger

from edsteva.probes.utils.filter_df import convert_uf_to_pole
from edsteva.probes.utils.prepare_df import (
    prepare_care_site,
    prepare_note,
    prepare_note_care_site,
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


def compute_completeness_predictor_per_note(
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
    self._metrics = ["c", "n_note"]
    check_tables(data=data, required_tables=["note"])

    note = prepare_note(
        data=data,
        start_date=start_date,
        end_date=end_date,
        note_types=note_types,
    )

    visit_occurrence = prepare_visit_occurrence(
        data=data,
        stay_types=stay_types,
        stay_durations=stay_durations,
    ).drop(columns=["visit_occurrence_source_value", "date"])

    care_site = prepare_care_site(
        data,
        care_site_ids,
        care_site_short_names,
        care_site_relationship,
    )

    note_hospital = get_hospital_note(note, visit_occurrence, care_site)
    hospital_name = CARE_SITE_LEVEL_NAMES["Hospital"]
    note_predictor_by_level = {hospital_name: note_hospital}

    # UF selection
    if not hospital_only(care_site_levels=care_site_levels):
        if extra_data:
            note_uf = get_uf_note(
                extra_data,
                note,
                visit_occurrence,
                care_site,
            )
            uf_name = CARE_SITE_LEVEL_NAMES["UF"]
            note_predictor_by_level[uf_name] = note_uf

            note_pole = get_pole_note(note_uf, care_site, care_site_relationship)
            pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
            note_predictor_by_level[pole_name] = note_pole
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

    n_note = (
        note_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"note_id": "nunique"})
        .rename(columns={"note_id": "n_note"})
    )
    n_note = to("pandas", n_note, hdfs_user_path=hdfs_user_path)

    partition_cols = list(set(partition_cols) - {"date"})
    max_note = (
        n_note.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"n_note": "max"})
        .rename(columns={"n_note": "max_n_note"})
    )

    note_predictor = n_note.merge(
        max_note,
        on=partition_cols,
    )

    note_predictor = to("pandas", note_predictor)
    note_predictor["c"] = note_predictor["max_n_note"].where(
        note_predictor["max_n_note"] == 0,
        note_predictor["n_note"] / note_predictor["max_n_note"],
    )
    note_predictor = note_predictor.drop(columns="max_n_note")

    return note_predictor


def get_hospital_note(note, visit_occurrence, care_site):
    note_hospital = note.merge(visit_occurrence, on="visit_occurrence_id", how="left")
    note_hospital = note_hospital.drop(columns="visit_occurrence_id")
    note_hospital = note_hospital.merge(care_site, on="care_site_id")
    if is_koalas(note_hospital):
        note_hospital.spark.cache()

    return note_hospital


def get_uf_note(
    extra_data,
    note,
    visit_occurrence,
    care_site,
):  # pragma: no cover
    note_uf = prepare_note_care_site(extra_data=extra_data, note=note)
    note_uf = note_uf.merge(care_site, on="care_site_id")
    note_uf = note_uf.merge(
        visit_occurrence.drop(columns="care_site_id"),
        on="visit_occurrence_id",
        how="left",
    )
    note_uf = note_uf.drop(columns="visit_occurrence_id")

    uf_name = CARE_SITE_LEVEL_NAMES["UF"]
    note_uf = note_uf[note_uf["care_site_level"] == uf_name]

    if is_koalas(note_uf):
        note_uf.spark.cache()

    return note_uf


def get_pole_note(note_uf, care_site, care_site_relationship):  # pragma: no cover
    note_pole = convert_uf_to_pole(
        table=note_uf.drop(columns=["care_site_short_name", "care_site_level"]),
        table_name="note_uf",
        care_site_relationship=care_site_relationship,
    )

    note_pole = note_pole.merge(care_site, on="care_site_id")

    pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
    note_pole = note_pole[note_pole["care_site_level"] == pole_name]

    if is_koalas(note_pole):
        note_pole.spark.cache()

    return note_pole
