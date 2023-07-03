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
    impute_missing_dates,
)
from edsteva.utils.checks import check_tables
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data, DataFrame


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
    care_site_specialties: List[str],
    care_sites_sets: Union[str, Dict[str, str]],
    specialties_sets: Union[str, Dict[str, str]],
    extra_data: Data,
    length_of_stays: List[float],
    note_types: Union[str, Dict[str, str]],
    **kwargs
):
    r"""Script to be used by [``compute()``][edsteva.probes.base.BaseProbe.compute]

    The ``per_note`` algorithm computes $c_(t)$ the availability of clinical documents as follow:

    $$
    c(t) = \frac{n_{note}(t)}{n_{max}}
    $$

    Where $n_{note}(t)$ is the number of clinical documents, $t$ is the month and $n_{max} = \max_{t}(n_{note}(t))$.
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
        length_of_stays=length_of_stays,
    ).drop(columns=["visit_occurrence_source_value", "date"])

    care_site = prepare_care_site(
        data=data,
        care_site_ids=care_site_ids,
        care_site_short_names=care_site_short_names,
        care_site_relationship=care_site_relationship,
        care_site_specialties=care_site_specialties,
        care_sites_sets=care_sites_sets,
        specialties_sets=specialties_sets,
    )

    note_hospital = get_hospital_note(note, visit_occurrence, care_site)
    hospital_name = CARE_SITE_LEVEL_NAMES["Hospital"]
    note_predictor_by_level = {hospital_name: note_hospital}

    # UF selection
    if not hospital_only(care_site_levels=care_site_levels):
        if extra_data:  # pragma: no cover
            note_uf, note_uc, note_uh = get_note_detail(
                extra_data,
                note,
                visit_occurrence,
                care_site,
            )
            uf_name = CARE_SITE_LEVEL_NAMES["UF"]
            note_predictor_by_level[uf_name] = note_uf
            uc_name = CARE_SITE_LEVEL_NAMES["UC"]
            note_predictor_by_level[uc_name] = note_uc
            uh_name = CARE_SITE_LEVEL_NAMES["UH"]
            note_predictor_by_level[uh_name] = note_uh

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

    return compute_completeness(self, note_predictor)


def compute_completeness(
    self,
    note_predictor: DataFrame,
):
    partition_cols = [*self._index.copy(), "date"]

    n_note = (
        note_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"note_id": "nunique"})
        .rename(columns={"note_id": "n_note"})
    )
    n_note = to("pandas", n_note)
    n_note = impute_missing_dates(
        start_date=self.start_date,
        end_date=self.end_date,
        predictor=n_note,
        partition_cols=partition_cols,
    )

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

    note_predictor["c"] = note_predictor["max_n_note"].where(
        note_predictor["max_n_note"] == 0,
        note_predictor["n_note"] / note_predictor["max_n_note"],
    )
    return note_predictor.drop(columns="max_n_note")


def get_hospital_note(
    note: DataFrame,
    visit_occurrence: DataFrame,
    care_site: DataFrame,
):
    note_hospital = note.merge(visit_occurrence, on="visit_occurrence_id")
    note_hospital = note_hospital.drop(columns="visit_occurrence_id")
    note_hospital = note_hospital.merge(care_site, on="care_site_id")
    if is_koalas(note_hospital):
        note_hospital = note_hospital.spark.cache()

    return note_hospital


def get_note_detail(
    extra_data: Data,
    note: DataFrame,
    visit_occurrence: DataFrame,
    care_site: DataFrame,
):  # pragma: no cover
    note_detail = prepare_note_care_site(extra_data=extra_data, note=note)
    note_detail = note_detail.merge(care_site, on="care_site_id")
    note_detail = note_detail.merge(
        visit_occurrence.drop(columns="care_site_id"), on="visit_occurrence_id"
    ).drop(columns="visit_occurrence_id")

    uf_name = CARE_SITE_LEVEL_NAMES["UF"]
    note_uf = note_detail[note_detail["care_site_level"] == uf_name]
    uc_name = CARE_SITE_LEVEL_NAMES["UC"]
    note_uc = note_detail[note_detail["care_site_level"] == uc_name]
    uh_name = CARE_SITE_LEVEL_NAMES["UH"]
    note_uh = note_detail[note_detail["care_site_level"] == uh_name]
    if is_koalas(note_detail):
        note_uf = note_uf.spark.cache()
        note_uc = note_uc.spark.cache()
        note_uh = note_uh.spark.cache()

    return note_uf, note_uc, note_uh


def get_pole_note(
    note_uf: DataFrame,
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
        ).intersection(note_uf.columns)
    )
    note_pole = convert_uf_to_pole(
        table=note_uf.drop(columns=care_site_cols),
        table_name="note_uf",
        care_site_relationship=care_site_relationship,
    )

    note_pole = note_pole.merge(care_site, on="care_site_id")

    pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
    note_pole = note_pole[note_pole["care_site_level"] == pole_name]

    if is_koalas(note_pole):
        note_pole = note_pole.spark.cache()

    return note_pole
