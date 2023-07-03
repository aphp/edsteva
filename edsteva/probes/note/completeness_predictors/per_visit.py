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
    impute_missing_dates,
)
from edsteva.utils.checks import check_tables
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

    The ``per_visit`` algorithm computes $c_(t)$ the availability of clinical documents linked to patients' administrative stays:

    $$
    c(t) = \frac{n_{with\,doc}(t)}{n_{visit}(t)}
    $$

    Where $n_{visit}(t)$ is the number of administrative stays, $n_{with\,doc}$ the number of visits having at least one document and $t$ is the month.
    """

    self._metrics = ["c", "n_visit", "n_visit_with_note"]
    check_tables(data=data, required_tables=["note"])

    visit_occurrence = prepare_visit_occurrence(
        data=data,
        start_date=start_date,
        end_date=end_date,
        stay_types=stay_types,
        length_of_stays=length_of_stays,
    )

    care_site = prepare_care_site(
        data=data,
        care_site_ids=care_site_ids,
        care_site_short_names=care_site_short_names,
        care_site_relationship=care_site_relationship,
        care_site_specialties=care_site_specialties,
        care_sites_sets=care_sites_sets,
        specialties_sets=specialties_sets,
    )

    note = prepare_note(data, note_types)

    hospital_visit = get_hospital_visit(note, visit_occurrence, care_site)
    hospital_name = CARE_SITE_LEVEL_NAMES["Hospital"]
    note_predictor_by_level = {hospital_name: hospital_visit}

    # UF selection
    if not hospital_only(care_site_levels=care_site_levels):
        if extra_data:  # pragma: no cover
            visit_detail = prepare_visit_detail(data, start_date, end_date)

            uf_visit, uc_visit, uh_visit = get_visit_detail(
                extra_data=extra_data,
                note=note,
                visit_occurrence=visit_occurrence,
                visit_detail=visit_detail,
                care_site=care_site,
            )
            uf_name = CARE_SITE_LEVEL_NAMES["UF"]
            note_predictor_by_level[uf_name] = uf_visit
            uc_name = CARE_SITE_LEVEL_NAMES["UC"]
            note_predictor_by_level[uc_name] = uc_visit
            uh_name = CARE_SITE_LEVEL_NAMES["UH"]
            note_predictor_by_level[uh_name] = uh_visit

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

    return compute_completeness(self, note_predictor)


def compute_completeness(
    self,
    note_predictor: DataFrame,
):
    # Visit with note
    partition_cols = [*self._index.copy(), "date"]
    n_visit_with_note = (
        note_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"has_note": "count"})
        .rename(columns={"has_note": "n_visit_with_note"})
    )
    n_visit_with_note = to("pandas", n_visit_with_note)
    n_visit_with_note = n_visit_with_note[n_visit_with_note.n_visit_with_note > 0]
    n_visit_with_note = impute_missing_dates(
        start_date=self.start_date,
        end_date=self.end_date,
        predictor=n_visit_with_note,
        partition_cols=partition_cols,
    )

    # Visit total
    note_columns = ["note_type"]
    partition_cols = list(set(partition_cols) - set(note_columns))
    n_visit = (
        note_predictor.groupby(
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
    note_predictor = n_visit_with_note.merge(
        n_visit,
        on=partition_cols,
    )

    # Compute completeness
    note_predictor["c"] = note_predictor["n_visit"].where(
        note_predictor["n_visit"] == 0,
        note_predictor["n_visit_with_note"] / note_predictor["n_visit"],
    )

    return note_predictor


def get_hospital_visit(
    note: DataFrame,
    visit_occurrence: DataFrame,
    care_site: DataFrame,
):
    note_hospital = note[["visit_occurrence_id", "note_type"]].drop_duplicates()
    note_hospital["has_note"] = True
    hospital_visit = visit_occurrence.merge(
        note_hospital, on="visit_occurrence_id", how="left"
    )
    hospital_visit = hospital_visit.rename(columns={"visit_occurrence_id": "visit_id"})
    hospital_visit = hospital_visit.merge(care_site, on="care_site_id")
    if is_koalas(hospital_visit):
        hospital_visit = hospital_visit.spark.cache()

    return hospital_visit


def get_visit_detail(
    extra_data: Data,
    note: DataFrame,
    visit_occurrence: DataFrame,
    visit_detail: DataFrame,
    care_site: DataFrame,
):  # pragma: no cover
    visit_detail = visit_detail.merge(
        visit_occurrence[
            visit_occurrence.columns.intersection(
                set(["visit_occurrence_id", "length_of_stay", "stay_type"])
            )
        ],
        on="visit_occurrence_id",
    )

    note_detail = prepare_note_care_site(extra_data=extra_data, note=note)
    note_detail = note_detail[
        ["visit_occurrence_id", "note_type", "care_site_id"]
    ].drop_duplicates()
    note_detail["has_note"] = True
    note_detail = visit_detail.merge(
        note_detail,
        on=["visit_occurrence_id", "care_site_id"],
        how="left",
    ).drop(columns="visit_occurrence_id")

    note_detail = note_detail.merge(care_site, on="care_site_id")

    uf_name = CARE_SITE_LEVEL_NAMES["UF"]
    uf_visit = note_detail[note_detail["care_site_level"] == uf_name]
    uc_name = CARE_SITE_LEVEL_NAMES["UC"]
    uc_visit = note_detail[note_detail["care_site_level"] == uc_name]
    uh_name = CARE_SITE_LEVEL_NAMES["UH"]
    uh_visit = note_detail[note_detail["care_site_level"] == uh_name]

    if is_koalas(note_detail):
        uf_visit = uf_visit.spark.cache()
        uc_visit = uc_visit.spark.cache()
        uh_visit = uh_visit.spark.cache()

    return uf_visit, uc_visit, uh_visit


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
