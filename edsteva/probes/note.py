from datetime import datetime
from typing import Dict, List, Union

import pandas as pd

from edsteva.probes.base import BaseProbe
from edsteva.probes.utils import (
    CARE_SITE_LEVEL_NAMES,
    concatenate_predictor_by_level,
    convert_table_to_pole,
    convert_table_to_uf,
    hospital_only,
    prepare_care_site,
    prepare_note,
    prepare_visit_detail,
    prepare_visit_occurrence,
)
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data


def compute_completeness(note_predictor):
    partition_cols = [
        "care_site_level",
        "care_site_id",
        "care_site_short_name",
        "stay_type",
        "note_type",
        "date",
    ]
    n_visit_with_note = (
        note_predictor.groupby(
            partition_cols,
            as_index=False,
        )
        .agg({"has_note": "count"})
        .rename(columns={"has_note": "n_visit_with_note"})
    )

    partition_cols = list(set(partition_cols) - {"note_type"})
    n_visit = (
        note_predictor.groupby(
            partition_cols,
            as_index=False,
        )
        .agg({"visit_id": "nunique"})
        .rename(columns={"visit_id": "n_visit"})
    )

    note_predictor = n_visit_with_note.merge(
        n_visit,
        on=partition_cols,
    )

    note_predictor = to("pandas", note_predictor)

    note_predictor["c"] = note_predictor["n_visit"].where(
        note_predictor["n_visit"] == 0,
        note_predictor["n_visit_with_note"] / note_predictor["n_visit"],
    )
    note_predictor = note_predictor.drop(columns=["n_visit_with_note"])

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
    care_site_relationship,
):
    # Load Orbis note and Uf for Note
    note_orbis = extra_data.orbis_document[
        [
            "ids_eds",
            "id_dm_doc_ufr",
            "id_dm_doc_us",
        ]
    ]
    note_orbis = note_orbis.rename(columns={"ids_eds": "note_id"})

    orbis_care_site = extra_data.orbis_ref_struct_list[
        [
            "id_ref_stuct",
            "ids_eds",
        ]
    ]
    orbis_care_site = orbis_care_site.rename(
        columns={
            "id_ref_stuct": "id_orbis",
            "ids_eds": "care_site_id",
        }
    )

    note = note.merge(note_orbis, on="note_id")
    note = note.melt(
        id_vars=["note_id", "note_type", "visit_occurrence_id"],
        value_name="id_orbis",
    )
    note = note.merge(
        orbis_care_site,
        on="id_orbis",
    )

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
    )
    visit_detail = visit_detail.drop(columns="visit_occurrence_id")

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


def get_pole_visit(uf_visit, care_site, care_site_relationship):

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


class NoteProbe(BaseProbe):
    r"""
    The ``NoteProbe`` computes $c(t)$ the availability of clinical documents linked to patients' visits:

    $$
    c(t) = \frac{n_{with\,doc}(t)}{n_{visit}(t)}
    $$

    Where $n_{visit}(t)$ is the number of visits, $n_{with\,doc}$ the number of visits having at least one document and $t$ is the month.

    Attributes
    ----------
    _index: List[str]
        Variable from which data is grouped

        **VALUE**: ``["care_site_level", "stay_type", "note_type", "care_site_id"]``
    """

    _index = ["care_site_level", "stay_type", "note_type", "care_site_id"]

    def compute_process(
        self,
        data: Data,
        care_site_relationship: pd.DataFrame,
        extra_data: Data = None,
        start_date: datetime = None,
        end_date: datetime = None,
        care_site_levels: List[str] = None,
        care_site_short_names: List[str] = None,
        stay_types: Union[str, Dict[str, str]] = None,
        note_types: Union[str, Dict[str, str]] = {
            "All": ".*",
            "Urgence": "urge",
            "Ordonnance": "ordo",
            "CRH": "crh",
        },
        care_site_ids: List[int] = None,
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
        visit_occurrence = prepare_visit_occurrence(
            data,
            start_date,
            end_date,
            stay_types,
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
        if not hospital_only(care_site_levels=care_site_levels) and extra_data:
            visit_detail = prepare_visit_detail(data, start_date, end_date)

            uf_visit = get_uf_visit(
                extra_data,
                note,
                visit_occurrence,
                visit_detail,
                care_site,
                care_site_relationship,
            )
            uf_name = CARE_SITE_LEVEL_NAMES["UF"]
            note_predictor_by_level[uf_name] = uf_visit

            pole_visit = get_pole_visit(uf_visit, care_site, care_site_relationship)
            pole_name = CARE_SITE_LEVEL_NAMES["Pole"]
            note_predictor_by_level[pole_name] = pole_visit

        # Concatenate all predictors
        note_predictor = concatenate_predictor_by_level(
            predictor_by_level=note_predictor_by_level,
            care_site_levels=care_site_levels,
        )

        note_predictor = note_predictor.drop_duplicates(
            ["visit_id", "care_site_id", "stay_type", "note_type", "date"]
        )

        if is_koalas(note_predictor):
            note_predictor.spark.cache()

        return compute_completeness(note_predictor)
