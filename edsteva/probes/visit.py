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
    prepare_visit_detail,
    prepare_visit_occurrence,
)
from edsteva.utils.framework import is_koalas, to
from edsteva.utils.typing import Data


def compute_completeness(visit_predictor):

    partition_cols = [
        "care_site_level",
        "care_site_id",
        "care_site_short_name",
        "stay_type",
        "date",
    ]
    n_visit = (
        visit_predictor.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"visit_id": "count"})
        .rename(columns={"visit_id": "n_visit"})
    )

    n_visit = to("pandas", n_visit)

    partition_cols = list(set(partition_cols) - {"date"})
    q_99_visit = (
        n_visit.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )[["n_visit"]]
        .quantile(q=0.99)
        .rename(columns={"n_visit": "q_99_visit"})
    )

    visit_predictor = n_visit.merge(
        q_99_visit,
        on=partition_cols,
    )

    visit_predictor["c"] = visit_predictor["q_99_visit"].where(
        visit_predictor["q_99_visit"] == 0,
        visit_predictor["n_visit"] / visit_predictor["q_99_visit"],
    )
    visit_predictor = visit_predictor.drop(columns="q_99_visit")

    return visit_predictor


def get_hospital_visit(visit_occurrence, care_site):
    hospital_visit = visit_occurrence.rename(
        columns={"visit_occurrence_id": "visit_id"}
    )
    hospital_visit = hospital_visit.merge(care_site, on="care_site_id")

    if is_koalas(hospital_visit):
        hospital_visit.spark.cache()

    return hospital_visit


def get_uf_visit(visit_occurrence, visit_detail, care_site, care_site_relationship):
    visit_detail = visit_detail.merge(
        visit_occurrence[["visit_occurrence_id", "stay_type"]],
        on="visit_occurrence_id",
    ).drop(columns="visit_occurrence_id")

    uf_visit = convert_table_to_uf(
        table=visit_detail,
        table_name="visit_detail",
        care_site_relationship=care_site_relationship,
    )
    uf_visit = uf_visit.merge(care_site, on="care_site_id")
    uf_visit = uf_visit[uf_visit["care_site_level"] == CARE_SITE_LEVEL_NAMES["UF"]]
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
    pole_visit = pole_visit[
        pole_visit["care_site_level"] == CARE_SITE_LEVEL_NAMES["Pole"]
    ]
    if is_koalas(pole_visit):
        pole_visit.spark.cache()

    return pole_visit


class VisitProbe(BaseProbe):
    r"""
    The ``VisitProbe`` computes $c_(t)$ the availability of administrative data related to visits for each care site according to time:

    $$
    c(t) = \frac{n_{visit}(t)}{n_{99}}
    $$

    Where $n_{visit}(t)$ is the number of visits, $n_{99}$ is the $99^{th}$ percentile of visits and $t$ is the month.

    Attributes
    ----------
    _index: List[str]
        Variable from which data is grouped

        **VALUE**: ``["care_site_level", "stay_type", "care_site_id"]``
    """

    _index = ["care_site_level", "stay_type", "care_site_id"]

    def compute_process(
        self,
        data: Data,
        care_site_relationship: pd.DataFrame,
        start_date: datetime = None,
        end_date: datetime = None,
        care_site_levels: List[str] = None,
        stay_types: Union[str, Dict[str, str]] = None,
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
            **EXAMPLE**: `{"All": ".*"}` or `{"All": ".*", "Urg_and_consult": "urgences|consultation"}` or `"hospitalisés`
        care_site_ids : List[int], optional
            **EXAMPLE**: `[8312056386, 8312027648]`
        care_site_short_names : List[str], optional
            **EXAMPLE**: `["HOSPITAL 1", "HOSPITAL 2"]`
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
                care_site_relationship,
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

        return compute_completeness(visit_predictor)
