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
    VISIT_DETAIL_TYPE,
    concatenate_predictor_by_level,
    hospital_only,
    impute_missing_dates,
)
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
    length_of_stays: List[float],
    **kwargs
):
    r"""Script to be used by [``compute()``][edsteva.probes.base.BaseProbe.compute]

    The ``per_visit`` algorithm computes $c_(t)$ the availability of administrative data related to visits for each care site according to time:

    $$
    c(t) = \frac{n_{visit}(t)}{n_{max}}
    $$

    Where $n_{visit}(t)$ is the number of administrative stays, $t$ is the month and $n_{max} = \max_{t}(n_{visit}(t))$.
    """
    self._metrics = ["c", "n_visit"]
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
        care_site_specialties=care_site_specialties,
        care_site_relationship=care_site_relationship,
        specialties_sets=specialties_sets,
        care_sites_sets=care_sites_sets,
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

        uc_name = CARE_SITE_LEVEL_NAMES["UC"]
        uc_visit = get_uc_visit(
            visit_occurrence,
            visit_detail,
            care_site,
        )
        visit_predictor_by_level[uc_name] = uc_visit

        uh_name = CARE_SITE_LEVEL_NAMES["UH"]
        uh_visit = get_uh_visit(
            visit_occurrence,
            visit_detail,
            care_site,
        )
        visit_predictor_by_level[uh_name] = uh_visit

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


def compute_completeness(
    self,
    visit_predictor: DataFrame,
):
    partition_cols = [*self._index.copy(), "date"]

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
    n_visit = impute_missing_dates(
        start_date=self.start_date,
        end_date=self.end_date,
        predictor=n_visit,
        partition_cols=partition_cols,
    )

    partition_cols = list(set(partition_cols) - {"date"})
    max_n_visit = (
        n_visit.groupby(
            partition_cols,
            as_index=False,
            dropna=False,
        )
        .agg({"n_visit": "max"})
        .rename(columns={"n_visit": "max_n_visit"})
    )

    visit_predictor = n_visit.merge(
        max_n_visit,
        on=partition_cols,
    )

    visit_predictor["c"] = visit_predictor["max_n_visit"].where(
        visit_predictor["max_n_visit"] == 0,
        visit_predictor["n_visit"] / visit_predictor["max_n_visit"],
    )
    return visit_predictor.drop(columns="max_n_visit")


def get_hospital_visit(
    visit_occurrence: DataFrame,
    care_site: DataFrame,
):
    hospital_visit = visit_occurrence.rename(
        columns={"visit_occurrence_id": "visit_id"}
    )

    hospital_visit = hospital_visit.merge(care_site, on="care_site_id")
    hospital_visit = hospital_visit[
        hospital_visit["care_site_level"] == CARE_SITE_LEVEL_NAMES["Hospital"]
    ]
    if is_koalas(hospital_visit):
        hospital_visit = hospital_visit.spark.cache()

    return hospital_visit


def get_uf_visit(
    visit_occurrence: DataFrame,
    visit_detail: DataFrame,
    care_site: DataFrame,
):
    uf_visit = visit_detail[visit_detail.visit_detail_type == VISIT_DETAIL_TYPE["UF"]]
    uf_visit = uf_visit.merge(
        visit_occurrence[
            visit_occurrence.columns.intersection(
                set(["visit_occurrence_id", "length_of_stay", "stay_type"])
            )
        ],
        on="visit_occurrence_id",
    ).drop(columns="visit_occurrence_id")
    uf_visit = uf_visit.merge(care_site, on="care_site_id")
    uf_visit = uf_visit[uf_visit["care_site_level"] == CARE_SITE_LEVEL_NAMES["UF"]]
    if is_koalas(uf_visit):
        uf_visit = uf_visit.spark.cache()

    return uf_visit


def get_uc_visit(
    visit_occurrence: DataFrame,
    visit_detail: DataFrame,
    care_site: DataFrame,
):
    uc_visit = visit_detail[visit_detail.visit_detail_type == VISIT_DETAIL_TYPE["UC"]]
    uc_visit = uc_visit.merge(
        visit_occurrence[
            visit_occurrence.columns.intersection(
                set(["visit_occurrence_id", "length_of_stay", "stay_type"])
            )
        ],
        on="visit_occurrence_id",
    ).drop(columns="visit_occurrence_id")
    uc_visit = uc_visit.merge(care_site, on="care_site_id")
    uc_visit = uc_visit[uc_visit["care_site_level"] == CARE_SITE_LEVEL_NAMES["UC"]]
    if is_koalas(uc_visit):
        uc_visit = uc_visit.spark.cache()

    return uc_visit


def get_uh_visit(
    visit_occurrence: DataFrame,
    visit_detail: DataFrame,
    care_site: DataFrame,
):
    uh_visit = visit_detail[visit_detail.visit_detail_type == VISIT_DETAIL_TYPE["UH"]]
    uh_visit = uh_visit.merge(
        visit_occurrence[
            visit_occurrence.columns.intersection(
                set(["visit_occurrence_id", "length_of_stay", "stay_type"])
            )
        ],
        on="visit_occurrence_id",
    ).drop(columns="visit_occurrence_id")
    uh_visit = uh_visit.merge(care_site, on="care_site_id")
    uh_visit = uh_visit[uh_visit["care_site_level"] == CARE_SITE_LEVEL_NAMES["UH"]]
    if is_koalas(uh_visit):
        uh_visit = uh_visit.spark.cache()

    return uh_visit


def get_pole_visit(
    uf_visit: DataFrame,
    care_site: DataFrame,
    care_site_relationship: DataFrame,
):
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
