from datetime import datetime, timedelta
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from loguru import logger

from edsteva.utils.checks import check_columns
from edsteva.utils.framework import get_framework, to
from edsteva.utils.typing import DataFrame

from .utils import CARE_SITE_LEVEL_NAMES, get_child_and_parent_cs


def filter_table_by_type(
    table: DataFrame,
    table_name: str,
    type_groups: Union[str, Dict],
    source_col: str,
    target_col: str,
):
    if isinstance(type_groups, str):
        type_groups = {type_groups: type_groups}
    if isinstance(type_groups, dict):
        table_per_types = []
        for type_name, type_value in type_groups.items():
            table_per_type_element = table[
                table[source_col]
                .astype(str)
                .str.contains(
                    type_value,
                    case=False,
                    regex=True,
                    na=False,
                )
            ].copy()
            table_per_type_element[target_col] = type_name
            table_per_types.append(table_per_type_element)
    else:
        raise TypeError(
            "{} must be str or dict not {}".format(
                target_col, type(type_groups).__name__
            )
        )

    logger.debug(
        "The following {} : {} have been selected on table {}",
        target_col,
        type_groups,
        table_name,
    )
    return get_framework(table).concat(table_per_types, ignore_index=True)


def filter_valid_observations(
    table: DataFrame,
    table_name: str,
    invalid_naming: str = None,
    valid_naming: str = None,
):
    check_columns(
        df=table,
        required_columns=["row_status_source_value"],
    )

    if valid_naming:
        table_valid = table[table["row_status_source_value"] == valid_naming]
    elif invalid_naming:
        table_valid = table[~(table["row_status_source_value"] == invalid_naming)]
    else:
        raise Exception("valid_naming or invalid_naming must be provided.")
    table_valid = table_valid.drop(columns=["row_status_source_value"])
    logger.debug("Valid observations have been selected for table {}.", table_name)
    return table_valid


def filter_table_by_date(
    table: DataFrame,
    table_name: str,
    start_date: Union[datetime, str] = None,
    end_date: Union[datetime, str] = None,
):
    check_columns(df=table, required_columns=["date"])

    table = table.dropna(subset=["date"])
    logger.debug("Droping observations with missing date in table {}.", table_name)
    table["date"] = table["date"].astype("datetime64[ns]")
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    if end_date and start_date:
        table = table[(table["date"] >= start_date) & (table["date"] < end_date)]
        logger.debug(
            "Observations between {} and {} have been selected for table {}.",
            start_date,
            end_date,
            table_name,
        )
    elif start_date:
        table = table[table["date"] >= start_date]
        logger.debug(
            "Observations after {} have been selected for table {}.",
            start_date,
            table_name,
        )
    elif end_date:
        table = table[table["date"] < end_date]
        logger.debug(
            "Observations before {} have been selected for table {}.",
            end_date,
            table_name,
        )
    # Truncate
    table["date"] = table["date"].dt.strftime("%Y-%m").astype("datetime64[ns]")
    return table


def filter_table_by_length_of_stay(
    visit_occurrence: DataFrame, length_of_stays: List[float]
):
    visit_occurrence = visit_occurrence.assign(
        length=(
            visit_occurrence.visit_end_datetime - visit_occurrence.visit_start_datetime
        )
        / np.timedelta64(timedelta(days=1))
    )

    # Incomplete stays
    visit_occurrence = visit_occurrence.assign(length_of_stay="Unknown")
    visit_occurrence["length_of_stay"] = visit_occurrence.length_of_stay.mask(
        visit_occurrence["visit_end_datetime"].isna(),
        "Incomplete stay",
    )

    # Complete stays
    min_duration = length_of_stays[0]
    max_duration = length_of_stays[-1]
    visit_occurrence["length_of_stay"] = visit_occurrence["length_of_stay"].mask(
        (visit_occurrence["length"] <= min_duration),
        "<= {} days".format(min_duration),
    )
    visit_occurrence["length_of_stay"] = visit_occurrence["length_of_stay"].mask(
        (visit_occurrence["length"] >= max_duration),
        ">= {} days".format(max_duration),
    )
    n_duration = len(length_of_stays)
    for i in range(0, n_duration - 1):
        min = length_of_stays[i]
        max = length_of_stays[i + 1]
        visit_occurrence["length_of_stay"] = visit_occurrence["length_of_stay"].mask(
            (visit_occurrence["length"] >= min) & (visit_occurrence["length"] < max),
            "{} days - {} days".format(min, max),
        )
    visit_occurrence = visit_occurrence.drop(columns="length")

    return visit_occurrence.drop(columns="visit_end_datetime")


def filter_table_by_care_site(
    table_to_filter: DataFrame,
    care_site_relationship: pd.DataFrame,
    care_site_ids: Union[int, List[int]] = None,
    care_site_short_names: Union[str, List[str]] = None,
    care_site_specialties: Union[str, List[str]] = None,
):
    care_site = care_site_relationship[
        [
            "care_site_id",
            "care_site_short_name",
            "care_site_level",
            "care_site_specialty",
        ]
    ]
    care_site_filter = []
    if care_site_ids:
        if not isinstance(care_site_ids, list):
            care_site_ids = [care_site_ids]
        care_site_filter.append(
            care_site[care_site["care_site_id"].isin(care_site_ids)]
        )
    if care_site_short_names:
        if not isinstance(care_site_short_names, list):
            care_site_short_names = [care_site_short_names]
        care_site_filter.append(
            care_site[care_site["care_site_short_name"].isin(care_site_short_names)]
        )

    if care_site_filter:
        care_site_filter = pd.concat(
            care_site_filter, ignore_index=True
        ).drop_duplicates()
        extended_care_site_id_to_filter = get_child_and_parent_cs(
            care_site_sample=care_site_filter,
            care_site_relationship=care_site_relationship,
        )
    else:
        extended_care_site_id_to_filter = care_site.copy()

    if care_site_specialties:
        if not isinstance(care_site_specialties, list):
            care_site_specialties = [care_site_specialties]
        extended_care_site_id_to_filter = extended_care_site_id_to_filter[
            extended_care_site_id_to_filter["care_site_specialty"].isin(
                care_site_specialties
            )
        ]
        extended_care_site_id_to_filter = get_child_and_parent_cs(
            care_site_sample=extended_care_site_id_to_filter,
            care_site_relationship=care_site_relationship,
        )

    extended_care_site_id_to_filter = list(
        extended_care_site_id_to_filter[
            (
                extended_care_site_id_to_filter.care_site_level.isin(
                    CARE_SITE_LEVEL_NAMES.values()
                )
            )
        ].care_site_id.unique()
    )

    return table_to_filter[
        table_to_filter.care_site_id.isin(extended_care_site_id_to_filter)
    ]


def convert_uf_to_pole(
    table: DataFrame,
    table_name: str,
    care_site_relationship: DataFrame,
):
    care_site_rel_uf_to_pole = _get_relationship_table_uf_to_hospital(
        care_site_relationship=care_site_relationship
    )[["care_site_id_uf", "care_site_id_pole"]].rename(
        columns={"care_site_id_uf": "care_site_id"}
    )
    care_site_rel_uf_to_pole = to(get_framework(table), care_site_rel_uf_to_pole)
    table = table.merge(care_site_rel_uf_to_pole, on="care_site_id", how="left")
    table["care_site_id"] = table["care_site_id_pole"].mask(
        table["care_site_id_pole"].isna(), table["care_site_id"]
    )
    table = table.drop(columns="care_site_id_pole")
    logger.debug(
        "For level {}, stays of the table {} located in {} have been linked to their corresponding {}",
        CARE_SITE_LEVEL_NAMES["Pole"],
        table_name,
        CARE_SITE_LEVEL_NAMES["UF"],
        CARE_SITE_LEVEL_NAMES["Pole"],
    )
    return table


def _get_relationship_table_uf_to_hospital(
    care_site_relationship: DataFrame,
):
    check_columns(
        df=care_site_relationship,
        required_columns=[
            "care_site_id",
            "care_site_level",
            "care_site_short_name",
            "parent_care_site_id",
            "parent_care_site_level",
            "parent_care_site_short_name",
        ],
    )

    care_site_rel_uf_pole = care_site_relationship.rename(
        columns={
            "care_site_level": "care_site_level_uf",
            "care_site_id": "care_site_id_uf",
            "care_site_short_name": "care_site_short_name_uf",
            "parent_care_site_level": "care_site_level_pole",
            "parent_care_site_id": "care_site_id_pole",
            "parent_care_site_short_name": "care_site_short_name_pole",
        }
    )

    uf_to_pole_care_site = care_site_rel_uf_pole[
        (care_site_rel_uf_pole["care_site_level_uf"] == CARE_SITE_LEVEL_NAMES["UF"])
    ]

    care_site_rel_pole_hospit = care_site_relationship.rename(
        columns={
            "care_site_id": "care_site_id_pole",
            "parent_care_site_level": "care_site_level_hospital",
            "parent_care_site_id": "care_site_id_hospital",
            "parent_care_site_short_name": "care_site_short_name_hospital",
        }
    ).drop(columns=["care_site_level", "care_site_short_name"])

    uf_to_hospital_care_site = uf_to_pole_care_site.merge(
        care_site_rel_pole_hospit, on="care_site_id_pole"
    )

    uf_to_hospital_care_site = uf_to_hospital_care_site[
        uf_to_hospital_care_site.care_site_level_pole == CARE_SITE_LEVEL_NAMES["Pole"]
    ]
    uf_to_hospital_care_site = uf_to_hospital_care_site[
        uf_to_hospital_care_site.care_site_level_hospital
        == CARE_SITE_LEVEL_NAMES["Hospital"]
    ]
    uf_to_hospital_care_site = uf_to_hospital_care_site.drop_duplicates(
        [
            "care_site_id_uf",
            "care_site_id_pole",
            "care_site_id_hospital",
        ]
    )
    return uf_to_hospital_care_site[
        [
            "care_site_id_uf",
            "care_site_short_name_uf",
            "care_site_id_pole",
            "care_site_short_name_pole",
            "care_site_id_hospital",
            "care_site_short_name_hospital",
        ]
    ]
