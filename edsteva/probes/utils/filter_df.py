from datetime import datetime, timedelta
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from IPython.display import display
from loguru import logger

from edsteva.utils.checks import check_columns
from edsteva.utils.framework import get_framework, to
from edsteva.utils.typing import DataFrame

from .utils import CARE_SITE_LEVEL_NAMES, UNSUPPORTED_CARE_SITE_LEVEL_NAMES


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
                table[source_col].str.contains(
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
            "{} must be str or dict not {}".format(target_col, type(type_groups))
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

    table.dropna(subset=["date"], inplace=True)
    logger.debug("Droping observations with missing date in table {}.", table_name)
    table["date"] = table["date"].astype("datetime64")
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
    table["date"] = table["date"].dt.strftime("%Y-%m").astype("datetime64")
    return table


def filter_table_by_stay_duration(
    visit_occurrence: DataFrame, stay_durations: List[float]
):
    if stay_durations:
        visit_occurrence["length"] = (
            visit_occurrence["visit_end_datetime"]
            - visit_occurrence["visit_start_datetime"]
        ) / np.timedelta64(timedelta(days=1))

        # Incomplete stays
        visit_occurrence["length_of_stay"] = "Unknown"
        visit_occurrence["length_of_stay"] = visit_occurrence["length_of_stay"].mask(
            visit_occurrence["visit_end_datetime"].isna(),
            "Incomplete stay",
        )

        # Complete stays
        min_duration = stay_durations[0]
        max_duration = stay_durations[-1]
        visit_occurrence["length_of_stay"] = visit_occurrence["length_of_stay"].mask(
            (visit_occurrence["length"] <= min_duration),
            "<= {} days".format(min_duration),
        )
        visit_occurrence["length_of_stay"] = visit_occurrence["length_of_stay"].mask(
            (visit_occurrence["length"] >= max_duration),
            ">= {} days".format(max_duration),
        )
        n_duration = len(stay_durations)
        for i in range(0, n_duration - 1):
            min = stay_durations[i]
            max = stay_durations[i + 1]
            visit_occurrence["length_of_stay"] = visit_occurrence[
                "length_of_stay"
            ].mask(
                (visit_occurrence["length"] >= min)
                & (visit_occurrence["length"] < max),
                "{} days - {} days".format(min, max),
            )
        visit_occurrence = visit_occurrence.drop(columns="length")

    else:
        visit_occurrence["length_of_stay"] = "All lengths"

    return visit_occurrence.drop(columns="visit_end_datetime")


def filter_table_by_care_site(
    table_to_filter: DataFrame,
    table_name: str,
    care_site_relationship: pd.DataFrame,
    care_site_short_names: Union[str, List[str]] = None,
    care_site_ids: Union[int, List[int]] = None,
):
    care_site = care_site_relationship[
        ["care_site_id", "care_site_short_name", "care_site_level"]
    ]
    care_site_filter = []
    if care_site_ids:
        if not isinstance(care_site_ids, list):
            care_site_ids = [care_site_ids]
        care_site_filter.append(
            care_site[care_site["care_site_id"].isin(care_site_ids)].copy()
        )
    if care_site_short_names:
        if not isinstance(care_site_short_names, list):
            care_site_short_names = [care_site_short_names]
        care_site_filter.append(
            care_site[
                care_site["care_site_short_name"].isin(care_site_short_names)
            ].copy()
        )
    if care_site_filter:
        care_site_filter = pd.concat(
            care_site_filter, ignore_index=True
        ).drop_duplicates("care_site_id")
    else:
        raise Exception("care_site_ids or care_site_short_names must be provided")

    # Get all UF from UC
    uc_care_site = care_site_filter[
        care_site_filter.care_site_level == UNSUPPORTED_CARE_SITE_LEVEL_NAMES["UC"]
    ]
    uc_care_site = uc_care_site[["care_site_id"]].drop_duplicates()
    care_site_rel_uc_to_uf = _get_relationship_table_uc_to_uf(
        care_site_relationship=care_site_relationship
    )
    care_site_rel_uc_to_uf = to("pandas", care_site_rel_uc_to_uf)
    related_uf_care_site = uc_care_site.merge(
        care_site_rel_uc_to_uf,
        on="care_site_id",
    )
    uf_from_uc = related_uf_care_site[
        ["care_site_id_uf", "care_site_short_name_uf"]
    ].drop_duplicates("care_site_id_uf")

    # Get all UF from Hospital
    care_site_rel_uf_to_hospital = _get_relationship_table_uf_to_hospital(
        care_site_relationship=care_site_relationship
    )
    care_site_rel_uf_to_hospital = to("pandas", care_site_rel_uf_to_hospital)

    hospital_care_site = care_site_filter[
        care_site_filter.care_site_level == CARE_SITE_LEVEL_NAMES["Hospital"]
    ].rename(
        columns={
            "care_site_id": "care_site_id_hospital",
            "care_site_short_name": "care_site_short_name_hospital",
        }
    )
    hospital_care_site = hospital_care_site[
        ["care_site_id_hospital", "care_site_short_name_hospital"]
    ].drop_duplicates("care_site_id_hospital")
    related_hospital_care_site = hospital_care_site.merge(
        care_site_rel_uf_to_hospital,
        on=[
            "care_site_id_hospital",
            "care_site_short_name_hospital",
        ],
        how="left",
    )
    uf_from_hospital = related_hospital_care_site[
        [
            "care_site_id_uf",
            "care_site_short_name_uf",
        ]
    ].drop_duplicates("care_site_id_uf")
    pole_from_hospital = related_hospital_care_site[
        [
            "care_site_id_pole",
            "care_site_short_name_pole",
        ]
    ].drop_duplicates("care_site_id_pole")

    # Get all UF from pole
    pole_care_site = care_site_filter[
        care_site_filter.care_site_level == CARE_SITE_LEVEL_NAMES["Pole"]
    ].rename(
        columns={
            "care_site_id": "care_site_id_pole",
            "care_site_short_name": "care_site_short_name_pole",
        }
    )
    pole_care_site = pole_care_site[
        ["care_site_id_pole", "care_site_short_name_pole"]
    ].drop_duplicates("care_site_id_pole")
    related_pole_care_site = pole_care_site.merge(
        care_site_rel_uf_to_hospital,
        on=[
            "care_site_id_pole",
            "care_site_short_name_pole",
        ],
        how="left",
    )
    uf_from_pole = related_pole_care_site[
        [
            "care_site_id_uf",
            "care_site_short_name_uf",
        ]
    ].drop_duplicates("care_site_id_uf")

    # Get all Hospital from Pole
    hospital_from_pole = related_pole_care_site[
        [
            "care_site_id_hospital",
            "care_site_short_name_hospital",
        ]
    ].drop_duplicates("care_site_id_hospital")

    # Get all Pole from UF
    uf_care_site = care_site_filter[
        care_site_filter.care_site_level == CARE_SITE_LEVEL_NAMES["UF"]
    ].rename(
        columns={
            "care_site_id": "care_site_id_uf",
            "care_site_short_name": "care_site_short_name_uf",
        }
    )
    uf_care_site = uf_care_site[
        ["care_site_id_uf", "care_site_short_name_uf"]
    ].drop_duplicates("care_site_id_uf")
    uf_care_site = pd.concat([uf_care_site, uf_from_uc])

    related_uf_care_site = uf_care_site.merge(
        care_site_rel_uf_to_hospital,
        on=[
            "care_site_id_uf",
            "care_site_short_name_uf",
        ],
        how="left",
    )
    pole_from_uf = related_uf_care_site[
        [
            "care_site_id_pole",
            "care_site_short_name_pole",
        ]
    ].drop_duplicates("care_site_id_pole")

    # Get all Hospital from UF
    hospital_from_uf = related_uf_care_site[
        [
            "care_site_id_hospital",
            "care_site_short_name_hospital",
        ]
    ].drop_duplicates("care_site_id_hospital")

    extended_uf_care_site = pd.concat(
        [uf_care_site, uf_from_pole, uf_from_hospital, uf_from_uc], ignore_index=True
    ).drop_duplicates("care_site_id_uf")
    extended_uf_care_site = extended_uf_care_site.rename(
        columns={
            "care_site_id_uf": "care_site_id",
            "care_site_short_name_uf": "care_site_short_name",
        }
    )

    extended_pole_care_site = pd.concat(
        [pole_care_site, pole_from_uf, pole_from_hospital], ignore_index=True
    ).drop_duplicates("care_site_id_pole")
    extended_pole_care_site = extended_pole_care_site.rename(
        columns={
            "care_site_id_pole": "care_site_id",
            "care_site_short_name_pole": "care_site_short_name",
        }
    )
    extended_hospital_care_site = pd.concat(
        [hospital_care_site, hospital_from_pole, hospital_from_uf], ignore_index=True
    ).drop_duplicates("care_site_id_hospital")
    extended_hospital_care_site = extended_hospital_care_site.rename(
        columns={
            "care_site_id_hospital": "care_site_id",
            "care_site_short_name_hospital": "care_site_short_name",
        }
    )
    unsupported_care_site = care_site_filter[
        ~(care_site_filter.care_site_level.isin(CARE_SITE_LEVEL_NAMES.values()))
    ]
    if not unsupported_care_site.empty:
        logger.warning(
            "The following care site ids are not supported because the associated care site levels are not in {}.",
            CARE_SITE_LEVEL_NAMES.values(),
        )
        display(unsupported_care_site)

    if not extended_hospital_care_site.empty:
        logger.debug(
            "The following hospitals {} have been selected from {}.",
            extended_hospital_care_site.care_site_short_name.to_list(),
            table_name,
        )
    if not extended_pole_care_site.empty:
        logger.debug(
            "The following poles {} have been selected from {}.",
            extended_pole_care_site.care_site_short_name.to_list(),
            table_name,
        )
    if not extended_uf_care_site.empty:
        logger.debug(
            "The following UF {} have been selected from {}.",
            extended_uf_care_site.care_site_short_name.to_list(),
            table_name,
        )

    extended_care_site_id_to_filter = pd.concat(
        [extended_hospital_care_site, extended_pole_care_site, extended_uf_care_site],
        ignore_index=True,
    ).care_site_id.to_list()
    return table_to_filter[
        table_to_filter["care_site_id"].isin(extended_care_site_id_to_filter)
    ]


def convert_uc_to_uf(
    table: DataFrame,
    table_name: str,
    care_site_relationship: DataFrame,
):
    care_site_rel_uc_to_uf = _get_relationship_table_uc_to_uf(
        care_site_relationship=care_site_relationship
    )
    care_site_rel_uc_to_uf = to(get_framework(table), care_site_rel_uc_to_uf)
    table = table.merge(care_site_rel_uc_to_uf, on="care_site_id", how="left")
    table["care_site_id"] = table["care_site_id_uf"].mask(
        table["care_site_id_uf"].isna(), table["care_site_id"]
    )
    table = table.drop(columns=["care_site_id_uf"])
    logger.debug(
        "For level {}, stays of the table {} located in {} have been linked to their corresponding {}",
        CARE_SITE_LEVEL_NAMES["UF"],
        table_name,
        UNSUPPORTED_CARE_SITE_LEVEL_NAMES["UC"],
        CARE_SITE_LEVEL_NAMES["UF"],
    )
    return table


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


def _get_relationship_table_uc_to_uf(
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

    care_site_relationship = care_site_relationship[
        [
            "care_site_id",
            "care_site_level",
            "care_site_short_name",
            "parent_care_site_id",
            "parent_care_site_level",
            "parent_care_site_short_name",
        ]
    ]

    uc_care_site = care_site_relationship[
        (
            care_site_relationship["care_site_level"]
            == UNSUPPORTED_CARE_SITE_LEVEL_NAMES["UC"]
        )
    ]

    care_site_rel_grandparent = care_site_relationship[
        [
            "care_site_id",
            "parent_care_site_id",
            "parent_care_site_short_name",
            "parent_care_site_level",
        ]
    ].rename(
        columns={
            "care_site_id": "parent_care_site_id",
            "parent_care_site_id": "grandparent_care_site_id",
            "parent_care_site_short_name": "grandparent_care_site_short_name",
            "parent_care_site_level": "grandparent_care_site_level",
        }
    )
    uc_care_site = uc_care_site.merge(
        care_site_rel_grandparent, on="parent_care_site_id"
    )
    uc_care_site["care_site_id_uf"] = None
    uc_care_site["care_site_id_uf"] = uc_care_site["care_site_id_uf"].mask(
        uc_care_site["grandparent_care_site_level"] == CARE_SITE_LEVEL_NAMES["UF"],
        uc_care_site["grandparent_care_site_id"],
    )
    uc_care_site["care_site_id_uf"] = uc_care_site["care_site_id_uf"].mask(
        uc_care_site["parent_care_site_level"] == CARE_SITE_LEVEL_NAMES["UF"],
        uc_care_site["parent_care_site_id"],
    )
    uc_care_site["care_site_short_name_uf"] = None
    uc_care_site["care_site_short_name_uf"] = uc_care_site[
        "care_site_short_name_uf"
    ].mask(
        uc_care_site["grandparent_care_site_level"] == CARE_SITE_LEVEL_NAMES["UF"],
        uc_care_site["care_site_short_name"],
    )
    uc_care_site["care_site_short_name_uf"] = uc_care_site[
        "care_site_short_name_uf"
    ].mask(
        uc_care_site["parent_care_site_level"] == CARE_SITE_LEVEL_NAMES["UF"],
        uc_care_site["care_site_short_name"],
    )
    return uc_care_site[
        [
            "care_site_id",
            "care_site_id_uf",
            "care_site_short_name_uf",
        ]
    ].drop_duplicates(["care_site_id", "care_site_id_uf"])


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
