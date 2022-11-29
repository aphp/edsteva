import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union

import _pickle as pickle
import pandas as pd
from IPython.display import display
from loguru import logger

from edsteva.utils.checks import check_columns
from edsteva.utils.framework import get_framework, to
from edsteva.utils.typing import Data, DataFrame

CARE_SITE_LEVEL_NAMES = {
    "Hospital": "Hôpital",
    "Pole": "Pôle/DMU",
    "UF": "Unité Fonctionnelle (UF)",
}

UNSUPPORTED_CARE_SITE_LEVEL_NAMES = {
    "UC": "Unité de consultation (UC)",
}


def prepare_visit_occurrence(data, start_date, end_date, stay_types):
    visit_occurrence = data.visit_occurrence[
        [
            "visit_occurrence_id",
            "visit_source_value",
            "visit_start_datetime",
            "care_site_id",
            "row_status_source_value",
        ]
    ]
    visit_occurrence = visit_occurrence.rename(
        columns={"visit_source_value": "stay_type", "visit_start_datetime": "date"}
    )
    visit_occurrence = get_valid_observations(
        table=visit_occurrence,
        table_name="visit_occurrence",
        invalid_naming="supprimé",
    )
    visit_occurrence = filter_table_by_date(
        table=visit_occurrence,
        table_name="visit_occurrence",
        start_date=start_date,
        end_date=end_date,
    )

    if stay_types:
        visit_occurrence = filter_table_by_type(
            table=visit_occurrence,
            table_name="visit_occurrence",
            type_groups=stay_types,
            name="stay_type",
        )

    return visit_occurrence


def prepare_care_site(
    data,
    care_site_ids,
    care_site_short_names,
    care_site_relationship,
):
    care_site = data.care_site[
        [
            "care_site_id",
            "care_site_type_source_value",
            "care_site_short_name",
        ]
    ]
    care_site = care_site.rename(
        columns={"care_site_type_source_value": "care_site_level"}
    )
    if care_site_ids or care_site_short_names:
        care_site = filter_table_by_care_site(
            table_to_filter=care_site,
            table_name="care_site",
            care_site_relationship=care_site_relationship,
            care_site_ids=care_site_ids,
            care_site_short_names=care_site_short_names,
        )

    return care_site


def prepare_note(data, note_types):
    note = data.note[
        [
            "note_id",
            "visit_occurrence_id",
            "note_class_source_value",
            "row_status_source_value",
            "note_text",
        ]
    ]
    note = note.rename(columns={"note_class_source_value": "note_type"})
    note = note[~(note["note_text"].isna())]
    note = note.drop(columns=["note_text"])
    note = get_valid_observations(table=note, table_name="note", valid_naming="Actif")
    if note_types:
        note = filter_table_by_type(
            table=note,
            table_name="note",
            type_groups=note_types,
            name="note_type",
        )

    return note


def prepare_visit_detail(data, start_date, end_date):
    visit_detail = data.visit_detail[
        [
            "visit_detail_id",
            "visit_occurrence_id",
            "visit_detail_start_datetime",
            "visit_detail_type_source_value",
            "care_site_id",
            "row_status_source_value",
        ]
    ]
    visit_detail = visit_detail.rename(
        columns={
            "visit_detail_id": "visit_id",
            "visit_detail_start_datetime": "date",
        }
    )
    visit_detail = get_valid_observations(
        table=visit_detail, table_name="visit_detail", valid_naming="Actif"
    )
    visit_detail = visit_detail[
        visit_detail["visit_detail_type_source_value"] == "PASS"
    ]  # Important to filter only "PASS" to remove duplicate visits
    visit_detail = visit_detail.drop(columns=["visit_detail_type_source_value"])
    visit_detail = filter_table_by_date(
        table=visit_detail,
        table_name="visit_detail",
        start_date=start_date,
        end_date=end_date,
    )

    return visit_detail


def hospital_only(care_site_levels: List[str]):
    if not isinstance(care_site_levels, list):
        care_site_levels = [care_site_levels]
    return len(care_site_levels) == 1 and (
        care_site_levels[0] == "Hospital"
        or care_site_levels[0] == CARE_SITE_LEVEL_NAMES["Hospital"]
    )


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


def filter_table_by_type(
    table: DataFrame,
    table_name: str,
    type_groups: Union[str, Dict],
    name: str,
):
    if isinstance(type_groups, str):
        type_groups = {type_groups: type_groups}
    if isinstance(type_groups, dict):
        table_per_types = []
        for type_name, type_value in type_groups.items():
            table_per_type_element = table[
                table[name].str.contains(
                    type_value,
                    case=False,
                    regex=True,
                    na=False,
                )
            ].copy()
            table_per_type_element[name] = type_name
            table_per_types.append(table_per_type_element)
    else:
        raise TypeError("{} must be str or dict not {}".format(name, type(type_groups)))

    logger.debug(
        "The following {} : {} have been selected on table {}",
        name,
        type_groups,
        table_name,
    )
    return get_framework(table).concat(table_per_types, ignore_index=True)


def convert_table_to_uf(
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


def convert_table_to_pole(
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


def get_valid_observations(
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


def save_object(obj, filename):
    if not isinstance(filename, Path):
        filename = Path(filename)
    os.makedirs(filename.parent, exist_ok=True)
    with open(filename, "wb") as outp:  # Overwrites any existing file.
        pickle.dump(obj, outp, -1)
        logger.info("Saved to {}", filename)


def load_object(filename):
    if os.path.isfile(filename):
        with open(filename, "rb") as obj:
            logger.info("Successfully loaded from {}", filename)
            return pickle.load(obj)
    else:
        raise FileNotFoundError(
            "There is no file found in {}".format(
                filename,
            )
        )


def delete_object(obj, filename: str):
    if os.path.isfile(filename):
        os.remove(filename)
        logger.info(
            "Removed from {}",
            filename,
        )
        if hasattr(obj, "path"):
            del obj.path
    else:
        logger.warning(
            "There is no file found in {}",
            filename,
        )


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


def get_care_site_relationship(data: Data) -> pd.DataFrame:
    """Computes hierarchical care site structure

    Parameters
    ----------
    data : Data
        Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]

    Example
    -------

    | care_site_id | care_site_level            | care_site_short_name | parent_care_site_id | parent_care_site_level     | parent_care_site_short_name |
    | :----------- | :------------------------- | :------------------- | :------------------ | :------------------------- | :-------------------------- |
    | 8312056386   | Unité Fonctionnelle (UF)   | UF A                 | 8312027648          | Pôle/DMU                   | Pole A                      |
    | 8312022130   | Pôle/DMU                   | Pole B               | 8312033550          | Hôpital                    | Hospital A                  |
    | 8312016782   | Service/Département        | Service A            | 8312033550          | Hôpital                    | Hospital A                  |
    | 8312010155   | Unité Fonctionnelle (UF)   | UF B                 | 8312022130          | Pôle/DMU                   | Pole B                      |
    | 8312067829   | Unité de consultation (UC) | UC A                 | 8312051097          | Unité de consultation (UC) | UC B                        |

    """
    fact_relationship = data.fact_relationship[
        [
            "fact_id_1",
            "fact_id_2",
            "domain_concept_id_1",
            "relationship_concept_id",
        ]
    ]
    fact_relationship = to("pandas", fact_relationship)

    care_site_relationship = fact_relationship[
        (fact_relationship["domain_concept_id_1"] == 57)  # Care_site domain
        & (fact_relationship["relationship_concept_id"] == 46233688)  # Included in
    ]
    care_site_relationship = care_site_relationship.drop(
        columns=["domain_concept_id_1", "relationship_concept_id"]
    )
    care_site_relationship = care_site_relationship.rename(
        columns={"fact_id_1": "care_site_id", "fact_id_2": "parent_care_site_id"}
    )

    care_site = data.care_site[
        [
            "care_site_id",
            "care_site_type_source_value",
            "care_site_short_name",
        ]
    ]
    care_site = to("pandas", care_site)
    care_site = care_site.rename(
        columns={
            "care_site_type_source_value": "care_site_level",
        }
    )
    care_site_relationship = care_site.merge(
        care_site_relationship, on="care_site_id", how="left"
    )

    parent_care_site = care_site.rename(
        columns={
            "care_site_level": "parent_care_site_level",
            "care_site_id": "parent_care_site_id",
            "care_site_short_name": "parent_care_site_short_name",
        }
    )
    logger.debug("Create care site relationship to link UC to UF and UF to Pole")

    return care_site_relationship.merge(
        parent_care_site, on="parent_care_site_id", how="left"
    )


def concatenate_predictor_by_level(
    predictor_by_level: Dict[str, DataFrame],
    care_site_levels: List[str] = None,
) -> DataFrame:
    predictors_to_concat = []
    if care_site_levels:
        if not isinstance(care_site_levels, list):
            care_site_levels = [care_site_levels]
        unknown_levels, selected_levels = [], []
        for level in care_site_levels:
            if level in predictor_by_level:
                predictors_to_concat.append(predictor_by_level[level])
                selected_levels.append(level)
            elif level in CARE_SITE_LEVEL_NAMES:
                predictors_to_concat.append(
                    predictor_by_level[CARE_SITE_LEVEL_NAMES[level]]
                )
                selected_levels.append(level)
            else:
                unknown_levels.append(level)

        logger.debug(
            "The following levels {} have been selected",
            selected_levels,
        )
        if unknown_levels:
            logger.warning(
                "Unrecognized levels {}.the only supported levels are: {}",
                unknown_levels,
                list(CARE_SITE_LEVEL_NAMES.values())
                + list(CARE_SITE_LEVEL_NAMES.keys()),
            )
    else:
        predictors_to_concat = list(predictor_by_level.values())
        logger.debug(
            "The following levels {} have been selected",
            list(predictor_by_level.keys()),
        )

    if not predictors_to_concat:
        raise AttributeError(
            "care site levels must include at least one of the following levels: {}".format(
                list(CARE_SITE_LEVEL_NAMES.values())
                + list(CARE_SITE_LEVEL_NAMES.keys())
            )
        )

    framework = get_framework(predictors_to_concat[0])
    return framework.concat(predictors_to_concat)
