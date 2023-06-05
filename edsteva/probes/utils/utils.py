from typing import Dict, List

import pandas as pd
from loguru import logger

from edsteva.utils.framework import get_framework
from edsteva.utils.typing import DataFrame

CARE_SITE_LEVEL_NAMES = {
    "Hospital": "Hôpital",
    "Pole": "Pôle/DMU",
    "UF": "Unité Fonctionnelle (UF)",
    "UC": "Unité de consultation (UC)",
    "UH": "Unité d’hébergement (UH)",
}

VISIT_DETAIL_TYPE = {
    "UF": "PASS UF",
    "UC": "PASS UC",
    "UH": "PASS UH",
}


def impute_missing_columns(
    predictor: DataFrame,
    target_column: str,
    missing_columns: List[str],
    index: List[str],
):
    missing_columns = list(set(predictor.columns).intersection(set(missing_columns)))
    missing_predictor = predictor[predictor[target_column] == 0].copy()
    full_predictor = predictor[predictor[target_column] > 0]
    for partition, _ in full_predictor.groupby(missing_columns):
        missing_predictor[missing_columns] = partition
        full_predictor = pd.concat([full_predictor, missing_predictor])
    return full_predictor.drop_duplicates(subset=index + ["date"], keep="first")


def hospital_only(care_site_levels: List[str]):
    if not isinstance(care_site_levels, list):
        care_site_levels = [care_site_levels]
    return len(care_site_levels) == 1 and (
        care_site_levels[0] == "Hospital"
        or care_site_levels[0] == CARE_SITE_LEVEL_NAMES["Hospital"]
    )


def concatenate_predictor_by_level(
    predictor_by_level: Dict[str, DataFrame],
    care_site_levels: List[str] = None,
) -> DataFrame:
    predictors_to_concat = []
    if care_site_levels:
        if not isinstance(care_site_levels, list):
            care_site_levels = [care_site_levels]
        unknown_levels, unavailable_levels, selected_levels = [], [], []
        for level in care_site_levels:
            if level in predictor_by_level.keys():
                predictors_to_concat.append(predictor_by_level[level])
                selected_levels.append(level)
            elif level in CARE_SITE_LEVEL_NAMES.keys():
                raw_level = CARE_SITE_LEVEL_NAMES[level]
                if raw_level in predictor_by_level.keys():
                    predictors_to_concat.append(predictor_by_level[raw_level])
                    selected_levels.append(level)
                else:
                    unavailable_levels.append(level)
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
        if unavailable_levels:
            logger.warning(
                "The following levels: {} are not available for this probe.",
                unavailable_levels,
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

    return get_framework(predictors_to_concat[0]).concat(predictors_to_concat)


def get_child_and_parent_cs(
    care_site_sample: DataFrame, care_site_relationship: DataFrame
):
    extended_care_site_id_to_filter = []

    # Parent care site to get
    parent_rel = care_site_relationship[
        ~care_site_relationship.parent_care_site_id.isna()
    ][
        [
            "care_site_id",
            "parent_care_site_id",
            "parent_care_site_level",
            "parent_care_site_specialty",
        ]
    ]
    parent_care_site_filter = care_site_sample.copy()
    while set(parent_care_site_filter.care_site_level.unique()).intersection(
        CARE_SITE_LEVEL_NAMES.values()
    ):
        extended_care_site_id_to_filter.append(
            parent_care_site_filter[
                ["care_site_id", "care_site_level", "care_site_specialty"]
            ]
        )
        parent_care_site_filter = parent_care_site_filter.merge(
            parent_rel,
            on="care_site_id",
        )[
            [
                "parent_care_site_id",
                "parent_care_site_level",
                "parent_care_site_specialty",
            ]
        ].rename(
            columns={
                "parent_care_site_id": "care_site_id",
                "parent_care_site_level": "care_site_level",
                "parent_care_site_specialty": "care_site_specialty",
            }
        )

    # Child care site to get
    child_rel = care_site_relationship[~care_site_relationship.care_site_id.isna()][
        [
            "care_site_id",
            "care_site_level",
            "care_site_specialty",
            "parent_care_site_id",
        ]
    ].rename(
        columns={
            "care_site_id": "child_care_site_id",
            "care_site_level": "child_care_site_level",
            "care_site_specialty": "child_care_site_specialty",
            "parent_care_site_id": "care_site_id",
        }
    )
    child_care_site_filter = care_site_sample.copy()
    while set(child_care_site_filter.care_site_level.unique()).intersection(
        CARE_SITE_LEVEL_NAMES.values()
    ):
        extended_care_site_id_to_filter.append(
            child_care_site_filter[
                ["care_site_id", "care_site_level", "care_site_specialty"]
            ]
        )
        child_care_site_filter = child_care_site_filter.merge(
            child_rel,
            on="care_site_id",
        )[
            [
                "child_care_site_id",
                "child_care_site_level",
                "child_care_site_specialty",
            ]
        ].rename(
            columns={
                "child_care_site_id": "care_site_id",
                "child_care_site_level": "care_site_level",
                "child_care_site_specialty": "care_site_specialty",
            }
        )

    extended_care_site_id_to_filter = pd.concat(
        extended_care_site_id_to_filter
    ).drop_duplicates()
    return extended_care_site_id_to_filter
