from typing import Dict, List

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

    framework = get_framework(predictors_to_concat[0])
    return framework.concat(predictors_to_concat)
