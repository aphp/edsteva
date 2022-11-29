import os
from datetime import datetime
from math import ceil, floor, log10
from pathlib import Path
from typing import List, Union

import altair as alt
import pandas as pd
from loguru import logger

from edsteva.probes.utils import CARE_SITE_LEVEL_NAMES, filter_table_by_date


def save_html(obj: alt.Chart, filename: str):
    """Save chart in the specified file

    Parameters
    ----------
    obj : alt.Chart
        Altair chart to be saved
    filename : str
        Folder path where to save the chart in HTML format.

        **EXAMPLE**: `"my_folder/my_file.html"`
    """
    if not isinstance(filename, Path):
        filename = Path(filename)
    os.makedirs(filename.parent, exist_ok=True)
    if hasattr(obj, "save"):
        obj.save(filename)
    else:
        with open(filename, "w") as f:
            f.write(obj)
    logger.info("The chart has been saved in {}", filename)


def round_it(x: float, sig: int):
    if x == 0:
        return 0
    else:
        return round(x, sig - int(floor(log10(abs(x)))) - 1)


def scale_it(x: float):
    if x == 0:
        return 1
    else:
        return 10 ** ceil(log10(x))


def filter_predictor(
    predictor: pd.DataFrame,
    care_site_level: str = None,
    stay_type: List[str] = None,
    care_site_id: List[int] = None,
    care_site_short_name: List[int] = None,
    start_date: Union[datetime, str] = None,
    end_date: Union[datetime, str] = None,
    **kwargs
):

    # Time
    predictor = filter_table_by_date(
        table=predictor,
        table_name="predictor",
        start_date=start_date,
        end_date=end_date,
    )

    # Care site level
    if not care_site_level:
        if len(predictor.care_site_level.unique()) == 1:
            care_site_level = predictor.care_site_level.unique()[0]
        else:
            care_site_level = CARE_SITE_LEVEL_NAMES["Hospital"]
    elif care_site_level in CARE_SITE_LEVEL_NAMES.keys():
        care_site_level = CARE_SITE_LEVEL_NAMES[care_site_level]

    if care_site_level not in predictor.care_site_level.unique():
        raise AttributeError(
            "The selected care site level {} is not part of the computed care site levels {}".format(
                care_site_level, list(predictor.care_site_level.unique())
            )
        )
    predictor = predictor[predictor["care_site_level"] == care_site_level].drop(
        columns=["care_site_level"]
    )
    logger.debug(
        "Predictor has been filtered on the selected care site level : {}",
        care_site_level,
    )

    # Stay type
    if stay_type:
        if not isinstance(stay_type, list):
            stay_type = [stay_type]
        predictor = predictor[predictor.stay_type.isin(stay_type)]
        logger.debug(
            "Predictor has been filtered on the selected stay type : {}",
            stay_type,
        )

    # Care site id
    if care_site_id:
        if not isinstance(care_site_id, list):
            care_site_id = [care_site_id]
        predictor = predictor[predictor.care_site_id.isin(care_site_id)]
        logger.debug(
            "Predictor has been filtered on the selected care site id : {}",
            care_site_id,
        )

    # Care site short name
    if care_site_short_name:
        if not isinstance(care_site_short_name, list):
            care_site_short_name = [care_site_short_name]
        predictor = predictor[predictor.care_site_short_name.isin(care_site_short_name)]
        logger.debug(
            "Predictor has been filtered on the selected care site short name : {}",
            care_site_short_name,
        )

    # Others
    for key, value in kwargs.items():
        if not isinstance(value, list):
            value = [value]
        predictor = predictor[predictor[key].isin(value)]
        logger.debug(
            "Predictor has been filtered on the selected {} : {}",
            key,
            care_site_short_name,
        )
    return predictor
