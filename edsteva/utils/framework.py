import os
from types import ModuleType
from typing import Optional

import pandas as _pandas
import pyarrow.parquet as pq
from databricks import koalas as _koalas
from loguru import logger

from edsteva.utils.typing import DataObject

VALID_FRAMEWORKS = {
    "pandas": _pandas,
    "koalas": _koalas,
}


def get_framework(obj: DataObject) -> Optional[ModuleType]:
    for _, framework in VALID_FRAMEWORKS.items():
        if obj.__class__.__module__.startswith(framework.__name__):
            return framework
    return None


def is_pandas(obj: DataObject) -> bool:
    return get_framework(obj) == _pandas


def is_koalas(obj: DataObject) -> bool:
    return get_framework(obj) == _koalas


def to(framework: str, obj: DataObject) -> DataObject:
    if framework == "koalas" or framework is _koalas:
        return koalas(obj)
    if framework == "pandas" or framework is _pandas:
        return pandas(obj)
    raise ValueError(f"Unknown framework: {framework}")


def pandas(obj: DataObject) -> DataObject:
    if get_framework(obj) is _pandas:
        return obj

    # Try using pyarrow via HDFS to convert object to pandas as it is way faster.
    user = os.environ["USER"]
    parquet_path = f"hdfs://bbsedsi/user/{user}/temp.parquet"
    try:  # pragma: no cover
        obj.to_parquet(parquet_path)
        obj = pq.read_table(parquet_path)
    except Exception as e:
        logger.debug(
            "Cannot convert object to parquet. It will skip this step and convert directly to pandas if possible. /n Following error: {}",
            e,
        )

    try:
        return obj.to_pandas()
    except AttributeError:
        pass
    raise ValueError("Could not convert object to pandas.")


def koalas(obj: DataObject) -> DataObject:
    if get_framework(obj) is _koalas:
        return obj
    try:
        return obj.to_koalas()
    except AttributeError:
        pass

    # will raise ValueError if impossible
    return _koalas.from_pandas(obj)
