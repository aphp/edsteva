import os
from types import ModuleType
from typing import Optional

import pandas as _pandas
import pyarrow as pa
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
    # raise ValueError(f"Object from unknown framework: {obj}")
    return None


def is_pandas(obj: DataObject) -> bool:
    return get_framework(obj) == _pandas


def is_koalas(obj: DataObject) -> bool:
    return get_framework(obj) == _koalas


def to(framework: str, obj: DataObject, hdfs_user_path: str = None) -> DataObject:
    if framework == "koalas" or framework is _koalas:
        return koalas(obj)
    elif framework == "pandas" or framework is _pandas:
        return pandas(obj, hdfs_user_path)
    else:
        raise ValueError(f"Unknown framework: {framework}")


def pandas(obj: DataObject, hdfs_user_path: str = None) -> DataObject:
    if get_framework(obj) is _pandas:
        return obj
    elif hdfs_user_path:
        parquet_path = hdfs_user_path + "/object.parquet"
        try:
            obj.to_parquet(parquet_path)
            obj = pa.parquet.read_table(parquet_path)
        except AttributeError:
            pass
        logger.warning(
            "Could not convert object to parquet. It will skip this step and convert directly to pandas if possible"
        )
    try:
        pandas_obj = obj.to_pandas()
        error = False
    except AttributeError:
        error = True
    if error:
        raise ValueError("Could not convert object to pandas.")
    elif hdfs_user_path and os.path.exists(parquet_path):
        os.remove(parquet_path)
    return pandas_obj


def koalas(obj: DataObject) -> DataObject:
    if get_framework(obj) is _koalas:
        return obj
    try:
        return obj.to_koalas()
    except AttributeError:
        pass

    # will raise ValueError if impossible
    return _koalas.from_pandas(obj)
