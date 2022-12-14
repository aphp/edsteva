from types import ModuleType
from typing import Optional

import pandas as _pandas
from databricks import koalas as _koalas

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


def to(framework: str, obj: DataObject) -> DataObject:
    if framework == "koalas" or framework is _koalas:
        return koalas(obj)
    elif framework == "pandas" or framework is _pandas:
        return pandas(obj)
    else:
        raise ValueError(f"Unknown framework: {framework}")


def pandas(obj: DataObject) -> DataObject:
    if get_framework(obj) is _pandas:
        return obj
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
