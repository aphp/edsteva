from typing import Union

import pandas
from databricks import koalas

from edsteva.io import HiveData, LocalData, PostgresData, SyntheticData

DataFrame = Union[koalas.DataFrame, pandas.DataFrame]
Series = Union[koalas.Series, pandas.Series]
DataObject = Union[DataFrame, Series]
Data = Union[HiveData, PostgresData, LocalData, SyntheticData]
