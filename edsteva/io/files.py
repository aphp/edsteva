import os
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from . import settings


class LocalData:  # pragma: no cover
    """Pandas interface to OMOP data stored as local parquet files/folders.


    Parameters
    ----------
    folder: str
        absolute path to a folder containing several parquet files with omop data

    Examples
    --------
    >>> data = LocalData(folder="/export/home/USER/my_data/")
    >>> person = data.person
    >>> person.shape
    (100, 10)

    Attributes
    ----------
    person: pd.DataFrame
        Pandas dataframe `person`. All dataframe attributes are
        dynamically generated to match the content of the
        selected folder.

    """

    def __init__(
        self,
        folder: str,
    ):
        (
            self.available_tables,
            self.tables_paths,
            self.available_omop_tables,
        ) = self.list_available_tables(folder)
        if not self.available_omop_tables:
            raise ValueError(f"Folder {folder} does not contain any parquet omop data.")

    @staticmethod
    def list_available_tables(folder: str) -> Tuple[List[str], List[str]]:
        available_tables = []
        available_omop_tables = []
        tables_paths = {}
        known_omop_tables = settings.tables_to_load.keys()
        for filename in os.listdir(folder):
            file = Path(folder) / filename
            table_name = file.stem
            extension = file.suffix
            if extension == ".parquet":
                abspath = Path.resolve(file)
                tables_paths[table_name] = abspath
                available_tables.append(table_name)
                if table_name in known_omop_tables:
                    available_omop_tables.append(table_name)

        return available_tables, tables_paths, available_omop_tables

    def _read_table(self, table_name: str) -> pd.DataFrame:
        path = self.tables_paths[table_name]
        return pd.read_parquet(path)

    def __getattr__(self, table_name: str) -> pd.DataFrame:
        if table_name in self.available_tables:
            return self._read_table(table_name)
        raise AttributeError(f"Table '{table_name}' is not available in chosen folder.")

    def __dir__(self) -> List[str]:
        return list(super().__dir__()) + list(self.available_tables)
