from typing import List, Union

from edsteva.utils.typing import Data, DataFrame


class MissingColumnError(Exception):
    """Exception raised when a concept is missing"""

    def __init__(
        self,
        required_columns: Union[List, dict],
        df_name: str = "",
    ):

        if isinstance(required_columns, dict):
            to_display_per_column = [
                f"- {column} ({msg})" if msg is not None else f"{column}"
                for column, msg in required_columns.items()
            ]
        else:
            to_display_per_column = [f"- {column}" for column in required_columns]
        str_to_display = "\n".join(to_display_per_column)

        if df_name:
            df_name = f" {df_name} "
        message = (
            f"The{df_name}DataFrame is missing some columns, "
            "namely:\n"
            f"{str_to_display}"
        )

        super().__init__(message)


class MissingTableError(Exception):
    """Exception raised when a table is missing in the Data"""

    def __init__(
        self,
        required_tables: Union[List, dict],
        data_name: str = "",
    ):

        if isinstance(required_tables, dict):
            to_display_per_concept = [
                f"- {concept} ({msg})" if msg is not None else f"{concept}"
                for concept, msg in required_tables.items()
            ]
        else:
            to_display_per_concept = [f"- {concept}" for concept in required_tables]
        str_to_display = "\n".join(to_display_per_concept)

        if data_name:
            data_name = f" {data_name} "
        message = (
            f"The{data_name}Data is missing some tables, "
            "namely:\n"
            f"{str_to_display}"
        )

        super().__init__(message)


def check_columns(df: DataFrame, required_columns: List[str], df_name: str = ""):
    present_columns = set(df.columns)
    missing_columns = set(required_columns) - present_columns
    if missing_columns:
        raise MissingColumnError(missing_columns, df_name=df_name)


def check_tables(data: Data, required_tables: List[str], data_name: str = ""):
    present_tables = set(data.available_tables)
    missing_tables = set(required_tables) - present_tables
    if missing_tables:
        raise MissingTableError(missing_tables, data_name=data_name)
