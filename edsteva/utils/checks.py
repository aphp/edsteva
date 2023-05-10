from typing import List

from edsteva.utils.typing import Data, DataFrame


class MissingColumnError(Exception):
    """Exception raised when a concept is missing"""

    def __init__(
        self,
        required_columns: List,
        df_name: str = "",
    ):
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
        required_tables: List,
    ):
        to_display_per_concept = [f"- {concept}" for concept in required_tables]
        str_to_display = "\n".join(to_display_per_concept)

        message = f"Data is missing some tables, namely:\n {str_to_display}"

        super().__init__(message)


def check_columns(df: DataFrame, required_columns: List[str], df_name: str = ""):
    present_columns = set(df.columns)
    missing_columns = set(required_columns) - present_columns
    if missing_columns:
        raise MissingColumnError(missing_columns, df_name=df_name)


def check_tables(data: Data, required_tables: List[str]):
    if isinstance(data, Data.__args__):
        present_tables = set(data.available_tables)
        missing_tables = set(required_tables) - present_tables
        if missing_tables:
            raise MissingTableError(missing_tables)
    else:
        raise AttributeError(
            "data should be a Data type please refer to this [page](https://aphp.github.io/edsteva/latest/components/loading_data/)"
        )


def check_condition_source_systems(
    source_systems: List[str], valid_source_systems: List[str] = ["AREM", "ORBIS"]
):
    if source_systems and isinstance(source_systems, list):
        valid = False
        for valid_source_system in valid_source_systems:
            if valid_source_system in source_systems:
                valid = True

        if not valid:
            raise AttributeError(
                "Source systems only accept {}".format(valid_source_systems)
            )
    else:
        raise AttributeError(
            "Source systems must be a non empty list and not {}".format(
                type(source_systems).__name__
            )
        )
