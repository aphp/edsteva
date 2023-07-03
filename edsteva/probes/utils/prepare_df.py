from datetime import datetime
from typing import Dict, List, Tuple, Union

import pandas as pd
from loguru import logger

from edsteva.utils.checks import check_columns, check_tables
from edsteva.utils.framework import get_framework, is_koalas, to
from edsteva.utils.typing import Data, DataFrame

from .filter_df import (
    filter_table_by_care_site,
    filter_table_by_date,
    filter_table_by_length_of_stay,
    filter_table_by_type,
    filter_valid_observations,
)


def prepare_visit_occurrence(
    data: Data,
    stay_types: Union[str, Dict[str, str]],
    length_of_stays: List[float],
    start_date: datetime = None,
    end_date: datetime = None,
):
    required_columns = [
        "visit_occurrence_id",
        "visit_source_value",
        "visit_start_datetime",
        "visit_end_datetime",
        "care_site_id",
        "row_status_source_value",
        "visit_occurrence_source_value",
    ]
    check_columns(
        data.visit_occurrence,
        required_columns=required_columns,
        df_name="visit_occurrence",
    )
    visit_occurrence = data.visit_occurrence[required_columns]

    visit_occurrence = filter_valid_observations(
        table=visit_occurrence,
        table_name="visit_occurrence",
        invalid_naming="supprimé",
    )

    if length_of_stays:
        visit_occurrence = filter_table_by_length_of_stay(
            visit_occurrence=visit_occurrence, length_of_stays=length_of_stays
        )

    visit_occurrence = visit_occurrence.rename(
        columns={"visit_source_value": "stay_type", "visit_start_datetime": "date"}
    )

    visit_occurrence = filter_table_by_date(
        table=visit_occurrence,
        table_name="visit_occurrence",
        start_date=start_date,
        end_date=end_date,
    )

    if stay_types:
        visit_occurrence = filter_table_by_type(
            table=visit_occurrence,
            table_name="visit_occurrence",
            type_groups=stay_types,
            source_col="stay_type",
            target_col="stay_type",
        )

    return visit_occurrence


def prepare_measurement(
    data: Data,
    biology_relationship: pd.DataFrame,
    concept_codes: List[str],
    concepts_sets: Union[str, Dict[str, str]],
    root_terminology: str,
    standard_terminologies: List[str],
    per_visit: bool,
    start_date: datetime = None,
    end_date: datetime = None,
):
    measurement_columns = [
        "measurement_id",
        "visit_occurrence_id",
        "measurement_datetime",
        "row_status_source_value",
        "measurement_source_concept_id",
    ]

    check_columns(
        data.measurement,
        required_columns=measurement_columns,
        df_name="measurement",
    )
    measurement = data.measurement[measurement_columns].rename(
        columns={
            "measurement_source_concept_id": "{}_concept_id".format(root_terminology),
            "measurement_datetime": "date",
        }
    )
    measurement = filter_valid_observations(
        table=measurement, table_name="measurement", valid_naming="Validé"
    )

    biology_relationship = biology_relationship[
        [
            "{}_{}".format(root_terminology, concept_col)
            for concept_col in ["concept_id", "concept_code", "concept_name"]
        ]
        + [
            "{}_{}".format(terminology, concept_col)
            for terminology in standard_terminologies
            for concept_col in ["concept_code", "concept_name", "vocabulary"]
        ]
    ]
    biology_relationship = to(get_framework(measurement), biology_relationship)
    if is_koalas(biology_relationship):
        biology_relationship = biology_relationship.spark.hint("broadcast")
    measurement = measurement.merge(
        biology_relationship, on="{}_concept_id".format(root_terminology)
    )
    if not per_visit:
        measurement = filter_table_by_date(
            table=measurement,
            table_name="measurement",
            start_date=start_date,
            end_date=end_date,
        )

    if concept_codes:
        measurement_by_terminology = []
        for standard_terminology in standard_terminologies:
            measurement_by_terminology.append(
                measurement[
                    measurement["{}_concept_code".format(standard_terminology)].isin(
                        concept_codes
                    )
                ]
            )
        measurement = get_framework(measurement).concat(measurement_by_terminology)

    if concepts_sets:
        measurement_by_terminology = []
        for standard_terminology in standard_terminologies:
            measurement_by_terminology.append(
                filter_table_by_type(
                    table=measurement,
                    table_name="measurement",
                    type_groups=concepts_sets,
                    source_col="{}_concept_code".format(standard_terminology),
                    target_col="concepts_set",
                )
            )
        measurement = get_framework(measurement).concat(measurement_by_terminology)
    return measurement


def prepare_condition_occurrence(
    data: Data,
    extra_data: Data,
    visit_occurrence: DataFrame,
    source_systems: List[str],
    diag_types: Union[str, Dict[str, str]],
    condition_types: Union[str, Dict[str, str]],
    start_date: datetime = None,
    end_date: datetime = None,
):
    condition_occurrence_tables = []
    if "AREM" in source_systems:  # pragma: no cover
        check_tables(
            data=extra_data,
            required_tables=["visit_occurrence", "condition_occurrence"],
        )
        # Fetch conditions from Data lake
        I2B2_visit = extra_data.visit_occurrence[
            ["visit_occurrence_id", "visit_occurrence_source_value"]
        ]
        I2B2_condition_occurrence = extra_data.condition_occurrence[
            [
                "visit_occurrence_id",
                "condition_occurrence_id",
                "condition_status_source_value",
                "condition_start_datetime",
                "condition_source_value",
                "care_site_source_value",
                "cdm_source",
            ]
        ]
        # Add visit_occurrence_source_value
        arem_condition_occurrence = I2B2_visit.merge(
            I2B2_condition_occurrence,
            on="visit_occurrence_id",
            how="inner",
        ).drop(columns="visit_occurrence_id")

        # Link with visit_occurrence_source_value
        arem_condition_occurrence = arem_condition_occurrence.merge(
            visit_occurrence[["visit_occurrence_source_value", "visit_occurrence_id"]],
            on="visit_occurrence_source_value",
        ).drop(columns="visit_occurrence_source_value")
        arem_condition_occurrence = arem_condition_occurrence[
            arem_condition_occurrence.cdm_source == "AREM"
        ]
        arem_condition_occurrence["visit_detail_id"] = None
        condition_occurrence_tables.append(arem_condition_occurrence)

    if "ORBIS" in source_systems:
        orbis_condition_occurrence = data.condition_occurrence[
            [
                "visit_occurrence_id",
                "condition_occurrence_id",
                "visit_detail_id",
                "condition_source_value",
                "condition_start_datetime",
                "condition_status_source_value",
                "row_status_source_value",
                "cdm_source",
            ]
        ]
        orbis_condition_occurrence = filter_valid_observations(
            table=orbis_condition_occurrence,
            table_name="orbis_condition_occurrence",
            valid_naming="Actif",
        )
        orbis_condition_occurrence = orbis_condition_occurrence[
            orbis_condition_occurrence.cdm_source == "ORBIS"
        ]
        condition_occurrence_tables.append(orbis_condition_occurrence)

    framework = get_framework(condition_occurrence_tables[0])
    condition_occurrence = framework.concat(
        condition_occurrence_tables, ignore_index=True
    )

    # Filter date
    condition_occurrence = condition_occurrence.rename(
        columns={"condition_start_datetime": "date"}
    )
    condition_occurrence = filter_table_by_date(
        table=condition_occurrence,
        table_name="condition_occurrence",
        start_date=start_date,
        end_date=end_date,
    )

    # Filter source system
    condition_occurrence = condition_occurrence.rename(
        columns={"cdm_source": "source_system"}
    )

    # Filter diagnostics
    condition_occurrence = condition_occurrence.rename(
        columns={"condition_status_source_value": "diag_type"}
    )
    if diag_types:
        condition_occurrence = filter_table_by_type(
            table=condition_occurrence,
            table_name="condition_occurrence",
            type_groups=diag_types,
            source_col="diag_type",
            target_col="diag_type",
        )

    # Filter conditions
    condition_occurrence = condition_occurrence.rename(
        columns={"condition_source_value": "condition_type"}
    )
    if condition_types:
        condition_occurrence = filter_table_by_type(
            table=condition_occurrence,
            table_name="condition_occurrence",
            type_groups=condition_types,
            source_col="condition_type",
            target_col="condition_type",
        )

    return condition_occurrence


def prepare_care_site(
    data: Data,
    care_site_ids: Union[int, List[int]],
    care_site_short_names: Union[str, List[str]],
    care_site_specialties: Union[str, List[str]],
    specialties_sets: Union[str, Dict[str, str]],
    care_sites_sets: Union[str, Dict[str, str]],
    care_site_relationship: pd.DataFrame,
):
    care_site = data.care_site[
        [
            "care_site_id",
            "care_site_type_source_value",
            "care_site_short_name",
            "place_of_service_source_value",
        ]
    ]
    care_site = care_site.rename(
        columns={
            "care_site_type_source_value": "care_site_level",
            "place_of_service_source_value": "care_site_specialty",
        }
    )
    if care_site_ids or care_site_short_names or care_site_specialties:
        care_site = filter_table_by_care_site(
            table_to_filter=care_site,
            care_site_relationship=care_site_relationship,
            care_site_ids=care_site_ids,
            care_site_short_names=care_site_short_names,
            care_site_specialties=care_site_specialties,
        )

    # Add care_sites_set
    if care_sites_sets:
        care_site = filter_table_by_type(
            table=care_site,
            table_name="care_site",
            type_groups=care_sites_sets,
            source_col="care_site_short_name",
            target_col="care_sites_set",
        )

    # Add specialties_set
    if specialties_sets:
        care_site = filter_table_by_type(
            table=care_site,
            table_name="care_site",
            type_groups=specialties_sets,
            source_col="care_site_specialty",
            target_col="specialties_set",
        )
    return care_site


def prepare_note(
    data: Data,
    note_types: Union[str, Dict[str, str]],
    start_date: datetime = None,
    end_date: datetime = None,
):
    note = data.note[
        [
            "note_id",
            "visit_occurrence_id",
            "note_datetime",
            "note_class_source_value",
            "row_status_source_value",
            "note_text",
        ]
    ]
    note = note.rename(
        columns={"note_class_source_value": "note_type", "note_datetime": "date"}
    )
    note = note[~(note["note_text"].isna())]
    note = note.drop(columns=["note_text"])

    note = filter_table_by_date(
        table=note,
        table_name="note",
        start_date=start_date,
        end_date=end_date,
    )

    note = filter_valid_observations(
        table=note, table_name="note", valid_naming="Actif"
    )

    # Add note type
    if note_types:
        note = filter_table_by_type(
            table=note,
            table_name="note",
            type_groups=note_types,
            source_col="note_type",
            target_col="note_type",
        )

    return note


def prepare_note_care_site(extra_data: Data, note: DataFrame):  # pragma: no cover
    check_tables(
        data=extra_data,
        required_tables=["note_ref", "care_site_ref"],
    )

    note_ref = extra_data.note_ref[
        [
            "note_id",
            "ufr_source_value",
            "us_source_value",
        ]
    ]
    care_site_ref = extra_data.care_site_ref[
        [
            "care_site_source_value",
            "care_site_id",
        ]
    ]

    note_ref = note_ref.melt(
        id_vars="note_id",
        value_name="care_site_source_value",
    )
    note_ref = note_ref.merge(care_site_ref, on="care_site_source_value")
    return note.merge(note_ref[["note_id", "care_site_id"]], on="note_id")


def prepare_visit_detail(
    data: Data,
    start_date: datetime,
    end_date: datetime,
):
    visit_detail = data.visit_detail[
        [
            "visit_detail_id",
            "visit_occurrence_id",
            "visit_detail_start_datetime",
            "visit_detail_type_source_value",
            "care_site_id",
            "row_status_source_value",
        ]
    ]
    visit_detail = visit_detail.rename(
        columns={
            "visit_detail_id": "visit_id",
            "visit_detail_start_datetime": "date",
            "visit_detail_type_source_value": "visit_detail_type",
        }
    )
    visit_detail = filter_valid_observations(
        table=visit_detail, table_name="visit_detail", valid_naming="Actif"
    )

    return filter_table_by_date(
        table=visit_detail,
        table_name="visit_detail",
        start_date=start_date,
        end_date=end_date,
    )


def prepare_care_site_relationship(data: Data) -> pd.DataFrame:
    """Computes hierarchical care site structure

    Parameters
    ----------
    data : Data
        Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]

    Example
    -------

    | care_site_id | care_site_level            | care_site_short_name | parent_care_site_id | parent_care_site_level     | parent_care_site_short_name |
    | :----------- | :------------------------- | :------------------- | :------------------ | :------------------------- | :-------------------------- |
    | 8312056386   | Unité Fonctionnelle (UF)   | UF A                 | 8312027648          | Pôle/DMU                   | Pole A                      |
    | 8312022130   | Pôle/DMU                   | Pole B               | 8312033550          | Hôpital                    | Hospital A                  |
    | 8312016782   | Service/Département        | Service A            | 8312033550          | Hôpital                    | Hospital A                  |
    | 8312010155   | Unité Fonctionnelle (UF)   | UF B                 | 8312022130          | Pôle/DMU                   | Pole B                      |
    | 8312067829   | Unité de consultation (UC) | UC A                 | 8312051097          | Unité de consultation (UC) | UC B                        |

    """
    fact_relationship = data.fact_relationship[
        [
            "fact_id_1",
            "fact_id_2",
            "domain_concept_id_1",
            "relationship_concept_id",
        ]
    ]
    fact_relationship = to("pandas", fact_relationship)

    care_site_relationship = fact_relationship[
        (fact_relationship["domain_concept_id_1"] == 57)  # Care_site domain
        & (fact_relationship["relationship_concept_id"] == 46233688)  # Included in
    ]
    care_site_relationship = care_site_relationship.drop(
        columns=["domain_concept_id_1", "relationship_concept_id"]
    )
    care_site_relationship = care_site_relationship.rename(
        columns={"fact_id_1": "care_site_id", "fact_id_2": "parent_care_site_id"}
    )

    care_site = data.care_site[
        [
            "care_site_id",
            "care_site_type_source_value",
            "care_site_short_name",
            "place_of_service_source_value",
        ]
    ]
    care_site = to("pandas", care_site)
    care_site = care_site.rename(
        columns={
            "care_site_type_source_value": "care_site_level",
            "place_of_service_source_value": "care_site_specialty",
        }
    )
    care_site_relationship = care_site.merge(
        care_site_relationship, on="care_site_id", how="left"
    )

    parent_care_site = care_site.rename(
        columns={
            "care_site_level": "parent_care_site_level",
            "care_site_id": "parent_care_site_id",
            "care_site_short_name": "parent_care_site_short_name",
            "care_site_specialty": "parent_care_site_specialty",
        }
    )
    logger.debug("Create care site relationship to link UC to UF and UF to Pole")

    return care_site_relationship.merge(
        parent_care_site, on="parent_care_site_id", how="left"
    )


def prepare_biology_relationship(
    data: Data,
    standard_terminologies: List[str],
    source_terminologies: Dict[str, str],
    mapping: List[Tuple[str, str, str]],
) -> pd.DataFrame:
    """Computes biology relationship

    Parameters
    ----------
    data : Data
        Instantiated [``HiveData``][edsteva.io.hive.HiveData], [``PostgresData``][edsteva.io.postgres.PostgresData] or [``LocalData``][edsteva.io.files.LocalData]

    Example
    -------

    | care_site_id | care_site_level            | care_site_short_name | parent_care_site_id | parent_care_site_level     | parent_care_site_short_name |
    | :----------- | :------------------------- | :------------------- | :------------------ | :------------------------- | :-------------------------- |
    | 8312056386   | Unité Fonctionnelle (UF)   | UF A                 | 8312027648          | Pôle/DMU                   | Pole A                      |
    | 8312022130   | Pôle/DMU                   | Pole B               | 8312033550          | Hôpital                    | Hospital A                  |
    | 8312016782   | Service/Département        | Service A            | 8312033550          | Hôpital                    | Hospital A                  |
    | 8312010155   | Unité Fonctionnelle (UF)   | UF B                 | 8312022130          | Pôle/DMU                   | Pole B                      |
    | 8312067829   | Unité de consultation (UC) | UC A                 | 8312051097          | Unité de consultation (UC) | UC B                        |

    """

    logger.debug(
        "Create biology relationship to link ANALYSES LABORATOIRE to ANABIO to LOINC"
    )

    check_tables(data=data, required_tables=["concept", "concept_relationship"])
    concept_columns = [
        "concept_id",
        "concept_name",
        "concept_code",
        "vocabulary_id",
    ]

    concept_relationship_columns = [
        "concept_id_1",
        "concept_id_2",
        "relationship_id",
    ]
    check_columns(
        data.concept,
        required_columns=concept_columns,
        df_name="concept",
    )

    check_columns(
        data.concept_relationship,
        required_columns=concept_relationship_columns,
        df_name="concept_relationship",
    )
    concept = to("pandas", data.concept[concept_columns])
    concept_relationship = to(
        "pandas", data.concept_relationship[concept_relationship_columns]
    )
    concept_by_terminology = {}
    for terminology, regex in source_terminologies.items():
        concept_by_terminology[terminology] = (
            concept[concept.vocabulary_id.str.contains(regex)]
            .rename(
                columns={
                    "concept_id": "{}_concept_id".format(terminology),
                    "concept_name": "{}_concept_name".format(terminology),
                    "concept_code": "{}_concept_code".format(terminology),
                }
            )
            .drop(columns="vocabulary_id")
        )
    root_terminology = mapping[0][0]
    biology_relationship = concept_by_terminology[root_terminology]
    for source, target, relationship_id in mapping:
        relationship = concept_relationship.rename(
            columns={
                "concept_id_1": "{}_concept_id".format(source),
                "concept_id_2": "{}_concept_id".format(target),
            }
        )[concept_relationship.relationship_id == relationship_id].drop(
            columns="relationship_id"
        )
        relationship = relationship.merge(
            concept_by_terminology[target], on="{}_concept_id".format(target)
        )
        biology_relationship = biology_relationship.merge(
            relationship, on="{}_concept_id".format(source), how="left"
        )

    # Get ITM code in priority and if not get GLIMS code
    for standard_terminology in standard_terminologies:
        biology_relationship[
            "{}_concept_code".format(standard_terminology)
        ] = biology_relationship[
            "{}_ITM_concept_code".format(standard_terminology)
        ].mask(
            biology_relationship[
                "{}_ITM_concept_code".format(standard_terminology)
            ].isna(),
            biology_relationship["GLIMS_{}_concept_code".format(standard_terminology)],
        )
        biology_relationship[
            "{}_concept_name".format(standard_terminology)
        ] = biology_relationship[
            "{}_ITM_concept_name".format(standard_terminology)
        ].mask(
            biology_relationship[
                "{}_ITM_concept_name".format(standard_terminology)
            ].isna(),
            biology_relationship["GLIMS_{}_concept_name".format(standard_terminology)],
        )
        biology_relationship["{}_vocabulary".format(standard_terminology)] = "ITM"
        biology_relationship[
            "{}_vocabulary".format(standard_terminology)
        ] = biology_relationship["{}_vocabulary".format(standard_terminology)].mask(
            biology_relationship[
                "{}_ITM_concept_code".format(standard_terminology)
            ].isna(),
            "GLIMS",
        )
    return biology_relationship
