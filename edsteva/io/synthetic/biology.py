import numpy as np
import pandas as pd
from loguru import logger


def generate_bio(
    generator: np.random.Generator,
    visit_care_site,
    t0_visit,
    date_col: str,
    bio_date_col: str,
    id_visit_col,
    unit: str,
    concept_code: str,
    mode: str,
):
    if mode == "step":
        return _generate_bio_step(
            generator=generator,
            visit_care_site=visit_care_site,
            t0_visit=t0_visit,
            date_col=date_col,
            bio_date_col=bio_date_col,
            id_visit_col=id_visit_col,
            unit=unit,
            concept_code=concept_code,
        )
    if mode == "rect":
        return _generate_bio_rect(
            generator=generator,
            visit_care_site=visit_care_site,
            t0_visit=t0_visit,
            date_col=date_col,
            bio_date_col=bio_date_col,
            id_visit_col=id_visit_col,
            unit=unit,
            concept_code=concept_code,
        )


def _generate_bio_step(
    generator: np.random.Generator,
    visit_care_site,
    t0_visit,
    date_col: str,
    bio_date_col: str,
    id_visit_col,
    unit: str,
    concept_code: str,
):
    t_end = visit_care_site[date_col].max()
    t0 = generator.integers(t0_visit, t_end)
    c_before = generator.uniform(0, 0.01)
    c_after = generator.uniform(0.8, 1)

    measurement_before_t0_visit = (
        visit_care_site[visit_care_site[date_col] <= t0_visit][[id_visit_col, date_col]]
        .sample(frac=c_before)
        .rename(columns={date_col: bio_date_col})
    )
    # Stratify visit between t0_visit and t0 to
    # ensure that these elements are represented
    # in the final measurements dataset.

    measurement_before_t0 = (
        visit_care_site[
            (visit_care_site[date_col] <= t0) & (visit_care_site[date_col] > t0_visit)
        ][[id_visit_col, date_col]]
        .sample(frac=c_before)
        .rename(columns={date_col: bio_date_col})
    )

    measurement_after_t0 = (
        visit_care_site[visit_care_site[date_col] > t0][[id_visit_col, date_col]]
        .sample(frac=c_after)
        .rename(columns={date_col: bio_date_col})
    )

    measurement = pd.concat(
        [measurement_before_t0_visit, measurement_before_t0, measurement_after_t0]
    )

    measurement[bio_date_col] = pd.to_datetime(measurement[bio_date_col], unit="s")
    measurement["unit_source_value"] = unit
    measurement["measurement_source_concept_id"] = concept_code
    measurement["t_0"] = t0

    logger.debug("Generate synthetic measurement deploying as step function")

    return measurement


def _generate_bio_rect(
    generator: np.random.Generator,
    visit_care_site,
    t0_visit,
    date_col: str,
    bio_date_col: str,
    id_visit_col,
    unit: str,
    concept_code: str,
):
    t1_visit = visit_care_site["t_1_min"].max()
    t0 = generator.integers(t0_visit, t0_visit + (t1_visit - t0_visit) / 3)
    t1 = generator.integers(t0_visit + 2 * (t1_visit - t0_visit) / 3, t1_visit)
    c_out = generator.uniform(0, 0.1)
    c_in = generator.uniform(0.8, 1)

    measurement_before_t0 = (
        visit_care_site[visit_care_site[date_col] <= t0][[id_visit_col, date_col]]
        .sample(frac=c_out)
        .rename(columns={date_col: bio_date_col})
    )
    measurement_between_t0_t1 = (
        visit_care_site[
            (visit_care_site[date_col] > t0) & (visit_care_site[date_col] <= t1)
        ][[id_visit_col, date_col]]
        .sample(frac=c_in)
        .rename(columns={date_col: bio_date_col})
    )

    measurement_after_t1 = (
        visit_care_site[(visit_care_site[date_col] > t1)][[id_visit_col, date_col]]
        .sample(frac=c_out)
        .rename(columns={date_col: bio_date_col})
    )

    measurement = pd.concat(
        [
            measurement_before_t0,
            measurement_between_t0_t1,
            measurement_after_t1,
        ]
    )

    measurement[bio_date_col] = pd.to_datetime(measurement[bio_date_col], unit="s")
    measurement["unit_source_value"] = unit
    measurement["measurement_source_concept_id"] = concept_code
    measurement["t_0"] = t0
    measurement["t_1"] = t1
    logger.debug("Generate synthetic measurement deploying as rectangle function")

    return measurement
