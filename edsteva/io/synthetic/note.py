import numpy as np
import pandas as pd
from loguru import logger


def generate_note(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    visit_care_site: pd.DataFrame,
    note_type: str,
    note_date_col: str,
    id_visit_col: str,
    note_type_col: str,
    t0_visit: int,
    care_site_id: int,
    date_col: str,
    mode: str,
):
    if mode == "step":
        return _generate_note_step(
<<<<<<< HEAD
=======
            generator=generator,
>>>>>>> main
            visit_care_site=visit_care_site,
            note_type=note_type,
            note_date_col=note_date_col,
            id_visit_col=id_visit_col,
            note_type_col=note_type_col,
            t0_visit=t0_visit,
            care_site_id=care_site_id,
            date_col=date_col,
        )

<<<<<<< HEAD
    elif mode == "rect":
        return _generate_note_rect(
=======
    if mode == "rect":
        return _generate_note_rect(
            generator=generator,
>>>>>>> main
            visit_care_site=visit_care_site,
            note_type=note_type,
            note_date_col=note_date_col,
            id_visit_col=id_visit_col,
            note_type_col=note_type_col,
            t0_visit=t0_visit,
            care_site_id=care_site_id,
            date_col=date_col,
        )


def _generate_note_step(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    visit_care_site,
    note_type,
    care_site_id,
    date_col,
    note_date_col,
    id_visit_col,
    note_type_col,
    t0_visit,
):
    t_end = visit_care_site[date_col].max()
<<<<<<< HEAD
    t0 = np.random.randint(t0_visit, t_end)
    c_before = np.random.uniform(0, 0.2)
    c_after = np.random.uniform(0.8, 1)
=======
    t0 = generator.integers(t0_visit, t_end)
    c_before = generator.uniform(0, 0.2)
    c_after = generator.uniform(0.8, 1)
>>>>>>> main
    note_before_t0_visit = (
        visit_care_site[visit_care_site[date_col] <= t0_visit][[id_visit_col, date_col]]
        .sample(frac=c_before)
        .rename(columns={date_col: note_date_col})
    )
    # Stratify visit between t0_visit and t0 to
    # ensure that these elements are represented
    # in the final notes dataset.
    note_before_t0 = (
        visit_care_site[
            (visit_care_site[date_col] <= t0) & (visit_care_site[date_col] > t0_visit)
        ][[id_visit_col, date_col]]
        .sample(frac=c_before)
        .rename(columns={date_col: note_date_col})
    )

    note_after_t0 = (
        visit_care_site[visit_care_site[date_col] > t0][[id_visit_col, date_col]]
        .sample(frac=c_after)
        .rename(columns={date_col: note_date_col})
    )

    note = pd.concat([note_before_t0_visit, note_before_t0, note_after_t0])

    note[note_date_col] = pd.to_datetime(note[note_date_col], unit="s")
    note[note_type_col] = note_type
    note["care_site_id"] = care_site_id
    note["t_0"] = t0
    logger.debug("Generate synthetic note deploying as step function")

    return note


def _generate_note_rect(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    visit_care_site,
    note_type,
    care_site_id,
    date_col,
    note_date_col,
    id_visit_col,
    note_type_col,
    t0_visit,
):
    t1_visit = visit_care_site["t_1_min"].max()
<<<<<<< HEAD
    t0 = np.random.randint(t0_visit, t0_visit + (t1_visit - t0_visit) / 3)
    t1 = np.random.randint(t0_visit + 2 * (t1_visit - t0_visit) / 3, t1_visit)
    c_out = np.random.uniform(0, 0.1)
    c_in = np.random.uniform(0.8, 1)
=======
    t0 = generator.integers(t0_visit, t0_visit + (t1_visit - t0_visit) / 3)
    t1 = generator.integers(t0_visit + 2 * (t1_visit - t0_visit) / 3, t1_visit)
    c_out = generator.uniform(0, 0.1)
    c_in = generator.uniform(0.8, 1)
>>>>>>> main

    note_before_t0 = (
        visit_care_site[visit_care_site[date_col] <= t0][[id_visit_col, date_col]]
        .sample(frac=c_out)
        .rename(columns={date_col: note_date_col})
    )
    note_between_t0_t1 = (
        visit_care_site[
            (visit_care_site[date_col] > t0) & (visit_care_site[date_col] <= t1)
        ][[id_visit_col, date_col]]
        .sample(frac=c_in)
        .rename(columns={date_col: note_date_col})
    )

    note_after_t1 = (
        visit_care_site[(visit_care_site[date_col] > t1)][[id_visit_col, date_col]]
        .sample(frac=c_out)
        .rename(columns={date_col: note_date_col})
    )

    note = pd.concat(
        [
            note_before_t0,
            note_between_t0_t1,
            note_after_t1,
        ]
    )

    note[note_date_col] = pd.to_datetime(note[note_date_col], unit="s")
    note[note_type_col] = note_type
    note["care_site_id"] = care_site_id
    note["t_0"] = t0
    note["t_1"] = t1
    logger.debug("Generate synthetic note deploying as rectangle function")

    return note
