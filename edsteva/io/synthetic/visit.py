import numpy as np
import pandas as pd
from loguru import logger

from edsteva.io.synthetic.utils import (
    generate_events_after_t0,
    generate_events_after_t1,
    generate_events_around_t0,
    generate_events_around_t1,
    generate_events_before_t0,
)


def generate_stays(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    t_start: int,
    t_end: int,
    n_events: int,
    increase_time: int,
    increase_ratio: float,
    care_site_id: int,
    date_col: str,
    mode: str,
):
    if mode == "step":
        return _generate_stays_step(
<<<<<<< HEAD
=======
            generator=generator,
>>>>>>> main
            t_start=t_start,
            t_end=t_end,
            n_events=n_events,
            increase_time=increase_time,
            increase_ratio=increase_ratio,
            care_site_id=care_site_id,
            date_col=date_col,
        )
    if mode == "rect":
        return _generate_stays_rect(
<<<<<<< HEAD
=======
            generator=generator,
>>>>>>> main
            t_start=t_start,
            t_end=t_end,
            n_events=n_events,
            increase_time=increase_time,
            increase_ratio=increase_ratio,
            care_site_id=care_site_id,
            date_col=date_col,
        )


def _generate_stays_step(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    t_start: int,
    t_end: int,
    n_events: int,
    increase_time: int,
    increase_ratio: float,
    care_site_id: int,
    date_col: str,
):
<<<<<<< HEAD
    t0 = np.random.randint(t_start + increase_time, t_end - increase_time)
    params = dict(
=======
    t0 = generator.integers(t_start + increase_time, t_end - increase_time)
    params = dict(
        generator=generator,
>>>>>>> main
        t_start=t_start,
        t_end=t_end,
        n_events=n_events,
        t0=t0,
        increase_ratio=increase_ratio,
        increase_time=increase_time,
    )
    df = pd.concat(
        [
            generate_events_before_t0(**params),
            generate_events_after_t0(**params),
            generate_events_around_t0(**params),
        ]
    ).to_frame()
    df.columns = [date_col]
    df["care_site_id"] = care_site_id
    df["t_0_min"] = t0 - increase_time / 2
    df["t_0_max"] = t0 + increase_time / 2
    logger.debug("Generate visit occurrences deploying as step function")

    return df


def _generate_stays_rect(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    t_start: int,
    t_end: int,
    n_events: int,
    increase_time: int,
    increase_ratio: float,
    care_site_id: int,
    date_col: str,
):
<<<<<<< HEAD
    t0 = np.random.randint(
        t_start + increase_time, (t_end + t_start) / 2 - increase_time
    )
    t1 = np.random.randint((t_end + t_start) / 2 + increase_time, t_end - increase_time)
    t0_params = dict(
        t_start=t_start,
        t_end=t1 - increase_time / 2,
        n_events=n_events,
        t0=t0,
        increase_ratio=increase_ratio,
        increase_time=increase_time,
    )
    before_t0 = generate_events_before_t0(**t0_params)
    around_t0 = generate_events_around_t0(**t0_params)
    # Raise n_visit to enforce a rectangle shape
    between_t0_t1 = generate_events_after_t0(**t0_params)
    t1_params = dict(
        t_start=t_start,
        t_end=t_end,
        n_events=n_events,
        t1=t1,
        increase_time=increase_time,
        increase_ratio=increase_ratio,
    )
    around_t1 = generate_events_around_t1(**t1_params)
    after_t1 = generate_events_after_t1(**t1_params)

    df = pd.concat(
        [
            before_t0,
            around_t0,
            between_t0_t1,
            around_t1,
            after_t1,
        ]
    ).to_frame()

    df.columns = [date_col]
    df["care_site_id"] = care_site_id
    df["t_0_min"] = t0 - increase_time / 2
    df["t_0_max"] = t0 + increase_time / 2
    df["t_1_min"] = t1 - increase_time / 2
    df["t_1_max"] = t1 + increase_time / 2
    logger.debug("Generate visit occurrences deploying as rectangle function")

=======
    t0 = generator.integers(
        t_start + increase_time, (t_end + t_start) / 2 - increase_time
    )
    t1 = generator.integers(
        (t_end + t_start) / 2 + increase_time, t_end - increase_time
    )
    t0_params = dict(
        generator=generator,
        t_start=t_start,
        t_end=t1 - increase_time / 2,
        n_events=n_events,
        t0=t0,
        increase_ratio=increase_ratio,
        increase_time=increase_time,
    )
    before_t0 = generate_events_before_t0(**t0_params)
    around_t0 = generate_events_around_t0(**t0_params)
    # Raise n_visit to enforce a rectangle shape
    between_t0_t1 = generate_events_after_t0(**t0_params)
    t1_params = dict(
        generator=generator,
        t_start=t_start,
        t_end=t_end,
        n_events=n_events,
        t1=t1,
        increase_time=increase_time,
        increase_ratio=increase_ratio,
    )
    around_t1 = generate_events_around_t1(**t1_params)
    after_t1 = generate_events_after_t1(**t1_params)

    df = pd.concat(
        [
            before_t0,
            around_t0,
            between_t0_t1,
            around_t1,
            after_t1,
        ]
    ).to_frame()

    df.columns = [date_col]
    df["care_site_id"] = care_site_id
    df["t_0_min"] = t0 - increase_time / 2
    df["t_0_max"] = t0 + increase_time / 2
    df["t_1_min"] = t1 - increase_time / 2
    df["t_1_max"] = t1 + increase_time / 2
    logger.debug("Generate visit occurrences deploying as rectangle function")

>>>>>>> main
    return df
