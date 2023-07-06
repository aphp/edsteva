import numpy as np
import pandas as pd


def generate_events_before_t0(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    t_start: int,
    t_end: int,
    n_events: int,
    t0: int,
    increase_time: int,
    increase_ratio: float,
):
    t0_before = t0 - increase_time / 2
    n_before = int(
        (t0_before - t_start)
        * n_events
        / ((t0 - t_start) + increase_ratio * (t_end - t0))
    )

    return pd.to_datetime(
<<<<<<< HEAD
        pd.Series(np.random.randint(t_start, t0_before, n_before)),
=======
        pd.Series(generator.integers(t_start, t0_before, n_before)),
>>>>>>> main
        unit="s",
    )


def generate_events_after_t0(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    t_start: int,
    t_end: int,
    n_events: int,
    t0: int,
    increase_time: int,
    increase_ratio: float,
):
    t0_after = t0 + increase_time / 2
    n_after = int(
        increase_ratio
        * (t_end - t0_after)
        * n_events
        / ((t0 - t_start) + increase_ratio * (t_end - t0))
    )

    return pd.to_datetime(
<<<<<<< HEAD
        pd.Series(np.random.randint(t0_after, t_end, n_after)),
=======
        pd.Series(generator.integers(t0_after, t_end, n_after)),
>>>>>>> main
        unit="s",
    )


def generate_events_around_t0(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    t_start: int,
    t_end: int,
    n_events: int,
    t0: int,
    increase_time: int,
    increase_ratio: float,
):
    t0_before = t0 - increase_time / 2
    t0_after = t0 + increase_time / 2
    n_middle = int(
        (increase_time / 2)
        * (increase_ratio + 1)
        * n_events
        / ((t0 - t_start) + increase_ratio * (t_end - t0))
    )

    return pd.to_datetime(
        pd.Series(
<<<<<<< HEAD
            np.random.triangular(
=======
            generator.triangular(
>>>>>>> main
                left=t0_before, right=t0_after, mode=t0_after, size=n_middle
            )
        ),
        unit="s",
    )


def generate_events_around_t1(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    t_start: int,
    t_end: int,
    n_events: int,
    t1: int,
    increase_time: int,
    increase_ratio: float,
):
    t1_before = t1 - increase_time / 2
    t1_after = t1 + increase_time / 2
    n_middle = int(
        (increase_time / 2)
        * (increase_ratio + 1)
        * n_events
        / ((t1 - t_start) * increase_ratio + (t_end - t1))
    )

    return pd.to_datetime(
        pd.Series(
<<<<<<< HEAD
            np.random.triangular(
=======
            generator.triangular(
>>>>>>> main
                left=t1_before, right=t1_after, mode=t1_before, size=n_middle
            )
        ),
        unit="s",
    )


def generate_events_after_t1(
<<<<<<< HEAD
=======
    generator: np.random.Generator,
>>>>>>> main
    t_start: int,
    t_end: int,
    n_events: int,
    t1: int,
    increase_time: int,
    increase_ratio: float,
):
    t1_after = t1 + increase_time / 2
    n_after = int(
        (t_end - t1_after) * n_events / ((t1 - t_start) * increase_ratio + (t_end - t1))
    )

    return pd.to_datetime(
<<<<<<< HEAD
        pd.Series(np.random.randint(t1_after, t_end, n_after)),
=======
        pd.Series(generator.integers(t1_after, t_end, n_after)),
>>>>>>> main
        unit="s",
    )


def recursive_items(dictionary):
    for key, value in dictionary.items():
        if type(value) is dict:
            if key[0] != "P":
                yield key
            yield from recursive_items(value)
        else:
            yield key
