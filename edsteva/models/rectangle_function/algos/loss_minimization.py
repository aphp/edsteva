from typing import Callable, List

import numpy as np
import pandas as pd
from joblib import Parallel, delayed

from edsteva.utils.checks import check_columns
from edsteva.utils.loss_functions import l2_loss


# TODO: rename "index" to be more specific, e.g. "partition_cols"
def loss_minimization(
    predictor: pd.DataFrame,
    index: List[str],
    x_col: str = "date",
    y_col: str = "c",
    loss_function: Callable = l2_loss,
    min_rect_month_width=3,
    n_jobs=-1,
):
    check_columns(df=predictor, required_columns=index + [x_col, y_col])

    cols = index + [x_col, y_col]
    iter = predictor[cols].groupby(index)
    with Parallel(n_jobs=n_jobs) as parallel:
        best_t0_c0_t1 = parallel(
            delayed(_compute_one_double_threshold)(
                group,
                x_col,
                y_col,
                loss_function,
                min_rect_month_width,
            )
            for _, group in iter
        )
    results = pd.DataFrame([dict(zip(index, partition)) for partition, _ in iter])
    results[["t_0", "c_0", "t_1"]] = best_t0_c0_t1

    return results


def _compute_one_double_threshold(
    group: pd.DataFrame,
    x_col: str,
    y_col: str,
    loss_func: Callable,
    min_rect_month_width: int,
):
    target = group[[x_col, y_col]].values
    best_x0 = best_y0 = best_x1 = None
    best_loss = np.inf
    for idx in range(1, len(target) - min_rect_month_width):
        x0 = target[idx, 0]
        y_before_x0 = target[:idx, 1]
        for jdx in range(idx + min_rect_month_width, len(target)):
            x1 = target[jdx, 0]
            y_between_x0_x1 = target[idx:jdx, 1]
            y_after_x1 = target[jdx:, 1]
            y0 = y_between_x0_x1.mean()
            residual = np.hstack(
                [
                    y_before_x0,
                    (y_between_x0_x1 - y0),
                    y_after_x1,
                ]
            )
            loss = loss_func(residual).mean()
            if loss < best_loss:
                best_x0 = x0
                best_y0 = y0
                best_x1 = x1
                best_loss = loss

    return best_x0, best_y0, best_x1
