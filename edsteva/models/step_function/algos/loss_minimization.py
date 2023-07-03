from typing import Callable, List

import numpy as np
import pandas as pd

from edsteva.utils.checks import check_columns
from edsteva.utils.loss_functions import l2_loss


def loss_minimization(
    predictor: pd.DataFrame,
    index: List[str],
    x_col: str = "date",
    y_col: str = "c",
    loss_function: Callable = l2_loss,
) -> pd.DataFrame:
    r"""Computes the threshold $t_0$ of a predictor $c(t)$ by minimizing the following loss function:

    $$
    \begin{aligned}
    \mathcal{L}(t_0) & = \frac{\sum_{t = t_{min}}^{t_{max}} \mathcal{l}(c(t), f_{t_0}(t))}{t_{max} - t_{min}} \\
    \hat{t_0} & = \underset{t_0}{\mathrm{argmin}}(\mathcal{L}(t_0))
    \end{aligned}
    $$

    Where the loss function $\mathcal{l}$ is by default the L2 distance and the estimated completeness $c_0$ is the mean completeness after $t_0$.

    $$
    \begin{aligned}
    \mathcal{l}(c(t), f_{t_0}(t)) & = |c(t) - f_{t_0}(t)|^2 \\
    c_0 & = \frac{\sum_{t = t_0}^{t_{max}} c(t)}{t_{max} - t_0}
    \end{aligned}
    $$


    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe
    index : List[str]
        Variable from which data is grouped

        **EXAMPLE**: `["care_site_level", "stay_type", "note_type", "care_site_id"]`
    x_col : str, optional
        Column name for the time variable $t$
    y_col : str, optional
        Column name  for the completeness variable $c(t)$
    loss_function : Callable, optional
        The loss function $\mathcal{L}$
    """
    check_columns(df=predictor, required_columns=[*index, x_col, y_col])
    predictor = predictor.sort_values(x_col)
    cols = [*index, x_col, y_col]
    iter = predictor[cols].groupby(index)
    results = []
    for partition, group in iter:
        row = dict(zip(index, partition))
        t_0, c_0 = _compute_one_threshold(
            group,
            x_col,
            y_col,
            loss_function,
        )
        row["t_0"] = t_0
        row["c_0"] = c_0
        results.append(row)

    return pd.DataFrame(results)


def _compute_one_threshold(
    group: pd.DataFrame,
    x_col: str,
    y_col: str,
    loss_func: Callable,
):
    target = group[[x_col, y_col]].to_numpy()
    best_loss, best_x0, best_y0 = np.inf, None, None
    for idx in range(len(target)):
        x0 = target[idx, 0]
        y_before_x0 = target[:idx, 1]
        y_after_x0 = target[idx:, 1]
        y0 = y_after_x0.mean()
        residual = np.hstack([y_before_x0, (y_after_x0 - y0)])
        loss = loss_func(residual).mean()
        if loss < best_loss:
            best_x0 = x0
            best_y0 = y0
            best_loss = loss

    return best_x0, best_y0
