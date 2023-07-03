from typing import Callable, List

import pandas as pd

from edsteva.utils import loss_functions
from edsteva.utils.checks import check_columns


def error_after_t0(
    predictor: pd.DataFrame,
    estimates: pd.DataFrame,
    index: List[str],
    loss_function: Callable = loss_functions.l2_loss,
    y: str = "c",
    y_0: str = "c_0",
    threshold: str = "t_0",
    x: str = "date",
    name: str = "error",
):
    r"""Compute the error between the predictor $c(t)$ and the prediction $\hat{c}(t)$ after $t_0$ as follow:

    $$
    error = \frac{\sum_{t_0 \leq  t \leq t_{max}} \mathcal{l}(c(t), \hat{c}(t))}{t_{max} - t_0}
    $$

    Where the loss function $\mathcal{l}$ can be the L1 distance or the L2 distance.

    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe
    estimates : pd.DataFrame
        $\hat{c}(t)$ computed in the Model
    index : List[str]
        Variable from which data is grouped
    loss_function : Callable, optional
        The loss function $\mathcal{l}$
    y : str, optional
        Column name for the completeness variable $c(t)$
    y_0 : str, optional
        Column name for the predicted completeness variable $\hat{c}(t)$
    threshold : str, optional
        Column name for the predicted threshold $t_0$
    x : str, optional
        Column name for the time variable $t$
    name : str, optional
        Column name for the metric output

    Example
    -------

    | care_site_level          | care_site_id | stay_type | error |
    | :----------------------- | :----------- | :---------| :---- |
    | Unité Fonctionnelle (UF) | 8312056386   | 'Urg'     | 0.040 |
    | Unité Fonctionnelle (UF) | 8312056386   | 'All'     | 0.028 |
    | Pôle/DMU                 | 8312027648   | 'Urg'     | 0.022 |
    | Pôle/DMU                 | 8312027648   | 'All'     | 0.014 |
    | Hôpital                  | 8312022130   | 'Urg'     | 0.027 |
    """
    check_columns(df=estimates, required_columns=[*index, y_0, threshold])
    check_columns(df=predictor, required_columns=[*index, x, y])

    fitted_predictor = predictor.merge(estimates, on=index)

    fitted_predictor = fitted_predictor.dropna(subset=[threshold])

    fitted_predictor["loss"] = loss_function(
        fitted_predictor[y] - fitted_predictor[y_0]
    )

    mask_after_t0 = fitted_predictor[x] >= fitted_predictor[threshold]
    fitted_predictor = fitted_predictor.loc[mask_after_t0]

    error = fitted_predictor.groupby(index)["loss"].mean().rename(name)

    return error.reset_index()
