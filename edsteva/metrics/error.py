from typing import Callable, List

import pandas as pd

from edsteva.utils import loss_functions
from edsteva.utils.checks import check_columns


def error(
    predictor: pd.DataFrame,
    estimates: pd.DataFrame,
    index: List[str],
    loss_function: Callable = loss_functions.l2_loss,
    y: str = "c",
    y_0: str = "c_0",
    x: str = "date",
    name: str = "error",
):
    r"""Compute the error between the predictor $c(t)$ and the prediction $\hat{c}(t)$ as follow:

    $$
    error = \frac{\sum_{t_{min} \leq  t \leq t_{max}} \mathcal{l}(c(t), \hat{c}(t))}{t_{max} - t_{min}}
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
    loss_function : str, optional
        The loss function $\mathcal{l}$
    y : str, optional
        Target column name of $c(t)$
    y_0 : str, optional
        Target column name of $\hat{c}(t)$
    x : str, optional
        Target column name of $t$
    name : str, optional
        Column name of the output

    Example
    -------

    | care_site_level          | care_site_id | stay_type    | error |
    | :----------------------- | :----------- | :----------- | :---- |
    | Unité Fonctionnelle (UF) | 8312056386   | 'Urg_Hospit' | 0.040 |
    | Unité Fonctionnelle (UF) | 8312056386   | 'All'        | 0.028 |
    | Pôle/DMU                 | 8312027648   | 'Urg_Hospit' | 0.022 |
    | Pôle/DMU                 | 8312027648   | 'All'        | 0.014 |
    | Hôpital                  | 8312022130   | 'Urg_Hospit' | 0.027 |
    """
    check_columns(df=estimates, required_columns=[*index, y_0])
    check_columns(df=predictor, required_columns=[*index, x, y])

    fitted_predictor = predictor.merge(estimates, on=index)

    fitted_predictor["loss"] = loss_function(
        fitted_predictor[y] - fitted_predictor[y_0]
    )

    error = fitted_predictor.groupby(index)["loss"].mean().rename(name)

    return error.reset_index()
