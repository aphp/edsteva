from typing import List

import numpy as np
import pandas as pd

from edsteva.utils.checks import check_columns


def quantile(
    predictor: pd.DataFrame,
    index: List[str],
    q: float = 0.8,
    x: str = "date",
    y: str = "c",
    threshold: str = "c_0",
):
    estimates = c_0_from_quantile(predictor=predictor, index=index, q=q, x=x, y=y)
    return t_0_from_c_0(predictor=estimates, index=index, x=x, y=y, threshold=threshold)


def c_0_from_quantile(
    predictor: pd.DataFrame,
    index: List[str],
    q: float = 0.8,
    x: str = "date",
    y: str = "c",
) -> pd.DataFrame:
    r"""Compute the quantile on the given y-axis. Column $c_0$ is created.

    $$
    \hat{c_0} = x^{th} \text{ quantile of } c(t)
    $$

    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe
    index : List[str]
        Variable from which data is grouped

        **EXAMPLE**: `["care_site_level", "stay_type", "note_type", "care_site_id"]`
    q : float, optional
        Quantile value
    x : str, optional
        Column name for the time variable $t$
    y : str, optional
        Column name  for the completeness variable $c(t)$
    """

    check_columns(df=predictor, required_columns=[*index, x, y])

    quantile = (
        predictor.groupby(index)[[y]]
        .agg(lambda g: np.quantile(g, q=q))
        .rename(columns={y: "c_0"})
    )

    return predictor.merge(quantile, on=index)


def t_0_from_c_0(
    predictor: pd.DataFrame,
    index: List[str],
    x: str = "date",
    y: str = "c",
    threshold: str = "c_0",
) -> pd.DataFrame:
    r"""Compute $t_0$ column using value of $c_0$

    Returns the first date at which values are greater than $c_0$:

    $$
    \hat{t_0} = \underset{t}{\mathrm{argmin}}(c(t) \geq \hat{c_0})
    $$

    Parameters
    ----------
    predictor : pd.DataFrame
        $c(t)$ computed in the Probe
    index : List[str]
        Variable from which data is grouped
    x : str, optional
        Column name for the time variable $t$
    y : str, optional
        Column name  for the completeness variable $c(t)$
    threshold : str, optional
        Column name  for the threshold variable $t_0$
    """

    check_columns(df=predictor, required_columns=[*index, x, y, threshold])

    threshold = (
        predictor[predictor[y] > predictor[threshold]]
        .groupby(index)[[x]]
        .min()
        .rename(columns={x: "t_0"})
    )

    return predictor.merge(threshold, on=index)
