from typing import Callable, List

import pandas as pd

from edsteva.metrics import error_between_t0_t1
from edsteva.models import BaseModel
from edsteva.models.rectangle_function import algos


class RectangleFunction(BaseModel):

    _coefs = ["t_0", "c_0", "t_1"]

    def fit_process(
        self,
        predictor: pd.DataFrame,
        index: List[str] = None,
        algo: Callable = algos.loss_minimization,
        **kwargs,
    ):
        return algo(predictor, index, **kwargs)

    def predict_process(
        self,
        predictor: pd.DataFrame,
        index: List[str],
    ):
        prediction = self._is_predictable_probe(predictor, index)

        rect_mask = (prediction["date"] >= prediction["t_0"]) & (
            prediction["date"] <= prediction["t_1"]
        )
        prediction["c_hat"] = prediction["c_0"].where(rect_mask, 0)

        return prediction.drop(columns=self._coefs + self._metrics)

    def _default_metrics(
        self,
        predictor: pd.DataFrame,
        estimates: pd.DataFrame,
        index: List[str],
    ):
        return error_between_t0_t1(
            predictor=predictor,
            estimates=estimates,
            index=index,
        )
