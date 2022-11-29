import numpy as np
import pandas as pd


def l2_loss(residual: pd.DataFrame):
    return np.square(residual)


def l1_loss(residual: pd.DataFrame):
    return np.abs(residual)
