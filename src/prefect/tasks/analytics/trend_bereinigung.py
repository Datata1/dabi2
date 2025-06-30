import os
import numpy as np
import pandas as pd
from typing import Optional, Union, Tuple
from prefect import task, get_run_logger

from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator, TransformerMixin

class SinCosTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, period: int) -> None:
        self.period = period

    def fit(self, X: pd.DataFrame, y: Optional[Union[pd.Series, pd.DataFrame]] = None) -> "SinCosTransformer":
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        if isinstance(X, (pd.Series, np.ndarray)):
            # Convert Series or NumPy array to DataFrame, preserving column names if possible
            if isinstance(X, pd.Series):
                X_transformed = pd.DataFrame(X.copy())
            else: # np.ndarray
                # If it's a 1D array, treat it as a single column
                if X.ndim == 1:
                    X_transformed = pd.DataFrame(X, columns=['feature'])
                else:
                    X_transformed = pd.DataFrame(X)
        elif isinstance(X, pd.DataFrame):
            X_transformed = X.copy()
        else:
            raise TypeError("Input X must be a pandas DataFrame, Series, or NumPy array.")

        for col in X_transformed.columns:
            X_transformed[f"{col}_sin"] = np.sin((2 * np.pi * X_transformed[col]) / self.period)
            X_transformed[f"{col}_cos"] = np.cos((2 * np.pi * X_transformed[col]) / self.period)

        return X_transformed.filter(regex="_sin|_cos")


@task(name="Remove trends and seasons")
def remove_trends_and_seasons(df: pd.DataFrame, lags: int, min_date_global: pd.Timestamp = None) -> Tuple[pd.DataFrame, ColumnTransformer]:
    
    logger = get_run_logger()

    logger.info("Begin Feature Preprocessing", "header")

    if min_date_global is None:
        min_date_global = df["order_date"].min()

    logger.info("Generate temporal feature", "bullet", 1)

    df["days_since_start"] = (df["order_date"] - min_date_global).dt.days
    df["weekday"] = df["order_date"].dt.dayofweek

    df.sort_values(by=["user_id", "order_date"], inplace=True)

    df["is_target_nan"] = df.tip.isna()

    logger.info("Preparing Data for Shifting", "bullet", 1)
    if df.tip.isna().any():
        df["tip_for_shifting"] = df.groupby("user_id").tip.ffill()
        df["tip_for_shifting"] = df.groupby("user_id").tip_for_shifting.bfill()
    else:
        df["tip_for_shifting"] = df["tip"]

    shifted_tip_features = []
    shifted_nan_indicators = []

    logger.info("Generating shifted Features", "bullet", 1)
    for lag in range(1, lags + 1):
        lag_feature_name = f"tip_t-{lag}"
        nan_indicator_name = f"tip_t-{lag}_is_nan"

        df[lag_feature_name] = df.groupby("user_id").tip_for_shifting.shift(lag)
        df[nan_indicator_name] = df[lag_feature_name].isna().astype(int)

        df[lag_feature_name] = df[lag_feature_name].fillna(-1)

        shifted_tip_features.append(lag_feature_name)
        shifted_nan_indicators.append(nan_indicator_name)

    df.drop(columns=["user_id"], inplace=True)

    numerical_features_passthrough = shifted_nan_indicators + shifted_tip_features + ["days_since_start"]

    logger.info("Initializing preprocessor", "bullet", 1, "start")
    preprocessor = ColumnTransformer(
        transformers=[
            ("weekly", SinCosTransformer(period=7), ["weekday"]),
            ("nan_indicators", "passthrough", numerical_features_passthrough)
        ],
        remainder="drop"
    )

    logger.info("Feature Preprocessing finished successfully!", "bullet", 1)

    return df