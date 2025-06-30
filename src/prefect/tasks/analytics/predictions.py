import os
import pandas as pd
import numpy as np
import gc
from typing import Tuple
from prefect import task, get_run_logger

from sklearn.linear_model import LogisticRegressionCV, LinearRegression
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.metrics import f1_score, accuracy_score, r2_score

from tasks.analytics.trend_bereinigung import remove_trends_and_seasons


@task(name="make predictions")
def make_predictions(
    data_frame: pd.DataFrame,
    trained_model: LogisticRegressionCV,
    trained_preprocessor: ColumnTransformer,
    lags: int,
    min_date_from_training: pd.Timestamp
) -> pd.DataFrame:
    
    data_frame_features = remove_trends_and_seasons(data_frame, lags=4,  min_date_global=min_date_from_training)
    
    categorical_features = ["user_id"]
    numerical_features_to_scale = [f"tip_t-{lag}" for lag in range(1, lags + 1)] + ["days_since_start"]
    numerical_features_passthrough = [f"tip_t-{lag}_is_nan" for lag in range(1, lags + 1)]
    temporal_features = ["weekday"]

    data_frame_features = data_frame_features[data_frame_features.is_target_nan == True]

    data_frame_features.drop(columns=["tip"], inplace=True)

    features_for_pred = data_frame_features[
        numerical_features_to_scale +
        numerical_features_passthrough +
        temporal_features
    ].dropna()

    X_transformed = trained_preprocessor.transform(features_for_pred)

    predictions = trained_model.predict(X_transformed)

    data_frame["prediction"] = np.nan

    data_frame.loc[features_for_pred.index, "prediction"] = predictions

    df_final = data_frame[data_frame.tip.isna()][["order_id", "prediction"]].reset_index(drop=True)

    if df_final.prediction.isna().values.any():
        raise ValueError("Predictions contain NaN values! Check preprocessing or input data completeness.")

    df_final["prediction"] = df_final.prediction.astype(bool)

    df_final.rename(columns={"prediction": "tip"}, inplace=True)

    return df_final