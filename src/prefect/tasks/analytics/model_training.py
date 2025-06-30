import os
import pandas as pd
import numpy as np
import gc
from typing import Tuple, Optional, Union
from prefect import task, get_run_logger

from sklearn.linear_model import LogisticRegressionCV, LinearRegression
from sklearn.compose import ColumnTransformer
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.metrics import f1_score, accuracy_score, r2_score


from sklearn.model_selection import train_test_split, TimeSeriesSplit


class SinCosTransformer(BaseEstimator, TransformerMixin):

    def __init__(self, period: int) -> None:
        self.period = period

    def fit(self, X: pd.DataFrame, y: Optional[Union[pd.Series, pd.DataFrame]] = None):
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

@task(name="Train prediction model")
def train_prediction_model(df_input: pd.DataFrame, lags: int) -> Tuple[LogisticRegressionCV, ColumnTransformer, pd.Timestamp, float]:
    logger = get_run_logger()

    df_cleaned_for_training = df_input[~df_input["is_target_nan"]].copy()

    min_date_for_pred = df_input.order_date.min()

    del df_input
    gc.collect()

    df_cleaned_for_training.set_index("order_date", inplace=True)

    y = df_cleaned_for_training.pop("tip")

    numerical_features_passthrough = [f"tip_t-{lag}_is_nan" for lag in range(1, lags + 1)] + [f"tip_t-{lag}" for lag in range(1, lags + 1)] + ["days_since_start"]
    temporal_features = ["weekday"]

    X_processed = df_cleaned_for_training[numerical_features_passthrough + temporal_features]

    del df_cleaned_for_training
    gc.collect()

    fold_acc_scores = []
    tscv = TimeSeriesSplit(n_splits=3)
    for i, (train_idx, test_idx) in enumerate(tscv.split(X_processed)):
        logger.info(f"Current fold: {i + 1}")
        X_train, X_test = X_processed.iloc[train_idx], X_processed.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

        del train_idx, test_idx
        gc.collect()

        #logger.info("Preprocessing", "bullet", 1)
        current_preprocessor = ColumnTransformer(
            transformers=[
                ("weekly", SinCosTransformer(period=7), ["weekday"]),
                ("nan_indicators", "passthrough", numerical_features_passthrough)
            ], remainder="drop"
        )
        current_preprocessor.fit(X_train)

        X_train_transformed = current_preprocessor.transform(X_train)
        X_test_transformed = current_preprocessor.transform(X_test)

        del current_preprocessor, X_train, X_test
        gc.collect()

        # logger.info("Training", "bullet", 1)
        current_model = LogisticRegressionCV(cv=3, n_jobs=-1, max_iter=1000)
        current_model.fit(X_train_transformed, y_train)

        del X_train_transformed, y_train
        gc.collect()

        #logger.info("Testing", "bullet", 1)
        y_pred = current_model.predict(X_test_transformed)
        accuracy = accuracy_score(y_test, y_pred)

        del X_test_transformed, y_pred, current_model, y_test
        gc.collect()

        fold_acc_scores.append(accuracy)

    del tscv
    gc.collect()

    acc_mean = np.mean(fold_acc_scores)

    logger.info("Finally")
    #logger.info("Preprocessing", "bullet", 1)
    final_preprocessor_for_pred = ColumnTransformer(
        transformers=[
            ("weekly", SinCosTransformer(period=7), ["weekday"]),
            ("nan_indicators", "passthrough", numerical_features_passthrough)
        ], remainder="drop"
    )
    final_preprocessor_for_pred.fit(X_processed)

    X_processed_transformed = final_preprocessor_for_pred.transform(X_processed)

    del X_processed
    gc.collect()

    # logger.info("Training", "bullet", 1)
    final_model_for_pred = LogisticRegressionCV(cv=5, n_jobs=-1, max_iter=1000)
    final_model_for_pred.fit(X_processed_transformed, y)
    #logger.info("Model training finished successfully", "bullet", 1)

    del X_processed_transformed
    gc.collect()

    return final_model_for_pred, final_preprocessor_for_pred, min_date_for_pred, acc_mean