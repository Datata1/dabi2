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
from tasks.analytics.feature_engineering import feature_engineering



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

@task(name="make final predictions") # Task-Namen angepasst, um Konflikte zu vermeiden
def make_final_predictions(
    data_frame: pd.DataFrame,
    trained_model: LogisticRegressionCV,
    trained_preprocessor: ColumnTransformer,
    lags: int,
    min_date_from_training: pd.Timestamp
) -> pd.DataFrame:
    logger = get_run_logger()

    # Erstelle eine Kopie des data_frame, um unerwünschte Nebeneffekte zu vermeiden,
    # falls feature_engineering den Index des Originals beeinflusst.
    data_frame_features = feature_engineering(
        data_frame.copy(),
        lags,
        min_date_global=min_date_from_training
    )

    # Filtern nach Zeilen, bei denen das Ziel (tip) NaN ist
    data_frame_features = data_frame_features[data_frame_features.is_target_nan == True]

    # Die 'tip'-Spalte entfernen, wenn sie existiert und nicht für die Feature-Auswahl benötigt wird
    if "tip" in data_frame_features.columns:
        data_frame_features.drop(columns=["tip"], inplace=True)

    # Definition der Features für die Vorhersage
    categorical_features = ["order_contains_organic", "is_holiday", "cluster"]
    numerical_features_to_scale = ["avg_no_prod", "overall_tip_proba",
                                   "tip_proba_per_hour", "tip_proba_per_weekday",
                                   "tip_proba_per_department"] + ["days_since_start"]
    numerical_features_to_minmax = ["no_orders", "cart_size"]
    numerical_features_passthrough = [f"tip_t-{lag}_is_nan" for lag in range(1, lags + 1)]
    shifted_features = [f"tip_t-{lag}" for lag in range(1, lags + 1)]
    temporal_features = ["weekday"]

    # Auswahl der Features und Entfernen von Zeilen mit NaN-Werten in diesen Features.
    features_for_pred = data_frame_features[
        categorical_features +
        numerical_features_to_scale +
        numerical_features_to_minmax +
        numerical_features_passthrough +
        temporal_features +
        shifted_features
    ].dropna()

    # Transformation der Features mit dem trainierten Preprocessor
    X_transformed = trained_preprocessor.transform(features_for_pred)

    # Vorhersagen treffen
    predictions = trained_model.predict(X_transformed)

    # Erstelle eine Arbeitskopie des ursprünglichen data_frame für die Zuweisung der Vorhersagen
    df_result = data_frame.copy()
    df_result["prediction"] = np.nan # Initialisiere die 'prediction'-Spalte mit NaN

    # Erstelle eine Pandas Series der Vorhersagen mit dem Index von features_for_pred.
    predictions_series = pd.Series(predictions, index=features_for_pred.index)

    # Überprüfe, ob alle Indizes aus predictions_series im df_result vorhanden sind.
    missing_indices_in_df_result = predictions_series.index.difference(df_result.index)
    if not missing_indices_in_df_result.empty:
        logger.warning(
            f"Warnung: Einige Indizes aus den Vorhersagen ({missing_indices_in_df_result.tolist()}) "
            f"wurden nicht im ursprünglichen data_frame gefunden. Sie werden bei der Zuweisung übersprungen."
        )
        predictions_series = predictions_series[predictions_series.index.isin(df_result.index)]

    # Weise die Vorhersagen der 'prediction'-Spalte im df_result zu.
    df_result.loc[predictions_series.index, "prediction"] = predictions_series

    # Filtere die finalen Ergebnisse: nur Zeilen, bei denen 'tip' ursprünglich NaN war
    df_final = df_result[df_result.tip.isna()][["order_id", "prediction"]].reset_index(drop=True)

    # Überprüfung auf NaN-Werte in den finalen Vorhersagen
    if df_final.prediction.isna().values.any():
        logger.error("Vorhersagen enthalten NaN-Werte nach der Zuweisung! Dies deutet auf fehlende Vorhersagen für einige Zielzeilen hin.")
        raise ValueError("Vorhersagen enthalten NaN-Werte! Überprüfe die Vorverarbeitung oder die Vollständigkeit der Eingabedaten.")

    # Konvertiere Vorhersagen in den booleschen Typ
    df_final["prediction"] = df_final.prediction.astype(bool)
    df_final.rename(columns={"prediction": "tip"}, inplace=True)

    gc.collect() # Speicherbereinigung
    return df_final