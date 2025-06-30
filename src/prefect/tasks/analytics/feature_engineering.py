import os
import pandas as pd
import numpy as np
import gc
import holidays


from typing import Tuple, Optional, Union

from sklearn.preprocessing import OneHotEncoder, StandardScaler, FunctionTransformer, MinMaxScaler
from sklearn.linear_model import LogisticRegressionCV, LinearRegression
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.metrics import f1_score, accuracy_score, r2_score
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.cluster import KMeans

from prefect import task, get_run_logger



class TipProbaPerDepartmentTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, log_print):
        self.log_print = log_print

    def fit(self, X, y=None):
        self.dept_df_ = pd.read_csv(f"/app/data/order_products_denormalized.csv")
        return self

    def transform(self, X):
        #_log_print(self.log_print, "Begin tip proba by department", "header")
        X = X.copy()
        X = X.merge(self.dept_df_)

        del self.dept_df_
        gc.collect()

        X = X.merge(X.groupby("department", as_index=False).agg(tip_proba_per_department=("tip", "mean")), how="left", on="department")

        X["tip_proba_per_department"] = X["tip_proba_per_department"] * 100

        X.sort_values("tip_proba_per_department", ascending=False, inplace=True)

        return X

class TipProbaPerWeekdayTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, log_print: bool = True):
        self.log_print = log_print

    def fit(self, X: pd.DataFrame, y: pd.Series = None):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        #_log_print(self.log_print, "Begin tip proba by weekday", "header")
        X = X.copy()
        X = X.merge(X.groupby("weekday", as_index=False).agg(tip_proba_per_weekday=("tip", "mean")), how="left", on="weekday")

        X["tip_proba_per_weekday"] = X["tip_proba_per_weekday"] * 100

        X.sort_values("tip_proba_per_weekday", ascending=False, inplace=True)

        return X
    
class TipProbaPerHourTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, log_print: bool = True):
        self.log_print = log_print

    def fit(self, X, y=None):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        #_log_print(self.log_print, "Begin tip proba per hour", "header")
        X = X.copy()
        X = X.merge(X.groupby("hour", as_index=False).agg(tip_proba_per_hour=("tip", "mean")), how="left", on="hour")

        X["tip_proba_per_hour"] = X["tip_proba_per_hour"] * 100

        X.sort_values("tip_proba_per_hour", ascending=False, inplace=True)

        return X
    
class OverallTipProbaTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, log_print: bool = True):
        self.log_print = log_print

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        #_log_print(self.log_print, "Begin overall tip", "header")
        X = X.copy()
        X["overall_tip_proba"] = X["tip"].mean() * 100

        return X
    

class LagFeatureGenerator(BaseEstimator, TransformerMixin):
    def __init__(self, lags: int, log_print: bool = True) -> None:
        self.lags = lags
        self.log_print = log_print

    def fit(self, X: pd.DataFrame, y: pd.Series = None):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        #_log_print(self.log_print, "Begin generating lag features", "header")
        X = X.copy()

        X.sort_values(["user_id", "order_date"], inplace=True)
        X["is_target_nan"] = X["tip"].isna()

        if X["tip"].isna().any():
            X["tip_for_shifting"] = X.groupby("user_id")["tip"].ffill()
            X["tip_for_shifting"] = X.groupby("user_id")["tip_for_shifting"].bfill()
        else:
            X["tip_for_shifting"] = X["tip"]

        for lag in range(1, self.lags + 1):
            lag_col = f"tip_t-{lag}"
            nan_col = f"tip_t-{lag}_is_nan"
            X[lag_col] = X.groupby("user_id")["tip_for_shifting"].shift(lag)
            X[nan_col] = X[lag_col].isna().astype(int)

        return X
    
class TemporalFeatureGenerator(BaseEstimator, TransformerMixin):
    def __init__(self, min_date_global: pd.Timestamp, log_print: bool = True) -> None:
        self.min_date_global = min_date_global
        self.log_print = log_print

    def fit(self, X: pd.DataFrame, y: pd.Series = None):
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        #_log_print(self.log_print, "Begin generating temporal features", "header")
        X = X.copy()
        X["weekday"] = X["order_date"].dt.day_of_week
        X["hour"] = X["order_date"].dt.hour

        us_holidays = holidays.UnitedStates(years=X["order_date"].dt.year.unique())
        X["is_holiday"] = X["order_date"].dt.date.isin(us_holidays.keys())

        return X
    
class MeanNoProductsOrderedTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, log_print: bool = True) -> None:
        self.log_print = log_print

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        #_log_print(self.log_print, "Making AVG cart size", "header")
        X = X.copy()

        X = X.merge(X.groupby("user_id", as_index=False).agg(avg_no_prod=("cart_size", "mean")), on="user_id")

        return X
    
class CartSizeTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, log_print: bool = True) -> None:
        self.log_print = log_print

    def fit(self, X, y=None):
        self.cart_size_df_ = pd.read_csv(f"/app/data/order_products_denormalized.csv")
        return self

    def transform(self, X):
        #_log_print(self.log_print, "Getting cart size", "header")
        X = X.copy()

        self.cart_size_df_.sort_values(["order_id", "add_to_cart_order"], inplace=True)
        X = X.merge(
            self.cart_size_df_.groupby("order_id", as_index=False).agg(cart_size=("add_to_cart_order", "max")),
            on="order_id"
        )

        del self.cart_size_df_
        gc.collect()

        return X
    
class OrderCountTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, log_print: bool = True) -> None:
        self.log_print = log_print

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        #_log_print(self.log_print, "Counting orders per user", "header")
        X = X.copy()
        X = X.merge(X.groupby("user_id").agg(no_orders=("order_id", "nunique")), how="left", on="user_id")

        return X
    

class OrganicProductsFlagTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, log_print: bool = True):
        self.log_print = log_print

    def fit(self, X: pd.DataFrame, y: pd.Series = None):

        df = pd.read_csv(f"/app/data/order_products_denormalized.csv")
        df["order_contains_organic"] = df["product_name"].str.lower().str.contains("organic")
        self.organic_df_ = df
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        #_log_print(self.log_print, "Begin organic-products-flagging", "header")
        X = X.copy()
        X = X.merge(self.organic_df_, how="left", on="order_id")

        del self.organic_df_
        gc.collect()

        X["order_contains_organic"] = X["order_contains_organic"].fillna(False)

        return X
    
class ClusterAssignmentTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, log_print: bool = True) -> None:
        self.log_print = log_print

    def fit(self, X: pd.DataFrame, y: pd.Series = None):
        user_dept_df = X.pivot_table(index="user_id", columns="tip", aggfunc="size", fill_value=0)

        scaler = StandardScaler()
        scaled = scaler.fit_transform(user_dept_df)

        kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
        self.cluster_mapping_ = pd.DataFrame({
            "user_id": user_dept_df.index,
            "cluster": kmeans.fit_predict(scaled)
        })

        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        #_log_print(self.log_print, "Begin cluster generation", "header")
        X = X.copy()
        X = X.merge(self.cluster_mapping_, how="left", on="user_id")
        X["cluster"] = X["cluster"].fillna(-1).astype(int).astype(str)

        return X
    
class DateConversionTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, min_date_global: pd.Timestamp = None, log_print: bool = True) -> None:
        self.min_date_global = min_date_global
        self.log_print = log_print

    def fit(self, X: pd.DataFrame, y: pd.Series = None):
        if self.min_date_global is None:
            self.min_date_global = pd.to_datetime(X["order_date"]).min()

        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        #_log_print(self.log_print, "Begin date conversion", "header")
        X = X.copy()
        if not pd.api.types.is_datetime64_any_dtype(X["order_date"]):
            X["order_date"] = pd.to_datetime(X["order_date"])
        X["days_since_start"] = (X["order_date"] - self.min_date_global).dt.days

        return X
    
class SinCosTransformer(BaseEstimator, TransformerMixin):
    """
    A scikit-learn compatible transformer that converts numerical features
    into their sine and cosine components. This is particularly useful for
    capturing cyclical patterns (e.g., time of day, day of week) in data
    where the absolute value is less important than its position within a cycle.

    For each input feature, it generates two new features:
    - `original_feature_name_sin`: The sine transformation of the feature.
    - `original_feature_name_cos`: The cosine transformation of the feature.

    The transformation is calculated as:
    sin_component = sin((2 * pi * value) / period)
    cos_component = cos((2 * pi * value) / period)

    Attributes:
        period (int): The length of the cycle for the data. For example, 24 for
                      hours in a day, 7 for days in a week.
    """
    def __init__(self, period: int) -> None:
        """
        Initializes the SinCosTransformer.

        Args:
            period (int): The cycle length used for the sine and cosine transformations.
        """
        self.period = period

    def fit(self, X: pd.DataFrame, y: Optional[Union[pd.Series, pd.DataFrame]] = None) -> "SinCosTransformer":
        """
        This method is a placeholder and does not perform any fitting.
        It simply returns the transformer instance itself, as the transformation
        is stateless and only depends on the 'period' attribute.

        Args:
            X (pd.DataFrame): The input data. Not used in fit, but required for
                              scikit-learn API compatibility.
            y (Optional[Union[pd.Series, pd.DataFrame]]): Target variable.
                                                            Not used.

        Returns:
            SinCosTransformer: The instance of the transformer.
        """
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        """
        Transforms the input features by calculating their sine and cosine components.
        For each numerical column in the input DataFrame, two new columns are created:
        one for the sine transformation and one for the cosine transformation.

        Args:
            X (pd.DataFrame | pd.Series | np.ndarray): The input data containing
                                                         numerical features to be transformed.
                                                         If a Series or NumPy array, it will
                                                         be converted to a DataFrame.

        Returns:
            pd.DataFrame: A new DataFrame containing only the newly generated
                          sine (`_sin`) and cosine (`_cos`) features.
                          Original features are dropped.

        Raises:
            TypeError: If the input `X` is not a pandas DataFrame, Series, or NumPy array.
        """
        # Ensure X is a DataFrame for consistent processing
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

        # Iterate over each column in the (potentially copied) DataFrame
        for col in X_transformed.columns:
            # Calculate the sine component for the current column
            # The formula (2 * pi * value) / period scales the values to
            # a range suitable for sine/cosine functions (0 to 2*pi)
            X_transformed[f"{col}_sin"] = np.sin((2 * np.pi * X_transformed[col]) / self.period)
            # Calculate the cosine component for the current column
            X_transformed[f"{col}_cos"] = np.cos((2 * np.pi * X_transformed[col]) / self.period)

        # Return only the newly created sine and cosine columns.
        # `filter(regex="_sin|_cos")` efficiently selects columns ending with these suffixes.
        return X_transformed.filter(regex="_sin|_cos")


def build_feature_pipeline(lags: int, min_date_global: pd.Timestamp = None, log_print: bool = True):
    categorical_features = ["order_contains_organic", "is_holiday", "cluster"]
    numerical_features_to_scale = ["avg_no_prod", "overall_tip_proba",
                                   "tip_proba_per_hour", "tip_proba_per_weekday",
                                   "tip_proba_per_department", "days_since_start"]
    numerical_features_to_minmax = ["no_orders", "cart_size"]
    shifted_features = [f"tip_t-{lag}" for lag in range(1, lags + 1)]
    nan_indicators = [f"tip_t-{lag}_is_nan" for lag in range(1, lags + 1)]

    preprocessor = ColumnTransformer([
        ("imputation", SimpleImputer(strategy="most_frequent"), shifted_features),
        ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
        ("num", StandardScaler(), numerical_features_to_scale),
        ("no", MinMaxScaler(), numerical_features_to_minmax),
        ("weekly", SinCosTransformer(period=7), ["weekday"]),
        ("nan_indicators", "passthrough", nan_indicators),
    ], remainder="drop")

    pipeline = Pipeline([
        ("date_conversion", DateConversionTransformer(min_date_global=min_date_global, log_print=log_print)),
        ("cluster_generation", ClusterAssignmentTransformer(log_print=log_print)),
        ("lag_features", LagFeatureGenerator(lags=lags, log_print=log_print)),
        ("organic_flag", OrganicProductsFlagTransformer(log_print=log_print)),
        ("count_orders", OrderCountTransformer(log_print=log_print)),
        ("cart_size", CartSizeTransformer(log_print=log_print)),
        ("avg_no_prod", MeanNoProductsOrderedTransformer(log_print=log_print)),
        ("temporal_features", TemporalFeatureGenerator(min_date_global=min_date_global, log_print=log_print)),
        ("overall_tip_proba", OverallTipProbaTransformer(log_print=log_print)),
        ("tip_proba_per_hour", TipProbaPerHourTransformer(log_print=log_print)),
        ("tip_proba_per_weekday", TipProbaPerWeekdayTransformer(log_print=log_print)),
        ("tip_proba_per_department", TipProbaPerDepartmentTransformer(log_print=log_print)),
        ("column_preprocessor", preprocessor)
    ])

    return pipeline


@task(name="Feature Enigineering")
def feature_engineering(df: pd.DataFrame, lags: int, min_date_global=None) -> pd.DataFrame:
    pipeline = build_feature_pipeline(lags, min_date_global)

    steps_except_last = pipeline.steps[:-1]

    for name, transformer in steps_except_last:
        df = transformer.fit_transform(df)

    return df