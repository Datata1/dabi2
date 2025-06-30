import os
from prefect import flow, get_run_logger
import clickhouse_connect

# task imports
from tasks.analytics.load_data_clickhouse import process_data_from_csv
from tasks.analytics.feature_engineering import feature_engineering
from tasks.analytics.predictions import make_predictions
from tasks.analytics.model_training import train_prediction_model
from tasks.analytics.model_evaluation import print_model_evaluation
from tasks.analytics.predictions import make_predictions

# utils imports

# clickhouse config
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "devpassword")


@flow(name="Analytics Flow")
def analytics2():

    logger = get_run_logger()
    logger.info("Starting analytics flow...")

    # 1.
    # load data (either csv or clickhouse is fine)
    tips, orders_tips, tip_temp_test = process_data_from_csv()

    logger.info(orders_tips.head())

    # 2.
    # trend bereinigung und saison bereinigung
    df = feature_engineering(df=orders_tips, lags=4, min_date_global=orders_tips.order_date.min())

    # 4.
    # modell training
    # -> save model?
    final_model_for_pred, final_preprocessor_for_pred, min_date_for_pred, acc_mean = train_prediction_model(df, 4)


    # 6. prediction 
    # make predictions on csv data set and save predictions to csv
    predictions = make_predictions(
        data_frame=tip_temp_test,
        trained_model=final_model_for_pred,
        trained_preprocessor=final_preprocessor_for_pred,
        lags=4,
        min_date_from_training=min_date_for_pred
    )

    predictions.to_csv("/app/data/task_2g_pred.csv")


    logger.info("Analytics flow completed successfully.")

