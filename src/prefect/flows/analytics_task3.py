import os
from prefect import flow, get_run_logger
import clickhouse_connect

# task imports
from tasks.analytics.load_data_clickhouse import process_data_from_csv
from tasks.analytics.trend_bereinigung import remove_trends_and_seasons
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
def analytics3():
    pass
    # logger = get_run_logger()
    # logger.info("Starting analytics flow...")

    # # 1.
    # # load data (either csv or clickhouse is fine)
    # df = load_data_clickhouse(
    #     host=CLICKHOUSE_HOST,
    #     port=CLICKHOUSE_PORT,
    #     user=CLICKHOUSE_USER,
    #     password=CLICKHOUSE_PASSWORD
    # )

    # # 2.
    # # trend bereinigung und saison bereinigung
    # df = remove_trends_and_seasons(df)

    # # 3.
    # # feature engineering
    # df = feature_engineering(df)

    # # 4.
    # # modell training
    # # -> save model?
    # model = train_prediction_model(df)

    # # 5. evaluation
    # # show metrics
    # print_model_evaluation(model)

    # # 6. prediction 
    # # make predictions on csv data set and save predictions to csv
    # predictions = make_predictions(model, df)
    # logger.info("Analytics flow completed successfully.")

