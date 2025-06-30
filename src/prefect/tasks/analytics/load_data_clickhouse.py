import os
import pandas as pd
import gc
import pyarrow.parquet as pq
from prefect import task, get_run_logger

DATA_PATH = os.getenv("DATA_PATH", "/app/data")
ORDERS_PATH = os.path.join(DATA_PATH, "orders.parquet")
TIP_TESTDATEN_TEMPLATE_PATH = os.path.join(DATA_PATH, "tip_testdaten_template_V2.csv")
TIP_PUBLIC_PATH = os.path.join(DATA_PATH, "tips_public.csv")

@task(name="Load data from DWH Clickhosue")
def process_data_from_csv():

    orders = pd.read_parquet(ORDERS_PATH)
    tip_temp = pd.read_csv(TIP_TESTDATEN_TEMPLATE_PATH, usecols=["order_id", "tip"])
    tips = pd.read_csv(TIP_PUBLIC_PATH, usecols=["order_id", "tip"])

    tips["tip"] = tips.tip.astype(int)
    orders_tips = orders.merge(tips).sort_values(["user_id", "order_date"])

    tip_temp_test = pd.concat([tips, tip_temp])
    tip_temp_test = tip_temp_test.merge(orders)
    tip_temp_test = tip_temp_test[tip_temp_test.user_id.isin(tip_temp_test[tip_temp_test.tip.isna()].user_id.unique())].sort_values(["user_id", "order_date"])

    return tips, orders_tips, tip_temp_test
    