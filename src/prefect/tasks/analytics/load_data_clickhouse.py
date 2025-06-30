import os
from prefect import task, get_run_logger

@task(name="Load data from DWH Clickhosue")
def load_data_clickhouse():
    pass