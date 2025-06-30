import os
from prefect import task, get_run_logger

@task(name="make predictions")
def make_predictions():
    pass