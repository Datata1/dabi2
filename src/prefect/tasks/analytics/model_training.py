import os
from prefect import task, get_run_logger

@task(name="Train prediction model")
def train_prediction_model():
    pass