import os
from prefect import task, get_run_logger

@task(name="Print model evaluation as an artifact")
def print_model_evaluation():
    pass