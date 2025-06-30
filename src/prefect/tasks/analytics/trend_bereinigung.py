import os
from prefect import task, get_run_logger

@task(name="Remove trends and seasons")
def remove_trends_and_seasons():
    pass