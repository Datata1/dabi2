import os
import sys
from datetime import timedelta
from prefect import flow, get_run_logger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.config import settings


@flow(log_prints=True)
async def test_flow():
    pass