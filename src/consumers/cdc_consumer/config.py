import os
import logging

# --- Kafka Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC_PATTERN = os.getenv('KAFKA_TOPIC_PATTERN', '^cdc\\.oltp_dabi\\.public\\.(.*)')
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'dabi2-minio-lake-writer')
KAFKA_POLL_TIMEOUT = float(os.getenv('KAFKA_POLL_TIMEOUT', '1.0'))
KAFKA_COMMIT_ASYNCHRONOUS = os.getenv('KAFKA_COMMIT_ASYNCHRONOUS', 'false').lower() == 'true'


# --- MinIO Configuration ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'datalake')
MINIO_USE_SSL = os.getenv('MINIO_USE_SSL', 'false').lower() == 'true'

# --- Batching Configuration ---
WRITE_INTERVAL_SECONDS = int(os.getenv('WRITE_INTERVAL_SECONDS', '20'))

# --- Logging Configuration ---
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# --- Prefect Configuration ---
PREFECT_FLOW_NAME = os.getenv('PREFECT_FLOW_NAME', "cdc_minio_to_duckdb_flow")
PREFECT_DEPLOYMENT_NAME = os.getenv('PREFECT_DEPLOYMENT_NAME', "dwh-pipeline")

# --- Application Information ---
APP_NAME = os.getenv('APP_NAME', 'CDCKafkaMinIOWriter')

def validate_critical_config():
    critical_vars = {
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    }
    missing_vars = [var for var, value in critical_vars.items() if value is None]
    if missing_vars:
        raise ValueError(f"Fehlende kritische Umgebungsvariablen: {', '.join(missing_vars)}")

def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    return logger

module_logger = get_logger(__name__)

try:
    validate_critical_config()
except ValueError as e:
    module_logger.error(f"Konfigurationsfehler: {e}")
    raise 