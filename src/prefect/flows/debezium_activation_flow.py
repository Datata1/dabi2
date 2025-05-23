import os
from prefect import flow, get_run_logger
from tasks.debezium_tasks import load_debezium_config_task, activate_debezium_connector_task

DEBEZIUM_CONFIG_FILE = os.getenv("DEBEZIUM_CONFIG_FILE", "/app/config/debezium-pg-connector.json")
DEBEZIUM_CONNECTOR_NAME = os.getenv("DEBEZIUM_CONNECTOR_NAME", "oltp-postgres-connector")
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect:8083/connectors")

@flow(name="Activate Debezium Connector Flow")
def activate_debezium_flow(): 
    logger = get_run_logger()
    logger.info(f"Attempting to activate Debezium connector '{DEBEZIUM_CONNECTOR_NAME}'...")
    try:
        config_data = load_debezium_config_task(config_file_path_str=DEBEZIUM_CONFIG_FILE)
        
        activate_debezium_connector_task(
            connector_name=DEBEZIUM_CONNECTOR_NAME,
            connect_url=KAFKA_CONNECT_URL,
            config_data=config_data
        )
        logger.info(f"Debezium connector '{DEBEZIUM_CONNECTOR_NAME}' activation process initiated by flow.")
    except Exception as e:
        logger.error(f"Error during Debezium activation flow: {e}", exc_info=True)
        raise 