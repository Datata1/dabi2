import requests
import time
import json
from pathlib import Path
from prefect import task, get_run_logger

@task(name="Load Debezium Config", retries=2, retry_delay_seconds=10)
def load_debezium_config_task(config_file_path_str: str) -> dict:
    """
    Lädt die Debezium Connector Konfiguration aus einer JSON-Datei.
    """
    logger = get_run_logger()
    config_file_path = Path(config_file_path_str)
    
    logger.info(f"Attempting to load Debezium configuration from: {config_file_path}")
    
    if not config_file_path.is_file():
        msg = f"Debezium config file not found at {config_file_path}"
        logger.error(msg)
        raise FileNotFoundError(msg)
    
    try:
        with open(config_file_path, 'r') as f:
            config_data = json.load(f)
        logger.info(f"Successfully loaded Debezium configuration from {config_file_path}")
        return config_data
    except json.JSONDecodeError as e:
        msg = f"Error decoding JSON from {config_file_path}: {e}"
        logger.error(msg)
        raise ValueError(msg)
    except Exception as e:
        msg = f"An unexpected error occurred while loading Debezium config from {config_file_path}: {e}"
        logger.error(msg)
        raise


@task(name="Activate Debezium Connector", retries=2, retry_delay_seconds=30)
def activate_debezium_connector_task(
    connector_name: str,
    connect_url: str,
    config_data: dict,
    wait_timeout: int = 300, 
    wait_interval: int = 10   
):
    """
    Aktiviert den Debezium Connector. Wartet auf Kafka Connect, prüft ob der
    Connector existiert und erstellt ihn ggf.
    """
    logger = get_run_logger()

    logger.info(f"Provisioner: Waiting for Kafka Connect at {connect_url}...")
    start_time = time.time()
    kafka_connect_ready = False
    while time.time() - start_time < wait_timeout:
        try:
            response = requests.get(connect_url, timeout=5)
            response.raise_for_status()
            logger.info("Provisioner: Kafka Connect is ready.")
            kafka_connect_ready = True
            break
        except requests.exceptions.RequestException as e:
            logger.info(f"Provisioner: Kafka Connect ({connect_url}) not ready yet (Error: {e}), waiting {wait_interval}s...")
            time.sleep(wait_interval)
    
    if not kafka_connect_ready:
        error_msg = f"Provisioner: Kafka Connect did not become ready within {wait_timeout} seconds at {connect_url}. Aborting."
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    specific_connector_url = f"{connect_url}/{connector_name}"
    logger.info(f"Provisioner: Checking for existing connector '{connector_name}' at {specific_connector_url}...")
    
    try:
        response = requests.get(specific_connector_url, timeout=10)
        
        if response.status_code == 200:
            logger.info(f"Provisioner: Connector '{connector_name}' already exists. No action needed.")
            return {"status": "already_exists", "connector_name": connector_name}

        elif response.status_code == 404:
            logger.info(f"Provisioner: Connector '{connector_name}' does not exist. Creating it...")
            post_response = requests.post(
                connect_url, 
                json=config_data, 
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                timeout=30
            )
            
            if 200 <= post_response.status_code < 300:
                logger.info(f"Provisioner: Request to create '{connector_name}' sent successfully. Status: {post_response.status_code}. Response: {post_response.text[:200]}")
                return {"status": "created", "connector_name": connector_name, "response_code": post_response.status_code}
            else:
                logger.error(f"Provisioner: Failed to create connector '{connector_name}'. Status: {post_response.status_code}. Response: {post_response.text}")
                post_response.raise_for_status() 
        
        else:
            logger.error(f"Provisioner: Error checking connector status for '{connector_name}' (HTTP Code: {response.status_code}). Response: {response.text}. Aborting.")
            response.raise_for_status()

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred: {http_err} - Response: {http_err.response.text if http_err.response else 'No response text'}")
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request error occurred: {req_err}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred in activate_debezium_connector_task: {e}")
        raise

    return {"status": "unknown_error", "connector_name": connector_name}