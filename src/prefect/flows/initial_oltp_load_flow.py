from pathlib import Path
from prefect import flow, get_run_logger

from tasks.load_oltp_initial import (
    read_source_files,
    load_dimension_tables,
    load_fact_tables
)
from tasks.create_oltp_schema import create_oltp_schema
from tasks.run_dbt_runner import run_dbt_command_runner
from tasks.debezium_tasks import load_debezium_config_task, activate_debezium_connector_task


APP_DIR = Path("/app")
DBT_PROJECT_DIR = APP_DIR / "dbt_setup"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

@flow(name="Initial OLTP Load from Files (Multi-Task)")
def initial_oltp_load_flow():
    """
    Liest Quelldateien und lädt dann Dimensions- und Faktentabellen in die OLTP DB
    mittels separater Tasks für bessere Sichtbarkeit im Graph.
    """
    logger = get_run_logger() 
    logger.info("Starting multi-task OLTP load flow...")

    debezium_connector_name = "oltp-postgres-connector"
    kafka_connect_url = "http://kafka-connect:8083/connectors"
    debezium_config_file = "/app/config/debezium-pg-connector.json" 


    logger.info("build DuckDB seeds...")
    staging_result = run_dbt_command_runner( 
            dbt_args=[
                "seed" 
            ],
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR
        )
    logger.info("build DuckDB seeds finished.")

    debezium_config_data_future = load_debezium_config_task(
        config_file_path_str=debezium_config_file
    )

    schema_ok = create_oltp_schema()
    source_data = read_source_files(wait_for=[schema_ok])
    orders_df, tips_df, order_products_df = source_data

    dims_loaded_result = load_dimension_tables(
        orders_df=orders_df,
        order_products_df=order_products_df,
        wait_for=[source_data] 
    )

    debezium_activated = activate_debezium_connector_task(
        connector_name=debezium_connector_name,
        connect_url=kafka_connect_url,
        config_data=debezium_config_data_future
    )

    facts_loaded_result = load_fact_tables(
        orders_df_raw=orders_df,
        tips_df_raw=tips_df,
        order_products_df_raw=order_products_df,
        wait_for=[source_data, debezium_activated] 
    )

    logger.info("Multi-task OLTP load flow finished.")
    return {"dims_loaded": dims_loaded_result, "facts_loaded": facts_loaded_result, "debezium_activation_status": debezium_activated}

