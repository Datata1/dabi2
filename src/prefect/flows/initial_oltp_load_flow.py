# src/prefect/flows/initial_oltp_load_flow.py
from pathlib import Path

from prefect import flow, get_run_logger
# Importiere die spezifischen Tasks aus der neuen Datei
from tasks.load_oltp_initial import (
    read_source_files,
    load_dimension_tables,
    load_fact_tables
)
from tasks.create_oltp_schema import create_oltp_schema
from tasks.run_dbt_runner import run_dbt_command_runner

APP_DIR = Path("/app")
DBT_PROJECT_DIR = APP_DIR / "prefect" / "dbt_setup"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

@flow(name="Initial OLTP Load from Files (Multi-Task)")
def initial_oltp_load_flow():
    """
    Liest Quelldateien und lädt dann Dimensions- und Faktentabellen in die OLTP DB
    mittels separater Tasks für bessere Sichtbarkeit im Graph.
    """
    logger = get_run_logger() # Logger für den Flow
    logger.info("Starting multi-task OLTP load flow...")

    logger.info("build DuckDB seeds...")
    staging_result = run_dbt_command_runner( 
            dbt_args=[
                "seed",                             
                "--vars",                            
                "{is_initial_snapshot_load: true}"  
            ],
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR
        )
    logger.info("build DuckDB seeds finished.")

    # Schritt 1: Lese alle Quelldateien
    schema_ok = create_oltp_schema()
    source_data = read_source_files(wait_for=[schema_ok])
    orders_df, tips_df, order_products_df = source_data

    # Schritt 2: Lade Dimensionstabellen (hängt vom Lesen ab)
    dims_loaded_result = load_dimension_tables(
        orders_df=orders_df,
        order_products_df=order_products_df,
        wait_for=[source_data] 
    )

    # Schritt 3: Lade Faktentabellen (hängt vom Lesen ab)
    facts_loaded_result = load_fact_tables(
        orders_df_raw=orders_df,
        tips_df_raw=tips_df,
        order_products_df_raw=order_products_df,
        wait_for=[source_data] 
    )

    logger.info("Multi-task OLTP load flow finished.")
    return {"dims_loaded": dims_loaded_result, "facts_loaded": facts_loaded_result}

