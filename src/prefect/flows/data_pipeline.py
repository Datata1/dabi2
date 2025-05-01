# src/prefect/flows/data_pipeline.py
from prefect import flow

from tasks.load_raw_data import load_raw_data
from tasks.run_dbt import run_dbt_models

@flow(name="Data Ingestion and Transformation Pipeline")
def data_pipeline():
    """
    Loads raw data and runs dbt transformations.
    """
    # Schritt 1: Lade Rohdaten
    raw_data_loaded = load_raw_data()

    # Schritt 2: Führe dbt aus, NACHDEM die Rohdaten geladen wurden
    if raw_data_loaded: 
         dbt_result = run_dbt_models(wait_for=[raw_data_loaded])