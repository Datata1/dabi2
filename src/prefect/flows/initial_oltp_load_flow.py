# src/prefect/flows/initial_oltp_load_flow.py

from prefect import flow, get_run_logger
# Importiere die spezifischen Tasks aus der neuen Datei
from tasks.load_oltp_initial import (
    read_source_files,
    load_dimension_tables,
    load_fact_tables
)
from tasks.create_oltp_schema import create_oltp_schema

# Benenne den Flow ggf. um, um die neue Struktur widerzuspiegeln
@flow(name="Initial OLTP Load from Files (Multi-Task)")
def initial_oltp_load_flow():
    """
    Liest Quelldateien und lädt dann Dimensions- und Faktentabellen in die OLTP DB
    mittels separater Tasks für bessere Sichtbarkeit im Graph.
    ACHTUNG: Tasks verwenden 'append'. Nur einmal ausführen oder Tabellen vorher leeren!
    """
    logger = get_run_logger() # Logger für den Flow
    logger.info("Starting multi-task OLTP load flow...")

    # Schritt 1: Lese alle Quelldateien
    # Gibt ein Tupel von DataFrames zurück
    schema_ok = create_oltp_schema()
    source_data = read_source_files(wait_for=[schema_ok])
    # Entpacke das Tupel in separate Variablen (optional, aber lesbarer)
    # Wir gehen davon aus, die Reihenfolge ist orders, tips, order_products
    orders_df, tips_df, order_products_df = source_data

    # Schritt 2: Lade Dimensionstabellen (hängt vom Lesen ab)
    # Übergib nur die DataFrames, die für Dimensionen benötigt werden
    dims_loaded_result = load_dimension_tables(
        orders_df=orders_df,
        order_products_df=order_products_df,
        wait_for=[source_data] # Explizite Abhängigkeit (obwohl implizit durch Datenübergabe)
    )

    # Schritt 3: Lade Faktentabellen (hängt vom Lesen ab)
    # Optional: Könnte auch von dims_loaded_result abhängen, wenn FKs wichtig sind
    facts_loaded_result = load_fact_tables(
        orders_df_raw=orders_df,
        tips_df_raw=tips_df,
        order_products_df_raw=order_products_df,
        wait_for=[source_data] # Hängt nur vom Lesen ab
    )

    logger.info("Multi-task OLTP load flow finished.")
    # Gib den Status der Ladevorgänge zurück
    return {"dims_loaded": dims_loaded_result, "facts_loaded": facts_loaded_result}

