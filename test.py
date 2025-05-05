import duckdb
import os
import argparse
import sys
import logging
import time
from pathlib import Path # Optional, aber gut für Pfade
from datetime import datetime

# --- Logging Konfiguration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)

# --- Konfiguration: Lesen aus Umgebungsvariablen ---
MINIO_ENDPOINT = os.getenv('MINIO_DUCKDB_ENDPOINT', 'http://localhost:9000/datalake') # Muss http://... sein
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = "minio_secret_password" # KEIN Default für Secret!
MINIO_USE_SSL = os.getenv('MINIO_USE_SSL', 'false').lower() == 'true'
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'datalake') # Wird für Pfad-Parsing gebraucht
STAGING_TABLE_PREFIX = "stg_raw_"

def load_single_file(duckdb_path: str, file_path: str):
    """
    Versucht, eine einzelne Parquet-Datei von MinIO in eine DuckDB Staging-Tabelle zu laden.
    """
    logger.info(f"Starte Ladevorgang für: {file_path}")
    logger.info(f"Zieldatenbank: {duckdb_path}")

    if not MINIO_SECRET_KEY:
        logger.error("FEHLER: Umgebungsvariable MINIO_SECRET_KEY ist nicht gesetzt.")
        return False

    conn = None
    target_staging_table = None # Definiere außerhalb des Try-Blocks

    try:
        # --- 1. Tabellennamen extrahieren ---
        logger.debug(f"Extrahiere Tabellennamen aus Pfad: {file_path}")
        try:
            # Extrahiere den Objekt-Key relativ zum Bucket
            if not file_path.startswith(f"s3://{MINIO_BUCKET}/"):
                 raise ValueError(f"Dateipfad '{file_path}' beginnt nicht mit 's3://{MINIO_BUCKET}/'")
            object_key = file_path.split(f"s3://{MINIO_BUCKET}/", 1)[-1] # -> cdc_events/aisles/year=...
            parts = object_key.split('/')
            base_prefix_index = parts.index('cdc_events') # Erwartet 'cdc_events'
            if len(parts) > base_prefix_index + 1:
                table_name = parts[base_prefix_index + 1]
                if not table_name or table_name.startswith('year='):
                    raise ValueError(f"Segment '{table_name}' sieht nicht wie ein Tabellenname aus.")
            else:
                raise ValueError("Kann Tabellennamen nicht nach 'cdc_events' finden.")
        except (ValueError, IndexError) as e_parse:
             logger.error(f"Fehler beim Extrahieren des Tabellennamens aus {file_path}: {e_parse}")
             return False

        target_staging_table = f"{STAGING_TABLE_PREFIX}{table_name}"
        logger.info(f"Zieltabelle in DuckDB: {target_staging_table}")

        # --- 2. Mit DuckDB verbinden und S3 konfigurieren ---
        conn = duckdb.connect(duckdb_path, read_only=False)
        logger.info(f"Verbunden mit DuckDB: {duckdb_path}")

        # Korrekte S3 Konfiguration für MinIO Path Style
        conn.sql(f"INSTALL httpfs;")
        conn.sql(f"LOAD httpfs;")
        endpoint_host_port = MINIO_ENDPOINT.split('//')[-1]
        conn.sql(f"SET s3_endpoint='{endpoint_host_port}';") # OHNE Bucket
        conn.sql(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
        conn.sql(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
        conn.sql(f"SET s3_use_ssl={str(MINIO_USE_SSL).lower()};")
        conn.sql(f"SET s3_url_style=true;") # PATH STYLE aktivieren
        logger.info("DuckDB S3 Konfiguration gesetzt (Path Style).")

        # --- 3. Tabelle erstellen/sicherstellen (inkl. load_ts) ---
        logger.info(f"Prüfe/Erstelle Tabelle {target_staging_table}...")
        try:
            # Teste Lesen der Datei zuerst (gibt frühes Feedback bei S3/Datei-Problemen)
            conn.execute(f"SELECT 1 FROM read_parquet('{file_path}') LIMIT 1;")

            # Tabelle erstellen/ersetzen basierend auf Parquet + load_ts hinzufügen
            conn.sql(f"DROP TABLE IF EXISTS \"{target_staging_table}\";")
            conn.sql(f"CREATE TABLE \"{target_staging_table}\" AS SELECT * FROM read_parquet('{file_path}') LIMIT 0;")
            conn.sql(f"ALTER TABLE \"{target_staging_table}\" ADD COLUMN load_ts TIMESTAMPTZ DEFAULT now();")
            logger.info(f"Staging-Tabelle '{target_staging_table}' erfolgreich sichergestellt.")
        except duckdb.IOException as e_io:
            logger.error(f"IOException beim Erstellen von Tabelle {target_staging_table} aus {file_path}: {e_io}. Prüfen Sie S3 Konfiguration/Pfad/Datei-Existenz!")
            return False # Abbruch hier sinnvoll
        except Exception as e_create:
            logger.error(f"Fehler beim Erstellen/Ändern der Tabelle {target_staging_table} aus {file_path}: {e_create}", exc_info=True)
            return False # Abbruch hier sinnvoll

        # --- 4. Daten laden mit INSERT INTO ... SELECT *, now() ---
        logger.info(f"Versuche Daten zu laden: {file_path} -> {target_staging_table}")
        insert_start_time = time.time()
        try:
            insert_sql = f"""
                INSERT INTO "{target_staging_table}"
                SELECT *, now()
                FROM read_parquet('{file_path}');
            """
            conn.sql(insert_sql)
            insert_end_time = time.time()
            # Prüfe, wie viele Zeilen geladen wurden
            rows_affected = conn.execute(f"SELECT count(*) FROM '{target_staging_table}' WHERE load_ts >= timestamp '{datetime.fromtimestamp(insert_start_time).isoformat()}'").fetchone()[0]
            logger.info(f"INSERT für {file_path} erfolgreich. {rows_affected} Zeilen hinzugefügt. Dauer: {insert_end_time - insert_start_time:.2f}s")
            return True # Erfolg

        except duckdb.BinderException as e_bind:
            logger.error(f"BinderException beim INSERT für {file_path}: {e_bind}. Spaltenanzahl passt immer noch nicht!", exc_info=True)
            return False
        except duckdb.IOException as e_io:
             logger.error(f"IOException beim INSERT für {file_path}: {e_io}. Prüfen Sie S3 Konfiguration/Pfad/Datei-Existenz!")
             return False
        except Exception as e_insert:
            logger.error(f"Anderer Fehler beim INSERT für {file_path}: {e_insert}", exc_info=True)
            return False

    except Exception as e:
        logger.error(f"Genereller Fehler im Skript: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()
            logger.info("DuckDB Verbindung geschlossen.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Lädt eine einzelne Parquet-Datei von MinIO in eine DuckDB Staging-Tabelle.')
    parser.add_argument('db_path', type=str, help='Pfad zur DuckDB Zieldatenbankdatei (z.B. dev.duckdb).')
    parser.add_argument('file_path_s3', type=str, help='Der vollständige S3-Pfad zur Parquet-Datei (z.B. s3://datalake/cdc_events/aisles/...).')
    args = parser.parse_args()

    if load_single_file(args.db_path, args.file_path_s3):
        print("\nLadevorgang erfolgreich abgeschlossen.")
        sys.exit(0)
    else:
        print("\nLadevorgang fehlgeschlagen.")
        sys.exit(1)