# flows/dwh_pipeline.py

import duckdb
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
from prefect import flow, task, get_run_logger
from prefect_aws.credentials import MinIOCredentials # Bleibt gleich
from pathlib import Path
import time

from tasks.run_dbt_runner import run_dbt_command_runner

# --- Konfiguration ---
# (Bleibt unverändert)
MINIO_BUCKET = "datalake"
MINIO_SERVICE_NAME = "minio"
MINIO_PORT = 9000
CDC_STAGING_PREFIX = "cdc_events/"
CDC_ARCHIVE_PREFIX = "cdc-archive/"
DUCKDB_PATH = "/app/prefect/dbt_setup/dev.duckdb"
STAGING_TABLE_PREFIX = "stg_raw_"
MINIO_BLOCK_NAME = "minio-credentials"
MINIO_RAW_ENDPOINT = f"{MINIO_SERVICE_NAME}:{MINIO_PORT}"
MINIO_DUCKDB_ENDPOINT = f"http://{MINIO_SERVICE_NAME}:{MINIO_PORT}/datalake"
MINIO_USE_SSL = False

APP_DIR = Path("/app")
DBT_PROJECT_DIR = APP_DIR / "prefect" / "dbt_setup"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

# --- Tasks ---

@task(retries=1, retry_delay_seconds=5)
def find_new_files_in_minio( # async entfernt
    bucket: str,
    staging_prefix: str,
    minio_endpoint: str,
) -> list[str]:
    logger = get_run_logger()
    logger.info(f"--- Suche neue Dateien ---")
    # ... (andere Logs bleiben gleich) ...
    try:
        # await entfernt - Prefect Block.load() kann synchron aufgerufen werden
        minio_creds = MinIOCredentials.load(MINIO_BLOCK_NAME)
        logger.info(f"Block '{MINIO_BLOCK_NAME}' geladen.")
        client = Minio(
            minio_endpoint,
            access_key=minio_creds.minio_root_user,
            secret_key=minio_creds.minio_root_password.get_secret_value(),
            secure=MINIO_USE_SSL,
        )
        logger.info(f"MinIO Client initialisiert: {client}")
    except Exception as e:
        logger.error(f"Fehler beim Laden des Blocks/Init Client: {e}", exc_info=True)
        raise

    new_files = []
    found_object_count = 0
    try:
        logger.info(f"Rufe client.list_objects(bucket='{bucket}', prefix='{staging_prefix}', recursive=True) auf...")
        objects = client.list_objects(bucket, prefix=staging_prefix, recursive=True) # Minio client ist synchron
        # ... (Rest der Logik bleibt gleich) ...
        for obj in objects:
            found_object_count += 1
            if not obj.is_dir and (obj.object_name.endswith('.parquet') or obj.object_name.endswith('.jsonl')):
                full_path = f"s3://{bucket}/{obj.object_name}"
                new_files.append(full_path)
            else:
                logger.info(f"    -> Objekt übersprungen (Verzeichnis oder falsche Endung).")
        # ... (Rest der Logik bleibt gleich) ...
    except S3Error as e:
        logger.error(f"S3 Fehler beim Auflisten der Objekte: {e}")
        raise
    except Exception as e_list:
         logger.error(f"Anderer Fehler beim Auflisten der Objekte: {e_list}", exc_info=True)
         raise
    return new_files

@task()
def load_files_to_duckdb_staging( # async entfernt
    files_to_process: list[str],
    duckdb_path: str,
    staging_table_prefix: str,
    minio_endpoint_for_duckdb: str,
):
    logger = get_run_logger()
    if not files_to_process:
        logger.info("Keine Dateien zum Laden in DuckDB vorhanden.")
        return False

    logger.info(files_to_process)
    conn = None
    loaded_tables = set()

    try:
        # await entfernt
        minio_creds = MinIOCredentials.load(MINIO_BLOCK_NAME)
        conn = duckdb.connect(duckdb_path, read_only=False) # DuckDB ist synchron
        logger.info(f"Verbunden mit DuckDB: {duckdb_path}")
        # ... (Rest der Logik für S3-Konfiguration und Laden bleibt gleich, da DuckDB und String-Operationen synchron sind) ...
        # DuckDB S3 Konfiguration
        conn.sql(f"INSTALL httpfs;")
        conn.sql(f"LOAD httpfs;")
        endpoint_host_port = minio_endpoint_for_duckdb.split('//')[-1]
        conn.sql(f"SET s3_endpoint='{endpoint_host_port}';")
        conn.sql(f"SET s3_access_key_id='{minio_creds.minio_root_user}';")
        conn.sql(f"SET s3_secret_access_key='{minio_creds.minio_root_password.get_secret_value()}';")
        conn.sql(f"SET s3_use_ssl={str(MINIO_USE_SSL).lower()};")
        conn.sql(f"SET s3_url_style=true;")

        logger.info("DuckDB S3 Konfiguration gesetzt (Path Style).")
        # ... (Rest der Dateiverarbeitungslogik bleibt gleich) ...
        processed_files_count = 0
        for i, file_path in enumerate(files_to_process):
            file_start_time = time.time()
            target_staging_table = None
            logger.info(f"--- Beginn Verarbeitung Datei {i+1}/{len(files_to_process)}: {file_path} ---")
            try:
                object_key = file_path.split(f"s3://{MINIO_BUCKET}/", 1)[-1]
                parts = object_key.split('/')
                try:
                    base_prefix_index = parts.index('cdc_events')
                    if len(parts) > base_prefix_index + 1:
                         table_name = parts[base_prefix_index + 1]
                         if not table_name or table_name.startswith('year='):
                             logger.info(f"Segment '{table_name}' nach 'cdc_events' sieht nicht wie ein Tabellenname aus in: {file_path}")
                             continue
                    else:
                         logger.info(f"Kann Tabellennamen nicht nach 'cdc_events' finden in: {file_path}")
                         continue
                except (ValueError, IndexError):
                    logger.info(f"Konnte 'cdc_events' oder Tabellennamen nicht im Pfad finden: {file_path}")
                    continue

                target_staging_table = f"{staging_table_prefix}{table_name}"
                logger.info(f"Zieltabelle: {target_staging_table}")

                if target_staging_table not in loaded_tables:
                    logger.info(f"Prüfe/Erstelle Tabelle {target_staging_table}...")
                    try:
                        # logger.info(conn.execute(f"SELECT 1 FROM read_parquet('{file_path}') LIMIT 1;")) # Test optional
                        conn.sql(f"DROP TABLE IF EXISTS \"{target_staging_table}\";")
                        conn.sql(f"CREATE TABLE \"{target_staging_table}\" AS SELECT * FROM read_parquet('{file_path}') LIMIT 0;")
                        conn.sql(f"ALTER TABLE \"{target_staging_table}\" ADD COLUMN load_ts TIMESTAMPTZ DEFAULT now();")
                        logger.info(f"Staging-Tabelle '{target_staging_table}' erfolgreich sichergestellt.")
                        loaded_tables.add(target_staging_table)
                    except duckdb.IOException as e_io:
                         logger.info(f"IOException beim Erstellen von Tabelle {target_staging_table} aus {file_path}: {e_io}. Überspringe Datei.")
                         continue
                    except Exception as e_create:
                         logger.info(f"Fehler beim Erstellen/Ändern der Tabelle {target_staging_table} aus {file_path}: {e_create}. Überspringe Datei.")
                         continue

                logger.info(f"Lade Daten aus {file_path} in {target_staging_table} via INSERT INTO SELECT *, now()...")
                try:
                    insert_start_time = time.time()
                    insert_sql = f"""
                        INSERT INTO "{target_staging_table}"  
                        SELECT *, now()
                        FROM read_parquet('{file_path}');
                    """ 
                    conn.sql(insert_sql)
                    insert_end_time = time.time()


                    logger.info(f"INSERT für {file_path} erfolgreich. Dauer: {insert_end_time - insert_start_time:.2f}s") # rows_affected entfernt für Einfachheit
                except Exception as e_insert:
                    logger.info(f"Fehler beim INSERT ... SELECT *, now() für {file_path}: {e_insert}", exc_info=True)
                    continue
                processed_files_count += 1
            except Exception as e_file:
                 logger.info(f"Unerwarteter Fehler beim Verarbeiten der Datei {file_path} für Tabelle {target_staging_table}: {e_file}", exc_info=True)
            finally:
                file_end_time = time.time()
                logger.info(f"--- Ende Verarbeitung Datei {i+1}/{len(files_to_process)}: {file_path}. Dauer: {file_end_time - file_start_time:.2f}s ---")

        logger.info(f"{processed_files_count} von {len(files_to_process)} Dateien erfolgreich via INSERT verarbeitet.")
        conn.close()
        return processed_files_count > 0 and len(files_to_process) > 0

    except Exception as e:
        logger.error(f"Genereller Fehler im Task 'load_files_to_duckdb_staging': {e}", exc_info=True)
        if conn:
            conn.close()
        raise

@task(retries=1)
def archive_processed_files( # async entfernt
    processed_files: list[str],
    bucket: str,
    staging_prefix: str,
    archive_prefix: str,
    minio_endpoint: str,
):
    logger = get_run_logger()
    if not processed_files:
        logger.info("Keine Dateien zum Archivieren.")
        return

    if not archive_prefix.endswith('/'):
        archive_prefix += '/'

    try:
        # await entfernt
        minio_creds = MinIOCredentials.load(MINIO_BLOCK_NAME)
        client = Minio(
            minio_endpoint,
            access_key=minio_creds.minio_root_user,
            secret_key=minio_creds.minio_root_password.get_secret_value(),
            secure=MINIO_USE_SSL,
        ) # Minio client ist synchron
    except Exception as e:
        logger.error(f"Fehler beim Laden des Blocks '{MINIO_BLOCK_NAME}' oder Initialisieren des MinIO Clients für Archivierung: {e}", exc_info=True)
        raise

    logger.info(f"Archiviere {len(processed_files)} Dateien...")
    archived_count = 0
    error_count = 0
    for file_path_s3 in processed_files:
        try:
            object_name = file_path_s3.replace(f"s3://{bucket}/", "")
            relative_path = object_name
            if object_name.startswith(staging_prefix):
                 relative_path = object_name[len(staging_prefix):]
            archive_object_name = archive_prefix + relative_path
            source_to_copy = CopySource(bucket, object_name)
            client.copy_object(
                bucket_name=bucket,
                object_name=archive_object_name,
                source=source_to_copy
            )
            client.remove_object(bucket, object_name)
            archived_count += 1
        except S3Error as e:
            logger.error(f"S3 Fehler beim Archivieren von {object_name}: {e}")
            error_count += 1
        except Exception as e:
             logger.error(f"Unerwarteter Fehler beim Archivieren von {object_name}: {e}", exc_info=False)
             error_count += 1
    logger.info(f"Archivierung abgeschlossen. {archived_count} Dateien verschoben, {error_count} Fehler.")

# --- Der Haupt-Flow ---
@flow(name="CDC MinIO to DWH (Synchronous)", log_prints=True) # Name angepasst
def cdc_minio_to_duckdb_flow(): # Name angepasst für Klarheit
    logger = get_run_logger()
    logger.info("Starte CDC MinIO zu DuckDB Flow (Synchronous)...")
    final_message = "Flow initialisiert."

    try:
        # 1. Neue Dateien finden 
        new_files_list = find_new_files_in_minio(
            bucket=MINIO_BUCKET,
            staging_prefix=CDC_STAGING_PREFIX,
            minio_endpoint=MINIO_RAW_ENDPOINT,
        )

        if not new_files_list:
            logger.info("Keine neuen Dateien gefunden, Flow wird regulär beendet.")
            return "Keine neuen Dateien."

        logger.info(f"Verarbeite {len(new_files_list)} neue Dateien.")

        # 2. Daten in DuckDB Staging laden 
        load_staging_success = load_files_to_duckdb_staging(
            files_to_process=new_files_list,
            duckdb_path=DUCKDB_PATH,
            staging_table_prefix=STAGING_TABLE_PREFIX,
            minio_endpoint_for_duckdb=MINIO_DUCKDB_ENDPOINT,
        )
        if not load_staging_success: # Task gibt True/False zurück
            logger.error("Laden der Staging-Daten fehlgeschlagen. Breche Flow ab.")
            return "Laden der Staging-Daten fehlgeschlagen."
        logger.info("Daten erfolgreich in DuckDB Staging geladen.")

        # 3. DBT sequentiell ausführen
        logger.info("Triggering sequential DBT tasks...")

        # 3.1 dbt debug (optional, await entfernt, falls es vorher da war)
        logger.info("Running DBT debug...")
        debug_status = run_dbt_command_runner( # run_dbt_command_runner ist bereits synchron
            dbt_args=["debug"],
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR
        )
        if not debug_status:
            logger.error("DBT debug fehlgeschlagen. Breche Flow ab.")
            return "DBT debug fehlgeschlagen."
        logger.info("DBT debug erfolgreich.")

        # 3.2 Staging Models 
        logger.info("Running DBT Staging models...")
        staging_result = run_dbt_command_runner( 
            dbt_args=["build", "--resource-type", "model", "--resource-type", "snapshot"],
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR
        )
        logger.info("DBT Staging Models abgeschlossen (Erfolg wird durch Task bestimmt).")


        # 4. Dateien archivieren (await entfernt)
        logger.info("Alle DBT Schritte erfolgreich. Archiviere Dateien...")
        archive_processed_files( # Kein await
             processed_files=new_files_list,
             bucket=MINIO_BUCKET,
             staging_prefix=CDC_STAGING_PREFIX,
             archive_prefix=CDC_ARCHIVE_PREFIX,
             minio_endpoint=MINIO_RAW_ENDPOINT,
        )
        logger.info("Dateien erfolgreich archiviert.")
        final_message = "CDC Flow erfolgreich abgeschlossen."

    except Exception as e:
        logger.error(f"Ein Fehler ist im Flow aufgetreten: {e}", exc_info=True)
        final_message = f"CDC Flow mit Fehlern beendet: {e}"
        raise
    finally:
        logger.info(final_message)

    return final_message

if __name__ == "__main__":
    cdc_minio_to_duckdb_flow()