import duckdb
import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
from prefect import flow, task, get_run_logger
from prefect_aws.credentials import MinIOCredentials 
from pathlib import Path
import time
import clickhouse_connect


from tasks.run_dbt_runner import run_dbt_command_runner
from utils.schema import get_staging_table_schema

# --- Konfiguration ---
MINIO_BUCKET = "datalake"
MINIO_SERVICE_NAME = "minio"
MINIO_PORT = 9000
CDC_STAGING_PREFIX = "cdc_events/"
CDC_ARCHIVE_PREFIX = "cdc-archive/"
DUCKDB_PATH = "/app/dbt_setup/dev.duckdb"
STAGING_TABLE_PREFIX = "stg_raw_"
MINIO_BLOCK_NAME = "minio-credentials"
MINIO_RAW_ENDPOINT = f"{MINIO_SERVICE_NAME}:{MINIO_PORT}"
MINIO_DUCKDB_ENDPOINT = f"http://{MINIO_SERVICE_NAME}:{MINIO_PORT}/datalake"
MINIO_USE_SSL = False

APP_DIR = Path("/app")
DBT_PROJECT_DIR = APP_DIR / "dbt_setup"
DBT_PROFILES_DIR = DBT_PROJECT_DIR

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse-server")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "devpassword")

# --- Tasks ---
@task(retries=1, retry_delay_seconds=5)
def find_new_files_in_minio( 
    bucket: str,
    staging_prefix: str,
    minio_endpoint: str,
) -> list[str]:
    logger = get_run_logger()
    logger.info(f"--- Suche neue Dateien ---")
    try:
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
        objects = client.list_objects(bucket, prefix=staging_prefix, recursive=True) 
        for obj in objects:
            found_object_count += 1
            if not obj.is_dir and (obj.object_name.endswith('.parquet') or obj.object_name.endswith('.jsonl')):
                full_path = f"s3://{bucket}/{obj.object_name}"
                new_files.append(full_path)
            else:
                logger.info(f"    -> Objekt übersprungen (Verzeichnis oder falsche Endung).")
    except S3Error as e:
        logger.error(f"S3 Fehler beim Auflisten der Objekte: {e}")
        raise
    except Exception as e_list:
         logger.error(f"Anderer Fehler beim Auflisten der Objekte: {e_list}", exc_info=True)
         raise
    return new_files

@task()
def load_files_to_clickhouse_staging(
    files_to_process: list[str],
    staging_table_prefix: str,
):
    """
    Lädt Parquet-Dateien aus MinIO direkt in Staging-Tabellen in ClickHouse.
    """
    logger = get_run_logger()
    if not files_to_process:
        logger.info("Keine neuen Dateien zum Laden in ClickHouse.")
        return False

    try:
        # Verbindung zum ClickHouse-Server herstellen
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database="default" # Die Zieldatenbank in ClickHouse
        )
        logger.info(f"Erfolgreich mit ClickHouse auf {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} verbunden.")
    except Exception as e:
        logger.error(f"Fehler bei der Verbindung zu ClickHouse: {e}")
        raise

    # S3-Credentials für die ClickHouse-Funktion holen
    minio_creds = MinIOCredentials.load(MINIO_BLOCK_NAME)
    access_key = minio_creds.minio_root_user
    secret_key = minio_creds.minio_root_password.get_secret_value()

    # S3-URL-Struktur definieren (ohne http://)
    s3_url_base = f"http://{MINIO_SERVICE_NAME}:{MINIO_PORT}/{MINIO_BUCKET}/"

    for file_path_s3 in files_to_process:
        try:
            # Extrahiere Tabellenname aus dem Dateipfad, z.B. "orders"
            object_key = file_path_s3.split(f"s3://{MINIO_BUCKET}/", 1)[-1]
            table_name_from_path = object_key.split('/')[1] # Annahme: /cdc_events/orders/...
            target_staging_table = f"{staging_table_prefix}{table_name_from_path}"
            
            # S3-URL für die ClickHouse-Funktion
            s3_full_url = f"{s3_url_base}{object_key}"

            create_table_ddl = get_staging_table_schema(table_name_from_path)
            client.command(create_table_ddl)

            parquet_schema_definition = ""
            if table_name_from_path == "aisles":
                parquet_schema_definition = "aisle_id Int64, aisle String, _op String, _ts_ms Int64"
            elif table_name_from_path == "products":
                parquet_schema_definition = "product_id Int64, product_name String, aisle_id Nullable(Int64), department_id Nullable(Int64), _op String, _ts_ms Int64"
            elif table_name_from_path == "departments":
                parquet_schema_definition = "department_id Int64, department String, _op String, _ts_ms Int64"
            elif table_name_from_path == "users":
                parquet_schema_definition = "user_id Int64, _op String, _ts_ms Int64"
            elif table_name_from_path == "orders":
                parquet_schema_definition = "order_id Int64, user_id Int64, order_date Int64, tip_given Nullable(Boolean), _op String, _ts_ms Int64"
            elif table_name_from_path == "order_products":
                parquet_schema_definition = "order_id Int64, product_id Int64, add_to_cart_order Int64, _op String, _ts_ms Int64"
            
            if not parquet_schema_definition:
                raise ValueError(f"Keine Parquet-Schema-Definition für Tabelle '{table_name_from_path}' gefunden. Kann nicht laden.")


            insert_sql = f"""
            INSERT INTO default_raw_seeds.{target_staging_table}
            SELECT 
                * EXCEPT (_ts_ms), -- Wähle alle Spalten außer _ts_ms
                fromUnixTimestamp64Milli(_ts_ms) AS _ts_ms,
                now() as load_ts
            FROM s3(
                '{s3_full_url}',
                '{access_key}',
                '{secret_key}',
                'Parquet',
                '{parquet_schema_definition}'
            );
            """
            client.command(insert_sql)
            logger.info(f"Erfolgreich Daten in {target_staging_table} eingefügt.")

        except Exception as e:
            logger.error(f"Fehler beim Laden der Datei {file_path_s3} nach ClickHouse: {e}", exc_info=True)
            # In einem echten Szenario würde man hier eine bessere Fehlerbehandlung implementieren
            continue

    logger.info("Ladevorgang nach ClickHouse abgeschlossen.")
    return True

@task()
def load_files_to_duckdb_staging( 
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
        minio_creds = MinIOCredentials.load(MINIO_BLOCK_NAME)
        conn = duckdb.connect(duckdb_path, read_only=False) 
        logger.info(f"Verbunden mit DuckDB: {duckdb_path}")
        conn.sql(f"INSTALL httpfs;")
        conn.sql(f"LOAD httpfs;")
        endpoint_host_port = minio_endpoint_for_duckdb.split('//')[-1]
        conn.sql(f"SET s3_endpoint='{endpoint_host_port}';")
        conn.sql(f"SET s3_access_key_id='{minio_creds.minio_root_user}';")
        conn.sql(f"SET s3_secret_access_key='{minio_creds.minio_root_password.get_secret_value()}';")
        conn.sql(f"SET s3_use_ssl={str(MINIO_USE_SSL).lower()};")
        conn.sql(f"SET s3_url_style=true;")

        logger.info("DuckDB S3 Konfiguration gesetzt (Path Style).")
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


                    logger.info(f"INSERT für {file_path} erfolgreich. Dauer: {insert_end_time - insert_start_time:.2f}s") 
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
def archive_processed_files(
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
        minio_creds = MinIOCredentials.load(MINIO_BLOCK_NAME)
        client = Minio(
            minio_endpoint,
            access_key=minio_creds.minio_root_user,
            secret_key=minio_creds.minio_root_password.get_secret_value(),
            secure=MINIO_USE_SSL,
        ) 
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
@flow(name="CDC MinIO to DWH (Synchronous)", log_prints=True) 
def cdc_minio_to_duckdb_flow():
    logger = get_run_logger()
    logger.info("Starte CDC MinIO zu DuckDB Flow (Synchronous)...")
    final_message = "Flow initialisiert."

    try:
        new_files_list = find_new_files_in_minio(
            bucket=MINIO_BUCKET,
            staging_prefix=CDC_STAGING_PREFIX,
            minio_endpoint=MINIO_RAW_ENDPOINT,
        )

        if not new_files_list:
            logger.info("Keine neuen Dateien gefunden, Flow wird regulär beendet.")
            return "Keine neuen Dateien."

        logger.info(f"Verarbeite {len(new_files_list)} neue Dateien.")

        load_staging_success = load_files_to_clickhouse_staging(
            files_to_process=new_files_list,
            staging_table_prefix=STAGING_TABLE_PREFIX,
        )
        if not load_staging_success: 
            logger.error("Laden der Staging-Daten fehlgeschlagen. Breche Flow ab.")
            return "Laden der Staging-Daten fehlgeschlagen."
        logger.info("Daten erfolgreich in DuckDB Staging geladen.")

        logger.info("Running DBT debug...")
        debug_status = run_dbt_command_runner( 
            dbt_args=["debug"],
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR
        )
        if not debug_status:
            logger.error("DBT debug fehlgeschlagen. Breche Flow ab.")
            return "DBT debug fehlgeschlagen."
        logger.info("DBT debug erfolgreich.")

        logger.info("Running DBT Staging models...")
        staging_result = run_dbt_command_runner( 
            dbt_args=["build", "--resource-type", "model", "--resource-type", "snapshot" ],
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR
        )
        logger.info("DBT Staging Models abgeschlossen (Erfolg wird durch Task bestimmt).")


        logger.info("Alle DBT Schritte erfolgreich. Archiviere Dateien...")
        archive_processed_files( 
             processed_files=new_files_list,
             bucket=MINIO_BUCKET,
             staging_prefix=CDC_STAGING_PREFIX,
             archive_prefix=CDC_ARCHIVE_PREFIX,
             minio_endpoint=MINIO_RAW_ENDPOINT,
        )
        logger.info("Dateien erfolgreich archiviert.")

        logger.info("Running DBT test...")
        test_status = run_dbt_command_runner( 
            dbt_args=["test"],
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR
        )
        if not test_status:
            logger.error("DBT debug fehlgeschlagen. Breche Flow ab.")
            return "DBT test fehlgeschlagen."
        logger.info("DBT test erfolgreich.")

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