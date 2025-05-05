# flows/dwh_pipeline.py

import duckdb
from datetime import datetime
from minio import Minio
from minio.error import S3Error
from prefect import flow, task, get_run_logger
from prefect_aws.credentials import MinIOCredentials
from pathlib import Path
import time 

from tasks.run_dbt_runner import run_dbt_command_runner

# --- Konfiguration ---
MINIO_BUCKET = "datalake"
MINIO_SERVICE_NAME = "minio"
MINIO_PORT = 9000
CDC_STAGING_PREFIX = "cdc_events/" # Übergeordneter Pfad
CDC_ARCHIVE_PREFIX = "cdc-archive/" # Übergeordneter Pfad
DUCKDB_PATH = "/app/prefect/dbt_setup/dev.duckdb" # Pfad zur DuckDB-Datei
# Staging Tabellen: Wahrscheinlich eine pro Quelltabelle oder ein generisches Raw-Schema
STAGING_TABLE_PREFIX = "stg_raw_" # Präfix für DuckDB Staging Tabellen
MINIO_BLOCK_NAME = "minio-credentials"
MINIO_RAW_ENDPOINT = f"{MINIO_SERVICE_NAME}:{MINIO_PORT}"
MINIO_DUCKDB_ENDPOINT = f"http://{MINIO_SERVICE_NAME}:{MINIO_PORT}/datalake"
MINIO_USE_SSL = False

# Konfiguration für Ihren DBT Task (Pfade müssen korrekt sein!)
# Diese Pfade müssen vom Prefect Worker aus erreichbar sein.
APP_DIR = Path("/app") # Annahme: Ihr Code liegt in /app im Container
DBT_PROJECT_DIR = APP_DIR / "prefect" / "dbt_setup"
DBT_PROFILES_DIR = DBT_PROJECT_DIR #profiles.yml ist im Projektordner, wie in Ihrem Task definiert

# --- Tasks (find_new_files_in_minio, load_files_to_duckdb_staging, archive_processed_files) ---
# (Diese Tasks bleiben unverändert wie in der vorherigen Antwort,
#  sie laden den MinIO Block korrekt und interagieren mit MinIO/DuckDB)
# --- BEGINN der unveränderten Tasks ---
@task(retries=1, retry_delay_seconds=5) # Weniger Retries für Debugging
async def find_new_files_in_minio(
    bucket: str,
    staging_prefix: str,
    minio_endpoint: str,
) -> list[str]:
    """
    Listet neue Dateien im Staging-Prefix auf. Mit erweitertem Debug-Logging.
    """
    logger = get_run_logger()
    logger.info(f"--- Suche neue Dateien ---")
    logger.info(f"bucket: {bucket}")
    logger.info(f"staging_prefix: {staging_prefix}")
    logger.info(f"minio_endpoint: {minio_endpoint}")
    logger.info(f"Bucket: '{bucket}', Prefix: '{staging_prefix}', Endpoint: '{minio_endpoint}'")
    try:
        minio_creds = await MinIOCredentials.load(MINIO_BLOCK_NAME)
        logger.info("test1")
        logger.info(f"Block '{MINIO_BLOCK_NAME}' geladen.")
        logger.info("test2")
        logger.info(minio_creds)
        logger.info(f"MinIO Credentials: {minio_creds.minio_root_user}")
        logger.info(f"{minio_creds.minio_root_password.get_secret_value()}")
        client = Minio(
            minio_endpoint,
            access_key=minio_creds.minio_root_user,
            secret_key=minio_creds.minio_root_password.get_secret_value(),
            secure=MINIO_USE_SSL,
        )
        logger.info("test3")
        logger.info(f"MinIO Client initialisiert: {client}")
    except Exception as e:
        logger.error(f"Fehler beim Laden des Blocks/Init Client: {e}", exc_info=True)
        raise

    new_files = []
    found_object_count = 0
    try:
        logger.info(f"Rufe client.list_objects(bucket='{bucket}', prefix='{staging_prefix}', recursive=True) auf...")
        objects = client.list_objects(bucket, prefix=staging_prefix, recursive=True)
        logger.info("test4")    
        logger.info(objects)
        # Iteriere über den Generator und logge jeden Eintrag
        for obj in objects:
            found_object_count += 1
            # Logge Details zu JEDEM gefundenen Objekt, bevor gefiltert wird
            # logger.info(f"  Gefundenes Objekt: Name='{obj.object_name}', Size={obj.size}, IsDir={obj.is_dir}")

            # Filterung anwenden
            if not obj.is_dir and (obj.object_name.endswith('.parquet') or obj.object_name.endswith('.jsonl')):
                full_path = f"s3://{bucket}/{obj.object_name}"
                # logger.info(f"    -> Datei hinzugefügt: {full_path}")
                new_files.append(full_path)
            else:
                logger.info(f"    -> Objekt übersprungen (Verzeichnis oder falsche Endung).")

        logger.info(f"list_objects hat {found_object_count} Objekte unter Prefix '{staging_prefix}' zurückgegeben.")

        if not new_files:
            logger.info("Keine passenden Dateien (.parquet/.jsonl) nach Filterung gefunden.")
        else:
            logger.info(f"{len(new_files)} passende Dateien gefunden.")

    except S3Error as e:
        logger.error(f"S3 Fehler beim Auflisten der Objekte: {e}")
        raise
    except Exception as e_list:
         logger.error(f"Anderer Fehler beim Auflisten der Objekte: {e_list}", exc_info=True)
         raise


    return new_files

@task()
async def load_files_to_duckdb_staging(
    files_to_process: list[str],
    duckdb_path: str,
    staging_table_prefix: str,
    minio_endpoint_for_duckdb: str,
):
    """
    Lädt Daten aus den angegebenen Dateien (von MinIO) in separate
    DuckDB Staging-Tabellen pro Quelltabelle basierend auf dem Dateipfad.
    VERWENDET JETZT VEREINFACHTEN INSERT INTO SELECT.
    """
    logger = get_run_logger()
    if not files_to_process:
        logger.info("Keine Dateien zum Laden in DuckDB vorhanden.")
        return False

    logger.info(files_to_process)
    conn = None
    loaded_tables = set()

    try:
        minio_creds = await MinIOCredentials.load(MINIO_BLOCK_NAME)
        conn = duckdb.connect(duckdb_path, read_only=False)
        logger.info(f"Verbunden mit DuckDB: {duckdb_path}")
        logger.info("test5")
        logger.info(f"MinIO Credentials: {minio_creds.minio_root_user}")
        logger.info("test5.1")
        logger.info(f"{minio_creds.minio_root_password.get_secret_value()}")
        logger.info(f"minio_creds: {minio_creds}")
        logger.info("test5.2")
        logger.info("test6")

        # DuckDB S3 Konfiguration
        conn.sql(f"INSTALL httpfs;")
        conn.sql(f"LOAD httpfs;")
        endpoint_host_port = minio_endpoint_for_duckdb.split('//')[-1]
        conn.sql(f"SET s3_endpoint='{endpoint_host_port}';")
        conn.sql(f"SET s3_access_key_id='{minio_creds.minio_root_user}';")
        conn.sql(f"SET s3_secret_access_key='{minio_creds.minio_root_password.get_secret_value()}';")
        conn.sql(f"SET s3_use_ssl={str(MINIO_USE_SSL).lower()};")
        conn.sql(f"SET s3_url_style=true;") # Korrigiert für MinIO

        logger.info("DuckDB S3 Konfiguration gesetzt (Path Style).")
        logger.info("test7")
        logger.info(f"MinIO Endpoint für DuckDB: {minio_endpoint_for_duckdb}")

        processed_files_count = 0
        for i, file_path in enumerate(files_to_process):
            file_start_time = time.time()
            target_staging_table = None
            logger.info(f"--- Beginn Verarbeitung Datei {i+1}/{len(files_to_process)}: {file_path} ---")
            try:
                # Extrahiere Tabellennamen (wie zuvor)
                # ... (Ihre korrigierte Logik hier) ...
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


                # Tabelle erstellen/sicherstellen (mit load_ts)
                if target_staging_table not in loaded_tables:
                    logger.info(f"Prüfe/Erstelle Tabelle {target_staging_table}...") # Log VOR Operationen
                    try:
                        # Teste zuerst, ob die Datei lesbar ist
                        # (optional, aber kann spezifischere Fehler geben)
                        logger.info(conn.execute(f"SELECT 1 FROM read_parquet('{file_path}') LIMIT 1;"))

                        # Jetzt Tabelle erstellen
                        conn.sql(f"DROP TABLE IF EXISTS \"{target_staging_table}\";")
                        conn.sql(f"CREATE TABLE \"{target_staging_table}\" AS SELECT * FROM read_parquet('{file_path}') LIMIT 0;")
                        # Und Spalte hinzufügen
                        conn.sql(f"ALTER TABLE \"{target_staging_table}\" ADD COLUMN load_ts TIMESTAMPTZ DEFAULT now();")

                        logger.info(f"Staging-Tabelle '{target_staging_table}' erfolgreich sichergestellt.")
                        loaded_tables.add(target_staging_table)

                    except duckdb.IOException as e_io:
                         # Fängt spezifisch IO Fehler beim CREATE TABLE (wahrscheinlich wegen S3 Config)
                         logger.info(f"IOException beim Erstellen von Tabelle {target_staging_table} aus {file_path}: {e_io}. Überspringe Datei. Prüfen Sie S3 Konfiguration!")
                         continue # SEHR WICHTIG: Gehe zur nächsten Datei, wenn Erstellung fehlschlägt
                    except Exception as e_create:
                         # Andere Fehler beim Erstellen/Ändern
                         logger.info(f"Fehler beim Erstellen/Ändern der Tabelle {target_staging_table} aus {file_path}: {e_create}. Überspringe Datei.")
                         continue # SEHR WICHTIG: Gehe zur nächsten Datei


                # --- DATEN LADEN: INSERT INTO SELECT *, now() ---
                # (Dieser Teil wird nur erreicht, wenn die Tabelle existiert)
                # ... (Code für INSERT INTO ... SELECT *, now() wie zuvor) ...
                logger.info(f"Lade Daten aus {file_path} in {target_staging_table} via INSERT INTO SELECT *, now()...")
                logger.info("test8")
                try:
                    insert_start_time = time.time()
                    logger.info("test9")
                    insert_sql = f"""
                        INSERT INTO '{target_staging_table}'
                        SELECT *, now()
                        FROM read_parquet('{file_path}');
                    """
                    conn.sql(insert_sql)
                    logger.info("test10")
                    insert_end_time = time.time()
                    rows_affected = conn.execute(f"SELECT count(*) FROM '{target_staging_table}' WHERE load_ts >= timestamp '{datetime.fromtimestamp(insert_start_time).isoformat()}'").fetchone()[0]

                    logger.info(f"INSERT für {file_path} erfolgreich. {rows_affected} Zeilen hinzugefügt. Dauer: {insert_end_time - insert_start_time:.2f}s")
                except Exception as e_insert:
                    logger.info(f"Fehler beim INSERT ... SELECT *, now() für {file_path}: {e_insert}", exc_info=True)
                    continue # Nächste Datei versuchen

                insert_end_time = time.time()
                logger.info(f"INSERT ... SELECT *, now() für {file_path} erfolgreich. Dauer: {insert_end_time - insert_start_time:.2f}s")
                processed_files_count += 1


            except Exception as e_file: # Fängt alle anderen Fehler pro Datei ab
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
async def archive_processed_files(
    processed_files: list[str],
    bucket: str,
    staging_prefix: str,
    archive_prefix: str,
    minio_endpoint: str,
):
    """
    Verschiebt erfolgreich verarbeitete Dateien vom Staging ins Archiv.
    Lädt Credentials aus dem Prefect Block.
    """
    logger = get_run_logger()
    if not processed_files:
        logger.info("Keine Dateien zum Archivieren.")
        return

    # Check if archive_prefix ends with '/', add if not
    if not archive_prefix.endswith('/'):
        archive_prefix += '/'

    try:
        minio_creds = await MinIOCredentials.load(MINIO_BLOCK_NAME)
        # logger.info(f"Block '{MINIO_BLOCK_NAME}' für Archivierung geladen.")
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
            # Stelle sicher, dass der Staging-Prefix korrekt entfernt wird, auch wenn er nicht exakt übereinstimmt
            relative_path = object_name
            if object_name.startswith(staging_prefix):
                 relative_path = object_name[len(staging_prefix):]

            # Erstelle Zielpfad im Archiv
            archive_object_name = archive_prefix + relative_path

            # 1. Kopieren
            client.copy_object(
                bucket_name=bucket,
                object_name=archive_object_name,
                source=f"/{bucket}/{object_name}",
            )

            # 2. Löschen
            client.remove_object(bucket, object_name)
            archived_count += 1

        except S3Error as e:
            logger.error(f"S3 Fehler beim Archivieren von {object_name}: {e}")
            error_count += 1
        except Exception as e:
             logger.error(f"Unerwarteter Fehler beim Archivieren von {object_name}: {e}", exc_info=False)
             error_count += 1

    logger.info(f"Archivierung abgeschlossen. {archived_count} Dateien verschoben, {error_count} Fehler.")
# --- ENDE der unveränderten Tasks ---


# --- Der Haupt-Flow ---
@flow(name="CDC MinIO to DWH", log_prints=True) # Angepasster Flow-Name
async def cdc_minio_to_duckdb_flow():
    logger = get_run_logger()
    logger.info("Starte CDC MinIO zu DuckDB Flow...")

    # 1. Neue Dateien finden
    # Annahme: Dateien liegen unterhalb von CDC_STAGING_PREFIX, z.B. cdc-staging/public/products/file.parquet
    new_files = await find_new_files_in_minio(
        bucket=MINIO_BUCKET,
        staging_prefix=CDC_STAGING_PREFIX, # Übergeordneter Prefix
        minio_endpoint=MINIO_RAW_ENDPOINT,
    )

    # Nur fortfahren, wenn neue Dateien gefunden wurden
    # Wichtig: `new_files` ist das Ergebnis des Tasks, nicht die Liste direkt
    # Prefect behandelt Abhängigkeiten basierend auf Task-Aufrufen
    if new_files and new_files: # Warten auf Ergebnis und prüfen ob Liste nicht leer
        active_new_files = new_files # Hol das Ergebnis explizit
        logger.info(f"Verarbeite {len(active_new_files)} neue Dateien.")
        # 2. Daten in DuckDB Staging laden
        load_success = await load_files_to_duckdb_staging(
            files_to_process=active_new_files, # Übergebe die Liste
            duckdb_path=DUCKDB_PATH,
            staging_table_prefix=STAGING_TABLE_PREFIX,
            minio_endpoint_for_duckdb=MINIO_DUCKDB_ENDPOINT
        )

        # Warte auf das Ergebnis und prüfe es
        if load_success and load_success:
            # 3. DBT sequentiell ausführen
            logger.info("Triggering sequential DBT tasks...")
            dbt_step_success = True # Flag für Gesamt-DBT-Erfolg
            last_dbt_task_result = load_success # Start-Abhängigkeit

            # 3.1 Staging Models
            logger.info("Running DBT Staging models...")
            staging_result = run_dbt_command_runner(
                # Selektiere Modelle im staging Unterverzeichnis
                dbt_args=["run", "--select", "models/staging"],
                project_dir=DBT_PROJECT_DIR,
                profiles_dir=DBT_PROFILES_DIR,
                # Wichtig: `wait_for` statt `upstream_result` für reine Abhängigkeit ohne Datenübergabe
                wait_for=[last_dbt_task_result]
            )
            # Warte auf das Ergebnis und prüfe es
            if not staging_result or not staging_result:
                logger.error("DBT Staging run failed.")
                dbt_step_success = False
            last_dbt_task_result = staging_result # Ergebnis für nächste Abhängigkeit

            # 3.2 Snapshots (nur wenn Staging erfolgreich war)
            if dbt_step_success:
                logger.info("Running DBT Snapshots...")
                snapshot_result = run_dbt_command_runner(
                    dbt_args=["snapshot"],
                    project_dir=DBT_PROJECT_DIR,
                    profiles_dir=DBT_PROFILES_DIR,
                    wait_for=[last_dbt_task_result] # Hängt vom Staging ab
                )
                if not snapshot_result or not snapshot_result:
                    logger.error("DBT Snapshot run failed.")
                    dbt_step_success = False
                last_dbt_task_result = snapshot_result # Ergebnis für nächste Abhängigkeit

            # 3.3 Intermediate & Marts Models (nur wenn Snapshots erfolgreich waren)
            if dbt_step_success:
                logger.info("Running DBT Intermediate and Marts models...")
                marts_result = run_dbt_command_runner(
                    # Selektiere Intermediate/Marts und deren Abhängigkeiten (+)
                    # Wichtig: Pfade müssen existieren, sonst Fehler
                    dbt_args=["run", "--select", "+models/intermediate", "+models/marts"],
                    project_dir=DBT_PROJECT_DIR,
                    profiles_dir=DBT_PROFILES_DIR,
                    wait_for=[last_dbt_task_result] # Hängt von Snapshots ab
                )
                if not marts_result or not marts_result:
                    logger.error("DBT Intermediate/Marts run failed.")
                    dbt_step_success = False
                last_dbt_task_result = marts_result # Letztes Ergebnis

            # Setze finalen DBT-Erfolgsstatus
            dbt_success = dbt_step_success

            # 4. Dateien archivieren (nur wenn ALLE DBT Schritte erfolgreich waren)
            if dbt_success:
                 logger.info("All DBT steps successfully completed. Archiving files...")
                 archive_processed_files(
                     processed_files=active_new_files, # Verwende die explizite Liste
                     bucket=MINIO_BUCKET,
                     staging_prefix=CDC_STAGING_PREFIX,
                     archive_prefix=CDC_ARCHIVE_PREFIX,
                     minio_endpoint=MINIO_RAW_ENDPOINT,
                     wait_for=[last_dbt_task_result] # Hängt vom letzten DBT Schritt ab
                 )
            else:
                logger.error("One or more DBT steps failed. Files will NOT be archived.")
        else:
            # Fall: Laden der Daten fehlgeschlagen (oder Task hat False zurückgegeben)
            if load_success and not load_success:
                logger.info("Data loading task returned False (likely no new files processed or error during load). Skipping DBT and Archiving.")
            else: # load_success ist None oder hat Exception geworfen
                logger.error("Data loading into DuckDB failed. Skipping DBT and Archiving.")
    else:
        # Fall: Keine neuen Dateien gefunden
        logger.info("Keine neuen Dateien gefunden, Flow wird beendet.")

    logger.info("CDC MinIO zu DuckDB Flow beendet.")

