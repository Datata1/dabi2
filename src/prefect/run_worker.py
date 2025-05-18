import asyncio
import asyncpg
import sys
import os
import requests
import uuid
from uuid import UUID
from pathlib import Path
import traceback
import logging

from prefect import get_client, deploy
from prefect.server.schemas.actions import WorkPoolCreate
from prefect.exceptions import ObjectNotFound
from prefect.workers.process import ProcessWorker
from prefect.filesystems import LocalFileSystem 
from prefect.deployments.runner import RunnerDeployment
from prefect_aws.credentials import MinIOCredentials

from flows.dwh_pipeline import cdc_minio_to_duckdb_flow as target_flow
from flows.initial_oltp_load_flow import initial_oltp_load_flow 
from flows.debezium_activation_flow import activate_debezium_flow 

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "oltp")
DB_USER = os.getenv("DB_USER", "datata1")
DB_PASSWORD = os.getenv("DB_PASSWORD", "devpassword")


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Block Konfiguration ---
MINIO_BLOCK_NAME = "minio-credentials" 
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin") 
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minio_secret_password") 

# --- Konfiguration --- 
WORK_POOL_NAME = "dabi2"
APP_BASE_PATH = Path("/app/")

# --- Konfiguration für DWH Flow ---
DWH_DEPLOYMENT_NAME = "dwh-pipeline"
DWH_FLOW_SCRIPT_PATH = Path("./flows/dwh_pipeline.py") 
DWH_FLOW_FUNCTION_NAME = target_flow.__name__ 
DWH_FLOW_ENTRYPOINT = f"./flows/dwh_pipeline.py:{DWH_FLOW_FUNCTION_NAME}" 
DWH_TAGS = ["dwh", "bucket", "autoscheduled"]
DWH_DESCRIPTION = "DWH Pipeline"
INTERVAL_SECONDS = 60


# --- Konfiguration für Initial OLTP Load Flow ---
OLTP_DEPLOYMENT_NAME = "oltp-initial-load" 
OLTP_FLOW_FUNCTION_NAME = initial_oltp_load_flow.__name__ 
OLTP_FLOW_ENTRYPOINT = f"./flows.initial_oltp_load_flow:{OLTP_FLOW_FUNCTION_NAME}" 
OLTP_TAGS = ["oltp", "initial-load"]
OLTP_DESCRIPTION = "Initial load of static files into OLTP database"
# INTERVAL_SECONDS = 180

# --- Konfiguration für Debezium Flow ---
DEBEZIUM_DEPLOYMENT_NAME = "activate-debezium-connector"
DEBEZIUM_FLOW_SCRIPT_PATH = Path("./flows/debezium_activation_flow.py") 
DEBEZIUM_FLOW_FUNCTION_NAME = activate_debezium_flow.__name__ 
DEBEZIUM_FLOW_ENTRYPOINT = f"./flows/debezium_activation_flow.py:{DEBEZIUM_FLOW_FUNCTION_NAME}" 
DEBEZIUM_TAGS = ["debezium", "activation"]
DEBEZIUM_DESCRIPTION = "Start Debezium Connector"

async def check_oltp_database_readiness(logger_param: logging.Logger) -> bool:
    logger_param.info("Checking OLTP database readiness for Debezium...")
    tables_to_check = ["aisles", "departments", "order_products", "orders", "products", "users"]
    conn = None
    try:
        conn = await asyncpg.connect(
            user=DB_USER, password=DB_PASSWORD, database=DB_NAME, host=DB_HOST, port=DB_PORT,
            timeout=30
        )
        logger_param.info("Successfully connected to OLTP database.")
        all_tables_ready = True
        for table in tables_to_check:
            try:
                table_exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
                    table
                )
                if not table_exists:
                    logger_param.warning(f"Table 'public.{table}' does not exist.")
                    all_tables_ready = False
                    break

                count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{table}")
                logger_param.info(f"Table 'public.{table}' has {count} rows.")
                if count is None or count == 0:
                    logger_param.warning(f"Table 'public.{table}' is empty.")
                    all_tables_ready = False
                    break
            except Exception as e_table:
                logger_param.error(f"Error checking table 'public.{table}': {e_table}")
                all_tables_ready = False
                break
        
        if all_tables_ready:
            logger_param.info("OLTP database is READY for Debezium (all target tables exist and contain data).")
            return True
        else:
            logger_param.warning("OLTP database is NOT ready for Debezium (one or more tables missing, empty, or error during check).")
            return False
    except Exception as e_conn:
        logger_param.error(f"Failed to connect to or query OLTP database: {e_conn}")
        return False
    finally:
        if conn:
            await conn.close()
            logger_param.info("Database connection closed.")


async def create_or_get_work_pool(client, name: str):
    logger.info(f"Prüfe Work Pool '{name}'...")
    try:
        pool = await client.read_work_pool(work_pool_name=name)
        logger.info(f"Work Pool '{name}' existiert bereits.")
        return pool
    except ObjectNotFound:
        logger.info(f"Work Pool '{name}' nicht gefunden. Erstelle...")
        try:
            pool_config = WorkPoolCreate(name=name, type="process", concurrency_limit=1)
            pool = await client.create_work_pool(work_pool=pool_config)
            logger.info(f"Work Pool '{name}' erstellt.")
            return pool
        except Exception as e:
            logger.error(f"FEHLER: Konnte Work Pool '{name}' nicht erstellen: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response:
                try:
                    error_detail = await e.response.json()
                    logger.error(f"Server Response: {error_detail}", file=sys.stderr)
                except:
                     logger.error(f"Server Response (raw): {await e.response.text()}", file=sys.stderr)
            sys.exit(1)

async def create_or_get_minio_block(block_name: str, access_key: str, secret_key: str):
    """Prüft, ob ein MinIOCredentials Block existiert und erstellt ihn ggf. via .save()."""
    logger.info(f"\n--- Sicherstellen des MinIO Credentials Blocks: {block_name} ---")

    if not access_key or not secret_key:
        logger.error("FEHLER: MinIO Access Key oder Secret Key nicht konfiguriert (z.B. über Env Vars MINIO_ROOT_USER/MINIO_ROOT_PASSWORD). Überspringe Block-Erstellung.")
        return None 

    try:
        loaded_block = await MinIOCredentials.load(block_name)
        logger.info(f"Block '{block_name}' vom Typ 'MinIOCredentials' existiert bereits.")
        return loaded_block

    except ValueError as e:
        if "Unable to find block document" in str(e) or f"Block document with name '{block_name}' not found" in str(e):
            logger.info(f"Block '{block_name}' nicht gefunden. Erstelle...")
            try:
                minio_block = MinIOCredentials(
                    minio_root_user=access_key,
                    minio_root_password=secret_key
                )
                await minio_block.save(block_name, overwrite=False)
                logger.info(f"Block '{block_name}' erfolgreich erstellt.")
                return minio_block 
            except Exception as e_create:
                logger.error(f"FEHLER beim Erstellen des Blocks '{block_name}' mit .save(): {e_create}", exc_info=True)
                return None 
        else:
            logger.error(f"Unerwarteter ValueError beim Laden des Blocks '{block_name}': {e}", exc_info=True)
            return None
    except Exception as e_load: 
        logger.error(f"Unerwarteter Fehler beim Laden des Blocks '{block_name}': {e_load}", exc_info=True)
        return None

async def main():
    """Hauptfunktion zum Einrichten und Starten des Prefect Workers via API."""
    oltp_deployment_id_to_trigger = None

    async with get_client() as client:
        await create_or_get_work_pool(client, WORK_POOL_NAME)

        minio_block = await create_or_get_minio_block(
            block_name=MINIO_BLOCK_NAME,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY
        )
        if not minio_block:
            logger.error("Abbruch aufgrund fehlenden/fehlerhaften MinIO Blocks.")
            sys.exit(1)

        logger.info(f"\n--- Deploying DWH Flow: {DWH_DEPLOYMENT_NAME} ---")
        try:
            logger.info(f"Ermittle Flow ID für Funktion: {DWH_FLOW_FUNCTION_NAME}")
            dwh_flow_id = await client.create_flow_from_name(DWH_FLOW_FUNCTION_NAME)
            logger.info(f"Flow ID für DWH: {dwh_flow_id}")

            logger.info(f"Sende POST request für DWH Deployment...")
            dwh_deployment_response = requests.post(
                f"http://prefect:4200/api/deployments", 
                json={
                    "name": DWH_DEPLOYMENT_NAME,
                    "flow_id": str(dwh_flow_id),
                    "work_pool_name": WORK_POOL_NAME,
                    "entrypoint": DWH_FLOW_ENTRYPOINT,
                    "enforce_parameter_schema": False,
                    "path": str(APP_BASE_PATH),
                    "tags": DWH_TAGS,
                    "description": DWH_DESCRIPTION,
                },
                headers={"Content-Type": "application/json"},
                timeout=30 
            )
            dwh_deployment_response.raise_for_status() 
            dwh_deployment_data = dwh_deployment_response.json()
            dwh_deployment_id = dwh_deployment_data.get('id')
            if dwh_deployment_id:
                 logger.info(f"DWH Deployment '{DWH_DEPLOYMENT_NAME}' (ID: {dwh_deployment_id}) erfolgreich erstellt/aktualisiert.")
            else:
                 logger.warning(f"DWH Deployment erstellt, aber keine ID in Antwort gefunden: {dwh_deployment_data}")
        except requests.exceptions.RequestException as e:
            logger.error(f"FEHLER bei OLTP Deployment HTTP-Anfrage: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response is not None: logger.info(f"Response Body: {e.response.text}", file=sys.stderr)
        except Exception as e:
            logger.error(f"FEHLER beim Erstellen/Verarbeiten des OLTP Deployments: {e}", file=sys.stderr)
            traceback.logger.info_exc(file=sys.stderr)
        
        logger.info(f"\n--- Überprüfe OLTP-Datenbankstatus für Startaktion ---")
        db_is_populated = await check_oltp_database_readiness(logger)
        print(f"status: {db_is_populated}")
        
        logger.info(f"\n--- Deploying OLTP Initial Load Flow: {OLTP_DEPLOYMENT_NAME} ---")
        try:
            logger.info(f"Ermittle Flow ID für Funktion: {OLTP_FLOW_FUNCTION_NAME}")
            oltp_flow_id = await client.create_flow_from_name(OLTP_FLOW_FUNCTION_NAME)
            logger.info(f"Flow ID for OLTP Load: {oltp_flow_id}")

            logger.info(f"Sende POST request für OLTP Deployment...")
            oltp_deployment_response = requests.post(
                f"http://prefect:4200/api/deployments",
                json={
                    "name": OLTP_DEPLOYMENT_NAME,
                    "flow_id": str(oltp_flow_id),
                    "work_pool_name": WORK_POOL_NAME,
                    "entrypoint": OLTP_FLOW_ENTRYPOINT,
                    "enforce_parameter_schema": False,
                    "path": str(APP_BASE_PATH),
                    "tags": OLTP_TAGS,
                    "description": OLTP_DESCRIPTION,
                },
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            oltp_deployment_response.raise_for_status()
            oltp_deployment_data = oltp_deployment_response.json()
            oltp_deployment_id_to_trigger = oltp_deployment_data.get('id')
            if oltp_deployment_id_to_trigger:
                 logger.info(f"OLTP Deployment '{OLTP_DEPLOYMENT_NAME}' (ID: {oltp_deployment_id_to_trigger}) erfolgreich erstellt/aktualisiert.")
            else:
                 logger.error(f"FEHLER: OLTP Deployment erstellt, aber keine ID in Antwort gefunden: {oltp_deployment_data}")
        except requests.exceptions.RequestException as e:
            logger.error(f"FEHLER bei DWH Deployment HTTP-Anfrage: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response is not None: logger.info(f"Response Body: {e.response.text}", file=sys.stderr)
        except Exception as e:
            logger.error(f"FEHLER beim Erstellen/Verarbeiten des DWH Deployments: {e}", file=sys.stderr)
            traceback.logger.info_exc(file=sys.stderr) # Gibt mehr Details aus

         # --- Deployment 3: Debezium ---
        logger.info(f"\n--- Deploying DEBEZIUM Flow: {DEBEZIUM_DEPLOYMENT_NAME} ---")
        try:
            logger.info(f"Ermittle Flow ID für Funktion: {DEBEZIUM_FLOW_FUNCTION_NAME}")
            debezium_flow_id = await client.create_flow_from_name(DEBEZIUM_FLOW_FUNCTION_NAME)
            logger.info(f"Flow ID for DEBEZIUM Load: {debezium_flow_id}")

            logger.info(f"Sende POST request für DEBEZIUM Deployment...")
            debezium_deployment_response = requests.post(
                f"http://prefect:4200/api/deployments",
                json={
                    "name": DEBEZIUM_DEPLOYMENT_NAME,
                    "flow_id": str(debezium_flow_id),
                    "work_pool_name": WORK_POOL_NAME,
                    "entrypoint": DEBEZIUM_FLOW_ENTRYPOINT,
                    "enforce_parameter_schema": False,
                    "path": str(APP_BASE_PATH),
                    "tags": DEBEZIUM_TAGS,
                    "description": DEBEZIUM_DESCRIPTION,
                },
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            debezium_deployment_response.raise_for_status()
            debezium_deployment_data = debezium_deployment_response.json()
            debezium_deployment_id_to_trigger = debezium_deployment_data.get('id')
            if debezium_deployment_id_to_trigger:
                 logger.info(f"debezium Deployment '{DEBEZIUM_DEPLOYMENT_NAME}' (ID: {debezium_deployment_id_to_trigger}) erfolgreich erstellt/aktualisiert.")
            else:
                 logger.error(f"FEHLER: debezium Deployment erstellt, aber keine ID in Antwort gefunden: {debezium_deployment_data}")
        except requests.exceptions.RequestException as e:
            logger.error(f"FEHLER bei DWH Deployment HTTP-Anfrage: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response is not None: logger.info(f"Response Body: {e.response.text}", file=sys.stderr)
        except Exception as e:
            logger.error(f"FEHLER beim Erstellen/Verarbeiten des DWH Deployments: {e}", file=sys.stderr)
            traceback.logger.info_exc(file=sys.stderr) 

        if not db_is_populated: 
            try:
                print(f"Triggere Flow Run für OLTP Deployment ID: {oltp_deployment_id_to_trigger}...")
                flow_run = await client.create_flow_run_from_deployment(
                    deployment_id=oltp_deployment_id_to_trigger, 
                    name="initial-oltp-load-run", 
                )
                print(f"Flow Run für OLTP Load (ID: {flow_run.id}) erfolgreich getriggert.")
            except Exception as e_run:
                logger.error(f"FEHLER beim Triggern des OLTP Load Flow Runs für Deployment {oltp_deployment_id_to_trigger}: {e_run}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
        elif db_is_populated:
            try:
                print(f"Triggere Flow Run für DEBZIUM Deployment ID: {debezium_deployment_id_to_trigger}...")
                flow_run = await client.create_flow_run_from_deployment(
                    deployment_id=debezium_deployment_id_to_trigger, 
                    name="debezium-activation-run", 
                )
                print(f"Flow Run für OLTP Load (ID: {flow_run.id}) erfolgreich getriggert.")
            except Exception as e_run:
                logger.error(f"FEHLER beim Triggern des OLTP Load Flow Runs für Deployment {debezium_deployment_id_to_trigger}: {e_run}", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

    
    logger.info(f"Starte Worker für Pool '{WORK_POOL_NAME}'...")
    try:
        worker = ProcessWorker(work_pool_name=WORK_POOL_NAME)
        await worker.start() 

    except KeyboardInterrupt:
        logger.info("\nWorker gestoppt.")
        sys.exit(0)
    except Exception as e:
        logger.info(f"Ein unerwarteter Fehler ist beim Starten des Workers aufgetreten: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.info(f"Ein kritischer Fehler ist aufgetreten: {e}", file=sys.stderr)
        sys.exit(1)

