import asyncio
import sys
import os
import requests
import uuid
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

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# TODO: hole die konfigurationen aus .env oder utils.config.settings()
# --- Block Konfiguration ---
MINIO_BLOCK_NAME = "minio-credentials" 
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin") 
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minio_secret_password") 

# --- Konfiguration --- 
WORK_POOL_NAME = "dabi2"
APP_BASE_PATH = Path("/app/prefect/")

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

# --- NEUE FUNKTION zum Erstellen des Blocks ---
async def create_or_get_minio_block(block_name: str, access_key: str, secret_key: str):
    """Prüft, ob ein MinIOCredentials Block existiert und erstellt ihn ggf. via .save()."""
    logger.info(f"\n--- Sicherstellen des MinIO Credentials Blocks: {block_name} ---")

    if not access_key or not secret_key:
        logger.error("FEHLER: MinIO Access Key oder Secret Key nicht konfiguriert (z.B. über Env Vars MINIO_ROOT_USER/MINIO_ROOT_PASSWORD). Überspringe Block-Erstellung.")
        return None # Signalisiert, dass der Block nicht verfügbar ist

    try:
        # 1. Versuchen, den Block mit der spezifischen Klasse zu laden
        loaded_block = await MinIOCredentials.load(block_name)
        logger.info(f"Block '{block_name}' vom Typ 'MinIOCredentials' existiert bereits.")
        # Optional: Hier könnte man eine Update-Logik einbauen, falls Credentials geändert wurden
        # loaded_block.minio_root_user = access_key
        # loaded_block.minio_root_password = secret_key # SecretStr behandelt das sicher
        # await loaded_block.save(block_name, overwrite=True)
        # logger.info(f"Block '{block_name}' aktualisiert.")
        return loaded_block

    except ValueError as e:
        # .load() wirft oft ValueError, wenn der Block nicht gefunden wird.
        # Prüfen Sie die Fehlermeldung genauer für Robustheit.
        if "Unable to find block document" in str(e) or f"Block document with name '{block_name}' not found" in str(e):
            logger.info(f"Block '{block_name}' nicht gefunden. Erstelle...")
            try:
                # 2. Block-Instanz mit den spezifischen Attributen erstellen
                minio_block = MinIOCredentials(
                    minio_root_user=access_key,
                    minio_root_password=secret_key
                    # region_name ist optional
                )
                # 3. Block speichern
                # overwrite=False stellt sicher, dass wir keinen existierenden Block überschreiben
                await minio_block.save(block_name, overwrite=False)
                logger.info(f"Block '{block_name}' erfolgreich erstellt.")
                return minio_block # Geben die neu erstellte Instanz zurück
            except Exception as e_create:
                logger.error(f"FEHLER beim Erstellen des Blocks '{block_name}' mit .save(): {e_create}", exc_info=True)
                return None # Signalisiert Fehler
        else:
            # Anderer ValueError beim Laden
            logger.error(f"Unerwarteter ValueError beim Laden des Blocks '{block_name}': {e}", exc_info=True)
            return None
    except Exception as e_load: # Fängt andere Fehler beim Laden ab
        logger.error(f"Unerwarteter Fehler beim Laden des Blocks '{block_name}': {e_load}", exc_info=True)
        return None

async def main():
    """Hauptfunktion zum Einrichten und Starten des Prefect Workers via API."""
    oltp_deployment_id_to_trigger = None

    async with get_client() as client:
        # --- Work Pool sicherstellen ---
        await create_or_get_work_pool(client, WORK_POOL_NAME)

        minio_block = await create_or_get_minio_block(
            block_name=MINIO_BLOCK_NAME,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY
        )
        if not minio_block:
            logger.error("Abbruch aufgrund fehlenden/fehlerhaften MinIO Blocks.")
            sys.exit(1)

        # --- Deployment 1: DWH Pipeline ---
        logger.info(f"\n--- Deploying DWH Flow: {DWH_DEPLOYMENT_NAME} ---")
        try:

            schedule_payload = [
                {
                    "schedule": {
                        "interval": INTERVAL_SECONDS         
                    },
                    "max_scheduled_runs": 1,
                }
            ]
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
                    "schedules": schedule_payload,
                },
                headers={"Content-Type": "application/json"},
                timeout=30 # Timeout hinzufügen
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
            # sys.exit(1) # Ggf. abbrechen
        except Exception as e:
            logger.error(f"FEHLER beim Erstellen/Verarbeiten des OLTP Deployments: {e}", file=sys.stderr)
            traceback.logger.info_exc(file=sys.stderr)
            # sys.exit(1) # Ggf. abbrechen
        
        # --- Deployment 2: Initial OLTP Load ---
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
            # Speichere die ID dieses Deployments für den Trigger
            oltp_deployment_id_to_trigger = oltp_deployment_data.get('id')
            if oltp_deployment_id_to_trigger:
                 logger.info(f"OLTP Deployment '{OLTP_DEPLOYMENT_NAME}' (ID: {oltp_deployment_id_to_trigger}) erfolgreich erstellt/aktualisiert.")
            else:
                 logger.error(f"FEHLER: OLTP Deployment erstellt, aber keine ID in Antwort gefunden: {oltp_deployment_data}")
                 # Hier ggf. abbrechen, da der Trigger fehlschlagen wird
                 # sys.exit(1)

        except requests.exceptions.RequestException as e:
            logger.error(f"FEHLER bei DWH Deployment HTTP-Anfrage: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response is not None: logger.info(f"Response Body: {e.response.text}", file=sys.stderr)
        except Exception as e:
            logger.error(f"FEHLER beim Erstellen/Verarbeiten des DWH Deployments: {e}", file=sys.stderr)
            traceback.logger.info_exc(file=sys.stderr) # Gibt mehr Details aus

    # --- Initialen Flow Run für OLTP Load triggern ---
        print(f"\n--- Triggering Initial OLTP Load Flow Run ---")
        if oltp_deployment_id_to_trigger: 
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
        else:
            logger.error("FEHLER: Kann OLTP Load Flow Run nicht triggern, da Deployment-ID fehlt oder Deployment fehlgeschlagen ist.")

    
    # --- Worker starten ---
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

