import asyncio
import sys
import requests
import uuid
from pathlib import Path
import traceback

from prefect import get_client, deploy
from prefect.server.schemas.actions import WorkPoolCreate
from prefect.exceptions import ObjectNotFound
from prefect.workers.process import ProcessWorker
from prefect.filesystems import LocalFileSystem 
from prefect.deployments.runner import RunnerDeployment

from flows.data_pipeline import dwh_pipeline as target_flow
from flows.initial_oltp_load_flow import initial_oltp_load_flow 

# --- Konfiguration --- TODO: hole die konfigurationen aus .env oder utils.config.settings()
WORK_POOL_NAME = "dabi2"
APP_BASE_PATH = Path("/app/prefect/")

# --- Konfiguration für DWH Flow ---
DWH_DEPLOYMENT_NAME = "dwh-pipeline"
DWH_FLOW_SCRIPT_PATH = Path("./flows/data_pipeline.py") 
DWH_FLOW_FUNCTION_NAME = target_flow.__name__ 
DWH_FLOW_ENTRYPOINT = f"./flows/data_pipeline.py:{DWH_FLOW_FUNCTION_NAME}" 
DWH_TAGS = ["dwh"]
DWH_DESCRIPTION = "DWH Pipeline for DABI2"

# --- Konfiguration für Initial OLTP Load Flow ---
OLTP_DEPLOYMENT_NAME = "oltp-initial-load" 
OLTP_FLOW_FUNCTION_NAME = initial_oltp_load_flow.__name__ 
OLTP_FLOW_ENTRYPOINT = f"./flows.initial_oltp_load_flow:{OLTP_FLOW_FUNCTION_NAME}" 
OLTP_TAGS = ["oltp", "initial-load"]
OLTP_DESCRIPTION = "Initial load of static files into OLTP database"
# INTERVAL_SECONDS = 180


async def create_or_get_work_pool(client, name: str):
    print(f"Prüfe Work Pool '{name}'...")
    try:
        pool = await client.read_work_pool(work_pool_name=name)
        print(f"Work Pool '{name}' existiert bereits.")
        return pool
    except ObjectNotFound:
        print(f"Work Pool '{name}' nicht gefunden. Erstelle...")
        try:
            pool_config = WorkPoolCreate(name=name, type="process")
            pool = await client.create_work_pool(work_pool=pool_config)
            print(f"Work Pool '{name}' erstellt.")
            return pool
        except Exception as e:
            print(f"FEHLER: Konnte Work Pool '{name}' nicht erstellen: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response:
                try:
                    error_detail = await e.response.json()
                    print(f"Server Response: {error_detail}", file=sys.stderr)
                except:
                     print(f"Server Response (raw): {await e.response.text()}", file=sys.stderr)
            sys.exit(1)



async def main():
    """Hauptfunktion zum Einrichten und Starten des Prefect Workers via API."""
    oltp_deployment_id_to_trigger = None

    async with get_client() as client:
        # --- Work Pool sicherstellen ---
        await create_or_get_work_pool(client, WORK_POOL_NAME)

        # --- Deployment 1: DWH Pipeline ---
        print(f"\n--- Deploying DWH Flow: {DWH_DEPLOYMENT_NAME} ---")
        try:
            print(f"Ermittle Flow ID für Funktion: {DWH_FLOW_FUNCTION_NAME}")
            dwh_flow_id = await client.create_flow_from_name(DWH_FLOW_FUNCTION_NAME)
            print(f"Flow ID für DWH: {dwh_flow_id}")

            print(f"Sende POST request für DWH Deployment...")
            dwh_deployment_response = requests.post(
                f"http://prefect:4200/api/deployments", # Stelle sicher, dass API URL stimmt
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
                timeout=30 # Timeout hinzufügen
            )
            dwh_deployment_response.raise_for_status() 
            dwh_deployment_data = dwh_deployment_response.json()
            dwh_deployment_id = dwh_deployment_data.get('id')
            if dwh_deployment_id:
                 print(f"DWH Deployment '{DWH_DEPLOYMENT_NAME}' (ID: {dwh_deployment_id}) erfolgreich erstellt/aktualisiert.")
            else:
                 logger.warning(f"DWH Deployment erstellt, aber keine ID in Antwort gefunden: {dwh_deployment_data}")
        except requests.exceptions.RequestException as e:
            logger.error(f"FEHLER bei OLTP Deployment HTTP-Anfrage: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response is not None: print(f"Response Body: {e.response.text}", file=sys.stderr)
            # sys.exit(1) # Ggf. abbrechen
        except Exception as e:
            logger.error(f"FEHLER beim Erstellen/Verarbeiten des OLTP Deployments: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            # sys.exit(1) # Ggf. abbrechen
        
        # --- Deployment 2: Initial OLTP Load ---
        print(f"\n--- Deploying OLTP Initial Load Flow: {OLTP_DEPLOYMENT_NAME} ---")
        try:
            print(f"Ermittle Flow ID für Funktion: {OLTP_FLOW_FUNCTION_NAME}")
            oltp_flow_id = await client.create_flow_from_name(OLTP_FLOW_FUNCTION_NAME)
            print(f"Flow ID for OLTP Load: {oltp_flow_id}")

            print(f"Sende POST request für OLTP Deployment...")
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
                 print(f"OLTP Deployment '{OLTP_DEPLOYMENT_NAME}' (ID: {oltp_deployment_id_to_trigger}) erfolgreich erstellt/aktualisiert.")
            else:
                 logger.error(f"FEHLER: OLTP Deployment erstellt, aber keine ID in Antwort gefunden: {oltp_deployment_data}")
                 # Hier ggf. abbrechen, da der Trigger fehlschlagen wird
                 # sys.exit(1)

        except requests.exceptions.RequestException as e:
            logger.error(f"FEHLER bei DWH Deployment HTTP-Anfrage: {e}", file=sys.stderr)
            if hasattr(e, 'response') and e.response is not None: print(f"Response Body: {e.response.text}", file=sys.stderr)
        except Exception as e:
            logger.error(f"FEHLER beim Erstellen/Verarbeiten des DWH Deployments: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr) # Gibt mehr Details aus

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
    print(f"Starte Worker für Pool '{WORK_POOL_NAME}'...")
    try:
        worker = ProcessWorker(work_pool_name=WORK_POOL_NAME)
        await worker.start() 

    except KeyboardInterrupt:
        print("\nWorker gestoppt.")
        sys.exit(0)
    except Exception as e:
        print(f"Ein unerwarteter Fehler ist beim Starten des Workers aufgetreten: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"Ein kritischer Fehler ist aufgetreten: {e}", file=sys.stderr)
        sys.exit(1)

