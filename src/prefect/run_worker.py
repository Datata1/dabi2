import asyncio
import sys
import requests
import uuid
from pathlib import Path

from prefect import get_client, deploy
from prefect.server.schemas.actions import WorkPoolCreate
from prefect.exceptions import ObjectNotFound
from prefect.workers.process import ProcessWorker
from prefect.filesystems import LocalFileSystem 
from prefect.deployments.runner import RunnerDeployment

from flows.data_ingestion import data_ingestion_flow as target_flow

# --- Konfiguration --- TODO: hole die konfigurationen aus .env oder utils.config.settings()
WORK_POOL_NAME = "timeseries"
DEPLOYMENT_NAME = "timeseries-data-ingestion"
FLOW_SCRIPT_PATH = Path("./flows/data_ingestion.py") 
FLOW_FUNCTION_NAME = "data_ingestion_flow" 
FLOW_ENTRYPOINT = f"./flows/data_ingestion.py:{FLOW_FUNCTION_NAME}" 
APP_BASE_PATH = Path("/app/ml_service/") 
DEFAULT_BOX_ID = "5faeb5589b2df8001b980304"
INITIAL_FETCH_DAYS = 7
CHUNK_DAYS = 2
INTERVAL_SECONDS = 180


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
    async with get_client() as client:
        # --- Work Pool sicherstellen ---
        await create_or_get_work_pool(client, WORK_POOL_NAME)

        # --- Deployment erstellen/aktualisieren ---
        deployment_params = {
            "box_id": DEFAULT_BOX_ID,
            "initial_fetch_days": INITIAL_FETCH_DAYS,
            "fetch_chunk_days": CHUNK_DAYS,
        }
        schedule_payload = [
            {
                "schedule": {
                    "interval": INTERVAL_SECONDS         
                },
            }
        ]
        deployment_tags = ["ingestion", "opensensemap", "scheduled"]
        deployment_description = f"Holt alle {INTERVAL_SECONDS // 60} Minuten Daten von OpenSenseMap für Box {DEFAULT_BOX_ID}"

        flow_id = await client.create_flow_from_name(FLOW_FUNCTION_NAME)

        # --- Deployment erstellen ---
        deployment = requests.post(
            f"http://prefect:4200/api/deployments",
            json={
                "name": DEPLOYMENT_NAME,
                "flow_id": str(flow_id),  # requests cant handle uuid 
                "work_pool_name": WORK_POOL_NAME,
                "entrypoint": FLOW_ENTRYPOINT,
                "path": str(APP_BASE_PATH),  # reguetsts cant handle Path objects
                "parameter_openapi_schema": deployment_params,
                "parameters": deployment_params,
                "schedules": schedule_payload,
                "tags": deployment_tags,
                "description": deployment_description,
            },
            headers={"Content-Type": "application/json"},
        )

    print(f"Deployment '{DEPLOYMENT_NAME}' (ID: {deployment}) erfolgreich erstellt.")

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

