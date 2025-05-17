import asyncio
import logging
from datetime import datetime
from prefect import get_client, exceptions as prefect_exceptions 

from . import config 
from .config import get_logger

logger = get_logger(__name__)

async def _trigger_prefect_dwh_flow_run_async_internal():
    """
    Interne asynchrone Funktion zum Triggern eines Prefect Flow Runs.
    """
    try:
        async with get_client() as client:
            flow_name_on_prefect = config.PREFECT_FLOW_NAME
            deployment_name_on_prefect = config.PREFECT_DEPLOYMENT_NAME
            prefect_deployment_identifier = f"{flow_name_on_prefect}/{deployment_name_on_prefect}"

            logger.info(f"Versuche, Prefect Deployment Details für '{prefect_deployment_identifier}' abzurufen...")
            try:
                deployment = await client.read_deployment_by_name(name=prefect_deployment_identifier)
            except prefect_exceptions.ObjectNotFound:
                logger.error(f"Prefect Deployment '{prefect_deployment_identifier}' NICHT GEFUNDEN.")
                return None
            except Exception as e_read: 
                 logger.error(f"Fehler beim Lesen des Prefect Deployments '{prefect_deployment_identifier}': {e_read}", exc_info=True)
                 return None


            if not deployment: 
                logger.error(f"Prefect Deployment '{prefect_deployment_identifier}' nicht gefunden (zweite Prüfung).")
                return None

            deployment_id_to_trigger = deployment.id
            flow_run_name = f"dwh-run-from-cdc-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}"

            logger.info(f"Triggere Prefect DWH Flow Run von Deployment ID: {deployment_id_to_trigger} (Name: '{prefect_deployment_identifier}') mit Flow Run Name: '{flow_run_name}'")

            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment_id_to_trigger,
                name=flow_run_name,
            )
            logger.info(f"Prefect DWH Flow Run (ID: {flow_run.id}, Name: '{flow_run.name}') erfolgreich getriggert.")
            return flow_run.id

    except prefect_exceptions.PrefectHTTPStatusError as e_http:
        logger.error(f"Prefect HTTP Fehler: Status {e_http.response.status_code} - {e_http.response.text}", exc_info=True)
        return None
    except ConnectionRefusedError:
        logger.error("Prefect API nicht erreichbar (ConnectionRefusedError). Läuft der Prefect Server/Agent?", exc_info=True)
        return None
    except Exception as e: 
        logger.error(f"Unerwarteter Fehler in _trigger_prefect_dwh_flow_run_async_internal: {e}", exc_info=True)
        return None

def trigger_prefect_dwh_flow_run_sync() -> str | None:
    """
    Synchroner Wrapper zum Triggern des Prefect DWH Flow Runs.
    Diese Funktion kann aus dem synchronen Haupt-Thread aufgerufen werden.
    """
    logger.info("Versuche, Prefect DWH Flow Run synchron zu triggern...")
    try:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        flow_run_id = loop.run_until_complete(_trigger_prefect_dwh_flow_run_async_internal())

        if flow_run_id:
            logger.info(f"Prefect DWH Flow Run wurde erfolgreich getriggert mit ID: {flow_run_id}.")
        else:
            logger.warning("Prefect DWH Flow Run konnte nicht getriggert werden (Details siehe vorherige Logs).")
        return flow_run_id

    except Exception as e_sync_wrapper:
        logger.error(f"Fehler im synchronen Wrapper für Prefect Trigger: {e_sync_wrapper}", exc_info=True)
        return None