import sys
import time
from collections import defaultdict

from . import config 
from .config import get_logger 
from .utils import GracefulKiller
from .kafka_handler import create_kafka_consumer, consume_message, commit_offsets
from .message_processor import transform_payloads_to_dataframe, prepare_dataframe_for_parquet_storage
from .minio_handler import get_minio_client, write_dataframe_to_minio
from .prefect_handler import trigger_prefect_dwh_flow_run_sync

# Globale Zustandsvariablen
message_buffer = defaultdict(list)
last_successful_write_time = time.time()

logger = get_logger(config.APP_NAME) 

def process_and_write_batches(minio_client, kafka_consumer_for_commit):
    """
    Verarbeitet alle Nachrichten im Puffer, schreibt sie nach MinIO und löst Prefect aus.
    Committet Kafka Offsets bei Erfolg.
    """
    global last_successful_write_time, message_buffer

    if not any(message_buffer.values()):
        logger.debug("Keine Nachrichten im Puffer zum Schreiben.")
        last_successful_write_time = time.time()
        return

    logger.info(f"Starte Schreibzyklus. {sum(len(msgs) for msgs in message_buffer.values())} Nachrichten in {len(message_buffer)} Topics im Puffer.")
    all_writes_this_cycle_successful = True
    topics_successfully_processed_this_cycle = set()

    for topic_name in list(message_buffer.keys()):
        payloads_for_topic = message_buffer[topic_name]
        if not payloads_for_topic:
            del message_buffer[topic_name] 
            continue

        table_name = topic_name.split('.')[-1] # Einfache Extraktion, ggf. anpassen
        logger.info(f"Verarbeite Batch für Topic '{topic_name}' (Tabelle: '{table_name}') mit {len(payloads_for_topic)} Nachrichten.")

        # 1. Nachrichten transformieren
        processed_df = transform_payloads_to_dataframe(payloads_for_topic, topic_name)
        if processed_df is None or processed_df.empty:
            logger.info(f"Keine validen Daten für Topic '{topic_name}' nach Transformation. Nachrichten werden aus Puffer entfernt.")
            message_buffer[topic_name] = [] 
            if not message_buffer[topic_name]:
                del message_buffer[topic_name]
            topics_successfully_processed_this_cycle.add(topic_name)
            continue 

        # 2. DataFrame für Parquet vorbereiten (Spalten entfernen, Partitionierungsinfo holen)
        df_for_parquet, partition_info = prepare_dataframe_for_parquet_storage(processed_df)
        if df_for_parquet.empty and partition_info is None: # Zusätzliche Prüfung
             logger.warning(f"DataFrame für Topic '{topic_name}' wurde nach Vorbereitung für Parquet leer. Überspringe Schreibvorgang.")
             message_buffer[topic_name] = []
             if not message_buffer[topic_name]:
                del message_buffer[topic_name]
             topics_successfully_processed_this_cycle.add(topic_name)
             continue

        # 3. Nach MinIO schreiben
        write_ok = write_dataframe_to_minio(minio_client, df_for_parquet, table_name, partition_info)

        if write_ok:
            logger.info(f"Batch für Topic '{topic_name}' erfolgreich nach MinIO geschrieben.")
            message_buffer[topic_name] = [] 
            if not message_buffer[topic_name]: 
                del message_buffer[topic_name] 
            topics_successfully_processed_this_cycle.add(topic_name)

            # 4. Prefect Flow Run triggern (nur wenn Schreiben erfolgreich war)
            logger.info(f"Versuche Prefect Flow für erfolgreichen Upload von Topic '{topic_name}' zu triggern.")
            flow_run_id = trigger_prefect_dwh_flow_run_sync()
            if flow_run_id:
                logger.info(f"Prefect Flow Run für Topic '{topic_name}' getriggert: ID {flow_run_id}")
            else:
                logger.warning(f"Prefect Flow Run für Topic '{topic_name}' konnte NICHT getriggert werden.")
        else:
            logger.error(f"FEHLER beim Schreiben des Batches für Topic '{topic_name}' nach MinIO. Nachrichten bleiben im Puffer.")
            all_writes_this_cycle_successful = False

    # Nach der Verarbeitung aller Topics im Puffer:
    if all_writes_this_cycle_successful and topics_successfully_processed_this_cycle: # Nur committen, wenn etwas erfolgreich war
        logger.info("Alle Schreibvorgänge in diesem Zyklus waren erfolgreich (oder Nachrichten wurden als leer/ungültig korrekt entfernt). Committe Kafka Offsets.")
        if not commit_offsets(kafka_consumer_for_commit):
            logger.error("KRITISCH: Offsets konnten NICHT committed werden, obwohl Schreibvorgänge erfolgreich schienen! Daten könnten erneut verarbeitet werden.")
        else:
            logger.info("Offsets erfolgreich committet.")
    elif not topics_successfully_processed_this_cycle and not any(message_buffer.values()):
        logger.info("Puffer ist nun leer, aber es gab keine explizit erfolgreichen Schreibvorgänge (nur leere/ungültige Nachrichten entfernt). Kein Commit notwendig.")
    else:
        logger.warning("Einige Batches konnten in diesem Zyklus nicht nach MinIO geschrieben werden. Offsets werden NICHT committed. Fehlgeschlagene Nachrichten bleiben im Puffer.")

    last_successful_write_time = time.time() 
    logger.info("Schreibzyklus beendet.")


def run():
    """Hauptfunktion zum Starten des Consumers."""
    logger.info(f"Starte {config.APP_NAME}...")
    logger.info(f"Kafka Server: {config.KAFKA_BOOTSTRAP_SERVERS}, Topic Pattern: {config.KAFKA_TOPIC_PATTERN}, Group ID: {config.CONSUMER_GROUP_ID}")
    logger.info(f"MinIO Endpoint: {config.MINIO_ENDPOINT}, Bucket: {config.MINIO_BUCKET}")
    logger.info(f"Schreibintervall: {config.WRITE_INTERVAL_SECONDS} Sekunden")
    logger.info(f"Log Level: {config.LOG_LEVEL}")

    # Graceful Shutdown Handler initialisieren
    killer = GracefulKiller()

    # Kafka Consumer und MinIO Client initialisieren
    kafka_consumer = create_kafka_consumer()
    minio_client = get_minio_client()

    if not kafka_consumer or not minio_client:
        logger.error("Fehler bei der Initialisierung von Kafka Consumer oder MinIO Client. Anwendung wird beendet.")
        sys.exit(1)

    logger.info("Initialisierung erfolgreich. Starte Konsumationsschleife (Strg+C zum Beenden)...")

    try:
        while not killer.kill_now:
            try:
                # 1. Nachricht konsumieren
                message_added_to_buffer = consume_message(kafka_consumer, message_buffer)

            except Exception as e_consume: 
                logger.error(f"Fehler beim Konsumieren/Verarbeiten einer Kafka-Nachricht: {e_consume}", exc_info=True)
                time.sleep(5)
                continue 

            # 2. Schreibbedingungen prüfen
            current_time = time.time()
            time_since_last_write = current_time - last_successful_write_time

            if (time_since_last_write >= config.WRITE_INTERVAL_SECONDS) and any(message_buffer.values()):
                logger.info(f"Schreibintervall ({config.WRITE_INTERVAL_SECONDS}s) erreicht ({time_since_last_write:.2f}s vergangen) und Puffer enthält Daten.")
                process_and_write_batches(minio_client, kafka_consumer)
            elif not message_added_to_buffer and not any(message_buffer.values()):
                time.sleep(0.1) 

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt in der Hauptschleife empfangen. Beende...")
    except Exception as e_main_loop:
        logger.error(f"Unerwarteter schwerwiegender Fehler in der Hauptschleife: {e_main_loop}", exc_info=True)
    finally:
        logger.info("Consumer wird heruntergefahren. Versuche, verbleibende Nachrichten im Puffer zu schreiben...")
        if any(message_buffer.values()):
            logger.info(f"Es sind noch {sum(len(msgs) for msgs in message_buffer.values())} Nachrichten in {len(message_buffer)} Topics im Puffer.")
            process_and_write_batches(minio_client, kafka_consumer) 
        else:
            logger.info("Keine Nachrichten mehr im Puffer beim Herunterfahren.")

        if kafka_consumer:
            logger.info("Schließe Kafka Consumer...")
            kafka_consumer.close()
            logger.info("Kafka Consumer geschlossen.")

        logger.info(f"{config.APP_NAME} wurde beendet.")