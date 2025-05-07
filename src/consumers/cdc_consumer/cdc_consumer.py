# src/consumers/cdc_consumer.py
import os
import sys
import json
import time
from datetime import datetime
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError, KafkaException
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error
from io import BytesIO
import signal
import logging

# --- Konfiguration aus Umgebungsvariablen ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC_PATTERN = os.getenv('KAFKA_TOPIC_PATTERN', '^cdc\\.oltp_dabi\\.public\\.(.*)')
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'dabi2-minio-lake-writer')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'datalake')
MINIO_USE_SSL = os.getenv('MINIO_USE_SSL', 'false').lower() == 'true'

# Batching-Konfiguration
WRITE_INTERVAL_SECONDS = 20  # Geändert auf 60 Sekunden
# MAX_MESSAGES_PER_TABLE_BATCH = 5000 # Optional: Maximale Nachrichten pro Tabelle pro Batch
                                   # Wenn diese Größe erreicht ist, schreiben, auch wenn 60s noch nicht um sind.
                                   # Vorerst konzentrieren wir uns auf das Zeitintervall.

# --- Globaler Zustand ---
message_buffer = defaultdict(list)  # Puffer: { 'topic_name': [msg_payload, msg_payload,...] }
last_write_attempt_time = time.time() # Zeit des letzten Schreibversuchs
running = True
# Logging konfigurieren (ersetzt die meisten print-Anweisungen)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- MinIO Client Initialisierung ---
def get_minio_client():
    if not MINIO_SECRET_KEY:
        print("FEHLER: MINIO_SECRET_KEY nicht gesetzt!")
        sys.exit(1)
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_USE_SSL
        )
        found = client.bucket_exists(MINIO_BUCKET)
        if not found:
            print(f"MinIO Bucket '{MINIO_BUCKET}' nicht gefunden. Versuche zu erstellen...")
            client.make_bucket(MINIO_BUCKET)
            print(f"MinIO Bucket '{MINIO_BUCKET}' erstellt.")
        else:
            print(f"Verbunden mit MinIO, Bucket '{MINIO_BUCKET}' gefunden.")
        return client
    except Exception as e:
        print(f"FEHLER bei MinIO Initialisierung: {e}", exc_info=True)
        sys.exit(1)

# --- Kafka Consumer Initialisierung ---
def get_kafka_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False  # Wichtig für manuelles Commit-Management
    }
    try:
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC_PATTERN]) # Topic-Pattern für Subscription
        print(f"Kafka Consumer erstellt, subscribed auf Pattern: {KAFKA_TOPIC_PATTERN}")
        return consumer
    except Exception as e:
        print(f"FEHLER bei Kafka Consumer Initialisierung: {e}", exc_info=True)
        sys.exit(1)

def write_batches_to_minio(client: Minio):
    global message_buffer, last_write_attempt_time

    # Zeit des aktuellen Schreibversuchs festhalten
    last_write_attempt_time = time.time()

    if not any(message_buffer.values()): # Prüfen, ob überhaupt Nachrichten im Puffer sind
        print("Schreibzyklus ausgelöst, aber Puffer ist leer.")
        return True # Nichts zu schreiben, gilt als Erfolg für die Commit-Logik

    print(f"({datetime.now()}) Starte Schreibvorgang für Batches nach MinIO...")
    all_writes_in_cycle_successful = True

    # Iteriere über eine Kopie der Topic-Namen, falls sich message_buffer währenddessen ändern sollte (unwahrscheinlich hier)
    for topic in list(message_buffer.keys()):
        # Hole alle aktuell für dieses Topic gepufferten Nachrichten für diesen Batch
        payloads_for_this_batch = message_buffer[topic][:] # Erstellt eine Kopie der Liste

        if not payloads_for_this_batch:
            continue # Sollte nicht passieren, wenn any(message_buffer.values()) wahr war, aber sicher ist sicher

        table_name = topic.split('.')[-1]
        print(f"Verarbeite Batch für Tabelle '{table_name}' ({len(payloads_for_this_batch)} Nachrichten)...")

        try:
            records_to_write = []
            processing_time_for_batch = datetime.now() # Konsistenter Zeitstempel für diesen Batch

            for payload in payloads_for_this_batch:
                if payload is None:
                    continue
                record_data = payload.copy()
                actual_op = record_data.get('__op')
                actual_ts_ms = record_data.get('__ts_ms')
                record_data['_op'] = actual_op
                record_data['_ts_ms'] = actual_ts_ms
                record_data.pop('__op', None)
                record_data.pop('__ts_ms', None)
                record_data.pop('__deleted', None)
                record_data['_kafka_topic'] = topic
                record_data['_processing_ts'] = processing_time_for_batch.isoformat()
                record_data['year'] = processing_time_for_batch.year
                record_data['month'] = f"{processing_time_for_batch.month:02d}"
                record_data['day'] = f"{processing_time_for_batch.day:02d}"
                records_to_write.append(record_data)

            if not records_to_write:
                logger.warning(f"Keine validen Records für Tabelle '{table_name}' in diesem Batch ({len(payloads_for_this_batch)} Eingangs-Msgs).")
                # Da keine Datei geschrieben wird, aber die Nachrichten "verarbeitet" wurden (als leer befunden),
                # entfernen wir sie aus dem Puffer, um Endlosschleifen zu vermeiden.
                message_buffer[topic] = message_buffer[topic][len(payloads_for_this_batch):]
                if not message_buffer[topic]:
                    del message_buffer[topic]
                continue

            df = pd.DataFrame(records_to_write)
            if '_ts_ms' in df.columns:
                df['_ts_ms'] = pd.to_numeric(df['_ts_ms'], errors='coerce').astype(pd.Int64Dtype())
            if '_op' in df.columns:
                df['_op'] = df['_op'].astype(str)
            if 'year' in df.columns: df['year'] = df['year'].astype(int)
            if 'month' in df.columns: df['month'] = df['month'].astype(str)
            if 'day' in df.columns: df['day'] = df['day'].astype(str)

            file_timestamp = processing_time_for_batch.strftime('%Y%m%d_%H%M%S_%f')
            # Verwende die Partitionsspalten, die den Records hinzugefügt wurden
            year_part = records_to_write[0]['year']
            month_part = records_to_write[0]['month']
            day_part = records_to_write[0]['day']
            object_name = f"cdc_events/{table_name}/year={year_part}/month={month_part}/day={day_part}/{table_name}_{file_timestamp}.parquet"

            print(f"Schreibe DataFrame ({len(df)} Zeilen) nach MinIO: {MINIO_BUCKET}/{object_name}")
            out_buffer = BytesIO()
            df.to_parquet(out_buffer, index=False, engine='pyarrow', compression='snappy')
            out_buffer.seek(0)

            client.put_object(
                MINIO_BUCKET,
                object_name,
                data=out_buffer,
                length=out_buffer.getbuffer().nbytes,
                content_type='application/parquet'
            )
            print(f"Upload für {object_name} erfolgreich.")
            # Nachrichten aus dem Puffer entfernen, die erfolgreich geschrieben wurden
            message_buffer[topic] = message_buffer[topic][len(payloads_for_this_batch):]
            if not message_buffer[topic]: # Wenn Liste jetzt leer ist
                del message_buffer[topic] # Topic-Eintrag aus Puffer entfernen

        except Exception as e_batch:
            print(f"FEHLER beim Verarbeiten/Upload des Batches für Topic {topic}: {e_batch}", exc_info=True)
            all_writes_in_cycle_successful = False
            # Nachrichten bleiben im Puffer für diesen Topic und werden beim nächsten Mal erneut versucht

    print(f"({datetime.now()}) Schreibzyklus beendet. Erfolg aller Topics in diesem Zyklus: {all_writes_in_cycle_successful}")
    return all_writes_in_cycle_successful

# --- Graceful Shutdown Handler ---
def shutdown_handler(signum, frame):
    global running
    print(f"Signal {signal.Signals(signum).name} empfangen. Beende Consumer...")
    running = False

# --- Haupt-Schleife ---
if __name__ == "__main__":
    print("Initialisiere Kafka Consumer und MinIO Client...")
    consumer = get_kafka_consumer()
    minio_client = get_minio_client()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    print("Consumer gestartet. Warte auf Nachrichten (Strg+C zum Beenden)...")

    try:
        while running:
            # 1. Nachrichten konsumieren und puffern
            msg = consumer.poll(timeout=1.0) # Poll für 1 Sekunde, dann zur Logik unten

            if msg is None:
                # Keine neue Nachricht in diesem Poll-Intervall, fahre fort, um Schreibbedingungen zu prüfen
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Ende der Partition für dieses Poll (normal bei CDC-Streams, wenn gerade nichts kommt)
                    pass
                else:
                    print(f"KAFKA ERROR: {msg.error()}")
                    time.sleep(5) # Kurze Pause bei Kafka-Fehler
            else:
                # Nachricht erfolgreich empfangen
                try:
                    value = msg.value().decode('utf-8')
                    data = json.loads(value)
                    payload = data.get('payload', {}) # Debezium oft mit 'payload'
                    if payload is None: payload = {}

                    topic_name = msg.topic()
                    message_buffer[topic_name].append(payload)
                    # logger.debug(f"Nachricht zu Puffer für Topic {topic_name} hinzugefügt. Aktuelle Puffergröße: {len(message_buffer[topic_name])}")
                except json.JSONDecodeError:
                    logger.warning(f"Konnte Nachricht nicht als JSON dekodieren: {msg.value()}")
                except Exception as e:
                    print(f"FEHLER bei Nachrichtenverarbeitung: {e}", exc_info=True)

            # 2. Schreibbedingungen prüfen (Zeitintervall)
            # Nur schreiben, wenn das Intervall abgelaufen ist UND Nachrichten im Puffer sind.
            current_time = time.time()
            if (current_time - last_write_attempt_time >= WRITE_INTERVAL_SECONDS) and any(message_buffer.values()):
                print(f"Schreibintervall ({WRITE_INTERVAL_SECONDS}s) erreicht und Puffer enthält Daten. Starte Schreibvorgang.")
                
                all_writes_successful = write_batches_to_minio(minio_client)
                # last_write_attempt_time wird innerhalb von write_batches_to_minio aktualisiert

                if all_writes_successful:
                    print("Alle Batches in diesem Zyklus erfolgreich nach MinIO geschrieben. Committe Kafka Offsets.")
                    consumer.commit(asynchronous=False) # Synchrones Commit für Sicherheit
                    print("Offsets committed.")
                else:
                    logger.warning("Einige Batches in diesem Zyklus konnten nicht geschrieben werden. Offsets werden NICHT committed. Fehlgeschlagene Nachrichten bleiben im Puffer.")
            
            # Optional: Fügen Sie hier eine Größen-basierte Batch-Logik hinzu, falls gewünscht
            # elif any(len(msgs) >= MAX_MESSAGES_PER_TABLE_BATCH for msgs in message_buffer.values()):
            #    print("Maximale Batch-Größe für ein Topic erreicht. Starte Schreibvorgang.")
            #    ... (ähnliche Logik wie oben für Schreiben und Committen)


    finally:
        print("Consumer wird heruntergefahren. Versuche verbleibende Nachrichten im Puffer zu schreiben...")
        if any(message_buffer.values()): # Nur wenn noch etwas im Puffer ist
            final_writes_successful = write_batches_to_minio(minio_client)
            if final_writes_successful:
                print("Finale Batches erfolgreich geschrieben. Committe Kafka Offsets.")
                if consumer: # Sicherstellen, dass Consumer noch existiert
                    try:
                        consumer.commit(asynchronous=False)
                        print("Finale Offsets committed.")
                    except Exception as e_commit:
                         print(f"Fehler beim Committen der finalen Offsets: {e_commit}", exc_info=True)
            else:
                logger.warning("Fehler beim Schreiben einiger finaler Batches. Einige Nachrichten wurden möglicherweise nicht committet.")

        if consumer:
            print("Schließe Kafka Consumer...")
            consumer.close()
            print("Kafka Consumer geschlossen.")