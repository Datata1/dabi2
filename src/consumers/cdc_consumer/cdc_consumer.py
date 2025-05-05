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
# Topic-Pattern, um alle Tabellen aus dem public Schema zu lesen
KAFKA_TOPIC_PATTERN = os.getenv('KAFKA_TOPIC_PATTERN', '^cdc\\.oltp_dabi\\.public\\.(.*)') # Regex!
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'dabi2-minio-lake-writer')

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000') 
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY') 
MINIO_BUCKET = os.getenv('MINIO_BUCKET', 'datalake') 
MINIO_USE_SSL = os.getenv('MINIO_USE_SSL', 'false').lower() == 'true'

# Batching-Konfiguration
WRITE_INTERVAL_SECONDS = 60 
MAX_BUFFER_SIZE = 1000 

# --- Globaler Zustand ---
message_buffer = defaultdict(list) # Puffer: { 'table_name': [msg_payload, msg_payload,...] }
last_write_time = time.time()
running = True 
logger = logging.getLogger(__name__)

# --- MinIO Client Initialisierung ---
def get_minio_client():
    if not MINIO_SECRET_KEY:
        logger.info("FEHLER: MINIO_SECRET_KEY nicht gesetzt!", file=sys.stderr)
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
            logger.info(f"MinIO Bucket '{MINIO_BUCKET}' nicht gefunden. Versuche zu erstellen...", file=sys.stderr)
            client.make_bucket(MINIO_BUCKET)
            logger.info(f"MinIO Bucket '{MINIO_BUCKET}' erstellt.")
        else:
            logger.info(f"Verbunden mit MinIO, Bucket '{MINIO_BUCKET}' gefunden.")
        return client
    except Exception as e:
        logger.info(f"FEHLER bei MinIO Initialisierung: {e}", file=sys.stderr)
        sys.exit(1)

# --- Kafka Consumer Initialisierung ---
def get_kafka_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': CONSUMER_GROUP_ID,
        'auto.offset.reset': 'earliest', 
        'enable.auto.commit': False 
    }
    try:
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC_PATTERN]) 
        logger.info(f"Kafka Consumer erstellt, subscribed auf Pattern: {KAFKA_TOPIC_PATTERN}")
        return consumer
    except Exception as e:
        logger.info(f"FEHLER bei Kafka Consumer Initialisierung: {e}", file=sys.stderr)
        sys.exit(1)

def write_batches_to_minio(client: Minio):
    global message_buffer, last_write_time
    # Verwende das konfigurierte Logging
    logger.info(f"({datetime.now()}) Checking if batches need to be written...")
    processed_topics_in_batch = [] # Track successfully processed topics for buffer clearing

    # Iteriere über Kopie der Items, um Dictionary während Iteration zu ändern
    for topic, payloads in list(message_buffer.items()):
        if not payloads:
            continue

        table_name = topic.split('.')[-1]
        logger.info(f"Processing batch for table '{table_name}' ({len(payloads)} messages)...")

        try:
            records_to_write = []
            for payload in payloads: # payload ist z.B.: {'aisle_id': ..., '__op': 'c', ...}
                if payload is None:
                    continue

                # Kopiere das Payload, das Tabellendaten und __op, __ts_ms enthält
                record_data = payload.copy()

                # --- CORE FIX: Extrahiere __op und __ts_ms, weise _op, _ts_ms zu ---
                actual_op = record_data.get('__op')
                actual_ts_ms = record_data.get('__ts_ms')

                # Weise den Zielspalten zu (z.B. mit einfachem Unterstrich)
                record_data['_op'] = actual_op
                record_data['_ts_ms'] = actual_ts_ms
                # --------------------------------------------------------------------

                # Entferne die ursprünglichen Felder mit doppeltem Unterstrich
                record_data.pop('__op', None)
                record_data.pop('__ts_ms', None)

                # Entferne __deleted, falls vorhanden und nicht benötigt
                record_data.pop('__deleted', None)

                # Füge andere Metadaten hinzu
                record_data['_kafka_topic'] = topic
                record_data['_processing_ts'] = datetime.now().isoformat()

                # Füge Partitionsspalten hinzu
                now_dt = datetime.now()
                record_data['year'] = now_dt.year
                record_data['month'] = f"{now_dt.month:02d}"
                record_data['day'] = f"{now_dt.day:02d}"

                records_to_write.append(record_data)

            if not records_to_write:
                 logger.warning(f"No valid records derived for table '{table_name}' in this batch.")
                 processed_topics_in_batch.append(topic) # Als verarbeitet markieren
                 continue

            # Erstelle DataFrame
            df = pd.DataFrame(records_to_write)

            # --- Wichtig: Typkonvertierung für korrekte Parquet/DB-Typen ---
            if '_ts_ms' in df.columns:
                 # Konvertiere zu numerisch, erzwinge Fehler zu NaT/NaN, dann zu Int64 (Nullable Integer)
                 df['_ts_ms'] = pd.to_numeric(df['_ts_ms'], errors='coerce').astype(pd.Int64Dtype())
            if '_op' in df.columns:
                 df['_op'] = df['_op'].astype(str) # 'c', 'u', 'd', 'r' als String

            # Konvertiere Partitionsspalten
            if 'year' in df.columns: df['year'] = df['year'].astype(int)
            if 'month' in df.columns: df['month'] = df['month'].astype(str)
            if 'day' in df.columns: df['day'] = df['day'].astype(str)
            # Fügen Sie hier bei Bedarf weitere Typkonvertierungen für Ihre Tabellenspalten hinzu

            # --- Pfad erstellen und Parquet schreiben ---
            last_record = records_to_write[-1]
            year_part = last_record.get('year', datetime.now().year)
            month_part = last_record.get('month', f"{datetime.now().month:02d}")
            day_part = last_record.get('day', f"{datetime.now().day:02d}")
            file_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
            object_name = f"cdc_events/{table_name}/year={year_part}/month={month_part}/day={day_part}/{table_name}_{file_timestamp}.parquet"

            logger.info(f"Writing DataFrame ({len(df)} rows, Columns: {list(df.columns)}) to MinIO: {MINIO_BUCKET}/{object_name}")

            out_buffer = BytesIO()
            try:
                # Schreibe DataFrame in den Buffer
                df.to_parquet(out_buffer, index=False, engine='pyarrow', compression='snappy')
            except Exception as e_parquet:
                logger.error(f"FEHLER beim Erstellen der Parquet-Datei für {topic}: {e_parquet}", exc_info=True)
                continue # Überspringe Upload für diesen Batch

            out_buffer.seek(0)

            # --- Upload nach MinIO ---
            try:
                client.put_object(
                    MINIO_BUCKET,
                    object_name,
                    data=out_buffer,
                    length=out_buffer.getbuffer().nbytes,
                    content_type='application/parquet'
                )
                logger.info(f"Upload for {object_name} successful.")
                # Markiere Topic als erfolgreich verarbeitet
                processed_topics_in_batch.append(topic)
            except S3Error as e_s3:
                logger.error(f"FEHLER beim Upload nach MinIO für {topic} ({object_name}): {e_s3}", exc_info=True)
                # Nicht als verarbeitet markieren, wird beim nächsten Mal erneut versucht

        except Exception as e_batch:
            logger.error(f"FEHLER beim Verarbeiten des Batches für {topic}: {e_batch}", exc_info=True)

    # --- Buffer nur für erfolgreich verarbeitete Topics leeren ---
    if processed_topics_in_batch:
        logger.info(f"Clearing buffer for successfully processed topics: {processed_topics_in_batch}")
        for topic in processed_topics_in_batch:
            if topic in message_buffer:
                # Sicherstellen, dass wir keine Race Condition haben (unwahrscheinlich hier)
                try:
                    del message_buffer[topic] # Entferne den Eintrag sicher
                except KeyError:
                    pass # Topic wurde möglicherweise schon entfernt
    else:
         logger.debug("No topics were successfully processed in this write cycle.") # Debug Level

    last_write_time = time.time() 

# --- Graceful Shutdown Handler ---
def shutdown_handler(signum, frame):
    global running
    logger.info(f"\nSignal {signal.Signals(signum).name} empfangen. Beende Consumer...")
    running = False

# --- Haupt-Schleife ---
if __name__ == "__main__":
    logger.info("Initialisiere Kafka Consumer und MinIO Client...")
    consumer = get_kafka_consumer()
    minio_client = get_minio_client()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    logger.info("Consumer gestartet. Warte auf Nachrichten (Strg+C zum Beenden)...")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Kein Fehler, nur keine Nachricht -> Prüfe, ob Batch geschrieben werden soll
                if time.time() - last_write_time >= WRITE_INTERVAL_SECONDS:
                    write_batches_to_minio(minio_client)
                continue # Zurück zum Pollen

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Ende der Partition erreicht (passiert bei nicht-kontinuierlichen Topics)
                    # logger.info(f"Ende der Partition erreicht: {msg.topic()} [{msg.partition()}]")
                    pass # Einfach ignorieren für CDC-Streams
                elif msg.error():
                    logger.info(f"KAFKA ERROR: {msg.error()}", file=sys.stderr)
                    # Schwerwiegender Fehler? Ggf. beenden oder pausieren.
                    # running = False # Beispiel: Bei Fehler beenden
                    time.sleep(5) # Kurz warten bei Fehler
                continue

            # Nachricht erfolgreich empfangen
            try:
                # Annahme: Nachricht ist JSON von Debezium
                value = msg.value().decode('utf-8')
                data = json.loads(value)

                # Debezium Nachricht hat oft eine 'payload'
                payload = data.get('payload', {})
                if payload is None: payload = {} # Falls Payload NULL ist

                topic = msg.topic()
                table_name = topic.split('.')[-1] # Extrahiere Tabellennamen

                # Füge die Payload dem Buffer hinzu
                message_buffer[table_name].append(payload)

                # Prüfe, ob Batch geschrieben werden soll (Intervall oder Größe)
                if time.time() - last_write_time >= WRITE_INTERVAL_SECONDS or \
                   len(message_buffer[table_name]) >= MAX_BUFFER_SIZE:
                    write_batches_to_minio(minio_client)
                    # WICHTIG: Offset manuell committen, NACHDEM erfolgreich geschrieben wurde
                    logger.info("Committing Kafka offsets...")
                    consumer.commit(asynchronous=False) # Synchrones Commit ist sicherer hier
                    logger.info("Offsets committed.")


            except json.JSONDecodeError:
                logger.error(f"WARNUNG: Konnte Nachricht nicht als JSON dekodieren: {msg.value()}", file=sys.stderr)
            except Exception as e:
                logger.error(f"FEHLER bei Nachrichtenverarbeitung: {e}", file=sys.stderr)
                # Hier entscheiden, ob Offset commited werden soll oder nicht

    finally:
        # Sauberes Schließen
        logger.info("Schließe Kafka Consumer...")
        if consumer:
            consumer.close()
        logger.info("Consumer geschlossen.")
        # MinIO Client hat keine explizite close-Methode