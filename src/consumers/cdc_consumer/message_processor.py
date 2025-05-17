import pandas as pd
from datetime import datetime

from .config import get_logger

logger = get_logger(__name__)

def transform_payloads_to_dataframe(payloads: list, topic_name: str) -> pd.DataFrame | None:
    """
    Transformiert eine Liste von rohen Nachrichten-Payloads in einen Pandas DataFrame,
    fügt Metadaten hinzu und führt Typkonvertierungen durch.
    """
    if not payloads:
        logger.debug(f"Keine Payloads zum Transformieren für Topic '{topic_name}'.")
        return None

    records_to_write = []
    processing_time_for_batch = datetime.now() 

    for payload in payloads:
        if not isinstance(payload, dict):
            logger.warning(f"Überspringe ungültiges Payload (kein Dictionary) für Topic '{topic_name}': {payload}")
            continue

        record_data = payload.copy() # Kopie erstellen, um das Original im Puffer nicht zu verändern

        # Debezium-spezifische Felder extrahieren und umbenennen/entfernen
        actual_op = record_data.pop('__op', None)
        actual_ts_ms = record_data.pop('__ts_ms', None)
        record_data.pop('__deleted', None) 
        record_data.pop('__source_ts_ms', None) 
        record_data.pop('__table', None)

        # Eigene Metadaten hinzufügen
        record_data['_op'] = actual_op
        record_data['_ts_ms'] = actual_ts_ms 

        records_to_write.append(record_data)

    if not records_to_write:
        logger.info(f"Keine validen Records nach Filterung für Topic '{topic_name}' gefunden.")
        return None

    try:
        df = pd.DataFrame(records_to_write)
    except Exception as e:
        logger.error(f"Fehler beim Erstellen des Pandas DataFrame für Topic '{topic_name}': {e}", exc_info=True)
        if len(records_to_write) > 0:
            logger.debug(f"Erste paar Records, die Probleme verursachten könnten: {records_to_write[:3]}")
        return None


    # Typkonvertierungen für fehlende Werte
    if '_source_ts_ms' in df.columns:
        df['_ts_ms'] = pd.to_numeric(df['_source_ts_ms'], errors='coerce').astype(pd.Int64Dtype())
    if '_op' in df.columns:
        df['_op'] = df['_op'].astype(str) 

    logger.debug(f"DataFrame für Topic '{topic_name}' mit {len(df)} Zeilen und Spalten {df.columns.tolist()} erstellt.")
    return df


def prepare_dataframe_for_parquet_storage(df: pd.DataFrame) -> tuple[pd.DataFrame, dict | None]:
    """
    Bereitet den DataFrame für die Speicherung als Parquet vor.
    Entfernt Spalten, die nicht im Parquet-File gespeichert werden sollen (z.B. temporäre Verarbeitungsspalten).
    Extrahiert Informationen für die Partitionierung des Dateipfads.
    """
    if df.empty:
        return df, None

    # Partitionierungsdetails aus der ersten Zeile extrahieren (da sie für den gesamten Batch gelten)
    try:
        year_val = df['year'].iloc[0]
        month_val = df['month'].iloc[0]
        day_val = df['day'].iloc[0]
        partition_details = {
            'year': int(year_val), 
            'month': str(month_val),
            'day': str(day_val)
        }
    except (KeyError, IndexError) as e:
        logger.error(f"Fehler beim Extrahieren der Partitionierungsdetails: {e}. Partitionierungsspalten fehlen oder DataFrame ist leer.", exc_info=True)
        now = datetime.now()
        partition_details = {
            'year': now.year,
            'month': f"{now.month:02d}",
            'day': f"{now.day:02d}"
        }
        logger.warning(f"Verwende aktuelle Zeit für Partitionierung aufgrund eines Fehlers: {partition_details}")


    # Spalten, die nicht im Parquet File selbst gespeichert werden sollen
    columns_to_drop_for_parquet = [
        '_kafka_topic',          
        '_processing_ts_iso',    
        '_cdc_processed_datetime',
    ]

    existing_columns_to_drop = [col for col in columns_to_drop_for_parquet if col in df.columns]
    df_for_parquet = df.drop(columns=existing_columns_to_drop)

    logger.debug(f"DataFrame für Parquet vorbereitet. Spalten: {df_for_parquet.columns.tolist()}")
    return df_for_parquet, partition_details