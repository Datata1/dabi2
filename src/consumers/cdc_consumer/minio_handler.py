from io import BytesIO
from datetime import datetime
import pandas as pd
from minio import Minio
from minio.error import S3Error

from . import config 
from .config import get_logger

logger = get_logger(__name__)

# Globale Variable für den MinIO-Client, um die Verbindung wiederzuverwenden
_minio_client_instance = None

def get_minio_client() -> Minio | None:
    """
    Initialisiert und gibt eine MinIO Client-Instanz zurück.
    Verwendet eine Singleton-ähnliche Instanz, um die Verbindung wiederzuverwenden.
    Stellt sicher, dass der Ziel-Bucket existiert und erstellt ihn bei Bedarf.
    """
    global _minio_client_instance
    if _minio_client_instance:
        return _minio_client_instance

    try:
        config.validate_critical_config() 
        client = Minio(
            config.MINIO_ENDPOINT,
            access_key=config.MINIO_ACCESS_KEY,
            secret_key=config.MINIO_SECRET_KEY,
            secure=config.MINIO_USE_SSL
        )
        logger.info(f"Überprüfe Existenz des MinIO Buckets '{config.MINIO_BUCKET}'...")
        found = client.bucket_exists(config.MINIO_BUCKET)
        if not found:
            logger.info(f"MinIO Bucket '{config.MINIO_BUCKET}' nicht gefunden. Versuche ihn zu erstellen...")
            client.make_bucket(config.MINIO_BUCKET)
            logger.info(f"MinIO Bucket '{config.MINIO_BUCKET}' erfolgreich erstellt.")
        else:
            logger.info(f"Erfolgreich mit MinIO verbunden. Bucket '{config.MINIO_BUCKET}' gefunden.")
        _minio_client_instance = client
        return client
    except S3Error as e:
        logger.error(f"S3 Fehler bei der MinIO Client Initialisierung oder Bucket-Prüfung: {e}", exc_info=True)
        return None
    except ValueError as e:
        logger.error(f"Konfigurationsfehler für MinIO: {e}")
        return None 
    except Exception as e:
        logger.error(f"Allgemeiner Fehler bei der MinIO Client Initialisierung: {e}", exc_info=True)
        return None


def write_dataframe_to_minio(
    minio_client: Minio,
    dataframe: pd.DataFrame,
    table_name: str,
    partition_details: dict | None
) -> bool:
    """
    Schreibt einen Pandas DataFrame als Parquet-Datei in den MinIO Bucket.
    Der Dateipfad wird basierend auf Tabellenname und Partitionierungsdetails konstruiert.
    """
    if dataframe.empty:
        logger.info(f"DataFrame für Tabelle '{table_name}' ist leer. Kein Upload nach MinIO.")
        return True 

    if partition_details:
        year = partition_details.get('year', 'unknown_year')
        month = partition_details.get('month', 'unknown_month')
        day = partition_details.get('day', 'unknown_day')
        object_name_prefix = f"cdc_events/{table_name}/year={year}/month={month}/day={day}"
    else:
        logger.warning(f"Keine Partitionierungsdetails für Tabelle '{table_name}'. Verwende Standardpfad.")
        object_name_prefix = f"cdc_events/{table_name}/unpartitioned"

    # Zeitstempel für Dateinamen, um Eindeutigkeit zu gewährleisten
    file_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
    object_name = f"{object_name_prefix}/{table_name}_{file_timestamp}.parquet"

    try:
        # DataFrame in einen In-Memory Buffer als Parquet schreiben
        out_buffer = BytesIO()
        dataframe.to_parquet(out_buffer, index=False, engine='pyarrow', compression='snappy')
        out_buffer.seek(0) 

        logger.info(f"Schreibe DataFrame ({len(dataframe)} Zeilen) für Tabelle '{table_name}' nach MinIO: {config.MINIO_BUCKET}/{object_name}")
        minio_client.put_object(
            config.MINIO_BUCKET,
            object_name,
            data=out_buffer,
            length=out_buffer.getbuffer().nbytes, 
            content_type='application/parquet'
        )
        logger.info(f"Upload für '{object_name}' erfolgreich.")
        return True
    except S3Error as e:
        logger.error(f"MinIO S3 Fehler beim Upload von '{object_name}': {e}", exc_info=True)
        return False
    except Exception as e: 
        logger.error(f"Allgemeiner Fehler beim Schreiben/Upload von '{object_name}': {e}", exc_info=True)
        return False