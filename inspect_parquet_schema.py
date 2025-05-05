import duckdb
import os
import argparse
import sys

# --- Konfiguration: Lesen aus Umgebungsvariablen ---
# Verwenden Sie dieselben Variablennamen wie in Ihrem Prefect/Consumer-Setup
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT_FOR_DUCKDB', 'http://localhost:9000') # Sollte http://... sein
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minio_secret_password') # Stellen Sie sicher, dass dies korrekt ist
MINIO_USE_SSL = os.getenv('MINIO_USE_SSL', 'false').lower() == 'true'
# Wichtig: Path Style für MinIO
MINIO_USE_PATH_STYLE = True

def inspect_schema(file_path: str):
    """
    Verbindet sich mit DuckDB, konfiguriert S3-Zugriff und beschreibt das Schema
    der angegebenen Parquet-Datei auf MinIO.
    """
    print(f"Versuche Schema für Datei zu lesen: {file_path}")

    if not MINIO_SECRET_KEY:
        print("FEHLER: Umgebungsvariable MINIO_SECRET_KEY ist nicht gesetzt.", file=sys.stderr)
        sys.exit(1)

    conn = None
    try:
        # Verbinde zu einer In-Memory DuckDB-Datenbank
        conn = duckdb.connect(database=':memory:', read_only=False)
        print("DuckDB In-Memory DB verbunden.")

        # DuckDB für S3/MinIO konfigurieren
        conn.sql(f"INSTALL httpfs;")
        conn.sql(f"LOAD httpfs;")

        # Extrahiere Hostname:Port aus dem Endpoint
        endpoint_host_port = MINIO_ENDPOINT.split('//')[-1]
        # Entferne ggf. den Bucket-Namen, falls er fälschlicherweise drin ist
        if '/' in endpoint_host_port:
            endpoint_host_port = endpoint_host_port.split('/')[0]

        conn.sql(f"SET s3_endpoint='{endpoint_host_port}/datalake';")
        conn.sql(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
        conn.sql(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
        conn.sql(f"SET s3_use_ssl={str(MINIO_USE_SSL).lower()};")
        conn.sql(f"SET s3_url_style={str(MINIO_USE_PATH_STYLE).lower()};")
        print("DuckDB S3 Konfiguration gesetzt (Path Style).")

        # Schema der Parquet-Datei abfragen
        describe_query = f"DESCRIBE SELECT * FROM read_parquet('{file_path}')"
        print(f"Führe Query aus: {describe_query}")

        schema_info = conn.execute(describe_query).fetchall()

        print("-" * 40)
        print(f"Schema für: {file_path}")
        print("-" * 40)
        if schema_info:
            print(f"{'Spaltenname':<30} | {'Datentyp':<20}")
            print("-" * 40)
            for column_name, column_type, *_ in schema_info: # Ignoriere weitere Spalten von DESCRIBE
                print(f"{column_name:<30} | {column_type:<20}")
        else:
            print("Konnte kein Schema lesen (Datei leer oder nicht lesbar?).")
        print("-" * 40)

    except duckdb.Error as e:
        print(f"\nFEHLER bei der DuckDB-Operation: {e}", file=sys.stderr)
        print("Mögliche Ursachen:")
        print("- Falscher Dateipfad?")
        print("- MinIO nicht erreichbar unter dem konfigurierten Endpoint?")
        print("- Falsche MinIO Credentials?")
        print("- Datei ist keine gültige Parquet-Datei?")
        print("- Berechtigungsproblem in MinIO?")
    except Exception as e:
        print(f"\nEin unerwarteter Fehler ist aufgetreten: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()
            print("DuckDB Verbindung geschlossen.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Inspiziert das Schema einer Parquet-Datei auf MinIO via DuckDB.')
    parser.add_argument('file_path', type=str, help='Der vollständige S3-Pfad zur Parquet-Datei (z.B. s3://datalake/cdc_events/aisles/year=...).')
    args = parser.parse_args()

    inspect_schema(args.file_path)