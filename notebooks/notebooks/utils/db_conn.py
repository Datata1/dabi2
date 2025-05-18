# notebooks/utils/db_conn.py
import os
import duckdb
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine 
from typing import Optional 

APP_DIR = Path("/dwh_data") 
DUCKDB_DWH_PATH = APP_DIR / "dev.duckdb" 

def get_postgres_oltp_engine() -> Optional[Engine]:
    """
    Erstellt und gibt eine SQLAlchemy Engine für die OLTP PostgreSQL Datenbank zurück.
    """
    pg_host = os.getenv("OLTP_DB_HOST")
    pg_port = os.getenv("OLTP_DB_PORT", "5432") 
    pg_dbname = os.getenv("OLTP_DB_NAME")
    pg_user = os.getenv("OLTP_DB_USER")
    pg_password = os.getenv("OLTP_DB_PASSWORD")

    if not all([pg_host, pg_dbname, pg_user, pg_password]):
        print("FEHLER: Mindestens eine PostgreSQL OLTP Umgebungsvariable fehlt!")
        print("Benötigt: OLTP_DB_HOST, OLTP_DB_NAME, OLTP_DB_USER, OLTP_PASSWORD")
        return None

    try:
        db_url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_dbname}"
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print(f"Verbindung zur PostgreSQL OLTP DB ({pg_host}:{pg_port}/{pg_dbname}) erfolgreich.")
        return engine
    except ImportError:
        print("FEHLER: Treiber nicht gefunden. Stelle sicher, dass 'psycopg2' oder 'psycopg2-binary' installiert ist.")
        return None
    except Exception as e:
        print(f"FEHLER beim Verbinden zur PostgreSQL OLTP DB: {e}")
        return None

def get_duckdb_dwh_connection(db_path: Path = DUCKDB_DWH_PATH, read_only: bool = True) -> Optional[duckdb.DuckDBPyConnection]:
    """
    Erstellt und gibt eine Verbindung zur DuckDB DWH Datei zurück.
    Standardmäßig im Read-Only-Modus.
    Gibt None zurück, wenn die Datei nicht existiert oder die Verbindung fehlschlägt.
    """
    resolved_path = str(db_path.resolve()) # Absoluter Pfad im Container

    if not db_path.is_file():
        print(f"FEHLER: DuckDB Datei nicht gefunden unter: {resolved_path}")
        return None

    try:
        con = duckdb.connect(database=resolved_path, read_only=read_only)
        print(f"Verbindung zur DuckDB DWH Datei ({resolved_path}) erfolgreich (Read-Only: {read_only}).")
        return con
    except Exception as e:
        print(f"FEHLER beim Verbinden zur DuckDB DWH: {e}")
        return None
