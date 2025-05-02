# notebooks/utils/db_conn.py
import os
import duckdb
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine # Für Type Hinting
from typing import Optional # Für Type Hinting

# --- Pfad zur DuckDB DWH Datei (innerhalb des Containers) ---
APP_DIR = Path("/app") 
DBT_SETUP_DIR = APP_DIR / "src" / "prefect" / "dbt_setup"
DUCKDB_DWH_PATH = DBT_SETUP_DIR / "dev.duckdb" 

# --- PostgreSQL OLTP Verbindungsfunktion ---
def get_postgres_oltp_engine() -> Optional[Engine]:
    """
    Erstellt und gibt eine SQLAlchemy Engine für die OLTP PostgreSQL Datenbank zurück.
    """
    # Lese Umgebungsvariablen (Namen ggf. an dein docker-compose.yaml anpassen)
    pg_host = os.getenv("OLTP_DB_HOST")
    pg_port = os.getenv("OLTP_DB_PORT", "5432") 
    pg_dbname = os.getenv("OLTP_DB_NAME")
    pg_user = os.getenv("OLTP_DB_USER")
    pg_password = os.getenv("OLTP_DB_PASSWORD")

    # Prüfe, ob alle Variablen gesetzt sind
    if not all([pg_host, pg_dbname, pg_user, pg_password]):
        print("FEHLER: Mindestens eine PostgreSQL OLTP Umgebungsvariable fehlt!")
        print("Benötigt: OLTP_DB_HOST, OLTP_DB_NAME, OLTP_DB_USER, OLTP_PASSWORD")
        return None

    try:
        # Baue die Verbindungs-URL
        db_url = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_dbname}"
        # Erstelle die SQLAlchemy Engine
        engine = create_engine(db_url)
        # Kurzer Verbindungstest (optional, wirft Exception bei Fehler)
        with engine.connect() as conn:
            print(f"Verbindung zur PostgreSQL OLTP DB ({pg_host}:{pg_port}/{pg_dbname}) erfolgreich.")
        return engine
    except ImportError:
        print("FEHLER: Treiber nicht gefunden. Stelle sicher, dass 'psycopg2' oder 'psycopg2-binary' installiert ist.")
        return None
    except Exception as e:
        print(f"FEHLER beim Verbinden zur PostgreSQL OLTP DB: {e}")
        return None

# --- DuckDB DWH Verbindungsfunktion ---
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
        # Verbinde mit DuckDB
        con = duckdb.connect(database=resolved_path, read_only=read_only)
        print(f"Verbindung zur DuckDB DWH Datei ({resolved_path}) erfolgreich (Read-Only: {read_only}).")
        return con
    except Exception as e:
        print(f"FEHLER beim Verbinden zur DuckDB DWH: {e}")
        return None
