from fastapi import HTTPException
import duckdb
import os


def get_db_connection(db_path: str = "/dwh_data/dev.duckdb", read_only: bool = True):
    """Stellt eine Verbindung zur DuckDB-Datenbank her."""
    DUCKDB_DWH_PATH = db_path
    try:
        if not os.path.exists(DUCKDB_DWH_PATH):
            raise HTTPException(status_code=503, detail=f"DuckDB database file not found at {DUCKDB_DWH_PATH}")
        return duckdb.connect(database=DUCKDB_DWH_PATH, read_only=read_only)
    except Exception as e:
        print(f"Schwerwiegender Fehler beim Verbinden mit DuckDB: {e}")
        raise HTTPException(status_code=503, detail=f"Konnte nicht mit DuckDB verbinden: {e}")