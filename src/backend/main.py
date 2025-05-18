import os
import tempfile 
from typing import List, Any, Dict 

import uvicorn
import duckdb
from fastapi import FastAPI, Request, HTTPException, Path 
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, FileResponse 
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field 

from utils.duckdb_conn import get_db_connection

app = FastAPI(
    title="DABI2 Daten-API",
    description="API zum Abfragen von Daten-Marts und Bereitstellung von Analyse-Endpunkten.",
    version="0.3.0"
)

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

DUCKDB_DWH_PATH = os.getenv("DUCKDB_DWH_PATH", "/dwh_data/dev.duckdb")

class StatusResponse(BaseModel):
    status: str
    version: str


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Liefert die Haupt-HTML-Seite (index.html) mit Links zu anderen Services.
    """
    service_links = [
        {"name": "Prefect Server UI", "url": os.getenv("LINK_PREFECT_UI", "http://localhost:4200"), "description": "Workflows überwachen."},
        {"name": "MinIO Konsole", "url": os.getenv("LINK_MINIO_CONSOLE", "http://localhost:9001"), "description": "Data Lake durchsuchen."},
        {"name": "AKHQ (Kafka UI)", "url": os.getenv("LINK_AKHQ_UI", "http://0.0.0.0:8080"), "description": "Kafka Topics inspizieren."},
        {"name": "JupyterLab", "url": os.getenv("LINK_JUPYTERLAB", "http://localhost:8888/jupyter"), "description": "Interaktive Datenanalyse."},
        {"name": "API Dokumentation (Swagger)", "url": "/docs", "description": "Interaktive API-Dokumentation."},
        {"name": "Alternative API Dokumentation (ReDoc)", "url": "/redoc", "description": "Alternative API-Dokumentation."},
    ]
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "links": service_links}
    )


@app.get("/api/v1/status")
async def get_status():
    """
    Ein einfacher Status-Endpunkt, um zu prüfen, ob die API läuft.
    """
    return {"status": "API ist betriebsbereit", "version": app.version}

@app.get("/api/v1/marts/{schema_name}/{mart_name}/export/parquet", response_class=FileResponse)
async def export_mart_to_parquet(
    request: Request, # Request-Objekt wird von FastAPI bereitgestellt
    schema_name: str = Path(..., title="Schema-Name", description="Der Name des Schemas in DuckDB (z.B. 'analytics_marts').", regex="^[a-zA-Z0-9_]+$"),
    mart_name: str = Path(..., title="Mart-Name", description="Der Name der Tabelle/View des Data Marts (z.B. 'mart_user_tipping_behavior_timeseries').", regex="^[a-zA-Z0-9_]+$")
):
    """
    Exportiert einen spezifizierten Data Mart aus DuckDB als Parquet-Datei.

    Beispielaufruf: `/api/v1/marts/main_analytics_marts/mart_user_tipping_behavior_timeseries/export/parquet`
    """
    qualified_mart_name = f"{schema_name}.{mart_name}"

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tmpfile:
            temp_file_path = tmpfile.name
        
        def _export_to_parquet_sync():
            conn = None 
            try:
                conn = get_db_connection(read_only=True)
                export_query = f"COPY (SELECT * FROM {qualified_mart_name}) TO '{temp_file_path}' (FORMAT PARQUET);"
                conn.execute(export_query)
            except duckdb.CatalogException: 
                raise HTTPException(status_code=404, detail=f"Data Mart '{qualified_mart_name}' nicht gefunden in DuckDB.")
            except duckdb.Error as db_err: 
                print(f"DuckDB Fehler beim Export von {qualified_mart_name}: {db_err}")
                raise HTTPException(status_code=500, detail=f"DuckDB Fehler beim Export: {db_err}")
            except Exception as e:
                print(f"Allgemeiner Fehler beim Export von {qualified_mart_name}: {e}")
                raise HTTPException(status_code=500, detail=f"Allgemeiner Fehler beim Export: {e}")
            finally:
                if conn:
                    conn.close()

        await run_in_threadpool(_export_to_parquet_sync)

        download_filename = f"{schema_name}_{mart_name}.parquet"
        
        return FileResponse(
            path=temp_file_path,
            media_type='application/vnd.apache.parquet', 
            filename=download_filename
        )
    except HTTPException as http_exc: 
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        raise http_exc
    except Exception as e:
        print(f"Unerwarteter Fehler in export_mart_to_parquet: {e}")
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        raise HTTPException(status_code=500, detail=f"Interner Serverfehler: {e}")


if __name__ == "__main__":
    uvicorn.run(
        "main:app",         
        host="0.0.0.0",     
        port=3001,          
        reload=True         
    )
