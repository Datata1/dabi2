# notebooks/utils/get_marts.py (oder direkt im Notebook)

import requests
import os
import shutil 
from pathlib import Path 


DEFAULT_FASTAPI_BASE_URL = os.getenv("FASTAPI_BASE_URL", "http://fastapi-backend-dabi2:3001") 

def download_mart_as_parquet(
    schema_name: str,
    mart_name: str,
    fastapi_base_url: str = DEFAULT_FASTAPI_BASE_URL,
    download_path: str = "./parquet_files", 
    filename: str = None
) -> str | None:
    """
    Lädt einen spezifizierten Data Mart als Parquet-Datei von der FastAPI-API herunter.

    Args:
        schema_name (str): Der Name des Schemas in DuckDB (z.B. 'analytics_marts').
        mart_name (str): Der Name der Tabelle/View des Data Marts.
        fastapi_base_url (str, optional): Die Basis-URL des FastAPI-Servers.
                                            Defaults to os.getenv("FASTAPI_BASE_URL", "http://fastapi-backend:3001").
        download_path (str, optional): Das Verzeichnis, in dem die Datei gespeichert werden soll.
                                       Defaults to "." (aktuelles Verzeichnis).
        filename (str, optional): Der gewünschte Dateiname (ohne .parquet Endung).
                                  Wenn None, wird schema_name_mart_name verwendet.
                                  Defaults to None.

    Returns:
        str | None: Der vollständige Pfad zur heruntergeladenen Parquet-Datei bei Erfolg, sonst None.
    """
    if not filename:
        filename = f"{schema_name}_{mart_name}.parquet"
    else:
        if not filename.endswith(".parquet"):
            filename += ".parquet"

    Path(download_path).mkdir(parents=True, exist_ok=True)
    
    full_file_path = os.path.join(download_path, filename)
    
    url = f"{fastapi_base_url.rstrip('/')}/api/v1/marts/{schema_name}/{mart_name}/export/parquet"

    print(f"Versuche Download von: {url}")

    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()  
            
            with open(full_file_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            
            print(f"Datei erfolgreich heruntergeladen und gespeichert unter: {full_file_path}")
            return full_file_path
            
    except requests.exceptions.RequestException as e:
        print(f"Fehler beim Download der Parquet-Datei von {url}: {e}")
        try:
            if r and r.text: 
                error_details = r.json()
                print(f"Server-Fehlerdetails: {error_details.get('detail', r.text)}")
        except Exception: 
            pass
        return None
    except Exception as e:
        print(f"Ein unerwarteter Fehler ist aufgetreten: {e}")
        return None

