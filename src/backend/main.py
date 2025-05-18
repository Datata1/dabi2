# app/main.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles 
import uvicorn 

app = FastAPI(
    title="DABI2 Daten-API",
    description="API zum Abfragen von Daten-Marts und Bereitstellung von Analyse-Endpunkten.",
    version="0.1.0"
)

templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Dieser Endpunkt liefert die Haupt-HTML-Seite.
    """
    service_links = [
        {
            "name": "Prefect Server UI",
            "url": "http://localhost:3000/prefect",
            "description": "Workflows und Orchestrierung überwachen."
        },
        {
            "name": "MinIO Konsole (Data Lake)",
            "url": "http://localhost:9001", 
            "description": "Objektspeicher für den Data Lake durchsuchen."
        },
        {
            "name": "AKHQ (Kafka UI)",
            "url": "http://0.0.0.0:8080", 
            "description": "Kafka Topics und Nachrichten inspizieren."
        },
        {
            "name": "JupyterLab (token: dabi2)",
            "url": "http://localhost:8888/jupyter", 
            "description": "Interaktive Datenanalyse und Notebooks."
        },
        {
            "name": "Lightdash (BI-Tool)",
            "url": "#", 
            "description": "Business Intelligence und Datenvisualisierung."
        },
        {
            "name": "API Dokumentation",
            "url": "http://localhost:3001/docs", 
            "description": "API Dokumentation."
        }
    ]

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "links": service_links
        }
    )


@app.get("/api/v1/status")
async def get_status():
    """
    Ein einfacher Status-Endpunkt, um zu prüfen, ob die API läuft.
    """
    return {"status": "API ist betriebsbereit", "version": app.version}



if __name__ == "__main__":
    uvicorn.run(
        "main:app",         
        host="0.0.0.0",     
        port=3001,          
        reload=True         
    )
