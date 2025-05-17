#### project tree
```sh
. # Hauptverzeichnis des Projekts
├── Caddyfile                       # Konfiguration für den Caddy Webserver/Reverse Proxy (z.B. für Prefect UI, Dashboards).
├── data                            # Verzeichnis für Rohdaten und externe Dateien.
│   ├── DSCB420 - 25_1 -Projektaufgabe DABI2.pdf  # Die Aufgabenstellung des Projekts.
│   ├── order_products_denormalized.csv     # Rohdaten: Produkte pro Bestellung.
│   ├── orders.parquet                  # Rohdaten: Bestellungen.
│   ├── tips_public.csv                 # Rohdaten: Trinkgeldinformationen pro Bestellung.
│   └── tip_testdaten_template.csv      # Vorlage für die Abgabe der Trinkgeld-Vorhersagen (Aufgabe 2g).
├── docker-compose.yaml             # Definiert die Docker-Services (Prefect, Worker, DB, Caddy etc.) und deren Zusammenspiel.
├── Dockerfile                      # Haupt-Dockerfile (wahrscheinlich für einen der Services oder als Basis).
├── init_scripts                    # Skripte, die beim Initialisieren von Services ausgeführt werden (z.B. DB-Setup).
│   └── init_db.sql                 # SQL-Skript zum initialen Einrichten der Datenbank (z.B. Schema erstellen).
├── Makefile                        # Definiert Kommandozeilen-Abkürzungen (z.B. make up, make dbt-deps).
├── notebooks                       # Jupyter Notebooks für explorative Datenanalyse (EDA), Modellentwicklung, Visualisierung.
│   ├── eda.ipynb                   # Beispiel-Notebook für Explorative Datenanalyse.
│   └── utils                       # (Optional) Hilfsfunktionen oder Module für die Notebooks.
├── pyproject.toml                  # Definiert Projektmetadaten und Python-Abhängigkeiten (für Tools wie pip, uv).
├── README.md                       # Dokumentation: Beschreibung des Projekts, Setup-Anleitung etc.
└── src                             # Hauptverzeichnis für den Quellcode.
    ├── backend                     # Verzeichnis für einen möglichen Backend-Service (API?), Zweck aktuell unklar.
    │   └── main.py                 # Hauptdatei des Backend-Service.
    └── prefect                     # Verzeichnis für alle Prefect- und dbt-bezogenen Codes.
        ├── config                  # Konfigurationsdateien oder -module für Prefect/Tasks.
        │   └── config.py           # Python-Modul für Konfigurationseinstellungen.
        ├── dbt_setup               # Das dbt-Projektverzeichnis.
        │   ├── analyses            # dbt: Für SQL-Analysen, die keine Tabellen/Views erzeugen.
        │   ├── dbt_packages        # dbt: Installierte externe Pakete (z.B. dbt_utils), via 'dbt deps'.
        │   ├── dbt_project.yml     # dbt: Hauptkonfigurationsdatei für das dbt-Projekt.
        │   ├── logs                # dbt: Log-Dateien von dbt-Ausführungen.
        │   │   └── dbt.log         # dbt: Standard-Logdatei.
        │   ├── macros              # dbt: Wiederverwendbare Jinja/SQL-Makros.
        │   ├── models              # dbt: Kernverzeichnis für alle Transformationsmodelle (.sql, .py).
        │   │   ├── intermediate    # dbt: Modelle für Zwischenschritte, kombinieren oft Staging-Modelle.
        │   │   │   └── int_user_orders_with_tip.sql # dbt: Beispiel Intermediate-Modell (kombiniert Orders & Tips).
        │   │   ├── marts           # dbt: Finale Modelle für Reporting/Analyse (Data Marts), oft Fakten & Dimensionen.
        │   │   │   ├── dim_date.sql # dbt: Dimensionstabelle für Kalenderdaten.
        │   │   │   ├── dim_products.sql # dbt: Dimensionstabelle für Produkte.
        │   │   │   ├── dim_users.sql  # dbt: Dimensionstabelle für Benutzer.
        │   │   │   ├── f_order_lines.sql # dbt: Faktentabelle für Bestellpositionen.
        │   │   │   └── f_orders.sql    # dbt: Faktentabelle für Bestellungen.
        │   │   └── staging         # dbt: Modelle für erste Bereinigung der Rohdaten (1:1 Mapping, Typisierung).
        │   │       ├── schema.yml  # dbt: Definiert Sources (Rohdatenquellen) und Tests/Dokus für Staging-Modelle.
        │   │       ├── stg_order_products.sql # dbt: Staging-Modell für Rohdaten der Bestellpositionen.
        │   │       ├── stg_orders.sql # dbt: Staging-Modell für Rohdaten der Bestellungen.
        │   │       └── stg_tips.sql   # dbt: Staging-Modell für Rohdaten der Trinkgelder.
        │   ├── package-lock.yml    # dbt: Sperrdatei für Paketversionen (nach 'dbt deps').
        │   ├── packages.yml        # dbt: Definiert externe dbt-Pakete (z.B. dbt_utils).
        │   ├── profiles.yml        # dbt: Definiert Datenbankverbindungen (Targets: dev, prod etc.). Hier im Projekt.
        │   ├── README.md           # dbt: Beschreibung spezifisch für das dbt-Projekt.
        │   ├── seeds               # dbt: Verzeichnis für CSV-Dateien, die als Tabellen geladen werden sollen (Seed data).
        │   ├── snapshots           # dbt: Verzeichnis für dbt Snapshots (Historisierung von Tabellen, z.B. für SCD).
        │   └── tests               # dbt: Verzeichnis für spezifische dbt-Tests (z.B. complex assertions).
        ├── Dockerfile              # Dockerfile spezifisch für den Prefect Worker Service.
        ├── flows                   # Verzeichnis für Prefect Flow-Definitionen.
        │   └── data_pipeline.py    # Python-Datei, die den Haupt-ETL/ELT-Flow definiert.
        ├── run_worker.py           # Python-Skript zum Starten des Prefect Workers.
        ├── tasks                   # Verzeichnis für Prefect Task-Definitionen.
        │   ├── load_raw_data.py    # Prefect Task zum Laden der Rohdaten in die Datenbank.
        │   └── run_dbt.py          # Prefect Task zum Ausführen von dbt-Befehlen.
        └── utils                   # Verzeichnis für allgemeine Hilfsfunktionen/-module für Prefect-Tasks/Flows.
            └── test.py             # Beispiel oder Test-Utility-Datei.
```

#### get tree structure of important files in project:
*useful when working with ai ;)*
```sh
tree -I '.venv|deliverables|__init__.py|.git*|__pycache__|.ipynb_checkpoints|*.pyc|*.lock'
```

#### start jupyter notebook without docker with desired kernel (you will need uv)
```sh
pip install  uv
uv venv .venv
uv sync
uv add --dev ipykernel
uv run ipython kernel install --user --env VIRTUAL_ENV $(pwd)/.venv --name=dabi2

uv run jupyter lab
```

#### set up environment
**When using windows you need to set up WSL and setup Docker Desktop to use WSL!**

1. start docker compose
```sh
make
```

2. tear down docker compose completly (when not developing on Docker images or compose we need to teardown volumes aswell sometimes)
```sh
make down
```

#### change permissions for duckdb file when dbeaver cant connect to it
```sh
sudo chown $(id -u):$(id -g) ~/path/to/dev.duckdb
```

#### remove docker stuff
```sh
docker system prune -a -f --volumes
```

#### TODO:
- draw architecture charts (ERM, star-schema, dataflow chart)
- reduce logs in general and create prefect artifacts wherever its useful
- work on comments, just leave the important 
- create a curated list of sources and tutorials about prefect and dbt on the Readme.md
- create a curated list of useful commands (Linux)
- refactor flows
- refactor run_worker
- refactor confog setting flow: control every constant with prefect/config/setting.py and a .env file
- add tests to dbt runs
. create data marts for use-case
