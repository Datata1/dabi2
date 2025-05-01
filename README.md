#### project tree
```sh
.
├── Caddyfile
├── data
│   ├── DSCB420 - 25_1 -Projektaufgabe DABI2.pdf
│   ├── order_products_denormalized.csv
│   ├── orders.parquet
│   ├── test.csv
│   ├── tips_public.csv
│   └── tip_testdaten_template.csv
├── docker-compose.yaml
├── Dockerfile
├── init_scripts
│   └── init_db.sql
├── notebooks
│   └── test.ipynb
├── pyproject.toml
├── README.md
└── src
    ├── backend
    │   └── main.py
    └── prefect
        ├── config
        │   └── config.py
        ├── dbt_project
        │   └── test.py
        ├── Dockerfile
        ├── flows
        │   └── test.py
        ├── run_worker.py
        ├── tasks
        │   └── test.py
        └── utils
            └── test.py
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

1. start docker-compose
```sh
make
```

2. tear down docker-compose completly (when not developing on Docker images or compose we need to teardown volumes aswell sometimes)
```sh
make down
```