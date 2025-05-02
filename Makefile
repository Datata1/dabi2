.PHONY: setup up down start 

.DEFAULT_GOAL := start

setup: 
	@echo ">>> Prüfe Host-Abhängigkeiten (uv)..."
	@if ! which uv > /dev/null; then \
		echo ">>> uv nicht gefunden, installiere uv via pip (Host)..."; \
		python -m pip install uv; \
	else \
		echo ">>> uv ist bereits auf dem Host installiert."; \
	fi
	@echo ">>> Synchronisiere Host Python-Pakete mit uv sync..."; \
	uv sync 
	@echo ">>> Prüfe dbt-Pakete im lokalen Projektverzeichnis..."; 
	@if [ ! -d "./src/prefect/dbt_setup/dbt_packages/dbt_utils" ]; then \
		echo ">>> dbt_packages lokal nicht gefunden, führe 'dbt deps' auf Host aus (benötigt uv)..."; \
		sudo uv run dbt deps --project-dir ./src/prefect/dbt_setup/; \
	else \
		echo ">>> dbt_packages lokal bereits vorhanden, überspringe 'dbt deps'."; \
	fi
	@echo ">>> Host-Setup abgeschlossen."

up: 
	@echo ">>> Starte Docker Services..."
	sudo docker-compose up --build --force-recreate

start: setup up 

down: 
	@echo ">>> Stoppe Docker Services..."
	sudo docker-compose down
	sudo docker-compose down --volumes 
	sudo rm -rf .venv

sudo:
	@echo ">>> Veränder Zugriffsrechte für volumes..."
	sudo chown $(id -u):$(id -g) ~/Documents/dabi2/src/prefect/dbt_setup/dev.duckdb
	sudo chown $(id -u):$(id -g) ~/Documents/dabi2/.venv


