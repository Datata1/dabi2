# tasks/run_dbt_runner.py
from prefect import task, get_run_logger
# Imports für den neuen Runner
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from pathlib import Path
from typing import List

# --- Pfade ---
APP_DIR = Path("/app")
DBT_PROJECT_DIR = APP_DIR / "prefect" / "dbt_setup"
DBT_PROFILES_DIR = DBT_PROJECT_DIR # profiles.yml ist im Projektordner

@task(name="Run dbt Command (PrefectDbtRunner)")
def run_dbt_command_runner(
    # Nur die Argumente für den dbt-Befehl selbst
    # z.B. ["run", "--select", "staging.*"] oder ["snapshot"] oder ["debug"]
    dbt_args: List[str],
    project_dir: Path = DBT_PROJECT_DIR,
    profiles_dir: Path = DBT_PROFILES_DIR,
    upstream_result = None # Für Prefect-Abhängigkeiten
):
    """
    Führt einen dbt Core Befehl mittels PrefectDbtRunner aus.
    Konfiguriert über PrefectDbtSettings.
    """
    logger = get_run_logger()
    command_str = "dbt " + " ".join(dbt_args)
    logger.info(f"--- Running dbt Task via PrefectDbtRunner ---")
    logger.info(f"Executing command: {command_str}")
    logger.info(f"Using project_dir: {project_dir}")
    logger.info(f"Using profiles_dir: {profiles_dir}")

    try:
        # 1. Einstellungen für dbt konfigurieren
        settings = PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=profiles_dir
            # Profil & Target werden automatisch aus profiles.yml/Umgebung gelesen
        )

        # 2. Runner instanziieren
        runner = PrefectDbtRunner(settings=settings)

        # 3. Befehl ausführen (invoke)
        # invoke erwartet eine Liste von Kommandozeilen-Argumenten für dbt
        logger.info(f"Invoking runner with args: {dbt_args}...")
        result = runner.invoke(dbt_args) # <-- invoke statt run()
        # invoke gibt bei Erfolg oft None zurück und wirft Exceptions bei Fehlern
        logger.info(f"dbt command '{command_str}' completed successfully (via PrefectDbtRunner).")
        return True # Erfolg signalisieren

    except Exception as e:
        logger.error(f"PrefectDbtRunner failed for command '{command_str}': {e}", exc_info=True)
        raise # Fehler weitergeben, um Task fehlschlagen zu lassen