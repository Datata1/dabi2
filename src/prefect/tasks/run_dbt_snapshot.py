# src/prefect/tasks/run_dbt.py (Neue Version mit ShellOperation)
from prefect import task, get_run_logger
from prefect_shell import ShellOperation # Importiere ShellOperation
from pathlib import Path
import os
import shlex # Zum sicheren Zusammenbauen von Shell-Befehlen

# --- Pfade und Config ---
APP_DIR = Path("/app")
DBT_PROJECT_DIR = APP_DIR / "prefect" / "dbt_setup"
DBT_PROFILES_DIR = DBT_PROJECT_DIR # profiles.yml ist im Projektordner
DBT_PROFILE_NAME = "dbt_setup"        # Name des zu verwendenden Profils
DBT_TARGET = "dev"            # Name des zu verwendenden Targets

@task(name="Run dbt Command via Shell")
def run_dbt_snapshot_shell(
    base_command: list[str] = ["dbt", "snapshot"],
    extra_args: list[str] = None,
    dbt_project_dir: Path = DBT_PROJECT_DIR,
    dbt_profiles_dir: Path = DBT_PROFILES_DIR,
    dbt_profile_name: str = DBT_PROFILE_NAME,
    dbt_target: str = DBT_TARGET,
    upstream_result = None
):
    """
    Führt einen dbt-Befehl aus, indem der vollständige Shell-Befehl
    manuell zusammengebaut und via ShellOperation ausgeführt wird.
    """
    logger = get_run_logger()
    logger.info(f"--- Running dbt Task via ShellOperation ---")

    # --- Baue den vollständigen Befehl manuell zusammen ---
    full_command_list = list(base_command) # z.B. ['dbt', 'run']

    # Globale dbt-Optionen hinzufügen
    full_command_list.extend([
        "--project-dir", str(dbt_project_dir.resolve()),
        "--profiles-dir", str(dbt_profiles_dir.resolve()), # Jetzt explizit!
        "--profile", dbt_profile_name,
        "--target", dbt_target
    ])

    # Zusätzliche Argumente anhängen (z.B. --select)
    if extra_args:
        full_command_list.extend(extra_args)

    # Baue einen sicheren String für Logging und Ausführung
    # shlex.join ist gut, falls Pfade Leerzeichen enthalten könnten
    safe_command_string = shlex.join(full_command_list)
    logger.info(f"Constructed shell command: {safe_command_string}")

    # --- Führe den Befehl mit ShellOperation aus ---
    shell_op = ShellOperation(
        # commands erwartet eine Liste von Befehls-Strings
        commands=[safe_command_string],
        stream_output=True
    )

    try:
        logger.info(f"Executing ShellOperation...")
        # ShellOperation ausführen. Gibt Output zurück oder wirft Fehler bei != 0 Exit Code.
        result = shell_op.run()
        logger.info(f"ShellOperation for dbt command completed.")
        # Hier könnte man den Output genauer prüfen, aber meist reicht die Fehlerbehandlung
        return result # Rückgabewert von ShellOperation.run() ist oft der Output
    except Exception as e:
        # Der RuntimeError von ShellOperation enthält meist den Exit Code
        logger.error(f"ShellOperation for dbt command failed: {e}", exc_info=True)
        raise # Fehler weiterwerfen