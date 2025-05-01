# src/prefect/tasks/run_dbt.py
from prefect import task
# Korrekter Import für neuere Versionen
from prefect_dbt.cli import DbtCoreOperation
from pathlib import Path
import os # Kann nützlich sein für Pfad-Debugging im Container

# --- WICHTIG: Pfade innerhalb des Docker-Containers ---
# Annahme: Dein Projekt-Root ist im Docker-Container unter /app gemountet oder kopiert
APP_DIR = Path("/app")
DBT_PROJECT_DIR = APP_DIR / "prefect" / "dbt_setup"

# Standard-Pfad für dbt-Profile ist ~/.dbt
# Innerhalb des Containers ist das Home-Verzeichnis des ausführenden Users
# oft /root oder /home/<user>
# Stelle sicher, dass deine profiles.yml dort liegt ODER gib einen expliziten Pfad an.
DBT_PROFILES_DIR = DBT_PROJECT_DIR
# Alternative, falls du die profiles.yml z.B. ins Projekt legst (nicht empfohlen für Secrets!)
# DBT_PROFILES_DIR = DBT_PROJECT_DIR

# --- Konfiguration aus dbt_project.yml und profiles.yml ---
# Name des Profils in deiner profiles.yml (muss existieren!)
DBT_PROFILE_NAME = "dabi2" # Oder wie dein Profil heißt
# Name des Targets in deinem Profil (z.B. 'dev')
DBT_TARGET = "dev" # Muss im Profil für 'dabi2' definiert sein

@task(name="Run dbt models")
def run_dbt_models(
    dbt_project_dir: Path = DBT_PROJECT_DIR,
    dbt_profiles_dir: Path = DBT_PROFILES_DIR,
    dbt_profile_name: str = DBT_PROFILE_NAME,
    dbt_target: str = DBT_TARGET,
):
    """
    Runs dbt run using DbtCoreOperation.
    Stellt sicher, dass Pfade als Strings übergeben werden.
    """
    # Debugging-Ausgaben für Pfade im Container
    print(f"--- Running dbt Task ---")
    print(f"Working directory: {os.getcwd()}")
    print(f"dbt project dir: {dbt_project_dir.resolve()}")
    print(f"dbt profiles dir: {dbt_profiles_dir.resolve()}")
    print(f"Profile name: {dbt_profile_name}")
    print(f"Target name: {dbt_target}")
    print(f"Profiles dir exists: {dbt_profiles_dir.exists()}")
    if dbt_profiles_dir.exists():
         print(f"Files in profiles dir: {list(dbt_profiles_dir.iterdir())}")


    # Verwende DbtCoreOperation
    # Übergib Pfade als Strings, wie von DbtCoreOperation erwartet
    dbt_op = DbtCoreOperation(
        commands=["dbt run"], # Befehle als Liste übergeben
        project_dir=str(dbt_project_dir.resolve()),
        profiles_dir=str(dbt_profiles_dir.resolve()), # Explizit übergeben
        profile_name=dbt_profile_name,
        target=dbt_target,
        stream_output=True # Zeigt dbt-Logs live im Prefect-Log an
    )

    try:
        result = dbt_op.run() # Führe die Operation aus
        print("dbt operation completed successfully.")
        return result
    except Exception as e:
        print(f"dbt operation failed: {e}")
        # Optional: Weitere Debug-Infos ausgeben
        # print(f"Check dbt logs in: {dbt_project_dir / 'logs' / 'dbt.log'}")
        raise # Fehler weiterwerfen, damit der Prefect Task als fehlgeschlagen markiert wird