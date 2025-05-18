from prefect import task, get_run_logger
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings
from pathlib import Path
from typing import List

APP_DIR = Path("/app")
DBT_PROJECT_DIR = APP_DIR / "dbt_setup"
DBT_PROFILES_DIR = DBT_PROJECT_DIR 

@task(name="Run dbt Command (PrefectDbtRunner)")
def run_dbt_command_runner(
    dbt_args: List[str],
    project_dir: Path = DBT_PROJECT_DIR,
    profiles_dir: Path = DBT_PROFILES_DIR,
    upstream_result = None 
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
        settings = PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=profiles_dir
        )

        runner = PrefectDbtRunner(settings=settings)

        logger.info(f"Invoking runner with args: {dbt_args}...")
        result = runner.invoke(dbt_args) 
        logger.info(f"dbt command '{command_str}' completed successfully (via PrefectDbtRunner).")
        return True 

    except Exception as e:
        logger.error(f"PrefectDbtRunner failed for command '{command_str}': {e}", exc_info=True)
        raise 