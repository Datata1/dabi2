# flows/data_pipeline.py (oder ersetze data_pipeline.py)
from prefect import flow, get_run_logger
from tasks.load_raw_data import load_raw_data
from tasks.run_dbt_runner import run_dbt_command_runner


@flow(name="DWH Pipeline (PrefectDbtRunner)")
def dwh_pipeline():
    """
    Verwendet PrefectDbtRunner, um dbt Befehle in korrekter Reihenfolge auszuführen.
    """
    logger = get_run_logger()

    # 1. Lade Rohdaten
    raw_data_loaded = load_raw_data()

    # 1.5. dbt debug (optional, aber gut zum Testen)
    # Übergib nur die Argumente für den 'debug' Befehl
    debug_result = run_dbt_command_runner(
        dbt_args=["debug"],
        upstream_result=raw_data_loaded
    )

    # 2. Baue Staging Models
    # Übergib die Argumente für 'run --select staging.*'
    staging_built = run_dbt_command_runner(
        dbt_args=["run", "--select", "staging.*"],
        upstream_result=debug_result
    )

    # 3. Führe Snapshots aus
    # Übergib die Argumente für 'snapshot'
    snapshot_taken = run_dbt_command_runner(
         dbt_args=["snapshot"],
         upstream_result=staging_built
    )

    # 4. Baue Marts und Intermediate
    # Übergib die Argumente für 'run --select marts.* intermediate.*'
    marts_built = run_dbt_command_runner(
        dbt_args=["run", "--select", "marts.*", "intermediate.*"],
        upstream_result=snapshot_taken
    )
    return marts_built 