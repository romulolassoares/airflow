"""One thin trigger DAG per Coda table we back up.

Each entry in BACKUP_TARGETS becomes its own DAG with its own schedule, so a
table can be paused, re-run or rescheduled without touching the others. All of
them delegate the actual work to the reusable `coda_table_backup` DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag

from coda_backup_config import BACKUP_DAG_ID, BACKUP_TARGETS


def build_trigger_dag(target: dict):
    name = target["name"]

    @dag(
        dag_id=f"coda_backup_{name}",
        description=target.get("description", f"Backup of Coda table '{name}'"),
        schedule=target.get("schedule", "@daily"),
        start_date=target.get("start_date", datetime(2026, 1, 1)),
        catchup=False,
        max_active_runs=1,
        default_args={
            "retries": target.get("retries", 1),
            "retry_delay": timedelta(minutes=5),
        },
        tags=["coda", "backup", name],
    )
    def trigger_dag():
        TriggerDagRunOperator(
            task_id="run_backup",
            trigger_dag_id=BACKUP_DAG_ID,
            conf={
                "doc_id": target["doc_id"],
                "table_id": target["table_id"],
                "target_table": target.get("target_table"),
            },
            wait_for_completion=True,
            deferrable=True,
            poke_interval=30,
            failed_states=["failed"],
        )

    return trigger_dag()


for _target in BACKUP_TARGETS:
    build_trigger_dag(_target)
