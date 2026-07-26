from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta
from typing import Any

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk import Param, dag, get_current_context, task

from coda_backup_config import BACKUP_DAG_ID, CODA_CONN_ID, MSSQL_CONN_ID
from coda_client import CodaClient

logger = logging.getLogger("airflow.task")


def _sql_identifier(name: str, fallback: str = "col") -> str:
    """Normalise an arbitrary Coda label into a safe bracket-quotable identifier."""
    cleaned = re.sub(r"\W+", "_", (name or "").strip(), flags=re.UNICODE).strip("_").lower()
    if not cleaned:
        cleaned = fallback
    if cleaned[0].isdigit():
        cleaned = f"_{cleaned}"
    return cleaned[:120]


def _unique_identifiers(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result = []
    for name in names:
        ident = _sql_identifier(name)
        seen[ident] = seen.get(ident, 0) + 1
        result.append(ident if seen[ident] == 1 else f"{ident}_{seen[ident]}")
    return result


def _serialise(value: Any) -> str | None:
    """Coda cells can be scalars, lists (multi-select) or dicts (people/refs)."""
    if value is None:
        return None
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


@dag(
    dag_id=BACKUP_DAG_ID,
    description="Reusable backup of a single Coda table into SQL Server",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=4,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "doc_id": Param(
            "",
            type="string",
            minLength=1,
            title="Coda doc ID",
            description="The doc ID, e.g. oXc0AHpFRO",
        ),
        "table_id": Param(
            "",
            type="string",
            minLength=1,
            title="Coda table ID",
            description="The table or view ID, e.g. grid-lphWOwH0Cf",
        ),
        "target_table": Param(
            None,
            type=["null", "string"],
            title="SQL Server table",
            description="Destination table. Defaults to a slug of the Coda table name.",
        ),
    },
    tags=["coda", "superhuman", "backup"],
)
def coda_table_backup():

    @task
    def fetch_table_metadata() -> dict:
        params = get_current_context()["params"]
        doc_id, table_id = params["doc_id"], params["table_id"]

        with CodaClient.from_connection(CODA_CONN_ID, logger=logger) as client:
            table = client.get_table(doc_id, table_id)
            columns = client.list_columns(doc_id, table_id)

        table_name = table.get("name", table_id)
        target_table = _sql_identifier(params.get("target_table") or table_name, fallback="coda_table")

        logger.info(f"Table '{table_name}' has {len(columns)} columns -> [{target_table}]")
        return {
            "table_name": table_name,
            "target_table": target_table,
            "column_names": {col["id"]: col["name"] for col in columns},
        }

    @task
    def fetch_rows(metadata: dict) -> list[dict]:
        params = get_current_context()["params"]
        column_names = metadata["column_names"]

        with CodaClient.from_connection(CODA_CONN_ID, logger=logger) as client:
            rows = client.list_rows(params["doc_id"], params["table_id"])

        records = [
            {
                "row_id": row["id"],
                "row_index": row.get("index"),
                **{
                    column_names.get(col_id, col_id): value
                    for col_id, value in row.get("values", {}).items()
                },
            }
            for row in rows
        ]

        logger.info(f"Fetched {len(records)} rows from '{metadata['table_name']}'")
        return records

    @task
    def load_to_sqlserver(records: list[dict], metadata: dict) -> int:
        if not records:
            raise ValueError(f"No rows returned for Coda table '{metadata['table_name']}'")

        # Union of keys: Coda omits empty cells, so rows are not uniformly shaped.
        source_columns: list[str] = []
        for record in records:
            source_columns.extend(k for k in record if k not in source_columns)
        columns = _unique_identifiers(source_columns)

        target = metadata["target_table"]
        staging = f"{target}__staging"
        hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

        cols_sql = ", ".join(f"[{col}] NVARCHAR(MAX)" for col in columns)
        hook.run([f"DROP TABLE IF EXISTS [{staging}]", f"CREATE TABLE [{staging}] ({cols_sql})"])

        rows = [[_serialise(record.get(key)) for key in source_columns] for record in records]
        hook.insert_rows(
            table=f"[{staging}]",
            rows=rows,
            target_fields=columns,
            executemany=True,
            fast_executemany=True,
        )

        # Swap in one transaction so the live table is never empty mid-load.
        hook.run(
            [
                f"DROP TABLE IF EXISTS [{target}]",
                f"EXEC sp_rename '{staging}', '{target}'",
            ]
        )

        logger.info(f"Loaded {len(rows)} records into [{target}]")
        return len(rows)

    metadata = fetch_table_metadata()
    load_to_sqlserver(fetch_rows(metadata), metadata)


coda_table_backup()
