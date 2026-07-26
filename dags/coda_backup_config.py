"""Shared configuration for the Coda -> SQL Server backup DAGs.

Contains no DAG objects on purpose: both `coda_table_backup` and
`coda_backup_triggers` import from here, and importing a module that defines a
DAG would register that DAG twice.
"""

from __future__ import annotations

from datetime import datetime

BACKUP_DAG_ID = "coda_table_backup"
CODA_CONN_ID = "coda_default"
MSSQL_CONN_ID = "mssql_default"

# One entry per table to back up -> one trigger DAG named coda_backup_<name>.
# To add a table, append a dict here. Nothing else to write.
BACKUP_TARGETS: list[dict] = [
    {
        "name": "superhuman",
        "doc_id": "oXc0AHpFRO",
        "table_id": "grid-lphWOwH0Cf",
        "target_table": "timesheet",
        "schedule": "@daily",
        "start_date": datetime(2026, 1, 1),
        "description": "Daily backup of the Superhuman doc",
    },
    {
        "name": "superhuman_periodos",
        "doc_id": "oXc0AHpFRO",
        "table_id": "grid-lphWOwH0Cf",
        "target_table": "periodos",
        "schedule": "@daily",
        "start_date": datetime(2026, 1, 1),
        "description": "Daily backup of the Superhuman doc - periodos",
    },
]
