from __future__ import annotations

import logging
from datetime import datetime, timedelta

import polars as pl
import requests
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.sdk import BaseHook, dag, task

from mongo_ingester import MongoIngester

USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
MSSQL_CONN_ID = "mssql_default"
MONGO_CONN_ID = "mongo_default"
SQLSERVER_TABLE = "geo_json_data"

logger = logging.getLogger("airflow.task")


def _build_mongo_uri(connection) -> str:
    auth = f"{connection.login}:{connection.password}@" if connection.login else ""
    port = f":{connection.port}" if connection.port else ""
    return f"mongodb://{auth}{connection.host}{port}"


@dag(
    dag_id="earthquake_etl",
    description="ETL pipeline for earthquake data from USGS API",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["earthquake", "etl"],
)
def earthquake_etl():

    @task
    def extract_data_from_api() -> dict:
        response = requests.get(USGS_FEED_URL, timeout=30)
        response.raise_for_status()
        data = response.json()

        logger.info(f"Extracted {len(data.get('features', []))} earthquake records")
        return data

    @task
    def insert_raw_data_to_mongo(data: dict) -> int:
        features = data.get("features")
        if not features:
            raise ValueError("No data received from extraction task")

        mongo_conn = BaseHook.get_connection(MONGO_CONN_ID)
        collection_name = mongo_conn.extra_dejson.get("collection_name")
        if not collection_name:
            raise ValueError(
                f"Connection '{MONGO_CONN_ID}' is missing 'collection_name' in its extra field"
            )

        ingester = MongoIngester(
            mongo_uri=_build_mongo_uri(mongo_conn),
            database_name=mongo_conn.schema,
            collection_name=collection_name,
            logger=logger,
        )

        try:
            ingester.connect()
            ingester.insert_many(features)
            removed = ingester.remove_duplicates(unique_fields="id", dry_run=False)
            logger.info(f"Inserted {len(features)} records to MongoDB, removed {removed} duplicates")
            return len(features)
        finally:
            ingester.disconnect()

    @task
    def process_data(data: dict) -> list[dict]:
        features = data.get("features")
        if not features:
            raise ValueError("No data received from extraction task")

        rows = [
            {
                "id": feature["id"],
                "longitude": feature["geometry"]["coordinates"][0],
                "latitude": feature["geometry"]["coordinates"][1],
                "depth": feature["geometry"]["coordinates"][2],
                **feature["properties"],
            }
            for feature in features
        ]

        df = pl.DataFrame(rows).drop(["url", "detail"])
        logger.info(f"Processed {len(df)} earthquake records")
        return df.to_dicts()

    @task
    def load_to_sqlserver(records: list[dict]) -> int:
        if not records:
            raise ValueError("No processed data received")

        df = pl.DataFrame(records)
        columns = df.columns
        hook = MsSqlHook(mssql_conn_id=MSSQL_CONN_ID)

        hook.run(f"DROP TABLE IF EXISTS {SQLSERVER_TABLE}")
        cols_sql = ", ".join(f"[{col}] NVARCHAR(MAX)" for col in columns)
        hook.run(f"CREATE TABLE {SQLSERVER_TABLE} ({cols_sql})")

        rows = [
            [None if v is None else str(v) for v in row]
            for row in df.iter_rows()
        ]
        hook.insert_rows(
            table=SQLSERVER_TABLE,
            rows=rows,
            target_fields=columns,
            executemany=True,
            fast_executemany=True,
        )

        logger.info(f"Loaded {len(df)} records to SQL Server")
        return len(df)

    @task
    def data_quality_check(
        extracted: dict,
        mongo_count: int,
        processed: list[dict],
        sqlserver_count: int,
    ) -> str:
        extracted_count = len(extracted.get("features", []))
        processed_count = len(processed)

        logger.info("Data Quality Check:")
        logger.info(f"  - Extracted: {extracted_count} records")
        logger.info(f"  - MongoDB: {mongo_count} records")
        logger.info(f"  - Processed: {processed_count} records")
        logger.info(f"  - SQL Server: {sqlserver_count} records")

        if processed_count != sqlserver_count:
            raise ValueError(
                f"Data count mismatch: Processed {processed_count} but loaded {sqlserver_count}"
            )

        return "Data quality check passed"

    extracted = extract_data_from_api()
    mongo_count = insert_raw_data_to_mongo(extracted)
    processed = process_data(extracted)
    sqlserver_count = load_to_sqlserver(processed)
    data_quality_check(extracted, mongo_count, processed, sqlserver_count)


earthquake_etl()
