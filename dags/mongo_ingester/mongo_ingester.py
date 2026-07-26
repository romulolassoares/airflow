from __future__ import annotations

import logging

from pymongo import MongoClient


class MongoIngester:
    def __init__(
        self,
        mongo_uri: str,
        database_name: str,
        collection_name: str,
        logger: logging.Logger | None = None,
    ):
        self.mongo_uri = mongo_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.client: MongoClient | None = None
        self.db = None
        self.collection = None
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    def connect(self) -> None:
        try:
            self.client = MongoClient(self.mongo_uri)
            self.client.admin.command("ping")
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            self.logger.info("Successfully connected to MongoDB!")
        except Exception as e:
            self.logger.error(f"Error connecting to MongoDB: {e}")
            raise

    def disconnect(self) -> None:
        if self.client:
            self.client.close()
            self.logger.info("Mongo connection closed!")

    def insert_one(self, document: dict) -> str:
        if self.collection is None:
            raise RuntimeError("Not connected to MongoDB. Call connect() first.")

        try:
            result = self.collection.insert_one(document)
            self.logger.info(f"Document inserted with ID {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            self.logger.error(f"Error inserting document: {e}")
            raise

    def insert_many(self, documents: list[dict]) -> list[str]:
        if self.collection is None:
            raise RuntimeError("Not connected to MongoDB. Call connect() first.")

        try:
            result = self.collection.insert_many(documents)
            self.logger.info(f"{len(result.inserted_ids)} documents inserted successfully!")
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            self.logger.error(f"Error inserting documents: {e}")
            raise

    def remove_duplicates(self, unique_fields: str | list[str], dry_run: bool = True) -> int:
        if self.collection is None:
            raise RuntimeError("Not connected to MongoDB. Call connect() first.")

        fields = [unique_fields] if isinstance(unique_fields, str) else unique_fields
        group_key = {field: f"${field}" for field in fields}

        pipeline = [
            {"$group": {"_id": group_key, "ids": {"$push": "$_id"}, "count": {"$sum": 1}}},
            {"$match": {"count": {"$gt": 1}}},
        ]

        duplicate_ids = [
            doc_id
            for group in self.collection.aggregate(pipeline)
            for doc_id in group["ids"][1:]
        ]

        if not duplicate_ids:
            self.logger.info("No duplicate documents found")
            return 0

        if dry_run:
            self.logger.info(f"Found {len(duplicate_ids)} duplicate documents (dry_run=True)")
            return len(duplicate_ids)

        result = self.collection.delete_many({"_id": {"$in": duplicate_ids}})
        self.logger.info(f"Removed {result.deleted_count} duplicate documents")
        return result.deleted_count
