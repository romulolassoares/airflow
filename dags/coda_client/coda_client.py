from __future__ import annotations

import logging
from typing import Any, Iterator

import requests
from airflow.sdk import BaseHook
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_CONN_ID = "coda_default"
DEFAULT_BASE_URL = "https://coda.io/apis/v1"
PAGE_LIMIT = 500


class CodaClient:
    """Minimal Coda API client with auth, retries and pagination handled."""

    def __init__(
        self,
        token: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: int = 30,
        logger: logging.Logger | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.logger = logger or logging.getLogger(__name__)

        # Coda throttles with 429 + Retry-After; urllib3 honours that header.
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
        )
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {token}"})
        self._session.mount("https://", HTTPAdapter(max_retries=retry))

    @classmethod
    def from_connection(
        cls,
        conn_id: str = DEFAULT_CONN_ID,
        **kwargs: Any,
    ) -> CodaClient:
        """Build a client from an Airflow connection (password = API token)."""
        conn = BaseHook.get_connection(conn_id)
        if not conn.password:
            raise ValueError(
                f"Connection '{conn_id}' has no password set; the Coda API token is expected there"
            )
        return cls(token=conn.password, base_url=conn.host or DEFAULT_BASE_URL, **kwargs)

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> CodaClient:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def _get(self, path: str, params: dict | None = None) -> dict:
        response = self._session.get(
            f"{self.base_url}{path}", params=params, timeout=self.timeout
        )
        response.raise_for_status()
        return response.json()

    def _paginate(self, path: str, params: dict | None = None) -> Iterator[dict]:
        """Yield every item of a paginated endpoint, following nextPageLink."""
        url = f"{self.base_url}{path}"
        query: dict | None = {**(params or {}), "limit": PAGE_LIMIT}

        while url:
            response = self._session.get(url, params=query, timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()

            yield from payload.get("items", [])

            # nextPageLink is absolute and already carries the query string.
            url, query = payload.get("nextPageLink"), None

    def get_table(self, doc_id: str, table_id: str) -> dict:
        return self._get(f"/docs/{doc_id}/tables/{table_id}")

    def list_columns(self, doc_id: str, table_id: str) -> list[dict]:
        return list(self._paginate(f"/docs/{doc_id}/tables/{table_id}/columns"))

    def list_rows(
        self,
        doc_id: str,
        table_id: str,
        value_format: str = "simpleWithArrays",
    ) -> list[dict]:
        return list(
            self._paginate(
                f"/docs/{doc_id}/tables/{table_id}/rows",
                params={"valueFormat": value_format},
            )
        )
