import requests
from pathlib import Path

from pulse.runtime import allow_prod_writes_in_test, is_test_mode, test_doc_id


class GristClient:

    def __init__(self, server, doc_id, api_key):
        self.server = server
        self.doc_id = doc_id
        self.api_key = api_key

    def _headers(self):
        return {"Authorization": f"Bearer {self.api_key}"}

    def _assert_write_allowed(self):
        if not is_test_mode():
            return
        if allow_prod_writes_in_test():
            return
        allowed_doc = test_doc_id()
        if allowed_doc and str(self.doc_id) == allowed_doc:
            return
        raise PermissionError(
            f"Writes are blocked in TEST mode for doc '{self.doc_id}'. "
            "Use PULSE_TEST_DOC_ID or set PULSE_TEST_ALLOW_PROD_WRITES=true to override."
        )

    def get_records(self, table):
        url = f"{self.server}/api/docs/{self.doc_id}/tables/{table}/records"
        r = requests.get(url, headers=self._headers())
        r.raise_for_status()
        return r.json()["records"]

    def get_columns(self, table):
        url = f"{self.server}/api/docs/{self.doc_id}/tables/{table}/columns"
        r = requests.get(url, headers=self._headers())
        r.raise_for_status()
        payload = r.json()
        return payload.get("columns", [])

    def list_tables(self):
        url = f"{self.server}/api/docs/{self.doc_id}/tables"
        r = requests.get(url, headers=self._headers())
        r.raise_for_status()
        payload = r.json()
        return payload.get("tables", [])

    def patch_record(self, table, record_id, fields):
        self._assert_write_allowed()
        url = f"{self.server}/api/docs/{self.doc_id}/tables/{table}/records"
        payload = {
            "records": [{"id": record_id, "fields": fields}]
        }
        r = requests.patch(url, json=payload, headers=self._headers())
        r.raise_for_status()
        return True

    def add_records(self, table, records):
        self._assert_write_allowed()
        url = f"{self.server}/api/docs/{self.doc_id}/tables/{table}/records"
        payload = {"records": [{"fields": record} for record in records]}
        r = requests.post(url, json=payload, headers=self._headers())
        r.raise_for_status()
        return r.json()

    def create_table(self, table_id, columns):
        self._assert_write_allowed()
        url = f"{self.server}/api/docs/{self.doc_id}/tables"
        payload = {
            "tables": [
                {
                    "id": table_id,
                    "columns": columns,
                }
            ]
        }
        r = requests.post(url, json=payload, headers=self._headers())
        r.raise_for_status()
        return r.json()

    def add_column(self, table, column_id, col_type):
        self._assert_write_allowed()
        url = f"{self.server}/api/docs/{self.doc_id}/tables/{table}/columns"
        payload = {
            "columns": [
                {
                    "id": column_id,
                    "fields": {
                        "type": col_type,
                    },
                }
            ]
        }
        r = requests.post(url, json=payload, headers=self._headers())
        r.raise_for_status()
        return r.json()

    def upload_attachment(self, file_path):
        self._assert_write_allowed()
        path = Path(file_path)
        url = f"{self.server}/api/docs/{self.doc_id}/attachments"
        with path.open("rb") as file_handle:
            response = requests.post(
                url,
                headers=self._headers(),
                files={"upload": (path.name, file_handle)},
            )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list) and payload:
            return int(payload[0])
        raise ValueError("Attachment upload failed: unexpected response payload.")
