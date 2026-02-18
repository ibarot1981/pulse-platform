import requests


class GristClient:

    def __init__(self, server, doc_id, api_key):
        self.server = server
        self.doc_id = doc_id
        self.api_key = api_key

    def _headers(self):
        return {"Authorization": f"Bearer {self.api_key}"}

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

    def patch_record(self, table, record_id, fields):
        url = f"{self.server}/api/docs/{self.doc_id}/tables/{table}/records"
        payload = {
            "records": [{"id": record_id, "fields": fields}]
        }
        r = requests.patch(url, json=payload, headers=self._headers())
        r.raise_for_status()
        return True

    def add_records(self, table, records):
        url = f"{self.server}/api/docs/{self.doc_id}/tables/{table}/records"
        payload = {"records": [{"fields": record} for record in records]}
        r = requests.post(url, json=payload, headers=self._headers())
        r.raise_for_status()
        return r.json()

    def create_table(self, table_id, columns):
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
