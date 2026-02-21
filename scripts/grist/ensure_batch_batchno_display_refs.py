from __future__ import annotations

import json

import requests

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


TARGET_TABLES = (
    "ProductBatchMS",
    "ProductBatchCNC",
    "ProductBatchStore",
    "BatchStatusHistory",
)


def _auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {COSTING_API_KEY}"}


def _apply_actions(actions: list) -> dict:
    url = f"{PULSE_GRIST_SERVER}/api/docs/{COSTING_DOC_ID}/apply"
    headers = {
        "Authorization": f"Bearer {COSTING_API_KEY}",
        "Content-Type": "application/json",
    }
    response = requests.post(url, data=json.dumps(actions), headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def _ensure_batch_id_ref_and_display(client: GristClient, table: str) -> None:
    columns_url = f"{PULSE_GRIST_SERVER}/api/docs/{COSTING_DOC_ID}/tables/{table}/columns"

    # 1) Ensure the column is a reference and points to batch_no as visible target.
    response = requests.patch(
        columns_url,
        json={
            "columns": [
                {
                    "id": "batch_id",
                    "fields": {
                        "type": "Ref:ProductBatchMaster",
                        "visibleCol": "batch_no",
                    },
                }
            ]
        },
        headers=_auth_headers(),
        timeout=30,
    )
    response.raise_for_status()

    # 2) Ensure helper display formula column exists and returns batch_no.
    display_col_id = "batch_no_display"
    formula = "$batch_id.batch_no"

    current_columns = client.get_columns(table)
    by_id = {col.get("id"): col for col in current_columns}

    if display_col_id in by_id:
        _apply_actions(
            [
                [
                    "ModifyColumn",
                    table,
                    display_col_id,
                    {"type": "Any", "isFormula": True, "formula": formula},
                ]
            ]
        )
        display_col_ref = by_id[display_col_id]["fields"]["colRef"]
    else:
        output = _apply_actions(
            [["AddColumn", table, display_col_id, {"type": "Any", "isFormula": True, "formula": formula}]]
        )
        display_col_ref = output["retValues"][0]["colRef"]

    # 3) Bind batch_id display rendering to helper formula column.
    response = requests.patch(
        columns_url,
        json={
            "columns": [
                {
                    "id": "batch_id",
                    "fields": {
                        "displayCol": display_col_ref,
                        "visibleCol": "batch_no",
                    },
                }
            ]
        },
        headers=_auth_headers(),
        timeout=30,
    )
    response.raise_for_status()


def main() -> None:
    client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)
    for table in TARGET_TABLES:
        _ensure_batch_id_ref_and_display(client, table)
        print(f"Configured {table}.batch_id to display ProductBatchMaster.batch_no")


if __name__ == "__main__":
    main()
