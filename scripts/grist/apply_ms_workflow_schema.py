from __future__ import annotations

import json
from typing import Iterable

import requests

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


def _auth_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {COSTING_API_KEY}",
        "Content-Type": "application/json",
    }


def _apply_actions(actions: list) -> dict:
    url = f"{PULSE_GRIST_SERVER}/api/docs/{COSTING_DOC_ID}/apply"
    response = requests.post(url, headers=_auth_headers(), data=json.dumps(actions), timeout=30)
    response.raise_for_status()
    return response.json()


def _table_exists(client: GristClient, table: str) -> bool:
    try:
        client.get_columns(table)
        return True
    except Exception:
        return False


def _columns_map(client: GristClient, table: str) -> dict[str, dict]:
    return {col.get("id"): col for col in client.get_columns(table)}


def _ensure_table(client: GristClient, table: str, columns: list[dict]) -> None:
    if _table_exists(client, table):
        return
    client.create_table(table, columns)


def _ensure_column(client: GristClient, table: str, column_id: str, column_type: str) -> None:
    columns = _columns_map(client, table)
    if column_id not in columns:
        _apply_actions([["AddColumn", table, column_id, {"type": column_type}]])
        return

    current_type = str(columns[column_id].get("fields", {}).get("type") or "")
    if current_type != column_type:
        _apply_actions([["ModifyColumn", table, column_id, {"type": column_type}]])


def _chunked(values: list[dict], size: int) -> Iterable[list[dict]]:
    for i in range(0, len(values), size):
        yield values[i : i + size]


def _remove_column_if_exists(client: GristClient, table: str, column_id: str) -> None:
    columns = _columns_map(client, table)
    if column_id in columns:
        _apply_actions([["RemoveColumn", table, column_id]])


def _copy_process_seq_values(client: GristClient) -> None:
    records = client.get_records("ProductPartMSList")
    updates = []
    for row in records:
        fields = row.get("fields", {})
        src = fields.get("Process_Seq")
        dst = fields.get("process_seq")
        if (dst is None or str(dst).strip() == "") and src not in (None, ""):
            updates.append({"id": row.get("id"), "fields": {"process_seq": src}})

    if not updates:
        return

    url = f"{PULSE_GRIST_SERVER}/api/docs/{COSTING_DOC_ID}/tables/ProductPartMSList/records"
    headers = {"Authorization": f"Bearer {COSTING_API_KEY}"}
    for batch in _chunked(updates, 200):
        response = requests.patch(url, headers=headers, json={"records": batch}, timeout=30)
        response.raise_for_status()


def _seed_process_stage_mapping(client: GristClient) -> None:
    records = client.get_records("ProcessStageMapping")
    existing = {}
    for row in records:
        stage_name = str(row.get("fields", {}).get("stage_name") or "").strip()
        if stage_name:
            existing[stage_name] = row

    ms_rows = client.get_records("ProductPartMSList")
    discovered_stages: set[str] = set()
    for row in ms_rows:
        seq = row.get("fields", {}).get("Process_Seq")
        if not seq:
            continue
        parts = [token.strip() for token in str(seq).split(" - ") if token.strip()]
        discovered_stages.update(parts)

    to_add = []
    for stage_name in sorted(discovered_stages):
        if stage_name in existing:
            continue
        lowered = stage_name.lower()
        if lowered == "production":
            role = "Production Supervisor"
            priority = 300
        elif "press job" in lowered:
            role = "Press Supervisor"
            priority = 200
        else:
            role = "Cutting Supervisor"
            priority = 100
        to_add.append(
            {
                "stage_name": stage_name,
                "supervisor_role": role,
                "stage_order_priority": priority,
            }
        )

    if to_add:
        client.add_records("ProcessStageMapping", to_add)


def main() -> None:
    client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)

    _ensure_table(
        client,
        "ProcessStageMapping",
        [
            {"id": "stage_name", "type": "Text"},
            {"id": "supervisor_role", "type": "Text"},
            {"id": "stage_order_priority", "type": "Int"},
        ],
    )

    _ensure_column(client, "ProcessStageMapping", "stage_name", "Text")
    _ensure_column(client, "ProcessStageMapping", "supervisor_role", "Text")
    _ensure_column(client, "ProcessStageMapping", "stage_order_priority", "Int")

    part_columns = _columns_map(client, "ProductPartMSList")
    if "Process_Seq" in part_columns:
        # Keep canonical existing choice column and remove accidental duplicate if present.
        _remove_column_if_exists(client, "ProductPartMSList", "process_seq2")
    else:
        _ensure_column(client, "ProductPartMSList", "process_seq", "Choice")
        _copy_process_seq_values(client)

    _ensure_column(client, "ProductBatchMS", "process_seq", "Text")
    _ensure_column(client, "ProductBatchMS", "total_qty", "Numeric")
    _ensure_column(client, "ProductBatchMS", "current_stage_index", "Int")
    _ensure_column(client, "ProductBatchMS", "current_stage_name", "Text")
    _ensure_column(client, "ProductBatchMS", "current_status", "Text")
    _ensure_column(client, "ProductBatchMS", "created_at", "DateTime")
    _ensure_column(client, "ProductBatchMS", "updated_at", "DateTime")
    _ensure_column(client, "ProductBatchMS", "last_updated_by", "Ref:Users")
    _remove_column_if_exists(client, "ProductBatchMS", "ms_cutlist_pdf")

    _ensure_column(client, "ProductBatchMaster", "ms_cutlist_pdf", "Attachments")
    _ensure_column(client, "ProductBatchMaster", "cnc_cutlist_pdf", "Attachments")

    _seed_process_stage_mapping(client)

    print("MS workflow schema migration applied.")


if __name__ == "__main__":
    main()
