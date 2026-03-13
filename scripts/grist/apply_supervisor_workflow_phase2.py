from __future__ import annotations

import requests

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


def _headers(api_key: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {api_key}"}


def _base_url() -> str:
    return str(PULSE_GRIST_SERVER).rstrip("/")


def _apply(client: GristClient, actions: list) -> None:
    url = f"{_base_url()}/api/docs/{client.doc_id}/apply"
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {client.api_key}", "Content-Type": "application/json"},
        json=actions,
        timeout=60,
    )
    response.raise_for_status()


def _get_columns(client: GristClient, table: str) -> dict[str, dict]:
    return {str(col.get("id")): col for col in client.get_columns(table)}


def _ensure_column(client: GristClient, table: str, col_id: str, col_type: str) -> None:
    cols = _get_columns(client, table)
    if col_id not in cols:
        _apply(client, [["AddColumn", table, col_id, {"type": col_type}]])
        return
    current_type = str(cols[col_id].get("fields", {}).get("type") or "")
    if col_type and current_type and current_type != col_type:
        _apply(client, [["ModifyColumn", table, col_id, {"type": col_type}]])


def _set_formula(client: GristClient, table: str, col_id: str, formula: str, col_type: str = "Text") -> None:
    _ensure_column(client, table, col_id, col_type)
    payload = {"isFormula": True, "formula": formula}
    if col_type:
        payload["type"] = col_type
    _apply(client, [["ModifyColumn", table, col_id, payload]])


def _set_non_formula(client: GristClient, table: str, col_id: str, col_type: str) -> None:
    _ensure_column(client, table, col_id, col_type)
    _apply(client, [["ModifyColumn", table, col_id, {"type": col_type, "isFormula": False, "formula": ""}]])


def _upsert_by_field(client: GristClient, table: str, key_field: str, rows: list[dict]) -> None:
    column_ids = set(_get_columns(client, table).keys())
    if key_field not in column_ids:
        return

    records = client.get_records(table)
    by_key: dict[str, int] = {}
    for record in records:
        rec_id = record.get("id")
        if not isinstance(rec_id, int):
            continue
        key_val = str(record.get("fields", {}).get(key_field) or "").strip()
        if key_val:
            by_key[key_val] = rec_id

    to_add = []
    to_update = []
    for row in rows:
        filtered = {k: v for k, v in row.items() if k in column_ids}
        if key_field not in filtered:
            continue
        key_val = str(filtered.get(key_field) or "").strip()
        if not key_val:
            continue
        if key_val in by_key:
            to_update.append({"id": by_key[key_val], "fields": filtered})
        else:
            to_add.append(filtered)

    if to_add:
        client.add_records(table, to_add)
    if to_update:
        url = f"{_base_url()}/api/docs/{client.doc_id}/tables/{table}/records"
        response = requests.patch(url, headers=_headers(client.api_key), json={"records": to_update}, timeout=60)
        response.raise_for_status()


def _ensure_ms_formulas(costing_client: GristClient) -> None:
    table = "ProductBatchMS"
    _set_non_formula(costing_client, table, "product_part", "RefList:ProductPartMSList")
    _set_non_formula(costing_client, table, "row_cutlist_pdf", "Attachments")
    _ensure_column(costing_client, table, "stage_due_date", "DateTime")

    _set_formula(
        costing_client,
        table,
        "next_stage_name",
        (
            "if not $process_seq:\n"
            "  return ''\n"
            "stages = ProcessStage.lookupRecords(process_seq_id=$process_seq)\n"
            "ordered = sorted(stages, key=lambda r: (r.seq_no or 0, r.id))\n"
            "stage_names = [r.stage_name for r in ordered if r.stage_name]\n"
            "if not stage_names:\n"
            "  return ''\n"
            "current = str($current_stage_name or '').strip()\n"
            "if current in stage_names:\n"
            "  idx = stage_names.index(current)\n"
            "else:\n"
            "  idx = int($current_stage_index or 0)\n"
            "next_idx = idx + 1\n"
            "if 0 <= next_idx < len(stage_names):\n"
            "  return stage_names[next_idx]\n"
            "return ''"
        ),
        "Text",
    )
    _set_formula(
        costing_client,
        table,
        "current_stage_role_name",
        (
            "if not $process_seq or not $current_stage_name:\n"
            "  return ''\n"
            "row = ProcessStage.lookupOne(process_seq_id=$process_seq, stage_name=$current_stage_name)\n"
            "if row and row.resolved_role_name:\n"
            "  return row.resolved_role_name\n"
            "return ''"
        ),
        "Text",
    )
    _set_formula(
        costing_client,
        table,
        "current_stage_supervisors",
        (
            "if not $current_stage_role_name:\n"
            "  return ''\n"
            "role = RoleMaster_Mirror.lookupOne(role_name=$current_stage_role_name)\n"
            "if not role:\n"
            "  return ''\n"
            "assignments = UserRoleAssignment_Mirror.lookupRecords(role_id=role, active=True)\n"
            "users = set()\n"
            "for a in assignments:\n"
            "  u = a.user_id\n"
            "  if not u:\n"
            "    continue\n"
            "  name = ''\n"
            "  for attr in ('Name', 'name', 'User_Name', 'UserName', 'Full_Name', 'Employee_Name', 'User_ID'):\n"
            "    value = getattr(u, attr, '')\n"
            "    if value:\n"
            "      name = str(value).strip()\n"
            "      break\n"
            "  if name:\n"
            "    users.add(name)\n"
            "users = sorted(users)\n"
            "return ', '.join(users)"
        ),
        "Text",
    )


def _ensure_notifications_and_rules(pulse_client: GristClient) -> None:
    _upsert_by_field(
        pulse_client,
        "Notification_Events",
        "Event_ID",
        [
            {
                "Event_ID": "production_batch_scheduled",
                "Description": "Batch scheduled by supervisor/manager",
                "Domain": "production",
                "Recipient_Mode": "OWNER_PLUS_SUBSCRIBERS",
                "Active": True,
            },
            {
                "Event_ID": "supervisor_batch_schedule_reminder",
                "Description": "Reminder to supervisor for pending batch scheduling",
                "Domain": "production",
                "Recipient_Mode": "SUBSCRIBERS_ONLY",
                "Active": True,
            },
            {
                "Event_ID": "ms_stage_pending_reminder",
                "Description": "Reminder to supervisor for pending MS stage action",
                "Domain": "production",
                "Recipient_Mode": "SUBSCRIBERS_ONLY",
                "Active": True,
            },
        ],
    )
    _upsert_by_field(
        pulse_client,
        "Reminder_Rules",
        "Rule_ID",
        [
            {
                "Rule_ID": "supervisor_batch_schedule_reminder",
                "Applies_To": "ProductBatchMaster",
                "Target_Domain": "production",
                "Condition_Type": "SCHEDULE_PENDING",
                "Escalation_Level": "SUPERVISOR",
                "Frequency": "DAILY",
                "Threshold_Days": 1,
                "Active": True,
            },
            {
                "Rule_ID": "ms_stage_pending_reminder",
                "Applies_To": "ProductBatchMS",
                "Target_Domain": "production",
                "Condition_Type": "STAGE_PENDING",
                "Escalation_Level": "SUPERVISOR",
                "Frequency": "DAILY",
                "Threshold_Days": 1,
                "Active": True,
            },
        ],
    )


def _ensure_supervisor_menu_permissions(pulse_client: GristClient) -> None:
    _upsert_by_field(
        pulse_client,
        "Permissions",
        "Permission_ID",
        [
            {
                "Permission_ID": "production_my_ms_schedule",
                "Menu_Label": "Schedule My Batches",
                "Menu_Parent": "MANAGE_PRODUCTION",
                "Action_Type": "RUN_STUB",
                "Action_Target": "MY_MS_SCHEDULE",
                "Active": True,
            },
            {
                "Permission_ID": "production_my_ms_jobs",
                "Menu_Label": "My MS Jobs",
                "Menu_Parent": "MANAGE_PRODUCTION",
                "Action_Type": "RUN_STUB",
                "Action_Target": "MY_MS_JOBS",
                "Active": True,
            },
        ],
    )

    roles = pulse_client.get_records("Roles")
    perms = pulse_client.get_records("Permissions")
    role_ids = []
    for role in roles:
        role_name = str(role.get("fields", {}).get("Role_Name") or "")
        if "Supervisor" in role_name:
            rec_id = role.get("id")
            if isinstance(rec_id, int):
                role_ids.append(rec_id)

    perm_ids = []
    for perm in perms:
        perm_code = str(perm.get("fields", {}).get("Permission_ID") or "")
        if perm_code in {"production_my_ms_schedule", "production_my_ms_jobs"}:
            rec_id = perm.get("id")
            if isinstance(rec_id, int):
                perm_ids.append(rec_id)

    existing = pulse_client.get_records("Role_Permissions")
    existing_pairs: set[tuple[int, int]] = set()
    for row in existing:
        fields = row.get("fields", {})
        role_ref = fields.get("Role")
        perm_ref = fields.get("Permission")
        if isinstance(role_ref, list):
            role_ref = role_ref[0] if role_ref else None
        if isinstance(perm_ref, list):
            perm_ref = perm_ref[0] if perm_ref else None
        if isinstance(role_ref, int) and isinstance(perm_ref, int):
            existing_pairs.add((role_ref, perm_ref))

    to_add = []
    for role_id in role_ids:
        for perm_id in perm_ids:
            if (role_id, perm_id) in existing_pairs:
                continue
            to_add.append({"Role": role_id, "Permission": perm_id, "Active": True})
    if to_add:
        pulse_client.add_records("Role_Permissions", to_add)


def main() -> None:
    costing_client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)
    pulse_client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)

    _ensure_ms_formulas(costing_client)
    _ensure_notifications_and_rules(pulse_client)
    _ensure_supervisor_menu_permissions(pulse_client)
    print("Supervisor workflow phase 2 updates applied.")


if __name__ == "__main__":
    main()
