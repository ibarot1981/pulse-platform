from __future__ import annotations

import requests

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


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


def _table_exists(client: GristClient, table_name: str) -> bool:
    return table_name in {str(t.get("id")) for t in client.list_tables()}


def _columns_by_id(client: GristClient, table_name: str) -> dict[str, dict]:
    return {str(c.get("id")): c for c in client.get_columns(table_name)}


def _ensure_table(client: GristClient, table_name: str, columns: list[dict]) -> None:
    if _table_exists(client, table_name):
        return
    client.create_table(table_name, columns)


def _ensure_column(client: GristClient, table_name: str, col_id: str, col_type: str) -> None:
    cols = _columns_by_id(client, table_name)
    if col_id not in cols:
        client.add_column(table_name, col_id, col_type)
        return
    current_type = str(cols[col_id].get("fields", {}).get("type") or "")
    if current_type and current_type != col_type:
        _apply(client, [["ModifyColumn", table_name, col_id, {"type": col_type}]])


def _upsert_rows(client: GristClient, table_name: str, key_col: str, rows: list[dict]) -> None:
    existing = client.get_records(table_name)
    by_key: dict[str, int] = {}
    for row in existing:
        rec_id = row.get("id")
        if not isinstance(rec_id, int):
            continue
        key = str(row.get("fields", {}).get(key_col) or "").strip()
        if key:
            by_key[key] = rec_id

    to_add: list[dict] = []
    to_update: list[dict] = []
    for row in rows:
        key = str(row.get(key_col) or "").strip()
        if not key:
            continue
        if key in by_key:
            to_update.append({"id": by_key[key], "fields": row})
        else:
            to_add.append(row)
    if to_add:
        client.add_records(table_name, to_add)
    if to_update:
        url = f"{_base_url()}/api/docs/{client.doc_id}/tables/{table_name}/records"
        response = requests.patch(
            url,
            headers={"Authorization": f"Bearer {client.api_key}"},
            json={"records": to_update},
            timeout=60,
        )
        response.raise_for_status()


def _normalize_ref(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _users_ref_by_user_id(costing_client: GristClient) -> dict[str, int]:
    mapping: dict[str, int] = {}
    for row in costing_client.get_records("Users"):
        rec_id = row.get("id")
        if not isinstance(rec_id, int):
            continue
        user_id = str(row.get("fields", {}).get("User_ID") or "").strip()
        if user_id:
            mapping[user_id] = rec_id
    return mapping


def _ensure_pulse_tables_and_seed(pulse_client: GristClient) -> None:
    _ensure_table(
        pulse_client,
        "UserRoleAssignment",
        [
            {"id": "Assignment_ID", "type": "Text"},
            {"id": "User", "type": "Ref:Users"},
            {"id": "Role", "type": "Ref:Roles"},
            {"id": "Assignment_Type", "type": "Text"},
            {"id": "Active", "type": "Bool"},
        ],
    )
    _ensure_table(
        pulse_client,
        "RoleHierarchy",
        [
            {"id": "Hierarchy_ID", "type": "Text"},
            {"id": "Parent_Role", "type": "Ref:Roles"},
            {"id": "Child_Role", "type": "Ref:Roles"},
            {"id": "Active", "type": "Bool"},
        ],
    )
    _ensure_column(pulse_client, "UserRoleAssignment", "Assignment_ID", "Text")
    _ensure_column(pulse_client, "UserRoleAssignment", "User", "Ref:Users")
    _ensure_column(pulse_client, "UserRoleAssignment", "Role", "Ref:Roles")
    _ensure_column(pulse_client, "UserRoleAssignment", "Assignment_Type", "Text")
    _ensure_column(pulse_client, "UserRoleAssignment", "Active", "Bool")
    _ensure_column(pulse_client, "RoleHierarchy", "Hierarchy_ID", "Text")
    _ensure_column(pulse_client, "RoleHierarchy", "Parent_Role", "Ref:Roles")
    _ensure_column(pulse_client, "RoleHierarchy", "Child_Role", "Ref:Roles")
    _ensure_column(pulse_client, "RoleHierarchy", "Active", "Bool")

    users = pulse_client.get_records("Users")
    role_rows = pulse_client.get_records("Roles")
    role_name_to_id = {
        str(r.get("fields", {}).get("Role_Name") or "").strip(): r.get("id")
        for r in role_rows
        if isinstance(r.get("id"), int)
    }

    assignments: list[dict] = []
    for row in users:
        user_rec_id = row.get("id")
        if not isinstance(user_rec_id, int):
            continue
        fields = row.get("fields", {})
        role_ref = _normalize_ref(fields.get("Role"))
        if not isinstance(role_ref, int):
            continue
        user_id = str(fields.get("User_ID") or user_rec_id)
        assignments.append(
            {
                "Assignment_ID": f"AUTO_{user_id}_{role_ref}",
                "User": user_rec_id,
                "Role": role_ref,
                "Assignment_Type": "PRIMARY",
                "Active": bool(fields.get("Active", True)),
            }
        )
    _upsert_rows(pulse_client, "UserRoleAssignment", "Assignment_ID", assignments)

    hierarchy_rows: list[dict] = []
    parent_id = role_name_to_id.get("Cutting_Supervisor")
    child_id = role_name_to_id.get("Machine-Shop Supervisor")
    if isinstance(parent_id, int) and isinstance(child_id, int):
        hierarchy_rows.append(
            {
                "Hierarchy_ID": "CUTTING_TO_MACHINE_SHOP",
                "Parent_Role": parent_id,
                "Child_Role": child_id,
                "Active": True,
            }
        )
    if hierarchy_rows:
        _upsert_rows(pulse_client, "RoleHierarchy", "Hierarchy_ID", hierarchy_rows)


def _sync_user_role_assignment_mirror(pulse_client: GristClient, costing_client: GristClient) -> None:
    pulse_users = pulse_client.get_records("Users")
    pulse_roles = pulse_client.get_records("Roles")
    pulse_assignments = pulse_client.get_records("UserRoleAssignment")

    user_code_by_rec_id = {
        row.get("id"): str(row.get("fields", {}).get("User_ID") or "").strip()
        for row in pulse_users
        if isinstance(row.get("id"), int)
    }
    role_code_by_rec_id = {
        row.get("id"): str(row.get("fields", {}).get("Role_ID") or "").strip()
        for row in pulse_roles
        if isinstance(row.get("id"), int)
    }

    role_mirror = costing_client.get_records("RoleMaster_Mirror")
    users_ref_by_user_id = _users_ref_by_user_id(costing_client)
    mirror_role_ref_by_code = {
        str(row.get("fields", {}).get("role_code") or "").strip(): row.get("id")
        for row in role_mirror
        if isinstance(row.get("id"), int)
    }

    rows: list[dict] = []
    for assignment in pulse_assignments:
        fields = assignment.get("fields", {})
        assignment_id = str(fields.get("Assignment_ID") or assignment.get("id") or "").strip()
        user_ref = _normalize_ref(fields.get("User"))
        role_ref = _normalize_ref(fields.get("Role"))
        if not isinstance(user_ref, int) or not isinstance(role_ref, int):
            continue
        user_code = user_code_by_rec_id.get(user_ref, "")
        role_code = role_code_by_rec_id.get(role_ref, "")
        mirror_user_ref = users_ref_by_user_id.get(user_code)
        mirror_role_ref = mirror_role_ref_by_code.get(role_code)
        if not isinstance(mirror_user_ref, int) or not isinstance(mirror_role_ref, int):
            continue
        rows.append(
            {
                "assignment_key": assignment_id or f"AUTO_{user_code}_{role_code}",
                "user_id": mirror_user_ref,
                "role_id": mirror_role_ref,
                "scope": str(fields.get("Assignment_Type") or "PRIMARY"),
                "active": bool(fields.get("Active", True)),
            }
        )

    _ensure_column(costing_client, "UserRoleAssignment_Mirror", "user_id", "Ref:Users")
    _ensure_column(costing_client, "UserRoleAssignment_Mirror", "assignment_key", "Text")
    _upsert_rows(costing_client, "UserRoleAssignment_Mirror", "assignment_key", rows)


def _ensure_costing_tables_and_seed(costing_client: GristClient) -> None:
    _ensure_column(costing_client, "ProductBatchMaster", "owner_user", "Ref:Users")
    _ensure_column(costing_client, "ProductBatchMaster", "notifier_users", "RefList:Users")

    _ensure_table(
        costing_client,
        "ProcessStageUserAssignment",
        [
            {"id": "assignment_id", "type": "Text"},
            {"id": "process_stage_id", "type": "Ref:ProcessStage"},
            {"id": "user_id", "type": "Ref:Users"},
            {"id": "can_notify", "type": "Bool"},
            {"id": "can_act", "type": "Bool"},
            {"id": "active", "type": "Bool"},
        ],
    )
    _ensure_table(
        costing_client,
        "BatchMSDelegation",
        [
            {"id": "delegation_id", "type": "Text"},
            {"id": "batch_ms_id", "type": "Ref:ProductBatchMS"},
            {"id": "delegated_to_user", "type": "Ref:Users"},
            {"id": "delegated_by_user", "type": "Ref:Users"},
            {"id": "can_notify", "type": "Bool"},
            {"id": "can_act", "type": "Bool"},
            {"id": "active", "type": "Bool"},
            {"id": "delegated_at", "type": "DateTime"},
            {"id": "remarks", "type": "Text"},
        ],
    )
    _ensure_column(costing_client, "ProcessStageUserAssignment", "assignment_id", "Text")
    _ensure_column(costing_client, "ProcessStageUserAssignment", "process_stage_id", "Ref:ProcessStage")
    _ensure_column(costing_client, "ProcessStageUserAssignment", "user_id", "Ref:Users")
    _ensure_column(costing_client, "ProcessStageUserAssignment", "can_notify", "Bool")
    _ensure_column(costing_client, "ProcessStageUserAssignment", "can_act", "Bool")
    _ensure_column(costing_client, "ProcessStageUserAssignment", "active", "Bool")
    _ensure_column(costing_client, "BatchMSDelegation", "delegation_id", "Text")
    _ensure_column(costing_client, "BatchMSDelegation", "batch_ms_id", "Ref:ProductBatchMS")
    _ensure_column(costing_client, "BatchMSDelegation", "delegated_to_user", "Ref:Users")
    _ensure_column(costing_client, "BatchMSDelegation", "delegated_by_user", "Ref:Users")
    _ensure_column(costing_client, "BatchMSDelegation", "can_notify", "Bool")
    _ensure_column(costing_client, "BatchMSDelegation", "can_act", "Bool")
    _ensure_column(costing_client, "BatchMSDelegation", "active", "Bool")
    _ensure_column(costing_client, "BatchMSDelegation", "delegated_at", "DateTime")
    _ensure_column(costing_client, "BatchMSDelegation", "remarks", "Text")

    stage_rows = costing_client.get_records("ProcessStage")
    user_rows = costing_client.get_records("Users")
    first_stage = next((row for row in stage_rows if isinstance(row.get("id"), int)), None)
    first_user = next((row for row in user_rows if isinstance(row.get("id"), int)), None)
    if first_stage and first_user:
        _upsert_rows(
            costing_client,
            "ProcessStageUserAssignment",
            "assignment_id",
            [
                {
                    "assignment_id": f"SAMPLE_STAGE_{first_stage['id']}_{first_user['id']}",
                    "process_stage_id": first_stage["id"],
                    "user_id": first_user["id"],
                    "can_notify": True,
                    "can_act": True,
                    "active": True,
                }
            ],
        )

    ms_rows = costing_client.get_records("ProductBatchMS")
    costing_users = costing_client.get_records("Users")
    first_ms = next((row for row in ms_rows if isinstance(row.get("id"), int)), None)
    first_costing_user = next((row for row in costing_users if isinstance(row.get("id"), int)), None)
    if first_ms and first_costing_user:
        _upsert_rows(
            costing_client,
            "BatchMSDelegation",
            "delegation_id",
            [
                {
                    "delegation_id": f"SAMPLE_DELEGATION_{first_ms['id']}_{first_costing_user['id']}",
                    "batch_ms_id": first_ms["id"],
                    "delegated_to_user": first_costing_user["id"],
                    "delegated_by_user": first_costing_user["id"],
                    "can_notify": True,
                    "can_act": True,
                    "active": True,
                    "remarks": "Sample delegation for testing",
                }
            ],
        )


def main() -> None:
    pulse_client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)
    costing_client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)

    _ensure_pulse_tables_and_seed(pulse_client)
    _ensure_costing_tables_and_seed(costing_client)
    _sync_user_role_assignment_mirror(pulse_client, costing_client)

    print("Phase-3 batch actor routing schema and sample data applied.")


if __name__ == "__main__":
    main()
