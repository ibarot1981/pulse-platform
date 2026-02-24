from __future__ import annotations

import json
import re
from typing import Any

import requests

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


def _base_url() -> str:
    return PULSE_GRIST_SERVER.rstrip("/")


def _headers(content_type: bool = True) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {COSTING_API_KEY}"}
    if content_type:
        headers["Content-Type"] = "application/json"
    return headers


def _apply(actions: list[Any]) -> None:
    url = f"{_base_url()}/api/docs/{COSTING_DOC_ID}/apply"
    response = requests.post(url, headers=_headers(), data=json.dumps(actions), timeout=60)
    response.raise_for_status()


def _table_exists(client: GristClient, table: str) -> bool:
    try:
        client.get_columns(table)
        return True
    except Exception:
        return False


def _columns_by_id(client: GristClient, table: str) -> dict[str, dict]:
    return {c.get("id"): c for c in client.get_columns(table)}


def _ensure_table(client: GristClient, table: str, columns: list[dict]) -> None:
    if _table_exists(client, table):
        return
    client.create_table(table, columns)


def _ensure_column(client: GristClient, table: str, col_id: str, col_type: str) -> None:
    cols = _columns_by_id(client, table)
    if col_id not in cols:
        _apply([["AddColumn", table, col_id, {"type": col_type}]])
        return
    current_type = str(cols[col_id].get("fields", {}).get("type") or "")
    if current_type != col_type:
        _apply([["ModifyColumn", table, col_id, {"type": col_type}]])


def _set_formula(table: str, col_id: str, formula: str, col_type: str | None = None) -> None:
    payload: dict[str, Any] = {"isFormula": True, "formula": formula}
    if col_type:
        payload["type"] = col_type
    _apply([["ModifyColumn", table, col_id, payload]])


def _set_visible_col(client: GristClient, table: str, col_id: str, ref_table: str, visible_col_id: str) -> None:
    visible_ref = None
    for col in client.get_columns(ref_table):
        if col.get("id") == visible_col_id:
            visible_ref = col.get("fields", {}).get("colRef")
            break
    if visible_ref is None:
        return
    _apply([["ModifyColumn", table, col_id, {"visibleCol": int(visible_ref)}]])


def _safe_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _slug(value: str, max_len: int = 24) -> str:
    raw = re.sub(r"[^A-Za-z0-9]+", "_", str(value or "").strip()).strip("_").upper()
    return raw[:max_len] or "X"


def _id_map_by_field(client: GristClient, table: str, field: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in client.get_records(table):
        key = str(row.get("fields", {}).get(field) or "").strip()
        if not key:
            continue
        row_id = row.get("id")
        if isinstance(row_id, int):
            result[key] = row_id
    return result


def _upsert_records(client: GristClient, table: str, key_field: str, rows: list[dict]) -> None:
    existing = _id_map_by_field(client, table, key_field)
    to_add = []
    updates = []
    for row in rows:
        key = str(row.get(key_field) or "").strip()
        if not key:
            continue
        rec_id = existing.get(key)
        if rec_id is None:
            to_add.append(row)
        else:
            updates.append({"id": rec_id, "fields": row})
    if to_add:
        client.add_records(table, to_add)
    if updates:
        url = f"{_base_url()}/api/docs/{COSTING_DOC_ID}/tables/{table}/records"
        response = requests.patch(url, headers={"Authorization": f"Bearer {COSTING_API_KEY}"}, json={"records": updates}, timeout=60)
        response.raise_for_status()


def _ensure_schema(client: GristClient) -> None:
    _ensure_table(
        client,
        "RoleMaster_Mirror",
        [
            {"id": "role_code", "type": "Text"},
            {"id": "role_name", "type": "Text"},
            {"id": "active", "type": "Bool"},
            {"id": "source_system", "type": "Text"},
        ],
    )
    _ensure_table(
        client,
        "UserMaster_Mirror",
        [
            {"id": "user_code", "type": "Text"},
            {"id": "user_name", "type": "Text"},
            {"id": "active", "type": "Bool"},
            {"id": "source_system", "type": "Text"},
        ],
    )
    _ensure_table(
        client,
        "UserRoleAssignment_Mirror",
        [
            {"id": "user_id", "type": "Ref:UserMaster_Mirror"},
            {"id": "role_id", "type": "Ref:RoleMaster_Mirror"},
            {"id": "scope", "type": "Text"},
            {"id": "active", "type": "Bool"},
            {"id": "user_name", "type": "Text"},
            {"id": "role_name", "type": "Text"},
        ],
    )
    _ensure_table(
        client,
        "StageMaster",
        [
            {"id": "stage_code", "type": "Text"},
            {"id": "stage_name", "type": "Text"},
            {"id": "default_role_id", "type": "Ref:RoleMaster_Mirror"},
            {"id": "default_role_name", "type": "Text"},
            {"id": "active", "type": "Bool"},
        ],
    )
    _ensure_table(
        client,
        "ProcessMaster",
        [
            {"id": "process_code", "type": "Text"},
            {"id": "process_name", "type": "Text"},
            {"id": "version", "type": "Int"},
            {"id": "status", "type": "Text"},
            {"id": "legacy_process_seq_text", "type": "Text"},
            {"id": "active", "type": "Bool"},
            {"id": "stage_count", "type": "Int"},
            {"id": "display_label", "type": "Text"},
            {"id": "display_summary", "type": "Text"},
        ],
    )
    _ensure_table(
        client,
        "ProcessStage",
        [
            {"id": "process_seq_id", "type": "Ref:ProcessMaster"},
            {"id": "seq_no", "type": "Int"},
            {"id": "stage_id", "type": "Ref:StageMaster"},
            {"id": "stage_name", "type": "Text"},
            {"id": "stage_level", "type": "Int"},
            {"id": "parent_stage_id", "type": "Ref:ProcessStage"},
            {"id": "role_override_id", "type": "Ref:RoleMaster_Mirror"},
            {"id": "resolved_role_id", "type": "Ref:RoleMaster_Mirror"},
            {"id": "resolved_role_name", "type": "Text"},
            {"id": "suggested_supervisors", "type": "Text"},
            {"id": "stage_label", "type": "Text"},
        ],
    )
    # Ensure column types remain correct on reruns.
    _ensure_column(client, "UserRoleAssignment_Mirror", "user_id", "Ref:UserMaster_Mirror")
    _ensure_column(client, "UserRoleAssignment_Mirror", "role_id", "Ref:RoleMaster_Mirror")
    _ensure_column(client, "StageMaster", "default_role_id", "Ref:RoleMaster_Mirror")
    _ensure_column(client, "ProcessStage", "process_seq_id", "Ref:ProcessMaster")
    _ensure_column(client, "ProcessStage", "stage_id", "Ref:StageMaster")
    _ensure_column(client, "ProcessStage", "parent_stage_id", "Ref:ProcessStage")
    _ensure_column(client, "ProcessStage", "role_override_id", "Ref:RoleMaster_Mirror")
    _ensure_column(client, "ProcessStage", "resolved_role_id", "Ref:RoleMaster_Mirror")
    _ensure_column(client, "ProcessMaster", "process_remarks", "Text")


def _sync_role_user_mirrors(client: GristClient) -> tuple[dict[str, int], dict[str, int]]:
    pulse = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)
    pulse_roles = pulse.get_records("Roles")
    pulse_users = pulse.get_records("Users")

    roles = []
    role_ref_to_code: dict[int, str] = {}
    for row in pulse_roles:
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        fields = row.get("fields", {})
        role_code = str(fields.get("Role_ID") or f"ROLE_{row_id}").strip()
        role_name = str(fields.get("Role_Name") or "").strip()
        if not role_code or not role_name:
            continue
        roles.append({"role_code": role_code, "role_name": role_name, "active": bool(fields.get("Active", True)), "source_system": "Pulse"})
        role_ref_to_code[row_id] = role_code
    _upsert_records(client, "RoleMaster_Mirror", "role_code", roles)

    users = []
    for row in pulse_users:
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        fields = row.get("fields", {})
        user_code = str(fields.get("User_ID") or f"USER_{row_id}").strip()
        user_name = str(fields.get("Name") or "").strip()
        if not user_code or not user_name:
            continue
        users.append({"user_code": user_code, "user_name": user_name, "active": bool(fields.get("Active", True)), "source_system": "Pulse"})
    _upsert_records(client, "UserMaster_Mirror", "user_code", users)

    role_ids = _id_map_by_field(client, "RoleMaster_Mirror", "role_code")
    user_ids = _id_map_by_field(client, "UserMaster_Mirror", "user_code")

    existing_pairs = set()
    for row in client.get_records("UserRoleAssignment_Mirror"):
        f = row.get("fields", {})
        uid = _safe_int(f.get("user_id"))
        rid = _safe_int(f.get("role_id"))
        if uid and rid:
            existing_pairs.add((uid, rid))

    to_add = []
    for row in pulse_users:
        fields = row.get("fields", {})
        user_code = str(fields.get("User_ID") or "").strip()
        role_ref = _safe_int(fields.get("Role"))
        if not user_code or role_ref is None:
            continue
        role_code = role_ref_to_code.get(role_ref)
        if not role_code:
            continue
        uid = user_ids.get(user_code)
        rid = role_ids.get(role_code)
        if not uid or not rid:
            continue
        if (uid, rid) in existing_pairs:
            continue
        scope = f"Dept-{fields.get('Department')}" if fields.get("Department") not in (None, "") else "Global"
        to_add.append({"user_id": uid, "role_id": rid, "scope": scope, "active": bool(fields.get("Active", True))})
        existing_pairs.add((uid, rid))
    if to_add:
        client.add_records("UserRoleAssignment_Mirror", to_add)

    return role_ids, user_ids


def _seed_stage_master(client: GristClient, role_ids: dict[str, int], legacy_sequences: list[str]) -> dict[str, int]:
    cutting_role = role_ids.get("R06") or next(iter(role_ids.values()))
    production_role = role_ids.get("R01") or cutting_role

    stage_names = set()
    for seq in legacy_sequences:
        for token in [part.strip() for part in str(seq).split(" - ") if part.strip()]:
            stage_names.add(token)

    stage_names.update(
        {
            "40Ton Angle Cutting Press",
            "30T Iron Worker",
            "CNC Turning",
            "17mm Hole Drilling",
            "Production",
        }
    )

    rows = []
    for stage_name in sorted(stage_names):
        lower = stage_name.lower()
        if "production" in lower:
            default_role = production_role
        elif any(word in lower for word in ("cnc", "turn", "drill", "machin")):
            default_role = production_role
        else:
            default_role = cutting_role
        rows.append(
            {
                "stage_code": _slug(stage_name),
                "stage_name": stage_name,
                "default_role_id": default_role,
                "active": True,
            }
        )
    _upsert_records(client, "StageMaster", "stage_code", rows)
    return _id_map_by_field(client, "StageMaster", "stage_code")


def _seed_process_master_and_stages(client: GristClient, stage_ids: dict[str, int], legacy_sequences: list[str]) -> dict[str, int]:
    canonical = {
        "Press Cutting - Press Job - Production": ("PRS-PJ-PROD", "Press + Press Job + Production"),
        "Plasma Cutting - Production": ("PLS-PROD", "Plasma + Production"),
        "Press Cutting - Production": ("PRS-PROD", "Press + Production"),
        "Plasma Cutting - Press Job - Production": ("PLS-PJ-PROD", "Plasma + Press Job + Production"),
    }

    process_rows = []
    code_by_sequence: dict[str, str] = {}
    for idx, seq in enumerate(sorted(set(legacy_sequences)), start=1):
        if seq in canonical:
            code, name = canonical[seq]
        else:
            code = f"CUSTOM-{idx:03d}"
            name = seq
        code_by_sequence[seq] = code
        process_rows.append(
            {
                "process_code": code,
                "process_name": name,
                "version": 1,
                "status": "Active",
                "legacy_process_seq_text": seq,
            }
        )
    process_rows.append(
        {
            "process_code": "PRS-DET-V1",
            "process_name": "Detailed Press Route",
            "version": 1,
            "status": "Active",
            "legacy_process_seq_text": "Press Cutting - Press Job - Production",
        }
    )
    _upsert_records(client, "ProcessMaster", "process_code", process_rows)
    process_ids = _id_map_by_field(client, "ProcessMaster", "process_code")

    existing_keys = set()
    for row in client.get_records("ProcessStage"):
        f = row.get("fields", {})
        p = _safe_int(f.get("process_seq_id"))
        seq_no = _safe_int(f.get("seq_no"))
        s = _safe_int(f.get("stage_id"))
        if p and seq_no is not None and s:
            existing_keys.add((p, seq_no, s))

    def add_process_stages(process_code: str, stage_names: list[str]) -> None:
        process_id = process_ids.get(process_code)
        if not process_id:
            return
        to_add = []
        for i, stage_name in enumerate(stage_names, start=1):
            seq_no = i * 10
            stage_code = _slug(stage_name)
            stage_id = stage_ids.get(stage_code)
            if not stage_id:
                continue
            key = (process_id, seq_no, stage_id)
            if key in existing_keys:
                continue
            to_add.append({"process_seq_id": process_id, "seq_no": seq_no, "stage_id": stage_id, "stage_level": 1})
            existing_keys.add(key)
        if to_add:
            client.add_records("ProcessStage", to_add)

    for legacy_seq in sorted(set(legacy_sequences)):
        process_code = code_by_sequence.get(legacy_seq)
        if not process_code:
            continue
        add_process_stages(process_code, [token.strip() for token in legacy_seq.split(" - ") if token.strip()])

    add_process_stages(
        "PRS-DET-V1",
        ["40Ton Angle Cutting Press", "30T Iron Worker", "CNC Turning", "17mm Hole Drilling", "Production"],
    )
    return process_ids


def _seed_process_remarks_from_summary(client: GristClient) -> None:
    updates = []
    for row in client.get_records("ProcessMaster"):
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        fields = row.get("fields", {})
        remarks = str(fields.get("process_remarks") or "").strip()
        if remarks:
            continue
        summary = str(fields.get("display_summary") or "").strip()
        if not summary:
            continue
        updates.append({"id": row_id, "fields": {"process_remarks": summary}})
    _patch_records("ProcessMaster", updates)


def _apply_formulas_and_display(client: GristClient) -> None:
    _set_formula("UserRoleAssignment_Mirror", "user_name", "$user_id.user_name if $user_id else ''", "Text")
    _set_formula("UserRoleAssignment_Mirror", "role_name", "$role_id.role_name if $role_id else ''", "Text")
    _set_formula("StageMaster", "default_role_name", "$default_role_id.role_name if $default_role_id else ''", "Text")

    _set_formula("ProcessMaster", "active", "$status == 'Active'", "Bool")
    _set_formula("ProcessMaster", "stage_count", "len(ProcessStage.lookupRecords(process_seq_id=$id))", "Int")
    _set_formula(
        "ProcessMaster",
        "display_label",
        "\"%s | %s | V%s | %s stages\" % ($process_code, $process_name, $version, $stage_count)",
        "Text",
    )
    _set_formula(
        "ProcessMaster",
        "display_summary",
        "stages = ProcessStage.lookupRecords(process_seq_id=$id)\nordered = sorted(stages, key=lambda r: (r.seq_no or 0, r.id))\nreturn ' > '.join([s.stage_name for s in ordered if s.stage_name])",
        "Text",
    )

    _set_formula("ProcessStage", "stage_name", "$stage_id.stage_name if $stage_id else ''", "Text")
    _set_formula(
        "ProcessStage",
        "resolved_role_id",
        "if $role_override_id:\n  return $role_override_id\nif $stage_id:\n  return $stage_id.default_role_id\nreturn None",
        "Ref:RoleMaster_Mirror",
    )
    _set_formula("ProcessStage", "resolved_role_name", "$resolved_role_id.role_name if $resolved_role_id else ''", "Text")
    _set_formula(
        "ProcessStage",
        "suggested_supervisors",
        "if not $resolved_role_id:\n  return ''\nassignments = UserRoleAssignment_Mirror.lookupRecords(role_id=$resolved_role_id, active=True)\nusers = sorted(set([a.user_id.user_name for a in assignments if a.user_id and a.user_id.user_name]))\nreturn ', '.join(users)",
        "Text",
    )
    _set_formula("ProcessStage", "stage_label", "\"%s. %s\" % ($seq_no, $stage_name) if $stage_name else ''", "Text")

    _set_visible_col(client, "UserRoleAssignment_Mirror", "user_id", "UserMaster_Mirror", "user_name")
    _set_visible_col(client, "UserRoleAssignment_Mirror", "role_id", "RoleMaster_Mirror", "role_name")
    _set_visible_col(client, "StageMaster", "default_role_id", "RoleMaster_Mirror", "role_name")
    _set_visible_col(client, "ProcessStage", "process_seq_id", "ProcessMaster", "display_label")
    _set_visible_col(client, "ProcessStage", "stage_id", "StageMaster", "stage_name")
    _set_visible_col(client, "ProcessStage", "resolved_role_id", "RoleMaster_Mirror", "role_name")


def _patch_records(table: str, updates: list[dict]) -> None:
    if not updates:
        return
    url = f"{_base_url()}/api/docs/{COSTING_DOC_ID}/tables/{table}/records"
    grouped: dict[tuple[str, ...], list[dict]] = {}
    for record in updates:
        fields = record.get("fields", {})
        key = tuple(sorted(fields.keys()))
        grouped.setdefault(key, []).append(record)

    chunk_size = 200
    for _, records in grouped.items():
        for i in range(0, len(records), chunk_size):
            batch = records[i : i + chunk_size]
            response = requests.patch(
                url,
                headers={"Authorization": f"Bearer {COSTING_API_KEY}"},
                json={"records": batch},
                timeout=60,
            )
            response.raise_for_status()


def _migrate_product_part_ms_list(client: GristClient, process_ids_by_legacy: dict[str, int]) -> None:
    rows = client.get_records("ProductPartMSList")
    valid_process_ids = {
        row.get("id")
        for row in client.get_records("ProcessMaster")
        if isinstance(row.get("id"), int)
    }
    snapshots = []
    for row in rows:
        f = row.get("fields", {})
        snapshots.append(
            {
                "id": row.get("id"),
                "process_seq_raw": f.get("Process_Seq"),
                "process_seq": str(f.get("Process_Seq") or "").strip(),
                "remarks": str(f.get("Process_Seq_Remarks") or "").strip(),
            }
        )

    _ensure_column(client, "ProductPartMSList", "Process_Seq", "Ref:ProcessMaster")
    _ensure_column(client, "ProductPartMSList", "Process_Seq_Remarks", "Ref:ProcessMaster")
    _set_visible_col(client, "ProductPartMSList", "Process_Seq", "ProcessMaster", "display_label")
    _set_visible_col(client, "ProductPartMSList", "Process_Seq_Remarks", "ProcessMaster", "process_remarks")
    _apply([["ModifyColumn", "ProductPartMSList", "Process_Seq_Remarks", {"isFormula": True, "formula": "$Process_Seq"}]])

    updates = []
    for snap in snapshots:
        row_id = snap["id"]
        if not isinstance(row_id, int):
            continue
        fields: dict[str, Any] = {}
        existing_ref = _safe_int(snap.get("process_seq_raw"))
        process_id = existing_ref if existing_ref in valid_process_ids else None
        if process_id is None:
            process_id = process_ids_by_legacy.get(snap["process_seq"])
        if process_id:
            fields["Process_Seq"] = process_id
        if fields:
            updates.append({"id": row_id, "fields": fields})
    _patch_records("ProductPartMSList", updates)


def _migrate_product_batch_ms(client: GristClient, process_ids_by_legacy: dict[str, int]) -> None:
    try:
        rows = client.get_records("ProductBatchMS")
    except Exception:
        return
    valid_process_ids = {
        row.get("id")
        for row in client.get_records("ProcessMaster")
        if isinstance(row.get("id"), int)
    }
    snapshots = []
    for row in rows:
        f = row.get("fields", {})
        snapshots.append(
            {
                "id": row.get("id"),
                "process_seq_raw": f.get("process_seq"),
                "process_seq": str(f.get("process_seq") or "").strip(),
            }
        )

    _ensure_column(client, "ProductBatchMS", "process_seq", "Ref:ProcessMaster")
    _set_visible_col(client, "ProductBatchMS", "process_seq", "ProcessMaster", "display_label")

    updates = []
    for snap in snapshots:
        row_id = snap["id"]
        if not isinstance(row_id, int):
            continue
        existing_ref = _safe_int(snap.get("process_seq_raw"))
        process_id = existing_ref if existing_ref in valid_process_ids else None
        if process_id is None:
            process_id = process_ids_by_legacy.get(snap["process_seq"])
        if not process_id:
            continue
        updates.append({"id": row_id, "fields": {"process_seq": process_id}})
    _patch_records("ProductBatchMS", updates)


def _repair_accidental_custom_process_mappings(client: GristClient) -> None:
    process_rows = client.get_records("ProcessMaster")
    canonical_by_id: dict[int, int] = {}
    process_by_code: dict[str, int] = {}
    for row in process_rows:
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        fields = row.get("fields", {})
        code = str(fields.get("process_code") or "").strip()
        if code:
            process_by_code[code] = row_id

    for row in process_rows:
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        fields = row.get("fields", {})
        legacy = str(fields.get("legacy_process_seq_text") or "").strip()
        code = str(fields.get("process_code") or "").strip()
        if not legacy.isdigit() or not code.startswith("CUSTOM-"):
            continue
        target = _safe_int(legacy)
        if target and any(r.get("id") == target for r in process_rows):
            canonical_by_id[row_id] = target

    if not canonical_by_id:
        return

    def _repair_table(table: str, column: str) -> None:
        updates = []
        for row in client.get_records(table):
            row_id = row.get("id")
            if not isinstance(row_id, int):
                continue
            current = _safe_int(row.get("fields", {}).get(column))
            target = canonical_by_id.get(current or -1)
            if target:
                updates.append({"id": row_id, "fields": {column: target}})
        _patch_records(table, updates)

    _repair_table("ProductPartMSList", "Process_Seq")
    _repair_table("ProductBatchMS", "process_seq")

    updates = []
    for custom_id in canonical_by_id:
        updates.append({"id": custom_id, "fields": {"status": "Retired"}})
    _patch_records("ProcessMaster", updates)


def _repair_process_stage_assignments(client: GristClient) -> None:
    process_ids = _id_map_by_field(client, "ProcessMaster", "process_code")
    prspj = process_ids.get("PRS-PJ-PROD")
    prsdet = process_ids.get("PRS-DET-V1")
    if not prspj or not prsdet:
        return

    updates = []
    for row in client.get_records("ProcessStage"):
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        f = row.get("fields", {})
        proc = _safe_int(f.get("process_seq_id"))
        stage_name = str(f.get("stage_name") or "").strip()
        seq_no = _safe_int(f.get("seq_no")) or 0

        if proc == prsdet and stage_name in {"Press Cutting", "Press Job"}:
            updates.append({"id": row_id, "fields": {"process_seq_id": prspj, "seq_no": seq_no}})
        if proc == prsdet and stage_name == "Production" and seq_no == 30:
            updates.append({"id": row_id, "fields": {"process_seq_id": prspj, "seq_no": 30}})

    _patch_records("ProcessStage", updates)


def _legacy_sequences_from_ms_list(client: GristClient) -> list[str]:
    process_legacy_by_id = {}
    if _table_exists(client, "ProcessMaster"):
        for row in client.get_records("ProcessMaster"):
            row_id = row.get("id")
            if not isinstance(row_id, int):
                continue
            legacy = str(row.get("fields", {}).get("legacy_process_seq_text") or "").strip()
            if legacy:
                process_legacy_by_id[row_id] = legacy

    rows = client.get_records("ProductPartMSList")
    values = []
    for row in rows:
        raw = row.get("fields", {}).get("Process_Seq")
        ref_id = _safe_int(raw)
        if ref_id and ref_id in process_legacy_by_id:
            seq = process_legacy_by_id[ref_id]
        else:
            seq = str(raw or "").strip()
        if seq:
            values.append(seq)
    if not values:
        values = ["Press Cutting - Press Job - Production"]
    return sorted(set(values))


def main() -> None:
    client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)
    legacy_sequences = _legacy_sequences_from_ms_list(client)
    _ensure_schema(client)
    role_ids, _ = _sync_role_user_mirrors(client)
    stage_ids = _seed_stage_master(client, role_ids, legacy_sequences)
    process_ids = _seed_process_master_and_stages(client, stage_ids, legacy_sequences)
    process_ids_by_legacy = {}
    for row in client.get_records("ProcessMaster"):
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        legacy = str(row.get("fields", {}).get("legacy_process_seq_text") or "").strip()
        if legacy and legacy not in process_ids_by_legacy:
            process_ids_by_legacy[legacy] = row_id
    _seed_process_remarks_from_summary(client)

    _apply_formulas_and_display(client)
    _repair_process_stage_assignments(client)
    _repair_accidental_custom_process_mappings(client)
    _migrate_product_part_ms_list(client, process_ids_by_legacy)
    _migrate_product_batch_ms(client, process_ids_by_legacy)

    print(
        f"Process Seq phase-1 applied. ProcessMaster={len(process_ids)} StageMaster={len(stage_ids)} "
        f"legacy_seq_mapped={len(process_ids_by_legacy)}"
    )


if __name__ == "__main__":
    main()
