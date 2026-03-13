from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


ROLE_REQUIRED_FIELDS = ("Role_ID", "Role_Name", "Active")
USER_REQUIRED_FIELDS = ("User_ID", "Name", "Telegram_ID", "Active", "Role")


@dataclass
class SyncStats:
    added: int = 0
    updated: int = 0
    deleted: int = 0
    unchanged: int = 0
    duplicates_removed: int = 0
    skipped_invalid: int = 0


@dataclass
class RoleSchema:
    key_col: str
    name_col: str
    active_col: str | None
    source_col: str | None


@dataclass
class UserSchema:
    key_col: str
    name_col: str
    telegram_col: str | None
    active_col: str | None
    role_col: str | None
    source_col: str | None


def _base_url() -> str:
    return PULSE_GRIST_SERVER.rstrip("/")


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {COSTING_API_KEY}", "Content-Type": "application/json"}


def _normalize_ref(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _safe_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fetch_columns(client: GristClient, table: str) -> set[str]:
    return {str(col.get("id")) for col in client.get_columns(table) if col.get("id")}


def _table_exists(client: GristClient, table: str) -> bool:
    try:
        client.get_columns(table)
        return True
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return False
        raise


def _list_tables() -> list[str]:
    url = f"{_base_url()}/api/docs/{COSTING_DOC_ID}/tables"
    response = requests.get(url, headers={"Authorization": f"Bearer {COSTING_API_KEY}"}, timeout=60)
    response.raise_for_status()
    payload = response.json()
    tables = payload.get("tables", []) if isinstance(payload, dict) else []
    return [str(row.get("id")) for row in tables if row.get("id")]


def _resolve_table_name(
    client: GristClient,
    explicit_name: str | None,
    candidates: tuple[str, ...],
    label: str,
) -> str:
    if explicit_name:
        if _table_exists(client, explicit_name):
            return explicit_name
        raise ValueError(
            f"Configured {label} table '{explicit_name}' not found in costing doc. "
            f"Pass a valid name with --costing-{label}-table."
        )

    for name in candidates:
        if _table_exists(client, name):
            return name

    tables = _list_tables()
    raise ValueError(
        f"Could not auto-detect costing {label} table. Tried: {', '.join(candidates)}. "
        f"Available tables: {', '.join(tables) if tables else 'none'}"
    )


def _patch_records(table: str, updates: list[dict]) -> None:
    if not updates:
        return
    url = f"{_base_url()}/api/docs/{COSTING_DOC_ID}/tables/{table}/records"
    chunk_size = 200
    for i in range(0, len(updates), chunk_size):
        payload = {"records": updates[i : i + chunk_size]}
        response = requests.patch(url, headers=_headers(), data=json.dumps(payload), timeout=60)
        response.raise_for_status()


def _delete_records(table: str, ids: list[int]) -> None:
    if not ids:
        return
    url = f"{_base_url()}/api/docs/{COSTING_DOC_ID}/tables/{table}/records"
    chunk_size = 200
    auth_headers = {"Authorization": f"Bearer {COSTING_API_KEY}"}
    for i in range(0, len(ids), chunk_size):
        chunk = ",".join(str(x) for x in ids[i : i + chunk_size])
        response = requests.delete(url, headers=auth_headers, params={"records": chunk}, timeout=60)
        response.raise_for_status()


def _assert_config() -> None:
    missing = []
    if not PULSE_GRIST_SERVER:
        missing.append("PULSE_GRIST_SERVER")
    if not PULSE_DOC_ID:
        missing.append("PULSE_DOC_ID")
    if not PULSE_API_KEY:
        missing.append("PULSE_API_KEY")
    if not COSTING_DOC_ID:
        missing.append("COSTING_DOC_ID")
    if not COSTING_API_KEY:
        missing.append("COSTING_API_KEY")
    if missing:
        raise ValueError(f"Missing required env vars: {', '.join(missing)}")


def _sanitize_role_fields(fields: dict, allowed: set[str]) -> dict:
    role_id = str(fields.get("Role_ID") or "").strip()
    role_name = str(fields.get("Role_Name") or "").strip()
    active = bool(fields.get("Active", True))
    payload = {"Role_ID": role_id, "Role_Name": role_name, "Active": active}
    return {k: v for k, v in payload.items() if k in allowed}


def _resolve_role_schema(columns: set[str]) -> RoleSchema:
    if "Role_ID" in columns and "Role_Name" in columns:
        active_col = "Active" if "Active" in columns else None
        source_col = "source_system" if "source_system" in columns else None
        return RoleSchema(key_col="Role_ID", name_col="Role_Name", active_col=active_col, source_col=source_col)
    if "role_code" in columns and "role_name" in columns:
        active_col = "active" if "active" in columns else None
        source_col = "source_system" if "source_system" in columns else None
        return RoleSchema(key_col="role_code", name_col="role_name", active_col=active_col, source_col=source_col)
    raise ValueError("Roles table schema not supported. Expected Role_ID/Role_Name or role_code/role_name.")


def _resolve_user_schema(columns: set[str]) -> UserSchema:
    if "User_ID" in columns and "Name" in columns:
        telegram_col = "Telegram_ID" if "Telegram_ID" in columns else None
        active_col = "Active" if "Active" in columns else None
        role_col = "Role" if "Role" in columns else None
        source_col = "source_system" if "source_system" in columns else None
        return UserSchema(
            key_col="User_ID",
            name_col="Name",
            telegram_col=telegram_col,
            active_col=active_col,
            role_col=role_col,
            source_col=source_col,
        )
    raise ValueError("Users table schema not supported. Expected User_ID/Name columns.")


def _build_role_payload(fields: dict, schema: RoleSchema) -> dict:
    role_id = str(fields.get("Role_ID") or "").strip()
    role_name = str(fields.get("Role_Name") or "").strip()
    payload = {
        schema.key_col: role_id,
        schema.name_col: role_name,
    }
    if schema.active_col:
        payload[schema.active_col] = bool(fields.get("Active", True))
    if schema.source_col:
        payload[schema.source_col] = "Pulse"
    return payload


def _sanitize_user_fields(
    fields: dict,
    schema: UserSchema,
    pulse_role_id_to_role_code: dict[int, str],
    costing_role_id_by_role_code: dict[str, int],
) -> dict:
    user_id = str(fields.get("User_ID") or "").strip()
    name = str(fields.get("Name") or "").strip()
    telegram_id_raw = fields.get("Telegram_ID")
    telegram_id = str(telegram_id_raw).strip() if telegram_id_raw is not None else ""
    active = bool(fields.get("Active", True))

    role_ref = _safe_int(_normalize_ref(fields.get("Role")))
    role_code = pulse_role_id_to_role_code.get(role_ref) if role_ref is not None else None
    costing_role_ref = costing_role_id_by_role_code.get(role_code or "")

    payload: dict[str, Any] = {
        schema.key_col: user_id,
        schema.name_col: name,
    }
    if schema.telegram_col:
        payload[schema.telegram_col] = telegram_id
    if schema.active_col:
        payload[schema.active_col] = active
    if schema.role_col:
        payload[schema.role_col] = costing_role_ref
    if schema.source_col:
        payload[schema.source_col] = "Pulse"
    return payload


def _rows_equal(lhs: dict, rhs: dict, fields: tuple[str, ...], ref_fields: set[str] | None = None) -> bool:
    ref_fields = ref_fields or set()
    for field in fields:
        left_val = _normalize_ref(lhs.get(field))
        right_val = _normalize_ref(rhs.get(field))
        if field in ref_fields:
            if _safe_int(left_val) != _safe_int(right_val):
                return False
            continue
        if str(left_val or "") != str(right_val or "") and not (
            isinstance(left_val, bool) and isinstance(right_val, bool) and left_val == right_val
        ):
            return False
    return True


def _dedupe_existing(records: list[dict], key_field: str) -> tuple[dict[str, dict], list[int]]:
    by_key: dict[str, dict] = {}
    to_delete: list[int] = []
    for record in sorted(records, key=lambda row: int(row.get("id") or 0)):
        rec_id = record.get("id")
        if not isinstance(rec_id, int):
            continue
        key = str(record.get("fields", {}).get(key_field) or "").strip()
        if not key:
            continue
        if key in by_key:
            to_delete.append(rec_id)
            continue
        by_key[key] = record
    return by_key, to_delete


def sync_roles(
    pulse_client: GristClient,
    costing_client: GristClient,
    table_name: str,
    delete_missing: bool,
) -> tuple[SyncStats, dict[str, int], dict[int, str]]:
    stats = SyncStats()
    table_columns = _fetch_columns(costing_client, table_name)
    schema = _resolve_role_schema(table_columns)

    pulse_rows = pulse_client.get_records("Roles")
    costing_rows = costing_client.get_records(table_name)
    costing_by_key, duplicate_ids = _dedupe_existing(costing_rows, schema.key_col)
    if duplicate_ids:
        _delete_records(table_name, duplicate_ids)
        stats.duplicates_removed += len(duplicate_ids)

    pulse_role_id_to_role_code: dict[int, str] = {}
    desired_rows: dict[str, dict] = {}

    for row in pulse_rows:
        row_id = row.get("id")
        if isinstance(row_id, int):
            role_code = str(row.get("fields", {}).get("Role_ID") or "").strip()
            if role_code:
                pulse_role_id_to_role_code[row_id] = role_code

        payload = _build_role_payload(row.get("fields", {}), schema)
        role_id = str(payload.get(schema.key_col) or "").strip()
        role_name = str(payload.get(schema.name_col) or "").strip()
        if not role_id or not role_name:
            stats.skipped_invalid += 1
            continue
        desired_rows[role_id] = payload

    adds: list[dict] = []
    updates: list[dict] = []

    for role_id, payload in desired_rows.items():
        existing = costing_by_key.get(role_id)
        if not existing:
            adds.append(payload)
            continue
        existing_fields = existing.get("fields", {})
        comparable_fields = [schema.key_col, schema.name_col]
        if schema.active_col:
            comparable_fields.append(schema.active_col)
        if schema.source_col:
            comparable_fields.append(schema.source_col)
        if _rows_equal(existing_fields, payload, tuple(comparable_fields)):
            stats.unchanged += 1
            continue
        updates.append({"id": existing["id"], "fields": payload})

    if adds:
        costing_client.add_records(table_name, adds)
        stats.added += len(adds)
    if updates:
        _patch_records(table_name, updates)
        stats.updated += len(updates)

    if delete_missing:
        delete_ids: list[int] = []
        desired_keys = set(desired_rows.keys())
        for role_id, record in costing_by_key.items():
            if role_id in desired_keys:
                continue
            rec_id = record.get("id")
            if isinstance(rec_id, int):
                delete_ids.append(rec_id)
        if delete_ids:
            _delete_records(table_name, delete_ids)
            stats.deleted += len(delete_ids)

    final_costing_rows = costing_client.get_records(table_name)
    role_id_by_role_code = {}
    for row in final_costing_rows:
        rec_id = row.get("id")
        if not isinstance(rec_id, int):
            continue
        role_code = str(row.get("fields", {}).get(schema.key_col) or "").strip()
        if role_code:
            role_id_by_role_code[role_code] = rec_id

    return stats, role_id_by_role_code, pulse_role_id_to_role_code


def sync_users(
    pulse_client: GristClient,
    costing_client: GristClient,
    table_name: str,
    delete_missing: bool,
    pulse_role_id_to_role_code: dict[int, str],
    costing_role_id_by_role_code: dict[str, int],
) -> SyncStats:
    stats = SyncStats()
    table_columns = _fetch_columns(costing_client, table_name)
    schema = _resolve_user_schema(table_columns)

    pulse_rows = pulse_client.get_records("Users")
    costing_rows = costing_client.get_records(table_name)
    costing_by_key, duplicate_ids = _dedupe_existing(costing_rows, schema.key_col)
    if duplicate_ids:
        _delete_records(table_name, duplicate_ids)
        stats.duplicates_removed += len(duplicate_ids)

    desired_rows: dict[str, dict] = {}
    for row in pulse_rows:
        payload = _sanitize_user_fields(
            row.get("fields", {}),
            schema=schema,
            pulse_role_id_to_role_code=pulse_role_id_to_role_code,
            costing_role_id_by_role_code=costing_role_id_by_role_code,
        )
        user_id = str(payload.get(schema.key_col) or "").strip()
        name = str(payload.get(schema.name_col) or "").strip()
        if not user_id or not name:
            stats.skipped_invalid += 1
            continue
        desired_rows[user_id] = payload

    adds: list[dict] = []
    updates: list[dict] = []

    for user_id, payload in desired_rows.items():
        existing = costing_by_key.get(user_id)
        if not existing:
            adds.append(payload)
            continue
        existing_fields = existing.get("fields", {})
        comparable_fields = [schema.key_col, schema.name_col]
        if schema.telegram_col:
            comparable_fields.append(schema.telegram_col)
        if schema.active_col:
            comparable_fields.append(schema.active_col)
        if schema.role_col:
            comparable_fields.append(schema.role_col)
        if schema.source_col:
            comparable_fields.append(schema.source_col)
        ref_fields = {schema.role_col} if schema.role_col else set()
        if _rows_equal(existing_fields, payload, tuple(comparable_fields), ref_fields=ref_fields):
            stats.unchanged += 1
            continue
        updates.append({"id": existing["id"], "fields": payload})

    if adds:
        costing_client.add_records(table_name, adds)
        stats.added += len(adds)
    if updates:
        _patch_records(table_name, updates)
        stats.updated += len(updates)

    if delete_missing:
        delete_ids: list[int] = []
        desired_keys = set(desired_rows.keys())
        for user_id, record in costing_by_key.items():
            if user_id in desired_keys:
                continue
            rec_id = record.get("id")
            if isinstance(rec_id, int):
                delete_ids.append(rec_id)
        if delete_ids:
            _delete_records(table_name, delete_ids)
            stats.deleted += len(delete_ids)

    return stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync Roles and Users from Pulse doc to Costing doc without duplication."
    )
    parser.add_argument(
        "--costing-roles-table",
        default=None,
        help="Target roles table in costing doc. If omitted, script auto-detects.",
    )
    parser.add_argument(
        "--costing-users-table",
        default=None,
        help="Target users table in costing doc. If omitted, script auto-detects.",
    )
    parser.add_argument(
        "--no-delete",
        action="store_true",
        help="Do not delete records in costing that are missing in pulse.",
    )
    return parser.parse_args()


def _print_summary(role_stats: SyncStats, user_stats: SyncStats) -> None:
    print("Sync completed.")
    print("")
    print("Roles:")
    print(f"  added={role_stats.added}")
    print(f"  updated={role_stats.updated}")
    print(f"  deleted={role_stats.deleted}")
    print(f"  unchanged={role_stats.unchanged}")
    print(f"  duplicates_removed={role_stats.duplicates_removed}")
    print(f"  skipped_invalid={role_stats.skipped_invalid}")
    print("")
    print("Users:")
    print(f"  added={user_stats.added}")
    print(f"  updated={user_stats.updated}")
    print(f"  deleted={user_stats.deleted}")
    print(f"  unchanged={user_stats.unchanged}")
    print(f"  duplicates_removed={user_stats.duplicates_removed}")
    print(f"  skipped_invalid={user_stats.skipped_invalid}")


def main() -> None:
    args = parse_args()
    _assert_config()

    pulse_client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)
    costing_client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)
    delete_missing = not args.no_delete
    roles_table = _resolve_table_name(
        costing_client,
        args.costing_roles_table,
        candidates=("RoleMaster_Mirror", "Roles"),
        label="roles",
    )
    users_table = _resolve_table_name(
        costing_client,
        args.costing_users_table,
        candidates=("Users",),
        label="users",
    )

    role_stats, costing_role_id_by_role_code, pulse_role_id_to_role_code = sync_roles(
        pulse_client,
        costing_client,
        table_name=roles_table,
        delete_missing=delete_missing,
    )
    user_stats = sync_users(
        pulse_client,
        costing_client,
        table_name=users_table,
        delete_missing=delete_missing,
        pulse_role_id_to_role_code=pulse_role_id_to_role_code,
        costing_role_id_by_role_code=costing_role_id_by_role_code,
    )
    print(f"Resolved tables: roles={roles_table}, users={users_table}")
    _print_summary(role_stats, user_stats)


if __name__ == "__main__":
    main()
