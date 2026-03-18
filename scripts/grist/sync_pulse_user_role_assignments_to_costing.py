from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


def _normalize_ref(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _safe_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_bool(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y"}:
        return True
    if text in {"false", "0", "no", "n"}:
        return False
    return default


def _base_url() -> str:
    return PULSE_GRIST_SERVER.rstrip("/")


def _patch_records(table: str, updates: list[dict]) -> None:
    if not updates:
        return
    url = f"{_base_url()}/api/docs/{COSTING_DOC_ID}/tables/{table}/records"
    headers = {"Authorization": f"Bearer {COSTING_API_KEY}", "Content-Type": "application/json"}
    chunk_size = 200
    for i in range(0, len(updates), chunk_size):
        payload = {"records": updates[i : i + chunk_size]}
        response = requests.patch(url, headers=headers, data=json.dumps(payload), timeout=60)
        response.raise_for_status()


def _require_env() -> None:
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


def _require_columns(client: GristClient, table: str, required_cols: set[str]) -> None:
    actual = {str(col.get("id")) for col in client.get_columns(table)}
    missing = sorted(required_cols - actual)
    if missing:
        raise ValueError(f"Table '{table}' missing required columns: {', '.join(missing)}")


def _columns_by_id(client: GristClient, table: str) -> dict[str, dict]:
    return {str(col.get("id")): col for col in client.get_columns(table)}


def _build_pulse_indexes(
    pulse_users: list[dict],
    pulse_roles: list[dict],
) -> tuple[dict[int, dict], dict[int, dict]]:
    users_by_rec_id: dict[int, dict] = {}
    roles_by_rec_id: dict[int, dict] = {}
    for row in pulse_users:
        rec_id = row.get("id")
        if isinstance(rec_id, int):
            users_by_rec_id[rec_id] = row.get("fields", {})
    for row in pulse_roles:
        rec_id = row.get("id")
        if isinstance(rec_id, int):
            roles_by_rec_id[rec_id] = row.get("fields", {})
    return users_by_rec_id, roles_by_rec_id


def _build_costing_indexes(
    costing_users: list[dict],
    costing_role_mirror: list[dict],
) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
    user_ref_by_user_id: dict[str, int] = {}
    for row in costing_users:
        rec_id = row.get("id")
        if not isinstance(rec_id, int):
            continue
        user_id = str(row.get("fields", {}).get("User_ID") or "").strip()
        if user_id:
            user_ref_by_user_id[user_id] = rec_id

    role_ref_by_role_code: dict[str, int] = {}
    role_ref_by_role_name_norm: dict[str, int] = {}
    for row in costing_role_mirror:
        rec_id = row.get("id")
        if not isinstance(rec_id, int):
            continue
        fields = row.get("fields", {})
        role_code = str(fields.get("role_code") or fields.get("Role_ID") or "").strip()
        role_name = str(fields.get("role_name") or fields.get("Role_Name") or "").strip()
        if role_code:
            role_ref_by_role_code[role_code] = rec_id
        if role_name:
            role_ref_by_role_name_norm[role_name.casefold()] = rec_id
    return user_ref_by_user_id, role_ref_by_role_code, role_ref_by_role_name_norm


def _rows_equal(existing_fields: dict, desired_fields: dict) -> bool:
    for key in desired_fields.keys():
        left = _normalize_ref(existing_fields.get(key))
        right = _normalize_ref(desired_fields.get(key))
        if key in {"user_id", "role_id"}:
            if _safe_int(left) != _safe_int(right):
                return False
        if str(left or "").strip() != str(right or "").strip():
            return False
    return True


def sync_user_role_assignment_mirror(
    pulse_client: GristClient,
    costing_client: GristClient,
    target_user_ids: set[str] | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    _require_columns(pulse_client, "Users", {"User_ID"})
    _require_columns(pulse_client, "Roles", {"Role_ID", "Role_Name"})
    _require_columns(pulse_client, "UserRoleAssignment", {"Assignment_ID", "User", "Role", "Assignment_Type", "Active"})
    _require_columns(costing_client, "Users", {"User_ID"})
    _require_columns(costing_client, "RoleMaster_Mirror", {"role_code", "role_name"})
    _require_columns(
        costing_client,
        "UserRoleAssignment_Mirror",
        {"assignment_key", "user_id", "role_id", "scope", "active"},
    )
    mirror_col_meta = _columns_by_id(costing_client, "UserRoleAssignment_Mirror")
    writable_cols = {
        col_id
        for col_id, col in mirror_col_meta.items()
        if not bool(col.get("fields", {}).get("isFormula"))
    }
    active_col_type = str(mirror_col_meta.get("active", {}).get("fields", {}).get("type") or "").strip()

    pulse_users = pulse_client.get_records("Users")
    pulse_roles = pulse_client.get_records("Roles")
    pulse_assignments = pulse_client.get_records("UserRoleAssignment")
    costing_users = costing_client.get_records("Users")
    costing_roles = costing_client.get_records("RoleMaster_Mirror")
    costing_assignments = costing_client.get_records("UserRoleAssignment_Mirror")

    users_by_rec_id, roles_by_rec_id = _build_pulse_indexes(pulse_users, pulse_roles)
    user_ref_by_user_id, role_ref_by_role_code, role_ref_by_role_name_norm = _build_costing_indexes(
        costing_users, costing_roles
    )

    desired_by_key: dict[str, dict] = {}
    skipped_missing_user = 0
    skipped_missing_role = 0
    skipped_filtered = 0
    skipped_invalid = 0

    normalized_targets = {str(item).strip() for item in (target_user_ids or set()) if str(item).strip()}

    for row in pulse_assignments:
        fields = row.get("fields", {})
        assignment_key = str(fields.get("Assignment_ID") or row.get("id") or "").strip()
        pulse_user_ref = _safe_int(_normalize_ref(fields.get("User")))
        pulse_role_ref = _safe_int(_normalize_ref(fields.get("Role")))
        if not assignment_key or pulse_user_ref is None or pulse_role_ref is None:
            skipped_invalid += 1
            continue

        pulse_user_fields = users_by_rec_id.get(pulse_user_ref, {})
        pulse_role_fields = roles_by_rec_id.get(pulse_role_ref, {})
        user_id = str(pulse_user_fields.get("User_ID") or "").strip()
        user_name = str(pulse_user_fields.get("Name") or user_id).strip()
        role_code = str(pulse_role_fields.get("Role_ID") or "").strip()
        role_name = str(pulse_role_fields.get("Role_Name") or "").strip()
        if not user_id:
            skipped_invalid += 1
            continue
        if normalized_targets and user_id not in normalized_targets:
            skipped_filtered += 1
            continue

        costing_user_ref = user_ref_by_user_id.get(user_id)
        if costing_user_ref is None:
            skipped_missing_user += 1
            continue

        costing_role_ref = None
        if role_code:
            costing_role_ref = role_ref_by_role_code.get(role_code)
        if costing_role_ref is None and role_name:
            costing_role_ref = role_ref_by_role_name_norm.get(role_name.casefold())
        if costing_role_ref is None:
            skipped_missing_role += 1
            continue

        active_value: Any = bool(fields.get("Active", True))
        if active_col_type.lower() == "text":
            active_value = "True" if active_value else "False"

        desired_fields = {
            "assignment_key": assignment_key,
            "user_id": costing_user_ref,
            "role_id": costing_role_ref,
            "scope": str(fields.get("Assignment_Type") or "PRIMARY").strip() or "PRIMARY",
            "active": active_value,
            "user_name": user_name,
            "role_name": role_name,
        }
        desired_by_key[assignment_key] = {
            key: value for key, value in desired_fields.items() if key in writable_cols
        }

    existing_by_key: dict[str, dict] = {}
    duplicate_existing_keys = 0
    for row in costing_assignments:
        rec_id = row.get("id")
        if not isinstance(rec_id, int):
            continue
        key = str(row.get("fields", {}).get("assignment_key") or "").strip()
        if not key:
            continue
        if key in existing_by_key:
            duplicate_existing_keys += 1
            continue
        existing_by_key[key] = row

    to_add: list[dict] = []
    to_update: list[dict] = []
    unchanged = 0

    for key, desired_fields in desired_by_key.items():
        existing = existing_by_key.get(key)
        if not existing:
            to_add.append(desired_fields)
            continue
        existing_fields = existing.get("fields", {})
        if _rows_equal(existing_fields, desired_fields):
            unchanged += 1
            continue
        to_update.append({"id": existing.get("id"), "fields": desired_fields})

    if not dry_run:
        if to_add:
            costing_client.add_records("UserRoleAssignment_Mirror", to_add)
        if to_update:
            _patch_records("UserRoleAssignment_Mirror", to_update)

    return {
        "desired": len(desired_by_key),
        "added": len(to_add),
        "updated": len(to_update),
        "unchanged": unchanged,
        "skipped_filtered": skipped_filtered,
        "skipped_invalid": skipped_invalid,
        "skipped_missing_user": skipped_missing_user,
        "skipped_missing_role": skipped_missing_role,
        "duplicate_existing_keys": duplicate_existing_keys,
        "dry_run": 1 if dry_run else 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Sync Pulse UserRoleAssignment into Costing UserRoleAssignment_Mirror via upsert only "
            "(no deletions, unrelated legacy rows untouched)."
        )
    )
    parser.add_argument(
        "--user-id",
        action="append",
        default=[],
        help="Optional Pulse Users.User_ID filter. Repeat to sync multiple users (e.g. --user-id U02 --user-id U03).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would change without writing.",
    )
    args = parser.parse_args()

    _require_env()
    pulse_client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)
    costing_client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)
    stats = sync_user_role_assignment_mirror(
        pulse_client,
        costing_client,
        target_user_ids=set(args.user_id or []),
        dry_run=bool(args.dry_run),
    )

    print("UserRoleAssignment_Mirror sync summary")
    print(f"  dry_run={bool(stats['dry_run'])}")
    print(f"  desired={stats['desired']}")
    print(f"  added={stats['added']}")
    print(f"  updated={stats['updated']}")
    print(f"  unchanged={stats['unchanged']}")
    print(f"  skipped_filtered={stats['skipped_filtered']}")
    print(f"  skipped_invalid={stats['skipped_invalid']}")
    print(f"  skipped_missing_user={stats['skipped_missing_user']}")
    print(f"  skipped_missing_role={stats['skipped_missing_role']}")
    print(f"  duplicate_existing_keys={stats['duplicate_existing_keys']}")


if __name__ == "__main__":
    main()
