from __future__ import annotations

import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from pulse.core.grist_client import GristClient
from pulse.runtime import test_api_key, test_doc_id

load_dotenv()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_client() -> GristClient:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    doc_id = test_doc_id()
    api_key = test_api_key()
    if not server or not doc_id or not api_key:
        raise ValueError("Missing PULSE_GRIST_SERVER / PULSE_TEST_DOC_ID / PULSE_TEST_API_KEY(PULSE_API_KEY).")
    return GristClient(server, doc_id, api_key)


def _build_pulse_client() -> GristClient:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    doc_id = str(os.getenv("PULSE_DOC_ID", "")).strip()
    api_key = str(os.getenv("PULSE_API_KEY", "")).strip()
    if not server or not doc_id or not api_key:
        raise ValueError("Missing PULSE_GRIST_SERVER / PULSE_DOC_ID / PULSE_API_KEY for auto actor lookup.")
    return GristClient(server, doc_id, api_key)


def _normalize_ref(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _auto_pick_actor_ids() -> tuple[str, str]:
    pulse_client = _build_pulse_client()
    roles = pulse_client.get_records("Roles")
    users = pulse_client.get_records("Users")

    role_name_by_id: dict[int, str] = {}
    for role in roles:
        rec_id = role.get("id")
        if not isinstance(rec_id, int):
            continue
        role_name_by_id[rec_id] = str(role.get("fields", {}).get("Role_Name") or "").strip()

    supervisor_id = ""
    manager_id = ""
    for user in users:
        fields = user.get("fields", {})
        if not fields.get("Active"):
            continue
        telegram_id = str(fields.get("Telegram_ID") or "").strip()
        if not telegram_id:
            continue
        role_ref = _normalize_ref(fields.get("Role"))
        role_name = role_name_by_id.get(role_ref, "") if isinstance(role_ref, int) else ""
        normalized = role_name.lower().replace(" ", "_")
        if not supervisor_id and "supervisor" in normalized and "production" in normalized:
            supervisor_id = telegram_id
        if not manager_id and "manager" in normalized and "production" in normalized:
            manager_id = telegram_id
        if supervisor_id and manager_id:
            break
    return supervisor_id, manager_id


def _insert_if_empty(client: GristClient, rows: list[dict]) -> None:
    existing = client.get_records("Test_Inbox")
    if existing:
        print("Test_Inbox already has data. Skipping seed.")
        return
    client.add_records("Test_Inbox", rows)
    print(f"Inserted {len(rows)} Test_Inbox seed rows.")


def main() -> None:
    supervisor_id = str(os.getenv("PULSE_TEST_SUPERVISOR_USER_ID", "")).strip()
    manager_id = str(os.getenv("PULSE_TEST_MANAGER_USER_ID", "")).strip()
    if not supervisor_id or not manager_id:
        auto_sup, auto_mgr = _auto_pick_actor_ids()
        supervisor_id = supervisor_id or auto_sup
        manager_id = manager_id or auto_mgr
    if not supervisor_id or not manager_id:
        raise ValueError("Set PULSE_TEST_SUPERVISOR_USER_ID and PULSE_TEST_MANAGER_USER_ID before seeding.")

    session_id = str(os.getenv("PULSE_TEST_SESSION_ID", "session-batch-approval-1")).strip()
    rows = [
        {
            "session_id": session_id,
            "actor_user_id": supervisor_id,
            "actor_role": "Production_Supervisor",
            "input_type": "text",
            "payload": "/start",
            "processed": False,
            "created_at": _utc_now_iso(),
        },
        {
            "session_id": session_id,
            "actor_user_id": manager_id,
            "actor_role": "Production_Manager",
            "input_type": "text",
            "payload": "/start",
            "processed": False,
            "created_at": _utc_now_iso(),
        },
    ]
    client = _build_client()
    _insert_if_empty(client, rows)
    print("Seed complete.")


if __name__ == "__main__":
    main()
