from __future__ import annotations

import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from pulse.core.grist_client import GristClient
from pulse.runtime import test_api_key, test_doc_id

load_dotenv()


TEST_TABLE_SCHEMAS: dict[str, list[dict]] = {
    "Test_Inbox": [
        {"id": "session_id", "fields": {"type": "Text"}},
        {"id": "actor_user_id", "fields": {"type": "Text"}},
        {"id": "actor_role", "fields": {"type": "Text"}},
        {"id": "input_type", "fields": {"type": "Text"}},
        {"id": "payload", "fields": {"type": "Text"}},
        {"id": "processed", "fields": {"type": "Bool"}},
        {"id": "processed_at", "fields": {"type": "DateTime"}},
        {"id": "error", "fields": {"type": "Text"}},
        {"id": "created_at", "fields": {"type": "DateTime"}},
    ],
    "Test_Outbox": [
        {"id": "session_id", "fields": {"type": "Text"}},
        {"id": "recipient_user_id", "fields": {"type": "Text"}},
        {"id": "recipient_role", "fields": {"type": "Text"}},
        {"id": "event_type", "fields": {"type": "Text"}},
        {"id": "source", "fields": {"type": "Text"}},
        {"id": "message_text", "fields": {"type": "Text"}},
        {"id": "parse_mode", "fields": {"type": "Text"}},
        {"id": "buttons_json", "fields": {"type": "Text"}},
        {"id": "payload_json", "fields": {"type": "Text"}},
        {"id": "created_at", "fields": {"type": "DateTime"}},
    ],
    "Test_UserContext": [
        {"id": "session_id", "fields": {"type": "Text"}},
        {"id": "actor_user_id", "fields": {"type": "Text"}},
        {"id": "role_name", "fields": {"type": "Text"}},
        {"id": "menu_state", "fields": {"type": "Text"}},
        {"id": "context_json", "fields": {"type": "Text"}},
        {"id": "updated_at", "fields": {"type": "DateTime"}},
    ],
    "Test_Attachments": [
        {"id": "session_id", "fields": {"type": "Text"}},
        {"id": "recipient_user_id", "fields": {"type": "Text"}},
        {"id": "source", "fields": {"type": "Text"}},
        {"id": "filename", "fields": {"type": "Text"}},
        {"id": "caption", "fields": {"type": "Text"}},
        {"id": "meta_json", "fields": {"type": "Text"}},
        {"id": "created_at", "fields": {"type": "DateTime"}},
    ],
    "Test_RunLog": [
        {"id": "level", "fields": {"type": "Text"}},
        {"id": "message", "fields": {"type": "Text"}},
        {"id": "session_id", "fields": {"type": "Text"}},
        {"id": "actor_user_id", "fields": {"type": "Text"}},
        {"id": "inbox_id", "fields": {"type": "Int"}},
        {"id": "details", "fields": {"type": "Text"}},
        {"id": "created_at", "fields": {"type": "DateTime"}},
    ],
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_client() -> GristClient:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    doc_id = test_doc_id()
    api_key = test_api_key()
    if not server or not doc_id or not api_key:
        raise ValueError("Missing PULSE_GRIST_SERVER / PULSE_TEST_DOC_ID / PULSE_TEST_API_KEY(PULSE_API_KEY).")
    return GristClient(server, doc_id, api_key)


def _table_exists(client: GristClient, table_name: str) -> bool:
    tables = client.list_tables()
    for table in tables:
        if str(table.get("id")) == table_name:
            return True
    return False


def _ensure_table(client: GristClient, table_name: str, columns: list[dict]) -> None:
    if not _table_exists(client, table_name):
        client.create_table(table_name, columns)
        return
    existing_columns = {str(col.get("id")) for col in client.get_columns(table_name)}
    for column in columns:
        col_id = str(column.get("id"))
        col_type = str(column.get("fields", {}).get("type", "Text"))
        if col_id not in existing_columns:
            client.add_column(table_name, col_id, col_type)


def _seed_sample_rows(client: GristClient) -> None:
    inbox_rows = client.get_records("Test_Inbox")
    if inbox_rows:
        return
    actor_user_id = str(os.getenv("PULSE_TEST_SAMPLE_ACTOR_USER_ID", "")).strip()
    if not actor_user_id:
        print("Skipping sample seed: set PULSE_TEST_SAMPLE_ACTOR_USER_ID to seed /start row.")
        return
    client.add_records(
        "Test_Inbox",
        [
            {
                "session_id": "demo-session-1",
                "actor_user_id": actor_user_id,
                "actor_role": "Production_Supervisor",
                "input_type": "text",
                "payload": "/start",
                "processed": False,
                "created_at": _utc_now_iso(),
            }
        ],
    )


def main() -> None:
    client = _build_client()
    for table_name, columns in TEST_TABLE_SCHEMAS.items():
        _ensure_table(client, table_name, columns)
        print(f"Ensured table: {table_name}")

    if str(os.getenv("PULSE_TEST_SEED_SAMPLE", "false")).strip().lower() in {"1", "true", "yes"}:
        _seed_sample_rows(client)
        print("Seeded sample Test_Inbox row.")

    print("TEST runtime schema setup complete.")


if __name__ == "__main__":
    main()
