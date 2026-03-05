from __future__ import annotations

import argparse
import os
import sys
import subprocess
from datetime import datetime, timezone

from dotenv import load_dotenv


if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pulse.core.grist_client import GristClient
from pulse.runtime import runtime_mode, test_api_key, test_doc_id
from pulse.testing.harness import process_pending_once


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


def _build_pulse_client() -> GristClient | None:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    doc_id = str(os.getenv("PULSE_DOC_ID", "")).strip()
    api_key = str(os.getenv("PULSE_API_KEY", "")).strip()
    if not server or not doc_id or not api_key:
        return None
    return GristClient(server, doc_id, api_key)


def _normalize_ref(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _resolve_actor_role_from_pulse(actor_telegram_id: str) -> str:
    pulse_client = _build_pulse_client()
    if pulse_client is None:
        return ""
    try:
        users = pulse_client.get_records("Users")
        roles = pulse_client.get_records("Roles")
    except Exception:
        return ""

    role_name_by_id: dict[int, str] = {}
    for role in roles:
        rec_id = role.get("id")
        if not isinstance(rec_id, int):
            continue
        role_name_by_id[rec_id] = str(role.get("fields", {}).get("Role_Name") or "").strip()

    actor_tg = str(actor_telegram_id or "").strip()
    for user in users:
        fields = user.get("fields", {})
        tg = str(fields.get("Telegram_ID") or "").strip()
        if tg != actor_tg:
            continue
        role_ref = _normalize_ref(fields.get("Role"))
        if isinstance(role_ref, int):
            return role_name_by_id.get(role_ref, "")
        return str(role_ref or "").strip()
    return ""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Insert one simulated user action into Test_Inbox.",
    )
    parser.add_argument("--session", required=True, help="Session ID, e.g. sim-e2e-001")
    parser.add_argument("--actor", required=True, help="Actor Telegram ID from Pulse Users.Telegram_ID")
    parser.add_argument("--role", default="", help="Optional actor role label for traceability")
    parser.add_argument("--process-now", action="store_true", help="Immediately process pending Test_Inbox rows once.")
    parser.add_argument("--render", action="store_true", help="Render HTML preview after insert/process.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--text", help="Text payload (simulates user message)")
    group.add_argument("--callback", help="Callback payload (simulates inline button click)")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if runtime_mode() != "TEST":
        raise RuntimeError("Set PULSE_RUNTIME_MODE=TEST before using this helper.")

    input_type = "text" if args.text is not None else "callback"
    payload = args.text if args.text is not None else args.callback
    payload = str(payload or "").strip()
    if not payload:
        raise ValueError("Payload cannot be empty.")
    actor_role = str(args.role or "").strip()
    if not actor_role:
        actor_role = _resolve_actor_role_from_pulse(str(args.actor).strip())

    client = _build_client()
    response = client.add_records(
        "Test_Inbox",
        [
            {
                "session_id": str(args.session).strip(),
                "actor_user_id": str(args.actor).strip(),
                "actor_role": actor_role,
                "input_type": input_type,
                "payload": payload,
                "processed": False,
                "created_at": _utc_now_iso(),
            }
        ],
    )
    row_id = None
    records = response.get("records", []) if isinstance(response, dict) else []
    if records and isinstance(records[0], dict):
        row_id = records[0].get("id")
    print(
        f"Inserted Test_Inbox row: id={row_id} session={args.session} actor={args.actor} "
        f"type={input_type} role={actor_role or '(blank)'}"
    )

    processed_count = 0
    if args.process_now:
        processed_count = process_pending_once()
        print(f"Processed pending rows immediately: {processed_count}")

    if args.render:
        env = os.environ.copy()
        env["PYTHONPATH"] = env.get("PYTHONPATH", ".") or "."
        result = subprocess.run(
            [sys.executable, "scripts/grist/render_test_outbox_preview.py"],
            check=True,
            env=env,
        )
        if result.returncode == 0:
            print("Rendered preview: artifacts/test_preview/outbox_preview.html")


if __name__ == "__main__":
    main()
