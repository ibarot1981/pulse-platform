from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

if __package__ in (None, ""):
    repo_root = str(Path(__file__).resolve().parents[2])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

from pulse.core.grist_client import GristClient
from pulse.runtime import test_api_key, test_doc_id


REPO_ROOT = Path(__file__).resolve().parents[2]
PUSH_SCRIPT_PATH = REPO_ROOT / "scripts" / "grist" / "push_test_inbox.py"
RENDER_SCRIPT_PATH = REPO_ROOT / "scripts" / "grist" / "render_test_outbox_preview.py"
VIEW_BY_BATCH_NO = "View By Batch No"
SELECT_BATCH_HEADER = "Select Batch No:"
NO_BATCH_ENTRIES_TEXT = "No batch entries available for your MS jobs."

load_dotenv(REPO_ROOT / ".env")


def _run_push(actor: str, session: str, text: str) -> None:
    args = [
        sys.executable,
        str(PUSH_SCRIPT_PATH),
        "--session",
        session,
        "--actor",
        str(actor),
        "--text",
        text,
        "--process-now",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PULSE_RUNTIME_MODE"] = "TEST"
    subprocess.run(args, check=True, env=env)


def _run_render() -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PULSE_RUNTIME_MODE"] = "TEST"
    subprocess.run([sys.executable, str(RENDER_SCRIPT_PATH)], check=True, env=env)


def _build_test_client() -> GristClient:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    doc_id = str(test_doc_id() or "").strip()
    api_key = str(test_api_key() or "").strip()
    if not server or not doc_id or not api_key:
        raise ValueError("Set PULSE_RUNTIME_MODE=TEST and PULSE_TEST_DOC_ID / PULSE_TEST_API_KEY (or PULSE_API_KEY).")
    return GristClient(server, doc_id, api_key)


def _parse_button_labels(buttons_json: str) -> list[str]:
    raw = str(buttons_json or "").strip()
    if not raw:
        return []
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, dict):
        return []
    rows = payload.get("keyboard")
    if not isinstance(rows, list):
        rows = payload.get("inline_keyboard")
    if not isinstance(rows, list):
        return []
    labels: list[str] = []
    for row in rows:
        if not isinstance(row, list):
            continue
        for button in row:
            if not isinstance(button, dict):
                continue
            text = str(button.get("text") or "").strip()
            if text:
                labels.append(text)
    return labels


def _extract_batch_serial_number(message_text: str, target_batch_no: str) -> str:
    wanted = str(target_batch_no or "").strip().casefold()
    if not wanted:
        return ""
    for line in str(message_text or "").splitlines():
        row = line.strip()
        if not row:
            continue
        if ". " not in row:
            continue
        prefix, value = row.split(". ", 1)
        serial = prefix.strip()
        if not serial.isdigit():
            continue
        if value.strip().casefold() == wanted:
            return serial
    return ""


def _latest_session_message(client: GristClient, session: str, actor: str) -> dict:
    rows = client.get_records("Test_Outbox")
    actor_text = str(actor).strip()
    session_text = str(session).strip()
    for row in reversed(rows):
        fields = row.get("fields", {})
        if str(fields.get("session_id") or "").strip() != session_text:
            continue
        if str(fields.get("recipient_user_id") or "").strip() != actor_text:
            continue
        return row
    return {}


def _resolve_batch_selector_serial(client: GristClient, actor: str, session: str, batch_no: str) -> str:
    wanted = str(batch_no or "").strip()
    if not wanted:
        raise ValueError("--batch-no cannot be empty")

    for _ in range(30):
        latest = _latest_session_message(client, session, actor)
        if not latest:
            break
        fields = latest.get("fields", {})
        message_text = str(fields.get("message_text") or "")
        labels = _parse_button_labels(str(fields.get("buttons_json") or ""))
        if NO_BATCH_ENTRIES_TEXT.casefold() in message_text.casefold():
            raise RuntimeError(
                f"Actor {actor} has no batch entries in 'My MS Jobs' for session '{session}', "
                "so batch selection is not available."
            )
        if SELECT_BATCH_HEADER in message_text:
            serial = _extract_batch_serial_number(message_text, wanted)
            if serial:
                return serial
            next_label = next((label for label in labels if "next" in label.casefold()), "")
            if next_label:
                _run_push(actor, session, next_label)
                continue
            raise ValueError(f"Batch '{wanted}' not found in batch selector options.")
        if VIEW_BY_BATCH_NO in message_text or VIEW_BY_BATCH_NO in labels:
            _run_push(actor, session, VIEW_BY_BATCH_NO)
            continue
        time.sleep(0.2)
    raise RuntimeError("Unable to reach 'Select Batch No' screen in Test_Outbox.")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run /start -> Manage Production -> My MS Jobs for a chosen user in TEST mode."
    )
    parser.add_argument("--actor", required=True, help="Telegram user id from Pulse Users.Telegram_ID")
    parser.add_argument("--session", default="sim-my-ms-jobs", help="Test session id")
    parser.add_argument("--refresh-session", action="store_true", help="Append timestamp to session id")
    parser.add_argument(
        "--view",
        default="",
        help="Optional next click text, e.g. 'View By Batch No' or 'View Created By' or 'View By Next Stage'",
    )
    parser.add_argument("--batch-no", default="", help="Batch No to auto-select when using 'View By Batch No'")
    parser.add_argument("--render", action="store_true", help="Render preview HTML after flow")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    session = str(args.session).strip()
    if args.refresh_session:
        session = f"{session}-{int(time.time())}"

    actor = str(args.actor).strip()
    if not actor:
        raise ValueError("--actor is required")

    batch_no = str(args.batch_no or "").strip()
    view = str(args.view or "").strip()
    if batch_no and not view:
        view = VIEW_BY_BATCH_NO
    if batch_no and view != VIEW_BY_BATCH_NO:
        raise ValueError("--batch-no requires --view 'View By Batch No' (or leave --view empty).")

    print(f"Running My MS Jobs flow for actor={actor} session={session}")
    _run_push(actor, session, "/start")
    _run_push(actor, session, "Manage Production")
    _run_push(actor, session, "My MS Jobs")
    if view:
        _run_push(actor, session, view)
    if batch_no:
        client = _build_test_client()
        serial = _resolve_batch_selector_serial(client, actor, session, batch_no)
        print(f"Selecting batch '{batch_no}' using serial no {serial}.")
        _run_push(actor, session, serial)

    if args.render:
        _run_render()
        print("Rendered preview: artifacts/test_preview/outbox_preview.html")

    print("Done.")


if __name__ == "__main__":
    main()
