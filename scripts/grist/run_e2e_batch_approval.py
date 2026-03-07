from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PUSH_SCRIPT_PATH = REPO_ROOT / "scripts" / "grist" / "push_test_inbox.py"
RENDER_SCRIPT_PATH = REPO_ROOT / "scripts" / "grist" / "render_test_outbox_preview.py"
BATCH_CREATED_TEXT = "Batch created:"

if __package__ in (None, ""):
    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

from dotenv import load_dotenv

from pulse.core.grist_client import GristClient
from pulse.runtime import test_api_key, test_doc_id

load_dotenv(REPO_ROOT / ".env")


def _build_client() -> GristClient:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    doc_id = test_doc_id()
    api_key = test_api_key()
    if not server or not doc_id or not api_key:
        raise ValueError("Set PULSE_RUNTIME_MODE=TEST and PULSE_TEST_DOC_ID / PULSE_TEST_API_KEY.")
    return GristClient(server, doc_id, api_key)


def _build_pulse_user_client() -> GristClient:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    pulse_doc = str(os.getenv("PULSE_DOC_ID", "")).strip()
    pulse_key = str(os.getenv("PULSE_API_KEY", "")).strip()
    if not server or not pulse_doc or not pulse_key:
        raise ValueError("Set PULSE_DOC_ID and PULSE_API_KEY to auto-detect actors.")
    return GristClient(server, pulse_doc, pulse_key)


def _build_batch_lookup_client() -> GristClient:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    if not server:
        raise ValueError("Set PULSE_GRIST_SERVER before resolving batch identifiers.")

    candidates = [
        (str(os.getenv("COSTING_DOC_ID", "")).strip(), str(os.getenv("COSTING_API_KEY", "")).strip()),
        (str(os.getenv("PULSE_DOC_ID", "")).strip(), str(os.getenv("PULSE_API_KEY", "")).strip()),
        (str(test_doc_id()).strip(), str(test_api_key()).strip()),
    ]

    for doc_id, api_key in candidates:
        if not doc_id or not api_key:
            continue
        client = GristClient(server, doc_id, api_key)
        try:
            tables = [table.get("id") for table in client.list_tables()]
        except Exception:
            continue
        if "ProductBatchMaster" in tables:
            return client

    raise RuntimeError("Unable to locate ProductBatchMaster in COSTING_DOC_ID/PULSE_DOC_ID/test doc.")


def _normalize_ref(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _auto_pick_actor_ids() -> tuple[str, str]:
    client = _build_pulse_user_client()
    roles = client.get_records("Roles")
    users = client.get_records("Users")

    role_name_by_id: dict[int, str] = {}
    for role in roles:
        role_id = role.get("id")
        if not isinstance(role_id, int):
            continue
        role_name_by_id[role_id] = str(role.get("fields", {}).get("Role_Name") or "").strip()

    supervisor_id = ""
    manager_id = ""
    for user in users:
        fields = user.get("fields", {})
        if not fields.get("Active"):
            continue
        actor_id = str(fields.get("Telegram_ID") or "").strip()
        if not actor_id:
            continue
        role_ref = _normalize_ref(fields.get("Role"))
        role_name = role_name_by_id.get(role_ref, "") if isinstance(role_ref, int) else ""
        lowered = role_name.lower()
        if not supervisor_id and "production" in lowered and "supervisor" in lowered:
            supervisor_id = actor_id
        if not manager_id and "production" in lowered and "manager" in lowered:
            manager_id = actor_id
        if supervisor_id and manager_id:
            break

    return supervisor_id, manager_id


def _max_batch_id(client: GristClient) -> int:
    max_batch_id = 0
    try:
        for record in client.get_records("ProductBatchMaster"):
            rec_id = record.get("id")
            if isinstance(rec_id, int):
                max_batch_id = max(max_batch_id, rec_id)
    except Exception:
        return 0
    return max_batch_id


def _run_push(actor: str, session: str, *, text: str | None = None, callback: str | None = None) -> None:
    if text is None and callback is None:
        raise ValueError("Either text or callback payload required.")
    payload_flag = "--text" if text is not None else "--callback"
    payload_val = text if text is not None else callback
    args = [
        sys.executable,
        str(PUSH_SCRIPT_PATH),
        "--session",
        session,
        "--actor",
        str(actor),
        payload_flag,
        str(payload_val),
        "--process-now",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PULSE_RUNTIME_MODE"] = "TEST"
    result = subprocess.run(args, check=True, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to push action for actor {actor}: {payload_val}")


def _parse_batch_no(text: str) -> str:
    value = str(text or "")
    if not value.startswith(BATCH_CREATED_TEXT):
        return ""
    after_prefix = value[len(BATCH_CREATED_TEXT):].strip()
    if not after_prefix:
        return ""
    return after_prefix.split("\n", 1)[0].strip()


def _max_batch_no_record_id(client: GristClient, batch_no: str) -> int | None:
    if not batch_no:
        return None
    try:
        for record in client.get_records("ProductBatchMaster"):
            if str(record.get("fields", {}).get("batch_no", "")).strip() == batch_no:
                rec_id = record.get("id")
                if isinstance(rec_id, int):
                    return rec_id
    except Exception as exc:
        raise RuntimeError(f"Unable to read ProductBatchMaster while resolving batch '{batch_no}': {exc}")
    return None


def _find_batch_id_from_test_outbox(
    client: GristClient,
    session: str,
    actor: str,
    batch_lookup_client: GristClient,
) -> tuple[int | None, str | None]:
    try:
        outbox_rows = client.get_records("Test_Outbox")
    except Exception as exc:
        raise RuntimeError(f"Unable to read Test_Outbox for batch resolution: {exc}")

    batch_no = None
    for row in reversed(outbox_rows):
        fields = row.get("fields", {})
        if str(fields.get("session_id", "")).strip() != session:
            continue
        if str(fields.get("recipient_user_id", "")).strip() != str(actor).strip():
            continue
        text = str(fields.get("message_text", ""))
        if not text.startswith(BATCH_CREATED_TEXT):
            continue
        batch_no = _parse_batch_no(text)
        if batch_no:
            break
    if not batch_no:
        return None, None
    batch_id = _max_batch_no_record_id(batch_lookup_client, batch_no)
    return batch_id, batch_no


def _collect_session_outbox_tail(
    client: GristClient,
    session: str,
    actor: str,
    limit: int = 8,
) -> list[str]:
    try:
        rows = client.get_records("Test_Outbox")
    except Exception:
        return ["<unable to read Test_Outbox>"]

    matches = []
    for row in rows:
        fields = row.get("fields", {})
        if str(fields.get("session_id", "")).strip() != session:
            continue
        if str(fields.get("recipient_user_id", "")).strip() != str(actor).strip():
            continue
        text = str(fields.get("message_text", ""))
        matches.append(f"- {text[:180]}")
    return matches[-limit:]


def _run_render_preview() -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PULSE_RUNTIME_MODE"] = "TEST"
    subprocess.run([sys.executable, str(RENDER_SCRIPT_PATH)], check=True, env=env)


def _resolve_actor_ids(args) -> tuple[str, str]:
    supervisor_id = str(args.supervisor or "").strip() or str(os.getenv("PULSE_TEST_SUPERVISOR_USER_ID", "")).strip()
    manager_id = str(args.manager or "").strip() or str(os.getenv("PULSE_TEST_MANAGER_USER_ID", "")).strip()
    if not supervisor_id or not manager_id:
        auto_sup, auto_mgr = _auto_pick_actor_ids()
        supervisor_id = supervisor_id or auto_sup
        manager_id = manager_id or auto_mgr
    if not supervisor_id or not manager_id:
        raise ValueError("Provide --supervisor and --manager or set PULSE_TEST_SUPERVISOR_USER_ID / PULSE_TEST_MANAGER_USER_ID.")
    return supervisor_id, manager_id


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run production batch approval flow in TEST mode.")
    parser.add_argument("--session", default=str(os.getenv("PULSE_TEST_SESSION_ID", "sim-e2e-001")).strip())
    parser.add_argument("--supervisor", default="", help="Test user Telegram ID for Production Supervisor")
    parser.add_argument("--manager", default="", help="Test user Telegram ID for Production Manager")
    parser.add_argument("--model-index", type=int, default=1)
    parser.add_argument("--qty", type=int, default=32)
    parser.add_argument("--refresh-session", action="store_true", help="Use a fresh unique session id")
    parser.add_argument("--skip-open", action="store_true", help="Skip opening approval button before approve")
    parser.add_argument("--render", action="store_true", help="Render outbox preview at the end")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    client = _build_client()
    batch_client = _build_batch_lookup_client()
    supervisor_id, manager_id = _resolve_actor_ids(args)

    session_id = str(args.session).strip()
    if args.refresh_session:
        session_id = f"{session_id}-{int(time.time())}"

    print(f"Running e2e flow with session={session_id}, supervisor={supervisor_id}, manager={manager_id}")

    before_batch_id = _max_batch_id(batch_client)
    _run_push(supervisor_id, session_id, text="/start")
    _run_push(supervisor_id, session_id, text="Manage Production")
    _run_push(supervisor_id, session_id, text="New Production Batch")
    _run_push(supervisor_id, session_id, text="By Product Model")
    _run_push(supervisor_id, session_id, text=str(args.model_index))
    _run_push(supervisor_id, session_id, text=str(args.qty))
    _run_push(supervisor_id, session_id, text="New Complete Batch (M-C-S)")
    _run_push(supervisor_id, session_id, text="Yes")

    batch_id = _max_batch_id(batch_client)
    batch_no = None

    if batch_id <= before_batch_id:
        outbox_batch_id, outbox_batch_no = _find_batch_id_from_test_outbox(
            client,
            session_id,
            supervisor_id,
            batch_client,
        )
        batch_id = outbox_batch_id or 0
        batch_no = outbox_batch_no

    if batch_id <= before_batch_id:
        tail = _collect_session_outbox_tail(client, session_id, supervisor_id)
        tail_text = "\n".join(tail) if tail else "- no outbox rows found"
        raise RuntimeError(
            "Batch creation did not produce a new ProductBatchMaster record.\n"
            "Recent outbox for supervisor session:\n"
            f"{tail_text}"
        )
    print(f"Detected new batch id: {batch_id}")
    if batch_no:
        print(f"Batch number from outbox: {batch_no}")

    if not args.skip_open:
        _run_push(manager_id, session_id, callback=f"prodappr:open:{batch_id}")
    _run_push(manager_id, session_id, callback=f"prodappr:approve:{batch_id}")

    if args.render:
        _run_render_preview()

    print("E2E approval flow completed.")


if __name__ == "__main__":
    main()
