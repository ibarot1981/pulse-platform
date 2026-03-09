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

    print(f"Running My MS Jobs flow for actor={actor} session={session}")
    _run_push(actor, session, "/start")
    _run_push(actor, session, "Manage Production")
    _run_push(actor, session, "My MS Jobs")
    if str(args.view or "").strip():
        _run_push(actor, session, str(args.view).strip())

    if args.render:
        _run_render()
        print("Rendered preview: artifacts/test_preview/outbox_preview.html")

    print("Done.")


if __name__ == "__main__":
    main()
