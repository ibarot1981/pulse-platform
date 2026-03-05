from __future__ import annotations

import html
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pulse.core.grist_client import GristClient
from pulse.runtime import test_api_key, test_doc_id

load_dotenv()


def _build_client() -> GristClient:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    doc_id = test_doc_id()
    api_key = test_api_key()
    if not server or not doc_id or not api_key:
        raise ValueError("Missing PULSE_GRIST_SERVER / PULSE_TEST_DOC_ID / PULSE_TEST_API_KEY(PULSE_API_KEY).")
    return GristClient(server, doc_id, api_key)


def _parse_json(raw: str) -> dict:
    try:
        value = json.loads(raw or "{}")
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _format_created_at(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, (int, float)):
        try:
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except (ValueError, OSError):
            return str(value)
    text = str(value).strip()
    if not text:
        return "-"
    try:
        normalized = text.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except ValueError:
        return text


def _buttons_html(buttons_json: str) -> str:
    payload = _parse_json(buttons_json)
    rows = payload.get("inline_keyboard") or payload.get("keyboard") or []
    if not isinstance(rows, list) or not rows:
        return ""
    parts = ['<div class="buttons">']
    for row in rows:
        if not isinstance(row, list):
            continue
        parts.append('<div class="btn-row">')
        for button in row:
            if not isinstance(button, dict):
                continue
            label = html.escape(str(button.get("text", "Button")))
            callback_raw = str(button.get("callback_data", "")).strip()
            url_raw = str(button.get("url", "")).strip()
            switch_inline = str(button.get("switch_inline_query", "")).strip()
            switch_current = str(button.get("switch_inline_query_current_chat", "")).strip()

            action_label = ""
            action_value = ""
            if callback_raw:
                action_label = "callback_data"
                action_value = callback_raw
            elif url_raw:
                action_label = "url"
                action_value = url_raw
            elif switch_inline:
                action_label = "switch_inline_query"
                action_value = switch_inline
            elif switch_current:
                action_label = "switch_inline_query_current_chat"
                action_value = switch_current
            else:
                action_label = "callback_data"
                action_value = "No callback_data (reply keyboard button)"

            tooltip = html.escape(f"{action_label}: {action_value}")
            parts.append(
                f'<span class="btn-wrap"><button class="btn" title="{tooltip}">{label}</button>'
                f'<span class="btn-tip">{tooltip}</span></span>'
            )
        parts.append("</div>")
    parts.append("</div>")
    return "".join(parts)


def render_html(rows: list[dict]) -> str:
    cards: list[str] = []
    for row in rows:
        fields = row.get("fields", {})
        recipient = html.escape(str(fields.get("recipient_user_id", "")))
        role = html.escape(str(fields.get("recipient_role", "")))
        created_at = html.escape(_format_created_at(fields.get("created_at")))
        source = html.escape(str(fields.get("source", "")))
        event_type = html.escape(str(fields.get("event_type", "")))
        parse_mode = html.escape(str(fields.get("parse_mode", "")))
        text = html.escape(str(fields.get("message_text", ""))).replace("\n", "<br>")
        buttons = _buttons_html(str(fields.get("buttons_json", "")))
        cards.append(
            f"""
            <article class="msg">
              <div class="meta">
                <span>User: {recipient}</span>
                <span>Role: {role or "-"}</span>
                <span>Event: {event_type}</span>
                <span>Source: {source}</span>
                <span>Parse Mode: {parse_mode or "-"}</span>
                <span>{created_at}</span>
              </div>
              <div class="bubble">{text or "&nbsp;"}</div>
              {buttons}
            </article>
            """
        )

    body = "\n".join(cards) if cards else '<p class="empty">No outbox rows found.</p>'
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pulse TEST Outbox Preview</title>
  <style>
    :root {{
      --bg: #e9f2ff;
      --ink: #0f172a;
      --meta: #475569;
      --bubble: #ffffff;
      --btn: #dbeafe;
      --btn-border: #93c5fd;
      --accent: #0ea5e9;
    }}
    body {{ margin: 0; font-family: "Segoe UI", Tahoma, sans-serif; background: radial-gradient(circle at top, #f8fbff, var(--bg)); color: var(--ink); }}
    main {{ max-width: 900px; margin: 0 auto; padding: 24px 14px 36px; }}
    h1 {{ margin: 0 0 12px; font-size: 24px; }}
    .hint {{ color: var(--meta); margin-bottom: 18px; }}
    .msg {{ background: #f8fbff80; border: 1px solid #cbd5e1; border-radius: 12px; padding: 10px; margin-bottom: 12px; }}
    .meta {{ display: flex; flex-wrap: wrap; gap: 8px 12px; color: var(--meta); font-size: 12px; margin-bottom: 8px; }}
    .bubble {{ background: var(--bubble); border-radius: 12px; padding: 10px 12px; box-shadow: 0 1px 0 #cbd5e1; line-height: 1.35; white-space: normal; }}
    .buttons {{ margin-top: 8px; display: flex; flex-direction: column; gap: 6px; }}
    .btn-row {{ display: flex; flex-wrap: wrap; gap: 6px; }}
    .btn-wrap {{ position: relative; display: inline-flex; }}
    .btn {{ background: var(--btn); border: 1px solid var(--btn-border); border-radius: 8px; padding: 4px 10px; font-size: 13px; color: #1e3a8a; }}
    .btn-tip {{
      display: none;
      position: absolute;
      top: 110%;
      left: 0;
      z-index: 10;
      background: #0f172a;
      color: #e2e8f0;
      border-radius: 6px;
      padding: 4px 8px;
      font-size: 12px;
      white-space: nowrap;
      max-width: 560px;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .btn-wrap:hover .btn-tip {{ display: block; }}
    .empty {{ color: var(--meta); font-style: italic; }}
    .badge {{ display: inline-block; font-size: 12px; padding: 2px 8px; border-radius: 999px; color: #fff; background: var(--accent); }}
  </style>
</head>
<body>
  <main>
    <h1>Pulse TEST Outbox Preview</h1>
    <div class="hint">Render is Telegram-like for message structure, text, and buttons. Hover a button to inspect callback payload.</div>
    <div class="badge">Rows: {len(rows)}</div>
    <section style="margin-top: 14px;">{body}</section>
  </main>
</body>
</html>"""


def main() -> None:
    client = _build_client()
    outbox = client.get_records("Test_Outbox")
    outbox.sort(key=lambda row: int(row.get("id") or 0))
    output = Path(os.getenv("PULSE_TEST_PREVIEW_PATH", "artifacts/test_preview/outbox_preview.html"))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_html(outbox), encoding="utf-8")
    print(f"Wrote preview: {output}")


if __name__ == "__main__":
    main()
