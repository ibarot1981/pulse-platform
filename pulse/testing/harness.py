from __future__ import annotations

import asyncio
import json
import os
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from pulse.core.grist_client import GristClient
from pulse.runtime import is_test_mode, test_api_key, test_doc_id, test_poll_interval_seconds


TEST_INBOX_TABLE = "Test_Inbox"
TEST_OUTBOX_TABLE = "Test_Outbox"
TEST_CONTEXT_TABLE = "Test_UserContext"
TEST_ATTACHMENTS_TABLE = "Test_Attachments"
TEST_RUN_LOG_TABLE = "Test_RunLog"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=True)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=True)


def _safe_int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _markup_to_dict(reply_markup: Any) -> dict[str, Any] | None:
    if reply_markup is None:
        return None
    if hasattr(reply_markup, "to_dict"):
        return reply_markup.to_dict()
    if isinstance(reply_markup, dict):
        return reply_markup
    return {"repr": repr(reply_markup)}


def _infer_parse_mode(text: str) -> str:
    if "<b>" in text or "<i>" in text or "<a " in text:
        return "HTML"
    if "*" in text or "_" in text or "`" in text:
        return "Markdown"
    return ""


@dataclass
class _FakeUser:
    id: int


@dataclass
class _FakeChat:
    id: int


class TestRuntimeClient:
    def __init__(self) -> None:
        server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
        doc_id = test_doc_id()
        api_key = test_api_key()
        if not server or not doc_id or not api_key:
            raise ValueError("Missing TEST runtime Grist config: PULSE_GRIST_SERVER, PULSE_TEST_DOC_ID, PULSE_TEST_API_KEY/PULSE_API_KEY")
        self.client = GristClient(server, doc_id, api_key)
        self.doc_id = doc_id

    def fetch_pending_inbox_rows(self) -> list[dict]:
        rows = self.client.get_records(TEST_INBOX_TABLE)
        pending: list[dict] = []
        for row in rows:
            fields = row.get("fields", {})
            processed = bool(fields.get("processed", False))
            if not processed:
                pending.append(row)
        pending.sort(key=lambda item: int(item.get("id") or 0))
        return pending

    def is_inbox_row_processed(self, inbox_id: int) -> bool:
        if inbox_id <= 0:
            return True
        try:
            rows = self.client.get_records(TEST_INBOX_TABLE)
        except Exception:
            return False
        for row in rows:
            if int(row.get("id") or 0) != inbox_id:
                continue
            fields = row.get("fields", {})
            return bool(fields.get("processed", False))
        return True

    def mark_inbox_processed(self, inbox_id: int, error: str = "") -> None:
        self.client.patch_record(
            TEST_INBOX_TABLE,
            inbox_id,
            {
                "processed": True,
                "processed_at": _utc_now_iso(),
                "error": error[:2000],
            },
        )

    def append_outbox(
        self,
        session_id: str,
        recipient_user_id: str,
        recipient_role: str,
        event_type: str,
        message_text: str,
        reply_markup: Any = None,
        parse_mode: str = "",
        source: str = "bot_reply",
        correlation_inbox_id: int | None = None,
    ) -> None:
        markup_dict = _markup_to_dict(reply_markup)
        self.client.add_records(
            TEST_OUTBOX_TABLE,
            [
                {
                    "session_id": session_id,
                    "recipient_user_id": recipient_user_id,
                    "recipient_role": recipient_role,
                    "event_type": event_type,
                    "source": source,
                    "message_text": message_text,
                    "parse_mode": parse_mode or _infer_parse_mode(message_text),
                    "buttons_json": _to_json(markup_dict or {}),
                    "payload_json": _to_json(
                        {
                            "reply_markup": markup_dict,
                            "correlation_inbox_id": correlation_inbox_id,
                        }
                    ),
                    "created_at": _utc_now_iso(),
                }
            ],
        )

    def append_attachment(
        self,
        session_id: str,
        recipient_user_id: str,
        filename: str,
        caption: str = "",
        source: str = "bot_document",
        correlation_inbox_id: int | None = None,
    ) -> None:
        self.client.add_records(
            TEST_ATTACHMENTS_TABLE,
            [
                {
                    "session_id": session_id,
                    "recipient_user_id": recipient_user_id,
                    "source": source,
                    "filename": filename,
                    "caption": caption,
                    "meta_json": _to_json({"correlation_inbox_id": correlation_inbox_id}),
                    "created_at": _utc_now_iso(),
                }
            ],
        )

    def log_run(
        self,
        level: str,
        message: str,
        session_id: str = "",
        actor_user_id: str = "",
        inbox_id: int | None = None,
        details: str = "",
    ) -> None:
        self.client.add_records(
            TEST_RUN_LOG_TABLE,
            [
                {
                    "level": level,
                    "message": message,
                    "session_id": session_id,
                    "actor_user_id": actor_user_id,
                    "inbox_id": inbox_id or 0,
                    "details": details[:10000],
                    "created_at": _utc_now_iso(),
                }
            ],
        )

    def load_user_context(self, session_id: str, actor_user_id: str) -> dict[str, Any]:
        rows = self.client.get_records(TEST_CONTEXT_TABLE)
        for row in rows:
            fields = row.get("fields", {})
            if str(fields.get("session_id", "")) != session_id:
                continue
            if str(fields.get("actor_user_id", "")) != actor_user_id:
                continue
            raw = fields.get("context_json", "{}")
            if isinstance(raw, str):
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    return {}
            if isinstance(raw, dict):
                return raw
            return {}
        return {}

    def save_user_context(
        self,
        session_id: str,
        actor_user_id: str,
        role_name: str,
        user_data: dict[str, Any],
    ) -> None:
        rows = self.client.get_records(TEST_CONTEXT_TABLE)
        existing_id: int | None = None
        for row in rows:
            fields = row.get("fields", {})
            if str(fields.get("session_id", "")) == session_id and str(fields.get("actor_user_id", "")) == actor_user_id:
                existing_id = int(row.get("id"))
                break
        payload = {
            "session_id": session_id,
            "actor_user_id": actor_user_id,
            "role_name": role_name,
            "menu_state": str(user_data.get("menu_state", "")),
            "context_json": _to_json(user_data),
            "updated_at": _utc_now_iso(),
        }
        if existing_id is None:
            self.client.add_records(TEST_CONTEXT_TABLE, [payload])
            return
        self.client.patch_record(TEST_CONTEXT_TABLE, existing_id, payload)


class _FakeMessage:
    def __init__(
        self,
        runtime: TestRuntimeClient,
        session_id: str,
        actor_user_id: str,
        actor_role: str,
        chat_id: int,
        text: str = "",
        correlation_inbox_id: int | None = None,
    ) -> None:
        self._runtime = runtime
        self._session_id = session_id
        self._actor_user_id = actor_user_id
        self._actor_role = actor_role
        self._correlation_inbox_id = correlation_inbox_id
        self.text = text
        self.chat_id = chat_id
        self.chat = _FakeChat(chat_id)

    async def reply_text(self, text: str, reply_markup: Any = None, parse_mode: str | None = None, **_: Any) -> None:
        self._runtime.append_outbox(
            session_id=self._session_id,
            recipient_user_id=self._actor_user_id,
            recipient_role=self._actor_role,
            event_type="message",
            message_text=str(text),
            reply_markup=reply_markup,
            parse_mode=str(parse_mode or ""),
            source="reply_text",
            correlation_inbox_id=self._correlation_inbox_id,
        )

    async def reply_document(self, document: Any, filename: str | None = None, caption: str = "", **_: Any) -> None:
        inferred_name = filename
        if not inferred_name and hasattr(document, "name"):
            inferred_name = str(getattr(document, "name"))
        self._runtime.append_attachment(
            session_id=self._session_id,
            recipient_user_id=self._actor_user_id,
            filename=str(inferred_name or "document.bin"),
            caption=str(caption or ""),
            source="reply_document",
            correlation_inbox_id=self._correlation_inbox_id,
        )
        self._runtime.append_outbox(
            session_id=self._session_id,
            recipient_user_id=self._actor_user_id,
            recipient_role=self._actor_role,
            event_type="document",
            message_text=str(caption or ""),
            reply_markup=None,
            parse_mode="",
            source="reply_document",
            correlation_inbox_id=self._correlation_inbox_id,
        )


class _FakeCallbackQuery:
    def __init__(self, data: str, message: _FakeMessage, user_id: int) -> None:
        self.data = data
        self.message = message
        self.from_user = _FakeUser(user_id)

    async def answer(self, text: str | None = None, **_: Any) -> None:
        if text:
            await self.message.reply_text(text)


class _FakeBot:
    def __init__(self, runtime: TestRuntimeClient, session_id: str, correlation_inbox_id: int | None = None) -> None:
        self._runtime = runtime
        self._session_id = session_id
        self._correlation_inbox_id = correlation_inbox_id

    async def send_message(self, chat_id: Any, text: str, reply_markup: Any = None, parse_mode: str | None = None, **_: Any) -> None:
        self._runtime.append_outbox(
            session_id=self._session_id,
            recipient_user_id=str(chat_id),
            recipient_role="",
            event_type="notification",
            message_text=str(text),
            reply_markup=reply_markup,
            parse_mode=str(parse_mode or ""),
            source="bot.send_message",
            correlation_inbox_id=self._correlation_inbox_id,
        )

    async def send_message_with_metadata(
        self,
        chat_id: Any,
        text: str,
        reply_markup: Any = None,
        parse_mode: str | None = None,
        recipient: dict[str, Any] | None = None,
        **_: Any,
    ) -> None:
        recipient_role = ""
        if isinstance(recipient, dict):
            recipient_role = str(recipient.get("role_name") or recipient.get("role_id") or "")
        self._runtime.append_outbox(
            session_id=self._session_id,
            recipient_user_id=str(chat_id),
            recipient_role=recipient_role,
            event_type="notification",
            message_text=str(text),
            reply_markup=reply_markup,
            parse_mode=str(parse_mode or ""),
            source="bot.send_message",
            correlation_inbox_id=self._correlation_inbox_id,
        )

    async def send_document(self, chat_id: Any, document: Any, filename: str | None = None, caption: str = "", **_: Any) -> None:
        inferred_name = filename
        if not inferred_name and hasattr(document, "name"):
            inferred_name = str(getattr(document, "name"))
        self._runtime.append_attachment(
            session_id=self._session_id,
            recipient_user_id=str(chat_id),
            filename=str(inferred_name or "document.bin"),
            caption=str(caption or ""),
            source="bot.send_document",
            correlation_inbox_id=self._correlation_inbox_id,
        )
        self._runtime.append_outbox(
            session_id=self._session_id,
            recipient_user_id=str(chat_id),
            recipient_role="",
            event_type="document",
            message_text=str(caption or ""),
            source="bot.send_document",
            correlation_inbox_id=self._correlation_inbox_id,
        )


class _FakeContext:
    def __init__(self, user_data: dict[str, Any], bot: _FakeBot) -> None:
        self.user_data = user_data
        self.bot = bot


class _FakeUpdate:
    def __init__(self, user_id: int, message: _FakeMessage, callback_data: str = "") -> None:
        self.effective_user = _FakeUser(user_id)
        self.effective_chat = _FakeChat(user_id)
        self.effective_message = message
        self.message = message
        self.callback_query = _FakeCallbackQuery(callback_data, message, user_id) if callback_data else None


async def _dispatch_inbox_row(runtime: TestRuntimeClient, row: dict) -> None:
    from pulse import main as main_module

    inbox_id = int(row.get("id") or 0)
    fields = row.get("fields", {})
    session_id = str(fields.get("session_id") or f"default-{inbox_id}")
    actor_user_id = str(fields.get("actor_user_id") or "")
    actor_role = str(fields.get("actor_role") or "")
    input_type = str(fields.get("input_type") or "text").strip().lower()
    payload = str(fields.get("payload") or "")

    actor_id_int = _safe_int(actor_user_id)
    if actor_id_int is None:
        raise ValueError(f"Invalid actor_user_id '{actor_user_id}' for inbox row {inbox_id}")

    context_data = runtime.load_user_context(session_id, actor_user_id)
    fake_message = _FakeMessage(
        runtime=runtime,
        session_id=session_id,
        actor_user_id=actor_user_id,
        actor_role=actor_role,
        chat_id=actor_id_int,
        text=payload if input_type == "text" else "",
        correlation_inbox_id=inbox_id,
    )
    context = _FakeContext(
        user_data=context_data,
        bot=_FakeBot(runtime=runtime, session_id=session_id, correlation_inbox_id=inbox_id),
    )
    update = _FakeUpdate(
        user_id=actor_id_int,
        message=fake_message,
        callback_data=payload if input_type == "callback" else "",
    )

    if input_type == "callback":
        await main_module.callback_router(update, context)
    elif payload.strip().startswith("/"):
        if payload.strip() == "/start":
            await main_module.start(update, context)
        else:
            await main_module.fallback_command(update, context)
    else:
        await main_module.fallback_text(update, context)

    role_name = ""
    user_obj = context.user_data.get("user")
    if isinstance(user_obj, dict):
        role_name = str(user_obj.get("role", ""))
    runtime.save_user_context(session_id, actor_user_id, role_name, context.user_data)


def run_test_runtime_loop() -> None:
    if not is_test_mode():
        raise RuntimeError("run_test_runtime_loop should only run in TEST mode")
    runtime = TestRuntimeClient()
    runtime.log_run(
        "INFO",
        "Starting TEST runtime loop",
        details=_to_json(
            {
                "mode": "TEST",
                "test_doc_id": runtime.doc_id,
                "poll_interval_seconds": test_poll_interval_seconds(),
            }
        ),
    )
    while True:
        processed = process_pending_once(runtime)
        if processed == 0:
            asyncio.run(asyncio.sleep(test_poll_interval_seconds()))


def process_pending_once(runtime: TestRuntimeClient | None = None) -> int:
    runtime_client = runtime or TestRuntimeClient()
    rows = runtime_client.fetch_pending_inbox_rows()
    is_row_processed = getattr(runtime_client, "is_inbox_row_processed", None)
    for row in rows:
        inbox_id = int(row.get("id") or 0)
        fields = row.get("fields", {})
        session_id = str(fields.get("session_id") or "")
        actor_user_id = str(fields.get("actor_user_id") or "")
        if callable(is_row_processed) and is_row_processed(inbox_id):
            continue
        try:
            asyncio.run(_dispatch_inbox_row(runtime_client, row))
            runtime_client.mark_inbox_processed(inbox_id, "")
            runtime_client.log_run("INFO", "Processed inbox row", session_id, actor_user_id, inbox_id)
        except Exception as exc:  # noqa: BLE001
            error_text = f"{type(exc).__name__}: {exc}"
            runtime_client.mark_inbox_processed(inbox_id, error_text)
            runtime_client.log_run(
                "ERROR",
                "Failed processing inbox row",
                session_id,
                actor_user_id,
                inbox_id,
                details=traceback.format_exc(),
            )
    return len(rows)
