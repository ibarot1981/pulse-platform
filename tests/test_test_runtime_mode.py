from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

from pulse.core.grist_client import GristClient
from pulse.integrations import production
from pulse.notifications.dispatcher import dispatch_event
from pulse.runtime import runtime_mode
from pulse.testing import harness


def test_runtime_mode_defaults_to_live(monkeypatch):
    monkeypatch.delenv("PULSE_RUNTIME_MODE", raising=False)
    assert runtime_mode() == "LIVE"


def test_grist_client_blocks_writes_for_non_test_doc(monkeypatch):
    monkeypatch.setenv("PULSE_RUNTIME_MODE", "TEST")
    monkeypatch.setenv("PULSE_TEST_DOC_ID", "doc_test")
    monkeypatch.setenv("PULSE_TEST_ALLOW_PROD_WRITES", "false")
    client = GristClient("https://example.test", "doc_prod", "token")
    with pytest.raises(PermissionError):
        client.add_records("SomeTable", [{"a": 1}])


def test_grist_client_allows_writes_for_test_doc(monkeypatch):
    monkeypatch.setenv("PULSE_RUNTIME_MODE", "TEST")
    monkeypatch.setenv("PULSE_TEST_DOC_ID", "doc_test")
    monkeypatch.setenv("PULSE_TEST_ALLOW_PROD_WRITES", "false")
    called = {"post": False}

    class _Resp:
        def raise_for_status(self):
            return None

        def json(self):
            return {}

    def _fake_post(*args, **kwargs):
        called["post"] = True
        return _Resp()

    monkeypatch.setattr("pulse.core.grist_client.requests.post", _fake_post)
    client = GristClient("https://example.test", "doc_test", "token")
    client.add_records("SomeTable", [{"a": 1}])
    assert called["post"] is True


def test_dispatch_inbox_row_routes_text(monkeypatch):
    calls = {"fallback_text": 0, "callback_router": 0, "fallback_command": 0, "start": 0}

    async def _fallback_text(update, context):
        calls["fallback_text"] += 1

    async def _callback_router(update, context):
        calls["callback_router"] += 1

    async def _fallback_command(update, context):
        calls["fallback_command"] += 1

    async def _start(update, context):
        calls["start"] += 1

    monkeypatch.setattr("pulse.main.fallback_text", _fallback_text)
    monkeypatch.setattr("pulse.main.callback_router", _callback_router)
    monkeypatch.setattr("pulse.main.fallback_command", _fallback_command)
    monkeypatch.setattr("pulse.main.start", _start)

    class _Runtime:
        def load_user_context(self, session_id, actor_user_id):
            return {}

        def save_user_context(self, session_id, actor_user_id, role_name, user_data):
            return None

    row = {
        "id": 1,
        "fields": {
            "session_id": "s1",
            "actor_user_id": "101",
            "actor_role": "Production_Supervisor",
            "input_type": "text",
            "payload": "hello",
        },
    }
    asyncio.run(harness._dispatch_inbox_row(_Runtime(), row))
    assert calls["fallback_text"] == 1
    assert calls["callback_router"] == 0
    assert calls["fallback_command"] == 0
    assert calls["start"] == 0


def test_dispatch_inbox_row_routes_callback(monkeypatch):
    calls = {"fallback_text": 0, "callback_router": 0}

    async def _fallback_text(update, context):
        calls["fallback_text"] += 1

    async def _callback_router(update, context):
        calls["callback_router"] += 1

    monkeypatch.setattr("pulse.main.fallback_text", _fallback_text)
    monkeypatch.setattr("pulse.main.callback_router", _callback_router)

    class _Runtime:
        def load_user_context(self, session_id, actor_user_id):
            return {}

        def save_user_context(self, session_id, actor_user_id, role_name, user_data):
            return None

    row = {
        "id": 2,
        "fields": {
            "session_id": "s1",
            "actor_user_id": "101",
            "actor_role": "Production_Manager",
            "input_type": "callback",
            "payload": "prodappr|open|1",
        },
    }
    asyncio.run(harness._dispatch_inbox_row(_Runtime(), row))
    assert calls["fallback_text"] == 0
    assert calls["callback_router"] == 1


def test_main_uses_test_loop_in_test_mode(monkeypatch):
    calls = {"test_loop": 0}

    def _test_loop():
        calls["test_loop"] += 1

    monkeypatch.setattr("pulse.main.is_test_mode", lambda: True)
    monkeypatch.setattr("pulse.main.run_test_runtime_loop", _test_loop)
    monkeypatch.setattr("pulse.main.test_doc_id", lambda: "doc_test")

    from pulse import main as pulse_main

    pulse_main.main()
    assert calls["test_loop"] == 1


def test_batch_created_notification_sent_only_to_approver(monkeypatch):
    sent: list[dict] = []

    class _FakeBot:
        async def send_message_with_metadata(
            self,
            chat_id,
            text,
            reply_markup=None,
            parse_mode=None,
            recipient=None,
            **_,
        ):
            sent.append(
                {
                    "chat_id": str(chat_id),
                    "text": str(text),
                    "reply_markup": reply_markup,
                    "recipient": recipient or {},
                }
            )

    recipients = [
        {
            "user_id": "U_SUP",
            "telegram_id": "1001",
            "role_id": "R03",
            "role_name": "Production_Supervisor",
        },
        {
            "user_id": "U_PM",
            "telegram_id": "1002",
            "role_id": "R01",
            "role_name": "Production_Manager",
        },
    ]

    monkeypatch.setattr(
        "pulse.notifications.dispatcher.get_subscribers",
        lambda event_type, context=None: recipients,
    )

    batch_id = 9
    asyncio.run(
        dispatch_event(
            event_type="production_batch_created",
            message="Batch created: B-1 | Model: M1 | Qty: 10 | Approval: Pending",
            telegram_bot=_FakeBot(),
            context={"batch_id": batch_id},
            recipient_renderer=production._batch_created_recipient_renderer(batch_id),
        )
    )

    assert len(sent) == 1
    assert sent[0]["chat_id"] == "1002"
    assert sent[0]["recipient"].get("role_name") == "Production_Manager"
    markup = sent[0]["reply_markup"]
    assert markup is not None
    assert "prodappr:open:9" in str(markup)


def test_create_batch_submit_sends_single_creator_confirmation(monkeypatch):
    class _FakeRepo:
        def get_costing_user_ref_by_user_id(self, user_id):
            return 77

        def get_existing_batch_numbers(self):
            return []

        def create_master_batch(self, fields):
            return 501

        def add_lifecycle_history(self, *args, **kwargs):
            return None

    class _FakeContext:
        def __init__(self):
            self.user_data = {
                "user": {"user_id": "U_SUP"},
                "production_batch_flow": {
                    "batch_mode": "By Product Part",
                    "batch_type": "MS Only",
                    "model_code": "S1KHFL-BASE",
                    "batch_qty": 32,
                    "selected_part_ids": [11, 12],
                },
            }
            self.bot = object()

    class _FakeUpdate:
        pass

    notify_mock = AsyncMock()
    reply_mock = AsyncMock()
    monkeypatch.setattr("pulse.integrations.production.ProductionRepo", lambda: _FakeRepo())
    monkeypatch.setattr("pulse.integrations.production._notify_event", notify_mock)
    monkeypatch.setattr("pulse.integrations.production._reply", reply_mock)

    context = _FakeContext()
    asyncio.run(production._create_batch_from_flow(_FakeUpdate(), context))

    assert notify_mock.await_count == 1
    assert notify_mock.await_args.args[1] == "production_batch_created"
    assert reply_mock.await_count == 1
    assert "Status: Pending Approval" in reply_mock.await_args.args[1]
