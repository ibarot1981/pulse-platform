from __future__ import annotations

import asyncio

import pytest

from pulse.core.grist_client import GristClient
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
