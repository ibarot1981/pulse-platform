from __future__ import annotations

import importlib.util
import inspect
import json
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "grist" / "run_e2e_dual_role_owner_flow.py"
MODULE_SPEC = importlib.util.spec_from_file_location("run_e2e_dual_role_owner_flow", MODULE_PATH)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
flow = importlib.util.module_from_spec(MODULE_SPEC)
MODULE_SPEC.loader.exec_module(flow)


def _keyboard(*labels: str) -> str:
    if not labels:
        return "{}"
    return json.dumps({"keyboard": [[{"text": label} for label in labels]]})


def _rows(message_text: str, *labels: str) -> list[dict]:
    return [
        {
            "id": 1,
            "fields": {
                "event_type": "message",
                "message_text": message_text,
                "buttons_json": _keyboard(*labels),
            },
        }
    ]


class _FakeDriver:
    def __init__(self, text_responses: dict[str, list[dict]]) -> None:
        self._text_responses = text_responses
        self.sent_text_payloads: list[str] = []
        self._counter = 0

    def send_text(self, actor: str, text: str) -> tuple[int, list[dict]]:
        _ = actor
        self.sent_text_payloads.append(str(text))
        self._counter += 1
        if text not in self._text_responses:
            raise AssertionError(f"Unexpected send_text payload: {text!r}")
        return self._counter, self._text_responses[text]


class _FakeTestClient:
    def get_records(self, table: str) -> list[dict]:
        _ = table
        return []


def test_open_my_ms_jobs_batch_view_uses_batch_overview_flow_only() -> None:
    driver = _FakeDriver(
        {
            "/start": _rows("Welcome", "Manage Production"),
            "Manage Production": _rows("Manage Production:", "My MS Jobs"),
            "My MS Jobs": _rows("Choose list view:", "View By Batch No"),
            "View By Batch No": _rows("Select Batch No:\n1. B-1"),
            "1": _rows(
                "Batch Overview\nFlow Snapshot:\n1. PLS-PROD\n0/2 stages: Plasma Cutting 🔄 -> Production ⏳\n\n"
                "⚪ Select flow number (1-1) to view actions."
            ),
        }
    )

    rows = flow._open_my_ms_jobs_batch_view(
        driver=driver,
        test_client=_FakeTestClient(),
        session="s1",
        actor="u1",
        batch_no="B-1",
    )

    assert "batch overview" in flow._latest_message_text(rows).casefold()
    assert "View This Batch" not in driver.sent_text_payloads
    assert "B1" not in driver.sent_text_payloads


def test_open_my_ms_jobs_batch_view_uses_b1_fallback_not_view_this_batch() -> None:
    driver = _FakeDriver(
        {
            "/start": _rows("Welcome", "Manage Production"),
            "Manage Production": _rows("Manage Production:", "My MS Jobs"),
            "My MS Jobs": _rows("Choose list view:", "View By Batch No"),
            "View By Batch No": _rows("Select Batch No:\n1. B-1"),
            "1": _rows(
                "🔵 My MS Jobs\n⚪ Choose entries using: `1` or `1,3`\n🟠 Quick actions: `D1` Done\n`B1` Batch Summary"
            ),
            "B1": _rows(
                "Batch Overview\nFlow Snapshot:\n1. PLS-PROD\n0/2 stages: Plasma Cutting 🔄 -> Production ⏳\n\n"
                "⚪ Select flow number (1-1) to view actions."
            ),
        }
    )

    rows = flow._open_my_ms_jobs_batch_view(
        driver=driver,
        test_client=_FakeTestClient(),
        session="s1",
        actor="u1",
        batch_no="B-1",
    )

    assert "batch overview" in flow._latest_message_text(rows).casefold()
    assert "B1" in driver.sent_text_payloads
    assert "View This Batch" not in driver.sent_text_payloads


def test_e2e_menu_path_source_has_no_view_this_batch_reference() -> None:
    open_src = inspect.getsource(flow._open_my_ms_jobs_batch_view)
    action_src = inspect.getsource(flow._perform_ms_action_via_menus)
    combined = f"{open_src}\n{action_src}"
    assert "View This Batch" not in combined

