from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PUSH_SCRIPT_PATH = REPO_ROOT / "scripts" / "grist" / "push_test_inbox.py"
RENDER_SCRIPT_PATH = REPO_ROOT / "scripts" / "grist" / "render_test_outbox_preview.py"

if __package__ in (None, ""):
    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

from dotenv import load_dotenv

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient
from pulse.runtime import test_api_key, test_doc_id

load_dotenv(REPO_ROOT / ".env")

_MS_PENDING_CONFIRMATION = "Done - Pending Confirmation"
_COMPLETE_STATUSES = {"Cutting Completed", "Done", "Completed"}
_MY_MS_FILTER_BATCH = "View By Batch No"
_SCHEDULE_STATE = "awaiting_schedule_date"


def _normalize_ref(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _normalize_process_seq_value(value):
    normalized = _normalize_ref(value)
    if isinstance(normalized, int):
        return normalized
    text = str(normalized or "").strip()
    if text.isdigit():
        return int(text)
    return text


def _process_seq_sort_key(process_seq_value) -> tuple[int, int | str]:
    normalized = _normalize_process_seq_value(process_seq_value)
    if isinstance(normalized, int):
        return (0, normalized)
    text = str(normalized or "").strip()
    if text.isdigit():
        return (0, int(text))
    return (1, text)


def _norm_role(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").replace("-", " ").split())


def _norm_label(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", str(value or "").casefold()))


def _parse_json_dict(raw: str) -> dict:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _build_test_client() -> GristClient:
    server = str(os.getenv("PULSE_GRIST_SERVER", "")).rstrip("/")
    doc = test_doc_id()
    api = test_api_key()
    if not server or not doc or not api:
        raise ValueError("Set PULSE_RUNTIME_MODE=TEST and PULSE_TEST_DOC_ID / PULSE_TEST_API_KEY.")
    return GristClient(server, doc, api)


def _build_pulse_client() -> GristClient:
    server = str(PULSE_GRIST_SERVER or "").rstrip("/")
    if not server or not PULSE_DOC_ID or not PULSE_API_KEY:
        raise ValueError("Missing PULSE doc configuration.")
    return GristClient(server, PULSE_DOC_ID, PULSE_API_KEY)


def _build_costing_client() -> GristClient:
    server = str(PULSE_GRIST_SERVER or "").rstrip("/")
    if not server or not COSTING_DOC_ID or not COSTING_API_KEY:
        raise ValueError("Missing COSTING doc configuration.")
    return GristClient(server, COSTING_DOC_ID, COSTING_API_KEY)


def _run_render() -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT)
    env["PULSE_RUNTIME_MODE"] = "TEST"
    subprocess.run([sys.executable, str(RENDER_SCRIPT_PATH)], check=True, env=env)


def _rows_for_session(rows: list[dict], session: str) -> list[dict]:
    session_text = str(session).strip()
    return [row for row in rows if str(row.get("fields", {}).get("session_id", "")).strip() == session_text]


def _correlation_inbox_id(row: dict) -> int | None:
    payload = _parse_json_dict(str(row.get("fields", {}).get("payload_json") or ""))
    raw = payload.get("correlation_inbox_id")
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _extract_button_entries(row: dict) -> list[tuple[str, str]]:
    fields = row.get("fields", {})
    payload = _parse_json_dict(str(fields.get("buttons_json") or ""))
    rows = payload.get("keyboard")
    if not isinstance(rows, list):
        rows = payload.get("inline_keyboard")
    if not isinstance(rows, list):
        return []

    out: list[tuple[str, str]] = []
    for button_row in rows:
        if not isinstance(button_row, list):
            continue
        for button in button_row:
            if not isinstance(button, dict):
                continue
            label = str(button.get("text") or "").strip()
            callback = str(button.get("callback_data") or "").strip()
            if label:
                out.append((label, callback))
    return out


def _button_labels_from_rows(rows: list[dict]) -> list[str]:
    for row in reversed(rows):
        labels = [label for label, _ in _extract_button_entries(row)]
        if labels:
            return labels
    return []


def _pick_callback_from_rows(
    rows: list[dict],
    *,
    exact: tuple[str, ...] = (),
    token_groups: tuple[tuple[str, ...], ...] = (),
) -> tuple[str, str]:
    for row in reversed(rows):
        entries = _extract_button_entries(row)
        labels = [label for label, callback in entries if callback]
        picked_label = _pick_label(labels, exact=exact, token_groups=token_groups)
        if not picked_label:
            continue
        for label, callback in entries:
            if label == picked_label and callback:
                return label, callback
    return "", ""


def _latest_outbox_for_actor(test_client: GristClient, session: str, actor: str) -> dict | None:
    rows = _rows_for_session(test_client.get_records("Test_Outbox"), session)
    actor_text = str(actor).strip()
    for row in reversed(rows):
        if str(row.get("fields", {}).get("recipient_user_id", "")).strip() == actor_text:
            return row
    return None


def _latest_text_for_actor(test_client: GristClient, session: str, actor: str) -> str:
    latest = _latest_outbox_for_actor(test_client, session, actor)
    if not latest:
        return ""
    return str(latest.get("fields", {}).get("message_text") or "")


def _session_activity_counts(test_client: GristClient, session: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in ("Test_Inbox", "Test_Outbox", "Test_UserContext", "Test_RunLog"):
        rows = _rows_for_session(test_client.get_records(table), session)
        counts[table] = len(rows)
    return counts


def _recent_actor_outbox_summary(test_client: GristClient, session: str, actor: str, limit: int = 8) -> str:
    rows = _rows_for_session(test_client.get_records("Test_Outbox"), session)
    actor_text = str(actor).strip()
    filtered = [row for row in rows if str(row.get("fields", {}).get("recipient_user_id", "")).strip() == actor_text]
    filtered.sort(key=lambda row: int(row.get("id") or 0))
    if not filtered:
        return "<no outbox rows for actor>"
    tail = filtered[-max(1, int(limit)) :]
    lines: list[str] = []
    for row in tail:
        fields = row.get("fields", {})
        text = str(fields.get("message_text") or "").replace("\n", " | ").strip()
        if len(text) > 220:
            text = text[:220] + "..."
        lines.append(
            f"- id={row.get('id')} event={fields.get('event_type')} text={text}"
        )
    return "\n".join(lines)


def _latest_message_row(rows: list[dict]) -> dict | None:
    for row in reversed(rows):
        if str(row.get("fields", {}).get("event_type") or "") == "message":
            return row
    return rows[-1] if rows else None


def _latest_message_text(rows: list[dict]) -> str:
    row = _latest_message_row(rows)
    if not row:
        return ""
    return str(row.get("fields", {}).get("message_text") or "")


def _all_message_text(rows: list[dict]) -> str:
    texts: list[str] = []
    for row in rows:
        fields = row.get("fields", {})
        if str(fields.get("event_type") or "") != "message":
            continue
        text = str(fields.get("message_text") or "").strip()
        if text:
            texts.append(text)
    return "\n".join(texts)


def _message_texts(rows: list[dict]) -> list[str]:
    texts: list[str] = []
    for row in rows:
        fields = row.get("fields", {})
        if str(fields.get("event_type") or "") != "message":
            continue
        text = str(fields.get("message_text") or "").strip()
        if text:
            texts.append(text)
    return texts


def _rows_contain_batch_list_view(rows: list[dict]) -> bool:
    for text in _message_texts(rows):
        normalized = text.casefold()
        if "choose entries using:" in normalized and "quick actions:" in normalized:
            return True
    return False


def _extract_batch_overview_text(rows: list[dict]) -> str:
    for text in _message_texts(rows):
        normalized = text.casefold()
        if "batch overview" in normalized and "flow snapshot" in normalized:
            return text
    return _latest_message_text(rows)


def _flow_candidates_from_overview(text: str, desired_action: str) -> list[int]:
    lines = [line.strip() for line in str(text or "").splitlines()]
    flows: list[tuple[int, str]] = []
    for idx, line in enumerate(lines):
        match = re.match(r"^(\d+)\.\s+.+$", line)
        if not match:
            continue
        flow_no = int(match.group(1))
        stage_line = ""
        for next_idx in range(idx + 1, len(lines)):
            candidate = lines[next_idx].strip()
            if not candidate:
                continue
            stage_line = candidate
            break
        flows.append((flow_no, stage_line))
    if not flows:
        return [1]

    scored: list[tuple[int, int]] = []
    for flow_no, stage_line in flows:
        stage_norm = stage_line.casefold()
        score = 0
        if desired_action == "confirm":
            if "🤝" in stage_line or "hand-off pending" in stage_norm:
                score += 10
            if "🔄" in stage_line or "running" in stage_norm:
                score += 1
        else:
            if "🔄" in stage_line or "running" in stage_norm:
                score += 10
            if "🤝" in stage_line or "hand-off pending" in stage_norm:
                score += 1
        scored.append((flow_no, score))

    scored.sort(key=lambda item: (-item[1], item[0]))
    ordered = [flow_no for flow_no, _ in scored]
    if not ordered:
        return [1]
    return ordered


def _pick_label(
    labels: list[str],
    *,
    exact: tuple[str, ...] = (),
    token_groups: tuple[tuple[str, ...], ...] = (),
) -> str:
    if not labels:
        return ""
    norm_to_label: dict[str, str] = {}
    for label in labels:
        norm = _norm_label(label)
        if norm and norm not in norm_to_label:
            norm_to_label[norm] = label

    for candidate in exact:
        target = _norm_label(candidate)
        if target in norm_to_label:
            return norm_to_label[target]

    for tokens in token_groups:
        parts = tuple(_norm_label(token) for token in tokens if _norm_label(token))
        if not parts:
            continue
        for label in labels:
            normalized = _norm_label(label)
            if all(part in normalized for part in parts):
                return label
    return ""


def _parse_numbered_options(text: str) -> dict[int, str]:
    options: dict[int, str] = {}
    for line in str(text or "").splitlines():
        match = re.match(r"^\s*(\d+)\.\s*(.+?)\s*$", line)
        if not match:
            continue
        options[int(match.group(1))] = match.group(2).strip()
    return options


def _find_batch_created_no_from_outbox(test_client: GristClient, session: str, actor: str) -> str:
    rows = _rows_for_session(test_client.get_records("Test_Outbox"), session)
    actor_text = str(actor).strip()
    for row in reversed(rows):
        fields = row.get("fields", {})
        if str(fields.get("recipient_user_id", "")).strip() != actor_text:
            continue
        text = str(fields.get("message_text") or "")
        if "Batch created:" not in text:
            continue
        return text.split("Batch created:", 1)[1].strip().splitlines()[0].strip()
    return ""


def _find_batch_id_by_no(costing_client: GristClient, batch_no: str, *, min_id: int | None = None) -> int | None:
    matched: list[int] = []
    for row in costing_client.get_records("ProductBatchMaster"):
        if str(row.get("fields", {}).get("batch_no", "")).strip() != batch_no:
            continue
        rec_id = row.get("id")
        if isinstance(rec_id, int):
            matched.append(rec_id)
    if isinstance(min_id, int):
        newer = [rec_id for rec_id in matched if rec_id > min_id]
        if newer:
            return max(newer)
        return None
    if not matched:
        return None
    return max(matched)


def _max_batch_id(costing_client: GristClient) -> int:
    max_id = 0
    for row in costing_client.get_records("ProductBatchMaster"):
        rec_id = row.get("id")
        if isinstance(rec_id, int):
            max_id = max(max_id, rec_id)
    return max_id


def _build_actor_maps(pulse_client: GristClient) -> tuple[dict[str, str], dict[str, str]]:
    users = pulse_client.get_records("Users")
    by_user_id: dict[str, str] = {}
    by_name: dict[str, str] = {}
    for row in users:
        fields = row.get("fields", {})
        if not fields.get("Active"):
            continue
        user_id = str(fields.get("User_ID") or "").strip()
        name = str(fields.get("Name") or "").strip()
        tg = str(fields.get("Telegram_ID") or "").strip()
        if user_id and tg:
            by_user_id[user_id] = tg
        if name and tg:
            by_name[name.casefold()] = tg
    return by_user_id, by_name


def _role_member_telegrams(pulse_client: GristClient) -> dict[str, list[str]]:
    roles = pulse_client.get_records("Roles")
    users = pulse_client.get_records("Users")
    assignments = pulse_client.get_records("UserRoleAssignment")
    role_name_by_rec: dict[int, str] = {}
    for row in roles:
        rec_id = row.get("id")
        if not isinstance(rec_id, int):
            continue
        role_name_by_rec[rec_id] = str(row.get("fields", {}).get("Role_Name") or "").strip()

    user_by_rec: dict[int, dict] = {}
    for row in users:
        rec_id = row.get("id")
        if not isinstance(rec_id, int):
            continue
        user_by_rec[rec_id] = row.get("fields", {})

    role_members: dict[str, list[str]] = {}

    def _add(role_name: str, telegram_id: str) -> None:
        key = _norm_role(role_name)
        if not key or not telegram_id:
            return
        bucket = role_members.setdefault(key, [])
        if telegram_id not in bucket:
            bucket.append(telegram_id)

    for row in users:
        fields = row.get("fields", {})
        if not fields.get("Active"):
            continue
        tg = str(fields.get("Telegram_ID") or "").strip()
        role_ref = _normalize_ref(fields.get("Role"))
        if isinstance(role_ref, int):
            _add(role_name_by_rec.get(role_ref, ""), tg)

    for row in assignments:
        fields = row.get("fields", {})
        if not bool(fields.get("Active", True)):
            continue
        user_ref = _normalize_ref(fields.get("User"))
        role_ref = _normalize_ref(fields.get("Role"))
        if not isinstance(user_ref, int) or not isinstance(role_ref, int):
            continue
        user_fields = user_by_rec.get(user_ref, {})
        if not user_fields.get("Active"):
            continue
        tg = str(user_fields.get("Telegram_ID") or "").strip()
        _add(role_name_by_rec.get(role_ref, ""), tg)

    return role_members


def _current_menu_state(test_client: GristClient, session: str, actor: str) -> str:
    actor_text = str(actor).strip()
    for row in test_client.get_records("Test_UserContext"):
        fields = row.get("fields", {})
        if str(fields.get("session_id", "")).strip() != session:
            continue
        if str(fields.get("actor_user_id", "")).strip() != actor_text:
            continue
        return str(fields.get("menu_state") or "").strip()
    return ""


def _build_stage_role_map(costing_client: GristClient) -> dict[tuple[int, str], str]:
    result: dict[tuple[int, str], str] = {}
    for row in costing_client.get_records("ProcessStage"):
        fields = row.get("fields", {})
        seq_ref = _normalize_ref(fields.get("process_seq_id"))
        if not isinstance(seq_ref, int):
            continue
        stage_name = str(fields.get("stage_name") or "").strip()
        role_name = str(fields.get("resolved_role_name") or "").strip()
        if not stage_name or not role_name:
            continue
        result[(seq_ref, stage_name)] = role_name
    return result


def _resolve_stage_role(stage_role_map: dict[tuple[int, str], str], process_seq, stage_name: str) -> str:
    seq_ref = _normalize_ref(process_seq)
    if not isinstance(seq_ref, int):
        text = str(seq_ref or "").strip()
        if text.isdigit():
            seq_ref = int(text)
    if not isinstance(seq_ref, int):
        return ""
    return str(stage_role_map.get((seq_ref, str(stage_name or "").strip()), "")).strip()


def _fetch_batch_rows(costing_client: GristClient, batch_id: int) -> list[dict]:
    rows: list[dict] = []
    for row in costing_client.get_records("ProductBatchMS"):
        fields = row.get("fields", {})
        row_batch = _normalize_ref(fields.get("batch_id"))
        if row_batch != batch_id:
            continue
        rows.append(row)
    rows.sort(key=lambda x: int(x.get("id") or 0))
    return rows


def _is_complete_status(status: str) -> bool:
    return str(status or "").strip() in _COMPLETE_STATUSES


def _resolve_flow_number_for_row(batch_rows: list[dict], row_id: int) -> int | None:
    if not isinstance(row_id, int) or row_id <= 0:
        return None
    ordered = list(batch_rows)
    ordered.sort(
        key=lambda row: (
            _process_seq_sort_key(row.get("fields", {}).get("process_seq")),
            int(row.get("id") or 0),
        )
    )
    for idx, row in enumerate(ordered, start=1):
        if int(row.get("id") or 0) == row_id:
            return idx
    return None


def _find_option_number_for_batch(text: str, batch_no: str) -> int | None:
    wanted = str(batch_no or "").strip().casefold()
    if not wanted:
        return None
    for line in str(text or "").splitlines():
        match = re.match(r"^\s*(\d+)\.\s*(.+?)\s*$", line)
        if not match:
            continue
        candidate = match.group(2).strip()
        leading = candidate.split("|", 1)[0].strip().casefold()
        if leading == wanted or wanted in candidate.casefold():
            return int(match.group(1))
    return None


def _contains_any(haystack: str, needles: tuple[str, ...]) -> bool:
    text = haystack.casefold()
    return any(needle in text for needle in needles)


def _assert_no_auth_error_text(text: str, actor: str) -> None:
    lowered = str(text or "").casefold()
    error_tokens = (
        "not authorized",
        "only the next-stage supervisor",
        "could not",
        "not waiting for",
        "not found",
        "you do not have access",
    )
    for token in error_tokens:
        if token in lowered:
            raise RuntimeError(f"Action failed for actor {actor}. Latest response: {text}")


def _assert_no_auth_error_rows(test_client: GristClient, session: str, actor: str, rows: list[dict]) -> None:
    all_text = _all_message_text(rows).strip()
    if all_text:
        _assert_no_auth_error_text(all_text, actor)
        return
    _assert_no_auth_error_text(_latest_text_for_actor(test_client, session, actor), actor)


class TestSessionDriver:
    def __init__(self, test_client: GristClient, session: str) -> None:
        self.test_client = test_client
        self.session = str(session).strip()
        self._processing_mode = "auto"  # auto -> runtime or local

    def send_text(self, actor: str, text: str) -> tuple[int, list[dict]]:
        return self._send(actor=actor, payload_flag="--text", payload_value=text)

    def send_callback(self, actor: str, callback: str) -> tuple[int, list[dict]]:
        return self._send(actor=actor, payload_flag="--callback", payload_value=callback)

    def _send(self, *, actor: str, payload_flag: str, payload_value: str) -> tuple[int, list[dict]]:
        inbox_id = self._insert_inbox(actor, payload_flag, payload_value)
        self._ensure_processed(inbox_id)
        rows = self._rows_for_actor_correlation(actor, inbox_id)
        return inbox_id, rows

    def _insert_inbox(self, actor: str, payload_flag: str, payload_value: str) -> int:
        args = [
            sys.executable,
            str(PUSH_SCRIPT_PATH),
            "--session",
            self.session,
            "--actor",
            str(actor),
            payload_flag,
            str(payload_value),
        ]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        env["PULSE_RUNTIME_MODE"] = "TEST"
        result = subprocess.run(args, env=env, capture_output=True, text=True)
        if result.returncode != 0:
            details = (result.stdout or "") + "\n" + (result.stderr or "")
            raise RuntimeError(
                f"Push insert failed for actor={actor} payload={payload_value!r}\n{details.strip()}"
            )

        out = (result.stdout or "") + "\n" + (result.stderr or "")
        match = re.search(r"id=(\d+)", out)
        if not match:
            raise RuntimeError(f"Could not parse inserted inbox id from push output:\n{out.strip()}")
        return int(match.group(1))

    def _process_pending_once_local(self) -> None:
        code = "from pulse.testing.harness import process_pending_once; print(process_pending_once())"
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT)
        env["PULSE_RUNTIME_MODE"] = "TEST"
        result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True)
        if result.returncode != 0:
            details = (result.stdout or "") + "\n" + (result.stderr or "")
            raise RuntimeError(f"Failed to process pending inbox locally:\n{details.strip()}")

    def _inbox_row(self, inbox_id: int) -> dict | None:
        for row in self.test_client.get_records("Test_Inbox"):
            if int(row.get("id") or 0) == inbox_id:
                return row
        return None

    def _wait_processed(self, inbox_id: int, timeout_s: float) -> bool:
        end = time.time() + timeout_s
        while time.time() < end:
            row = self._inbox_row(inbox_id)
            if row:
                fields = row.get("fields", {})
                if bool(fields.get("processed", False)):
                    error = str(fields.get("error") or fields.get("error_text") or "").strip()
                    if error:
                        raise RuntimeError(f"Inbox row {inbox_id} failed: {error}")
                    return True
            time.sleep(0.2)
        return False

    def _ensure_processed(self, inbox_id: int) -> None:
        if self._processing_mode == "local":
            self._process_pending_once_local()
        elif self._processing_mode == "runtime":
            if not self._wait_processed(inbox_id, 8.0):
                self._process_pending_once_local()
                self._processing_mode = "local"
        else:
            if self._wait_processed(inbox_id, 4.5):
                self._processing_mode = "runtime"
            else:
                self._process_pending_once_local()
                self._processing_mode = "local"

        if not self._wait_processed(inbox_id, 15.0):
            raise RuntimeError(f"Inbox row {inbox_id} was not processed in time.")

    def _rows_for_actor_correlation(self, actor: str, inbox_id: int) -> list[dict]:
        actor_text = str(actor).strip()
        for _ in range(40):
            outbox = _rows_for_session(self.test_client.get_records("Test_Outbox"), self.session)
            rows = []
            for row in outbox:
                fields = row.get("fields", {})
                if str(fields.get("recipient_user_id", "")).strip() != actor_text:
                    continue
                if _correlation_inbox_id(row) != inbox_id:
                    continue
                rows.append(row)
            rows.sort(key=lambda item: int(item.get("id") or 0))
            if rows:
                return rows
            time.sleep(0.1)
        return []


def _open_manage_production(driver: TestSessionDriver, test_client: GristClient, session: str, actor: str) -> list[dict]:
    _, rows = driver.send_text(actor, "/start")
    _assert_no_auth_error_rows(test_client, session, actor, rows)
    _, rows = driver.send_text(actor, "Manage Production")
    _assert_no_auth_error_rows(test_client, session, actor, rows)
    return rows


def _open_my_ms_jobs_batch_view(
    driver: TestSessionDriver,
    test_client: GristClient,
    session: str,
    actor: str,
    batch_no: str,
) -> list[dict]:
    for attempt in range(4):
        manage_rows = _open_manage_production(driver, test_client, session, actor)
        labels = _button_labels_from_rows(manage_rows)
        my_jobs_label = _pick_label(
            labels,
            exact=("My MS Jobs",),
            token_groups=(("my", "ms", "jobs"),),
        ) or "My MS Jobs"
        _, rows = driver.send_text(actor, my_jobs_label)
        _assert_no_auth_error_rows(test_client, session, actor, rows)

        filter_labels = _button_labels_from_rows(rows)
        batch_filter_label = _pick_label(
            filter_labels,
            exact=(_MY_MS_FILTER_BATCH,),
            token_groups=(("view", "batch", "no"), ("batch", "no")),
        ) or _MY_MS_FILTER_BATCH
        _, rows = driver.send_text(actor, batch_filter_label)
        _assert_no_auth_error_rows(test_client, session, actor, rows)

        selected = False
        # Batch selector is paginated; pick the target batch.
        for _ in range(30):
            selector_text = ""
            for candidate in _message_texts(rows):
                if _find_option_number_for_batch(candidate, batch_no) is not None:
                    selector_text = candidate
                    break
                if "select batch no:" in candidate.casefold():
                    selector_text = candidate
            if not selector_text:
                selector_text = _latest_message_text(rows)
            if "no batch entries available for your ms jobs".casefold() in selector_text.casefold():
                break
            pick_number = _find_option_number_for_batch(selector_text, batch_no)
            if isinstance(pick_number, int):
                _, rows = driver.send_text(actor, str(pick_number))
                _assert_no_auth_error_rows(test_client, session, actor, rows)
                selected = True
                break
            next_label = _pick_label(
                _button_labels_from_rows(rows),
                token_groups=(("next",),),
            )
            if not next_label:
                break
            _, rows = driver.send_text(actor, next_label)
            _assert_no_auth_error_rows(test_client, session, actor, rows)

        if not selected:
            if attempt < 3:
                time.sleep(1.0)
                continue
            raise RuntimeError(f"Batch {batch_no} not found in 'View By Batch No' selector for actor {actor}.")

        # Expected human path: selecting the batch should show Batch Overview directly.
        overview_text = _extract_batch_overview_text(rows)
        if "batch overview" in overview_text.casefold() and "select flow number" in overview_text.casefold():
            return rows

        # If UI lands in list view, jump back to overview via quick action.
        if _rows_contain_batch_list_view(rows):
            _, rows = driver.send_text(actor, "B1")
            _assert_no_auth_error_rows(test_client, session, actor, rows)
            overview_text = _extract_batch_overview_text(rows)
            if "batch overview" in overview_text.casefold() and "select flow number" in overview_text.casefold():
                return rows

        if attempt < 3:
            time.sleep(1.0)
            continue
        raise RuntimeError(
            f"Unable to open Batch Overview after selecting batch {batch_no} for actor {actor}.\n"
            f"Latest:\n{_latest_message_text(rows)}"
        )

    raise RuntimeError(f"Unable to open Batch Overview for batch {batch_no} and actor {actor}.")


def _clear_schedule_prompt_if_needed(
    driver: TestSessionDriver,
    test_client: GristClient,
    session: str,
    actor: str,
    schedule_date_text: str,
) -> None:
    for _ in range(4):
        state = _current_menu_state(test_client, session, actor)
        if state != _SCHEDULE_STATE:
            return
        _, rows = driver.send_text(actor, schedule_date_text)
        _assert_no_auth_error_rows(test_client, session, actor, rows)


def _evaluate_ms_action_rows(rows: list[dict]) -> str:
    text = _all_message_text(rows).casefold()
    if _contains_any(
        text,
        (
            "current stage marked done",
            "stage handover confirmed",
            "marked ",
            "scheduled ",
            "schedule selected batch",
            "batch scheduled",
            "date scheduled",
        ),
    ):
        return "success"

    if _contains_any(
        text,
        (
            "this row is not waiting for handoff confirmation",
            "only the next-stage supervisor can confirm this handover",
            "you are not authorized for this stage",
            "no selected row found",
        ),
    ):
        return "retry"

    if _contains_any(
        text,
        (
            "could not",
            "ms row not found",
            "you do not have access",
            "unsupported action",
        ),
    ):
        return "fatal"
    return "unknown"


def _perform_ms_action_via_menus(
    driver: TestSessionDriver,
    test_client: GristClient,
    session: str,
    actor: str,
    desired_action: str,
    batch_no: str,
    preferred_flow_number: int | None,
    schedule_date_text: str,
) -> str:
    # Human path: Batch Overview -> flow number -> action button.
    overview_rows = _open_my_ms_jobs_batch_view(driver, test_client, session, actor, batch_no)
    overview_text = _extract_batch_overview_text(overview_rows)
    flow_candidates = _flow_candidates_from_overview(overview_text, desired_action)
    if isinstance(preferred_flow_number, int) and preferred_flow_number > 0:
        flow_candidates = [preferred_flow_number] + [num for num in flow_candidates if num != preferred_flow_number]

    if desired_action == "confirm":
        button_candidates = [
            ("confirm", ("Confirm Stage Handover",), (("confirm", "stage", "handover"), ("confirm", "handover"))),
            ("done", ("Current Stage Done",), (("current", "stage", "done"), ("stage", "done"))),
        ]
    else:
        button_candidates = [
            ("done", ("Current Stage Done",), (("current", "stage", "done"), ("stage", "done"))),
            ("confirm", ("Confirm Stage Handover",), (("confirm", "stage", "handover"), ("confirm", "handover"))),
        ]

    for flow_no in flow_candidates:
        _, flow_rows = driver.send_text(actor, str(flow_no))
        _assert_no_auth_error_rows(test_client, session, actor, flow_rows)

        for resolved_action, exact_labels, token_groups in button_candidates:
            _, callback_data = _pick_callback_from_rows(
                flow_rows,
                exact=exact_labels,
                token_groups=token_groups,
            )
            if not callback_data:
                continue
            _, action_rows = driver.send_callback(actor, callback_data)
            _clear_schedule_prompt_if_needed(driver, test_client, session, actor, schedule_date_text)
            outcome = _evaluate_ms_action_rows(action_rows)
            if outcome == "success":
                return resolved_action
            if outcome == "fatal":
                text = _all_message_text(action_rows)
                raise RuntimeError(
                    f"MS action failed for actor {actor} using flow {flow_no} callback '{callback_data}'.\n{text}"
                )

    details = _latest_text_for_actor(test_client, session, actor)
    raise RuntimeError(
        f"Could not complete desired MS action for actor {actor}. "
        f"Desired={desired_action}, flow_candidates={flow_candidates}, latest:\n{details}"
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run full TEST-mode e2e flow for dual-role owner using menu-driven interactions only: "
            "create batch, approve, progress all MS stages with handoffs, and validate notifications."
        )
    )
    parser.add_argument("--session", default="sim-e2e-dual-role")
    parser.add_argument("--refresh-session", action="store_true", help="Append unix timestamp to session id.")
    parser.add_argument("--creator-telegram", default="", help="Batch creator actor. Defaults to owner actor.")
    parser.add_argument("--owner-telegram", default="8492411029")
    parser.add_argument("--owner-user-id", default="U02")
    parser.add_argument("--owner-name", default="Chetan Patel")
    parser.add_argument("--manager-telegram", default="900000004")
    parser.add_argument("--machine-telegram", default="900000006")
    parser.add_argument("--model-index", type=int, default=1)
    parser.add_argument("--qty", type=int, default=32)
    parser.add_argument("--batch-type", default="New Complete Batch (M-C-S)")
    parser.add_argument("--notifiers", default="0")
    parser.add_argument("--schedule-date", default="Today")
    parser.add_argument("--render", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    session = str(args.session).strip() or "sim-e2e-dual-role"
    if args.refresh_session:
        session = f"{session}-{int(time.time())}"

    test_client = _build_test_client()
    if not args.refresh_session:
        existing = _session_activity_counts(test_client, session)
        existing_total = sum(existing.values())
        if existing_total > 0:
            old_session = session
            session = f"{session}-{int(time.time())}"
            print(
                f"Session '{old_session}' has existing runtime rows {existing}. "
                f"Auto-switching to fresh session '{session}'."
            )
    driver = TestSessionDriver(test_client, session)
    pulse_client = _build_pulse_client()
    costing_client = _build_costing_client()
    before_max_batch_id = _max_batch_id(costing_client)

    by_user_id, by_name = _build_actor_maps(pulse_client)
    role_members = _role_member_telegrams(pulse_client)
    owner_actor = str(args.owner_telegram or "").strip()
    if not owner_actor:
        owner_actor = by_user_id.get(str(args.owner_user_id or "").strip(), "")
    if not owner_actor and args.owner_name:
        owner_actor = by_name.get(str(args.owner_name).strip().casefold(), "")
    manager_actor = str(args.manager_telegram or "").strip()
    machine_actor = str(args.machine_telegram or "").strip()
    creator_actor = str(args.creator_telegram or "").strip() or owner_actor
    if not owner_actor or not manager_actor or not machine_actor:
        raise ValueError("Could not resolve owner/manager/machine actor Telegram IDs.")

    print(f"Session: {session}")
    print(f"Creator actor: {creator_actor}")
    print(f"Owner actor (dual-role): {owner_actor}")
    print(f"Manager actor: {manager_actor}")
    print(f"Machine-stage actor: {machine_actor}")

    # 1) Create batch from creator via menu buttons/text flow only.
    manage_rows = _open_manage_production(driver, test_client, session, creator_actor)
    create_label = _pick_label(
        _button_labels_from_rows(manage_rows),
        exact=("New Production Batch",),
        token_groups=(("new", "production", "batch"), ("new", "batch")),
    ) or "New Production Batch"
    _, rows = driver.send_text(creator_actor, create_label)
    _assert_no_auth_error_rows(test_client, session, creator_actor, rows)
    _, rows = driver.send_text(creator_actor, "By Product Model")
    _, rows = driver.send_text(creator_actor, str(args.model_index))
    _, rows = driver.send_text(creator_actor, str(args.qty))
    _, rows = driver.send_text(creator_actor, str(args.batch_type))
    _, rows = driver.send_text(creator_actor, "Yes")

    owner_user_id = str(args.owner_user_id or "").strip().casefold()
    owner_name = str(args.owner_name or "").strip().casefold()
    for _ in range(30):
        latest_owner_text = _latest_message_text(rows) or _latest_text_for_actor(test_client, session, creator_actor)
        if "Batch created:" in latest_owner_text:
            break
        if "Select Batch Owner" in latest_owner_text:
            options = _parse_numbered_options(latest_owner_text)
            picked = None
            for idx, label in options.items():
                label_norm = label.casefold()
                if owner_user_id and owner_user_id in label_norm:
                    picked = idx
                    break
                if owner_name and owner_name in label_norm:
                    picked = idx
                    break
            if picked is None:
                raise RuntimeError(f"Owner selection prompt shown, but owner not found in options: {options}")
            _, rows = driver.send_text(creator_actor, str(picked))
            continue
        if "Select Batch Notifiers" in latest_owner_text:
            _, rows = driver.send_text(creator_actor, str(args.notifiers))
            continue
        if "Select Yes or No" in latest_owner_text:
            yes_label = (
                _pick_label(
                    _button_labels_from_rows(rows),
                    exact=("Yes",),
                    token_groups=(("yes",),),
                )
                or "Yes"
            )
            _, rows = driver.send_text(creator_actor, yes_label)
            continue
        if "Select Batch Type" in latest_owner_text:
            _, rows = driver.send_text(creator_actor, str(args.batch_type))
            continue
        if "Confirm Batch:" in latest_owner_text:
            _, rows = driver.send_text(creator_actor, "Yes")
            continue
        time.sleep(0.2)
        latest_row = _latest_outbox_for_actor(test_client, session, creator_actor)
        if latest_row:
            rows = [latest_row]

    _assert_no_auth_error_rows(test_client, session, creator_actor, rows)

    batch_no = _find_batch_created_no_from_outbox(test_client, session, creator_actor)
    batch_id = (
        _find_batch_id_by_no(costing_client, batch_no, min_id=before_max_batch_id)
        if batch_no
        else None
    )
    if batch_id is None:
        max_now = _max_batch_id(costing_client)
        if max_now > before_max_batch_id:
            batch_id = max_now
    if not isinstance(batch_id, int):
        creator_tail = _recent_actor_outbox_summary(test_client, session, creator_actor, limit=10)
        raise RuntimeError(
            "Failed to resolve created batch id from outbox/ProductBatchMaster.\n"
            f"Recent creator outbox rows:\n{creator_tail}"
        )
    print(f"Created batch id={batch_id} batch_no={batch_no or '-'}")

    # 2) Approve from manager using Pending Approvals menu flow.
    manage_rows = _open_manage_production(driver, test_client, session, manager_actor)
    approval_label = _pick_label(
        _button_labels_from_rows(manage_rows),
        exact=("Pending Approvals",),
        token_groups=(("pending", "approval"), ("approval",)),
    )
    if approval_label:
        _, rows = driver.send_text(manager_actor, approval_label)
        _assert_no_auth_error_rows(test_client, session, manager_actor, rows)

        for _ in range(30):
            page_text = _latest_message_text(rows)
            pick_number = _find_option_number_for_batch(page_text, batch_no)
            if isinstance(pick_number, int):
                _, rows = driver.send_text(manager_actor, str(pick_number))
                _assert_no_auth_error_rows(test_client, session, manager_actor, rows)
                break
            next_label = _pick_label(
                _button_labels_from_rows(rows),
                token_groups=(("next",),),
            )
            if not next_label:
                raise RuntimeError(f"Batch {batch_no} not found in pending approvals list.\n{page_text}")
            _, rows = driver.send_text(manager_actor, next_label)
            _assert_no_auth_error_rows(test_client, session, manager_actor, rows)
        else:
            raise RuntimeError(f"Unable to select batch {batch_no} in pending approvals.")

        yes_label = _pick_label(
            _button_labels_from_rows(rows),
            exact=("Yes",),
            token_groups=(("yes",),),
        ) or "Yes"
        _, rows = driver.send_text(manager_actor, yes_label)
        _assert_no_auth_error_rows(test_client, session, manager_actor, rows)
        print("Approval completed by manager (menu-driven text path).")
    else:
        manager_rows = _rows_for_session(test_client.get_records("Test_Outbox"), session)
        manager_rows = [
            row
            for row in manager_rows
            if str(row.get("fields", {}).get("recipient_user_id", "")).strip() == str(manager_actor).strip()
        ]
        manager_rows.sort(key=lambda item: int(item.get("id") or 0))
        open_rows: list[dict] = []
        for row in reversed(manager_rows):
            message_text = str(row.get("fields", {}).get("message_text") or "")
            if f"Batch created: {batch_no}" not in message_text:
                continue
            _, open_callback = _pick_callback_from_rows(
                [row],
                token_groups=(("approve",),),
            )
            if not open_callback:
                continue
            _, open_rows = driver.send_callback(manager_actor, open_callback)
            break
        if not open_rows:
            raise RuntimeError(
                "Could not find manager approval inline button for created batch notification."
            )
        _assert_no_auth_error_rows(test_client, session, manager_actor, open_rows)
        _, approve_callback = _pick_callback_from_rows(
            open_rows,
            exact=("Yes",),
            token_groups=(("yes",), ("approve",)),
        )
        if not approve_callback:
            raise RuntimeError(f"Approve callback button not found in manager approval card.\n{_all_message_text(open_rows)}")
        _, rows = driver.send_callback(manager_actor, approve_callback)
        _assert_no_auth_error_rows(test_client, session, manager_actor, rows)
        print("Approval completed by manager (inline button path).")

    # 3) Progress all rows until complete with role-based actors via My MS Jobs menus.
    stage_role_map = _build_stage_role_map(costing_client)
    action_log: list[tuple[str, str, str]] = []
    max_steps = 500

    for _ in range(max_steps):
        rows = _fetch_batch_rows(costing_client, batch_id)
        incomplete = []
        for row in rows:
            fields = row.get("fields", {})
            status = str(fields.get("current_status") or fields.get("status") or "").strip()
            if _is_complete_status(status):
                continue
            incomplete.append(row)
        if not incomplete:
            break

        pending = []
        for row in incomplete:
            fields = row.get("fields", {})
            status = str(fields.get("current_status") or fields.get("status") or "").strip()
            if status == _MS_PENDING_CONFIRMATION:
                pending.append(row)

        if pending:
            target = pending[0]
            fields = target.get("fields", {})
            process_seq = fields.get("process_seq")
            next_stage = str(fields.get("next_stage_name") or "").strip()
            role_name = _resolve_stage_role(stage_role_map, process_seq, next_stage)
            desired_action = "confirm"
        else:
            target = incomplete[0]
            fields = target.get("fields", {})
            process_seq = fields.get("process_seq")
            current_stage = str(fields.get("current_stage_name") or "").strip()
            role_name = _resolve_stage_role(stage_role_map, process_seq, current_stage)
            desired_action = "done"

        row_id = int(target.get("id") or 0)
        preferred_flow_number = _resolve_flow_number_for_row(rows, row_id)

        role_norm = _norm_role(role_name)
        if "production supervisor" in role_norm:
            actor = owner_actor
        elif "cutting supervisor" in role_norm:
            actor = owner_actor
        elif "machine shop supervisor" in role_norm:
            actor = machine_actor
        else:
            candidates = role_members.get(role_norm, [])
            if not candidates:
                raise RuntimeError(f"No actor resolved for role '{role_name}' while processing row {target.get('id')}.")
            actor = candidates[0]

        resolved_action = _perform_ms_action_via_menus(
            driver,
            test_client,
            session,
            actor,
            desired_action,
            batch_no,
            preferred_flow_number,
            str(args.schedule_date),
        )
        action_log.append((actor, desired_action, resolved_action))
        time.sleep(0.1)

    final_rows = _fetch_batch_rows(costing_client, batch_id)
    unfinished = []
    for row in final_rows:
        fields = row.get("fields", {})
        status = str(fields.get("current_status") or fields.get("status") or "").strip()
        if not _is_complete_status(status):
            unfinished.append((row.get("id"), status, fields.get("current_stage_name"), fields.get("next_stage_name")))
    if unfinished:
        raise RuntimeError(f"Not all rows completed. Remaining: {unfinished}")
    print(f"All flows completed. Total row actions={len(action_log)}")

    # 4) Verify key notification expectations.
    outbox = _rows_for_session(test_client.get_records("Test_Outbox"), session)
    notifications = [row for row in outbox if str(row.get("fields", {}).get("event_type") or "") == "notification"]

    def _has_notification(actor: str, contains_text: str) -> bool:
        for row in notifications:
            fields = row.get("fields", {})
            if str(fields.get("recipient_user_id", "")).strip() != str(actor).strip():
                continue
            msg = str(fields.get("message_text") or "")
            if contains_text in msg:
                return True
        return False

    if not _has_notification(owner_actor, "Batch approved:"):
        raise RuntimeError("Owner did not receive expected 'Batch approved' notification.")
    if not _has_notification(owner_actor, "MS stage completed"):
        raise RuntimeError("Owner did not receive expected MS completion notifications.")
    if not (
        _has_notification(machine_actor, "Stage Confirmation Required")
        or _has_notification(machine_actor, "MS Stage Task")
    ):
        raise RuntimeError("Next-stage supervisor did not receive expected handoff/task notification.")

    machine_confirms = [item for item in action_log if item[0] == machine_actor and item[1] == "confirm"]
    owner_actions = [item for item in action_log if item[0] == owner_actor]
    if not machine_confirms:
        raise RuntimeError("No handoff confirmation was performed by next-stage supervisor.")
    if not owner_actions:
        raise RuntimeError("Owner did not perform stage actions.")

    master = None
    for row in costing_client.get_records("ProductBatchMaster"):
        if row.get("id") == batch_id:
            master = row
            break
    master_status = str((master or {}).get("fields", {}).get("overall_status") or "")
    print(f"Master overall_status={master_status!r}")
    print(f"Notifications in session={len(notifications)}")
    print(f"Machine handoff confirmations={len(machine_confirms)}")
    print(f"Owner stage actions={len(owner_actions)}")

    if args.render:
        _run_render()
        print("Rendered preview: artifacts/test_preview/outbox_preview.html")

    print("E2E dual-role owner flow (menu-driven) completed successfully.")


if __name__ == "__main__":
    main()
