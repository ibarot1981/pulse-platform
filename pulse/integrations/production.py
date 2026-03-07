from __future__ import annotations

from datetime import datetime, timedelta
from io import BytesIO
import tempfile
import re
from datetime import timezone
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
import requests

from pulse.config import NOTIFICATION_DATETIME_FORMAT, NOTIFICATION_TIMEZONE
from pulse.data.production_repo import ProductionRepo
from pulse.menu.submenu import BACK_LABEL, MAIN_MENU_LABEL, MAIN_STATE, set_main_menu_state
from pulse.notifications.dispatcher import dispatch_event
from pulse.settings import settings
from pulse.utils.pdf_export import write_grouped_ms_cutlist_pdf

SELECTING_BATCH_MODE_STATE = "selecting_batch_mode"
SELECTING_PRODUCT_MODEL_STATE = "selecting_product_model"
SELECTING_PRODUCT_PARTS_STATE = "selecting_product_parts"
ENTERING_BATCH_QTY_STATE = "entering_batch_qty"
SELECTING_BATCH_TYPE_STATE = "selecting_batch_type"
CONFIRMING_BATCH_STATE = "confirming_batch"
AWAITING_APPROVAL_STATE = "awaiting_approval"
PENDING_APPROVALS_SELECTION_STATE = "pending_approvals_selection"
PENDING_APPROVALS_CONFIRM_STATE = "pending_approvals_confirm"
MY_MS_JOBS_SELECTION_STATE = "my_ms_jobs_selection"
MY_MS_JOBS_FILTER_STATE = "my_ms_jobs_filter"
MY_MS_JOBS_NEXT_STAGE_SELECTION_STATE = "my_ms_jobs_next_stage_selection"
MY_MS_JOBS_CREATED_BY_SELECTION_STATE = "my_ms_jobs_created_by_selection"
MY_MS_JOBS_BATCH_SELECTION_STATE = "my_ms_jobs_batch_selection"
VIEW_BATCH_SELECTION_STATE = "view_batch_selection"
MY_MS_JOBS_ACTION_STATE = "my_ms_jobs_action"
MY_MS_JOBS_CONFIRM_STATE = "my_ms_jobs_confirm"
MY_MS_JOBS_BULK_ACTION_STATE = "my_ms_jobs_bulk_action"
MY_MS_JOBS_BULK_REMARKS_STATE = "my_ms_jobs_bulk_remarks"
MY_MS_JOBS_HANDOFF_REJECT_REMARKS_STATE = "my_ms_jobs_handoff_reject_remarks"
MY_MS_BATCH_ACTION_STATE = "my_ms_batch_action"
MY_MS_BATCH_REMARKS_STATE = "my_ms_batch_remarks"
MY_MS_SCHEDULE_SELECTION_STATE = "my_ms_schedule_selection"
MY_MS_SCHEDULE_CONFIRM_STATE = "my_ms_schedule_confirm"
AWAITING_SCHEDULE_DATE_STATE = "awaiting_schedule_date"
MY_MS_JOBS_REMARKS_STATE = "my_ms_jobs_remarks"

ACTION_NEW_PRODUCTION_BATCH = "NEW_PRODUCTION_BATCH"
ACTION_PENDING_APPROVALS = "PRODUCTION_PENDING_APPROVALS"
ACTION_MY_MS_JOBS = "MY_MS_JOBS"
ACTION_MY_MS_SCHEDULE = "MY_MS_SCHEDULE"
ACTION_VIEW_BATCH = "VIEW_BATCH"

_PAGE_PREV = "Prev"
_PAGE_NEXT = "Next"
_MODE_BY_MODEL = "By Product Model"
_MODE_BY_PART = "By Product Part"
_YES = "Yes"
_NO = "No"
_REJECT = "Reject"
_TODAY = "Today"
_TOMORROW = "Tomorrow"
_MS_VIEW_ACTION_REQUIRED = "My Pending Actions"
_MS_VIEW_ALL = "View All"
_MS_VIEW_BY_NEXT_STAGE = "View By Next Stage"
_MS_VIEW_BY_CREATED_BY = "View Created By"
_MS_VIEW_BY_BATCH_NO = "View By Batch No"
_MS_ACTION_DONE = "Mark as Done"
_MS_ACTION_REMARKS = "Add Remarks"
_MS_ACTION_VIEW_LIST = "View MS List"
_MS_ACTION_HOLD = "Mark as Hold"
_MS_ACTION_CONFIRM_HANDOFF = "Accept Handoff"
_MS_ACTION_REJECT_HANDOFF = "Reject Handoff"
_MS_BULK_ACTION_DONE = "Mark Selected Done"
_MS_BULK_ACTION_HOLD = "Mark Selected Hold"
_MS_BULK_ACTION_REMARKS = "Add Selected Remarks"
_MS_BATCH_ACTION_DONE = "Done Current Stage (Batch)"
_MS_BATCH_ACTION_HOLD = "Hold Batch Rows"
_MS_BATCH_ACTION_REMARKS = "Add Batch Remarks"
_MS_BATCH_ACTION_SUMMARY = "View Batch Summary"
_MS_BATCH_ACTION_VIEW = "View This Batch"
_MS_BATCH_ACTION_SCHEDULE = "Schedule Batch"

_TYPE_COMPLETE = "New Complete Batch (M-C-S)"
_TYPE_MS = "MS Only"
_TYPE_CNC = "CNC Only"
_TYPE_STORE = "Store Only"
_APPROVAL_CB_PREFIX = "prodappr"
_SUPV_CB_PREFIX = "prodsv"
_MS_BATCH_CB_PREFIX = "msbatch"
_APPROVER_ROLE_IDS = {"R01", "R02"}
_APPROVER_ROLE_NAMES = {"Production_Manager", "System_Admin"}
_MS_PENDING_CONFIRMATION = "Done - Pending Confirmation"
_MS_HANDOFF_PENDING_ICON = "🤝"


def _normalize_ref(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _process_code(include_ms: bool, include_cnc: bool, include_store: bool) -> str:
    parts = []
    if include_ms:
        parts.append("M")
    if include_cnc:
        parts.append("C")
    if include_store:
        parts.append("S")
    return "".join(parts) or "NA"


def _get_flow(context):
    return context.user_data.setdefault("production_batch_flow", {})


def _clear_flow(context):
    context.user_data.pop("production_batch_flow", None)
    context.user_data.pop("production_paging", None)
    context.user_data.pop("pending_approvals_selection", None)
    context.user_data.pop("pending_approvals_confirm", None)
    context.user_data.pop("my_ms_jobs_selection", None)
    context.user_data.pop("my_ms_jobs_confirm", None)
    context.user_data.pop("my_ms_jobs_bulk_action", None)
    context.user_data.pop("my_ms_jobs_bulk_remarks", None)
    context.user_data.pop("my_ms_batch_action", None)
    context.user_data.pop("my_ms_batch_remarks", None)
    context.user_data.pop("my_ms_jobs_action", None)
    context.user_data.pop("my_ms_schedule_selection", None)
    context.user_data.pop("my_ms_schedule_confirm", None)
    context.user_data.pop("schedule_date_context", None)
    context.user_data.pop("my_ms_jobs_filter", None)
    context.user_data.pop("my_ms_jobs_filter_value", None)
    context.user_data.pop("my_ms_jobs_creator_by_batch", None)
    context.user_data.pop("my_ms_jobs_batch_no_by_id", None)
    context.user_data.pop("my_ms_jobs_action_records", None)
    context.user_data.pop("my_ms_jobs_remarks", None)
    context.user_data.pop("my_ms_jobs_next_stage_selection", None)
    context.user_data.pop("my_ms_jobs_created_by_selection", None)
    context.user_data.pop("my_ms_jobs_batch_selection", None)
    context.user_data.pop("view_batch_selection", None)
    context.user_data.pop("my_ms_jobs_handoff_reject_remarks", None)


def _build_keyboard(rows: list[list[str]]):
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _format_menu_text(text: str) -> str:
    raw_lines = str(text or "").splitlines()
    expanded_lines: list[str] = []
    for raw in raw_lines:
        if "|" in raw:
            parts = [part.strip() for part in raw.split("|")]
            expanded_lines.extend([part for part in parts if part])
        else:
            expanded_lines.append(raw.rstrip())

    emoji_prefixes = [
        ("My MS Jobs", "\U0001F535 My MS Jobs"),
        ("View:", "\u26AA View:"),
        ("Choose entries using:", "\u26AA Choose entries using:"),
        ("Quick actions:", "\U0001F7E0 Quick actions:"),
        ("Optional:", "\u26AA Optional:"),
        ("Batch No:", "\U0001F535 Batch No:"),
        ("Current Stage:", "\U0001F7E0 Current Stage:"),
        ("Next Stage:", "\U0001F7E2 Next Stage:"),
        ("Created By:", "\u26AA Created By:"),
        ("Qty:", "\u26AA Qty:"),
        ("Status:", "\U0001F7E2 Status:"),
        ("Remarks:", "\u26AA Remarks:"),
        ("Batch Summary", "\U0001F535 Batch Summary"),
        ("Batch Actions", "\U0001F7E0 Batch Actions"),
        ("Choose one action:", "\u26AA Choose one action:"),
        ("Select ", "\u26AA Select "),
        ("Schedule ", "\U0001F7E0 Schedule "),
        ("No ", "\U0001F534 No "),
    ]

    styled_lines: list[str] = []
    for line in expanded_lines:
        stripped = line.strip()
        if not stripped:
            styled_lines.append("")
            continue
        updated = stripped
        for prefix, replacement in emoji_prefixes:
            if updated.startswith(prefix):
                updated = replacement + updated[len(prefix):]
                break
        styled_lines.append(updated)

    final_lines: list[str] = []
    seen_entry = False
    for line in styled_lines:
        is_entry = bool(re.match(r"^\d+\.\s", line))
        if is_entry:
            if seen_entry and final_lines and final_lines[-1] != "":
                final_lines.append("")
            seen_entry = True
        final_lines.append(line)

    while final_lines and not final_lines[-1]:
        final_lines.pop()
    return "\n".join(final_lines)


async def _reply(update, text: str, rows: list[list[str]] | None = None):
    if rows:
        has_main_menu = any(row and row[0] == MAIN_MENU_LABEL for row in rows)
        if not has_main_menu:
            rows = rows + [[MAIN_MENU_LABEL]]
    markup = _build_keyboard(rows) if rows else None
    await update.effective_message.reply_text(_format_menu_text(text), reply_markup=markup)


def _paginate(records: list, page: int, page_size: int):
    start = page * page_size
    end = start + page_size
    return records[start:end], start, end


async def _show_model_page(update, context):
    paging = context.user_data.get("production_paging", {})
    records = paging.get("records", [])
    page = paging.get("page", 0)
    page_size = paging.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_records, _, end = _paginate(records, page, page_size)

    lines = ["Select Product Model:\n"]
    for idx, model_code in enumerate(page_records, start=1):
        lines.append(f"{idx}. {model_code}")

    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(records):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


async def _show_parts_page(update, context):
    paging = context.user_data.get("production_paging", {})
    records = paging.get("records", [])
    page = paging.get("page", 0)
    page_size = paging.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_records, _, end = _paginate(records, page, page_size)

    lines = ["Select Product Parts (comma-separated numbers):\n"]
    for idx, part in enumerate(page_records, start=1):
        lines.append(f"{idx}. {part['part_name']}")

    flow = _get_flow(context)
    chosen_names = flow.get("selected_part_names", [])
    if chosen_names:
        lines.append("\nSelected so far:")
        lines.append(", ".join(chosen_names))

    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(records):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


def _get_limits_from_config(repo: ProductionRepo) -> tuple[int, int]:
    config = repo.get_production_config()
    min_qty = config.get("min_batch_qty")
    max_qty = config.get("max_batch_qty")

    try:
        min_value = int(min_qty)
    except (TypeError, ValueError):
        min_value = 1
    try:
        max_value = int(max_qty)
    except (TypeError, ValueError):
        max_value = 999999
    return min_value, max_value


def _batch_summary_text(flow: dict) -> str:
    lines = ["Confirm Batch:\n"]
    lines.append(f"Product Model: {flow.get('model_code', '')}")
    if flow.get("batch_mode") == _MODE_BY_PART:
        lines.append(f"Selected Parts: {', '.join(flow.get('selected_part_names', []))}")
    lines.append(f"Batch Qty: {flow.get('batch_qty')}")
    lines.append(f"Batch Type: {flow.get('batch_type', '')}")
    return "\n".join(lines)


async def start_new_production_batch(update, context) -> None:
    _clear_flow(context)
    _get_flow(context)
    context.user_data["menu_state"] = SELECTING_BATCH_MODE_STATE
    await _reply(
        update,
        "New Production Batch:\nChoose batch mode.",
        [[_MODE_BY_MODEL], [_MODE_BY_PART], [BACK_LABEL]],
    )


async def _show_batch_type_prompt(update, context) -> None:
    context.user_data["menu_state"] = SELECTING_BATCH_TYPE_STATE
    await _reply(
        update,
        "Select Batch Type:",
        [[_TYPE_COMPLETE], [_TYPE_MS], [_TYPE_CNC], [_TYPE_STORE], [BACK_LABEL]],
    )


def generate_batch_number(
    repo: ProductionRepo,
    model_code: str,
    include_ms: bool,
    include_cnc: bool,
    include_store: bool,
) -> str:
    month_key = datetime.utcnow().strftime("%b%y").upper()
    process = _process_code(include_ms, include_cnc, include_store)
    existing = repo.get_existing_batch_numbers()
    seq = 0
    for number in existing:
        parts = number.split("-")
        if len(parts) < 4:
            continue
        if parts[0] != month_key:
            continue
        try:
            value = int(parts[-1])
        except ValueError:
            continue
        if value > seq:
            seq = value
    return f"{month_key}-{model_code}-{process}-{seq + 1:03d}"


def _resolve_type_flags(batch_type_text: str) -> tuple[str, bool, bool, bool]:
    if batch_type_text == _TYPE_COMPLETE:
        return "M-C-S", True, True, True
    if batch_type_text == _TYPE_MS:
        return "MS", True, False, False
    if batch_type_text == _TYPE_CNC:
        return "CNC", False, True, False
    if batch_type_text == _TYPE_STORE:
        return "STORE", False, False, True
    raise ValueError("Invalid batch type.")


def _to_int_list_csv(values: list[int]) -> str:
    return ",".join(str(value) for value in values)


def _from_int_list_csv(value: str) -> list[int]:
    if not value:
        return []
    result = []
    for token in str(value).split(","):
        token = token.strip()
        if not token:
            continue
        try:
            result.append(int(token))
        except ValueError:
            continue
    return result


def _normalize_process_seq(fields: dict):
    value = fields.get("process_seq")
    if value in (None, "", 0):
        value = fields.get("Process_Seq")
    return _normalize_ref(value)


def _resolve_ms_row_part_text(repo: ProductionRepo, row_or_fields: dict) -> str:
    fields = row_or_fields.get("fields", row_or_fields)
    return repo.format_product_parts(fields.get("product_part"))


def _format_qty(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:g}"


def _normalize_menu_text(value: str) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _get_next_stage_name(stages: list[str], current_index: int) -> str:
    next_index = current_index + 1
    if 0 <= next_index < len(stages):
        return str(stages[next_index] or "")
    return ""


def _build_ms_stage_pending_message(
    batch_no: str,
    batch_by: str,
    part_name: str,
    current_stage: str,
    next_stage: str,
    qty: str,
    title: str = "New Batch Approved !!!",
) -> str:
    lines = [
        f"\U0001F7E2 {title}",
        f"\U0001F4E6 Batch No: {batch_no}",
        f"\U0001F464 Batch By: {batch_by or '-'}",
        f"\U0001F9E9 Product Part: {part_name}",
        f"\U0001F504 Current Stage: {current_stage or '-'}",
        f"\u23ED\uFE0F Next Stage: {next_stage or '-'}",
        f"\U0001F4CF Qty: {qty}",
        "",
        "Use the inline buttons below for actions.",
    ]
    return "\n".join(lines)


def _resolve_next_stage_for_row(repo: ProductionRepo, fields: dict) -> str:
    row = fields or {}
    next_stage = str(row.get("next_stage_name") or "").strip()
    if next_stage:
        return next_stage
    process_seq = _normalize_process_seq(row)
    stages = repo.get_process_stage_names(process_seq)
    if not stages:
        return ""
    stage_name = str(row.get("current_stage_name") or "").strip()
    try:
        current_index = int(row.get("current_stage_index") or 0)
    except (TypeError, ValueError):
        current_index = 0
    if not (0 <= current_index < len(stages)):
        try:
            current_index = stages.index(stage_name)
        except ValueError:
            current_index = 0
    return _get_next_stage_name(stages, current_index)


def _build_ms_batch_approval_summary_message(
    repo: ProductionRepo,
    batch_no: str,
    batch_by: str,
    approved_by: str,
    status: str,
    rows: list[dict],
) -> str:
    lines = [
        f"\U0001F7E2 New Batch Approved : {batch_no}",
        f"\U0001F464 Batch By: {batch_by}, \U0001F464 Approved By: {approved_by}",
        f"Status : {status}",
        "Stages:",
    ]
    if not rows:
        lines.append("-")
        return "\n".join(lines)

    for idx, row in enumerate(rows, start=1):
        fields = row.get("fields", row)
        current_stage = str(fields.get("current_stage_name") or "-")
        next_stage = _resolve_next_stage_for_row(repo, fields)
        lines.append(f"{idx}. {current_stage} to {next_stage or '-'}")
    return "\n".join(lines)


def _build_ms_batch_view_detail_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "View Batch Detail",
                    callback_data=_ms_batch_callback_data("vd", batch_id),
                )
            ]
        ]
    )

def _build_ms_job_entry_text(
    index: int,
    part_name: str,
    next_stage: str,
    qty: str,
    status: str,
    remarks: str,
    mode: str,
) -> str:
    status_text = status or "-"
    row = f"{index}. {part_name or '-'} | Qty: {qty} | Status: {status_text}"
    if mode == _MS_VIEW_ALL:
        row += f" | Next: {next_stage or '-'}"
    if remarks:
        row += f" | Remarks: {remarks}"
    return row


def _build_ms_jobs_group_header(
    mode: str,
    batch_no: str,
    current_stage: str,
    next_stage: str,
    created_by: str,
) -> list[str]:
    lines = [
        f"Batch No: {batch_no or '-'}",
        f"Current Stage: {current_stage or '-'}",
    ]
    if mode == _MS_VIEW_BY_NEXT_STAGE:
        lines.append(f"Next Stage: {next_stage or '-'}")
    if mode == _MS_VIEW_BY_CREATED_BY:
        lines.append(f"Created By: {created_by or '-'}")
    return lines


def _parse_iso_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        # Grist DateTime values can be numeric (Unix seconds).
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            return None
    raw_value = str(value).strip()
    if not raw_value:
        return None
    if raw_value.replace(".", "", 1).isdigit():
        try:
            return datetime.fromtimestamp(float(raw_value), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            return None
    try:
        parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        if parsed.tzinfo is None and "T" in raw_value:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw_value, fmt)
        except ValueError:
            continue
    return None


def _format_dt_short(value: datetime | None) -> str:
    if not value:
        return "-"
    try:
        return _format_notification_datetime(value).split(" ")[0]
    except Exception:
        return value.date().isoformat()


def _format_notification_datetime(value) -> str:
    parsed = _parse_iso_datetime(value)
    if not parsed:
        return "-"
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    target_tz = _resolve_notification_timezone()
    parsed = parsed.astimezone(target_tz)
    try:
        return parsed.strftime(NOTIFICATION_DATETIME_FORMAT)
    except Exception:
        return parsed.strftime("%d-%m-%Y %H:%M:%S %Z")


def _resolve_notification_timezone():
    tz_name = str(NOTIFICATION_TIMEZONE or "Asia/Calcutta").strip() or "Asia/Calcutta"
    for candidate in (tz_name, "Asia/Kolkata"):
        try:
            return ZoneInfo(candidate)
        except Exception:
            continue
    if tz_name in {"Asia/Calcutta", "Asia/Kolkata"}:
        return timezone(timedelta(hours=5, minutes=30), name="IST")
    return timezone.utc


def _elapsed_days_since(value: datetime | None) -> int | None:
    if not value:
        return None
    start = value
    now = datetime.utcnow()
    if start.tzinfo is not None:
        start = start.astimezone().replace(tzinfo=None)
    return max((now - start).days, 0)


def _build_stage_history_indexes(repo: ProductionRepo, batch_id: int) -> dict[int, list[dict]]:
    history_by_row: dict[int, list[dict]] = {}
    for rec in repo.costing_client.get_records("BatchStatusHistory"):
        fields = rec.get("fields", {})
        if str(fields.get("entity_type") or "").strip() != "MS":
            continue
        hist_batch = _normalize_ref(fields.get("batch_id"))
        if hist_batch != batch_id:
            continue
        row_id = _normalize_ref(fields.get("entity_id"))
        if not isinstance(row_id, int):
            continue
        history_by_row.setdefault(row_id, []).append(fields)
    for row_id in history_by_row:
        history_by_row[row_id].sort(key=lambda row: str(row.get("timestamp") or ""))
    return history_by_row


def _find_stage_start_from_history(stage_name: str, history_rows: list[dict]) -> datetime | None:
    stage_key = str(stage_name or "").strip().lower()
    if not stage_key:
        return None
    for hist in history_rows:
        new_status = str(hist.get("new_status") or "").strip().lower()
        if new_status in (f"{stage_key} pending", f"in {stage_key}"):
            return _parse_iso_datetime(hist.get("timestamp"))
    return None


def _find_stage_completed_from_history(stage_name: str, next_stage: str, history_rows: list[dict]) -> datetime | None:
    stage_key = str(stage_name or "").strip().lower()
    next_key = str(next_stage or "").strip().lower()
    next_tokens = []
    if next_key:
        next_tokens = [f"{next_key} pending", f"in {next_key}"]
    for hist in history_rows:
        new_status = str(hist.get("new_status") or "").strip().lower()
        if next_tokens and new_status in next_tokens:
            return _parse_iso_datetime(hist.get("timestamp"))
        if not next_key and new_status in ("cutting completed", "done", "completed"):
            return _parse_iso_datetime(hist.get("timestamp"))
    return None


def _build_ms_batch_summary_text(repo: ProductionRepo, batch_id: int, batch_no: str) -> str:
    rows = repo.list_ms_rows_for_batch(batch_id)
    if not rows:
        return f"Batch Summary\nBatch No: {batch_no or '-'}\nNo MS rows found for this batch."

    master = repo.get_master_by_id(batch_id) or {}
    master_fields = master.get("fields", {})
    batch_start = _parse_iso_datetime(master_fields.get("start_date"))
    batch_age_source = "start_date"
    if not batch_start:
        batch_start = _parse_iso_datetime(master_fields.get("created_date"))
        batch_age_source = "created_date"
    if not batch_start:
        ms_created_dates = [
            _parse_iso_datetime(row.get("fields", {}).get("created_at"))
            for row in rows
        ]
        ms_created_dates = [dt for dt in ms_created_dates if dt]
        if ms_created_dates:
            batch_start = min(ms_created_dates)
            batch_age_source = "ms_created_at(min)"
    batch_age_days = _elapsed_days_since(batch_start)
    history_by_row = _build_stage_history_indexes(repo, batch_id)
    if not batch_start:
        all_hist_times: list[datetime] = []
        for hist_rows in history_by_row.values():
            for hist in hist_rows:
                hist_time = _parse_iso_datetime(hist.get("timestamp"))
                if hist_time:
                    all_hist_times.append(hist_time)
        if all_hist_times:
            batch_start = min(all_hist_times)
            batch_age_days = _elapsed_days_since(batch_start)
            batch_age_source = "status_history(min)"
    if not batch_start:
        batch_age_source = "unavailable"

    total = len(rows)
    pending = 0
    pending_confirm = 0
    hold = 0
    completed = 0
    stage_counts: dict[str, int] = {}
    next_stage_counts: dict[str, int] = {}
    for row in rows:
        fields = row.get("fields", {})
        status = str(fields.get("current_status") or fields.get("status") or "").strip()
        if status == _MS_PENDING_CONFIRMATION:
            pending_confirm += 1
        elif "hold" in status.lower():
            hold += 1
        elif status in ("Cutting Completed", "Done", "Completed"):
            completed += 1
        elif _is_pending_ms_status(status):
            pending += 1
        stage_name = str(fields.get("current_stage_name") or "").strip()
        if stage_name:
            stage_counts[stage_name] = stage_counts.get(stage_name, 0) + 1
        next_stage = str(fields.get("next_stage_name") or "").strip()
        if next_stage:
            next_stage_counts[next_stage] = next_stage_counts.get(next_stage, 0) + 1

    stage_summary = ", ".join(
        f"{name}({count})" for name, count in sorted(stage_counts.items(), key=lambda item: (-item[1], item[0]))[:5]
    ) or "-"
    next_summary = ", ".join(
        f"{name}({count})" for name, count in sorted(next_stage_counts.items(), key=lambda item: (-item[1], item[0]))[:5]
    ) or "-"

    lines = [
        "Batch Summary",
        f"Batch No: {batch_no or '-'}",
        f"Batch Age Days: {batch_age_days if batch_age_days is not None else '-'}",
        f"Batch Age Start: {_format_dt_short(batch_start)} ({batch_age_source})",
        f"Total Rows: {total}",
        f"Pending: {pending} | Pending Confirm: {pending_confirm} | On Hold: {hold} | Completed: {completed}",
        f"Current Stages: {stage_summary}",
        f"Next Stages: {next_summary}",
        "",
        "Item Stage Timeline:",
    ]

    for idx, row in enumerate(rows, start=1):
        row_id = row.get("id")
        fields = row.get("fields", {})
        part_name = _resolve_ms_row_part_text(repo, fields)
        process_seq = _normalize_process_seq(fields)
        stages = repo.get_process_stage_names(process_seq)
        current_stage = str(fields.get("current_stage_name") or "")
        status = str(fields.get("current_status") or fields.get("status") or "")
        history_rows = history_by_row.get(int(row_id), []) if isinstance(row_id, int) else []

        current_stage_started = _find_stage_start_from_history(current_stage, history_rows)
        stage_age_source = "status_history"
        if not current_stage_started:
            current_stage_started = _parse_iso_datetime(fields.get("created_at")) or batch_start
            stage_age_source = "row_created_at_or_batch_start"
        if not current_stage_started:
            stage_age_source = "unavailable"
        current_stage_age = _elapsed_days_since(current_stage_started)

        lines.append(f"{idx}. {part_name or '-'}")
        lines.append(
            f"Current: {current_stage or '-'} ({(stages.index(current_stage) + 1) if current_stage in stages else '-'}"
            f"/{len(stages) if stages else '-'}) | Status: {status or '-'} | Stage Age Days: {current_stage_age if current_stage_age is not None else '-'}"
        )
        lines.append(f"Stage Age Start: {_format_dt_short(current_stage_started)} ({stage_age_source})")

        if stages:
            timeline_tokens: list[str] = []
            for stage_index, stage in enumerate(stages):
                next_stage = stages[stage_index + 1] if stage_index + 1 < len(stages) else ""
                completed_at = _find_stage_completed_from_history(stage, next_stage, history_rows)
                if completed_at:
                    timeline_tokens.append(f"{stage}({_format_dt_short(completed_at)})")
                elif stage == current_stage:
                    timeline_tokens.append(f"{stage}(Current)")
                else:
                    timeline_tokens.append(f"{stage}(-)")
            lines.append("Timeline: " + " | ".join(timeline_tokens))
        else:
            lines.append("Timeline: -")
        lines.append("")

    return "\n".join(lines)


def _is_ms_row_completed_status(status: str) -> bool:
    return str(status or "").strip() in ("Cutting Completed", "Done", "Completed")


def _process_seq_sort_key(process_seq_value) -> tuple[int, int | str]:
    normalized = _normalize_ref(process_seq_value)
    if isinstance(normalized, int):
        return (0, normalized)
    text = str(normalized or "").strip()
    if text.isdigit():
        return (0, int(text))
    return (1, text)


def _ordered_ms_rows_for_batch(repo: ProductionRepo, batch_id: int) -> list[dict]:
    rows = repo.list_ms_rows_for_batch(batch_id)
    rows.sort(
        key=lambda row: (
            _process_seq_sort_key(_normalize_process_seq(row.get("fields", {}))),
            int(row.get("id") or 0),
        )
    )
    return rows


def _resolve_stage_index(fields: dict, stages: list[str]) -> int:
    if not stages:
        return 0
    current_stage_name = str(fields.get("current_stage_name") or "").strip()
    if current_stage_name in stages:
        return stages.index(current_stage_name)
    try:
        idx = int(fields.get("current_stage_index") or 0)
    except (TypeError, ValueError):
        idx = 0
    return max(0, min(idx, len(stages) - 1))


def _stage_chain_tokens(stages: list[str], current_stage_index: int, status: str) -> list[str]:
    if not stages:
        return []
    status_value = str(status or "").strip()
    is_complete = _is_ms_row_completed_status(status_value)
    tokens: list[str] = []
    for idx, stage in enumerate(stages):
        if is_complete or idx < current_stage_index:
            icon = "✅"
        elif idx > current_stage_index:
            icon = "⏳"
        else:
            if status_value == _MS_PENDING_CONFIRMATION:
                icon = _MS_HANDOFF_PENDING_ICON
            else:
                icon = "🔄"
        tokens.append(f"{stage} {icon}")
    return tokens


def _first_valid_datetime(*values) -> datetime | None:
    for value in values:
        parsed = _parse_iso_datetime(value)
        if parsed:
            return parsed
    return None


def _infer_batch_expected_completion(master_fields: dict, rows: list[dict]) -> datetime | None:
    expected = _first_valid_datetime(
        master_fields.get("expected_completion_date"),
        master_fields.get("completion_date"),
    )
    if expected:
        return expected
    due_dates = [
        _parse_iso_datetime(row.get("fields", {}).get("stage_due_date"))
        for row in rows
    ]
    valid_due_dates = [value for value in due_dates if value]
    if valid_due_dates:
        return max(valid_due_dates)
    return _parse_iso_datetime(master_fields.get("scheduled_date"))


def _build_ms_batch_snapshot_overview_text(repo: ProductionRepo, batch_id: int, batch_no: str) -> str:
    rows = _ordered_ms_rows_for_batch(repo, batch_id)
    if not rows:
        return f"Batch Overview\nBatch No: {batch_no or '-'}\nNo MS flows found for this batch."

    master_fields = (repo.get_master_by_id(batch_id) or {}).get("fields", {})
    started_at = _first_valid_datetime(
        master_fields.get("start_date"),
        master_fields.get("created_date"),
    )
    if not started_at:
        created_candidates = [_parse_iso_datetime(row.get("fields", {}).get("created_at")) for row in rows]
        valid_created = [value for value in created_candidates if value]
        started_at = min(valid_created) if valid_created else None
    elapsed_days = _elapsed_days_since(started_at)
    expected_completion = _infer_batch_expected_completion(master_fields, rows)

    done_count = 0
    running_count = 0
    pending_count = 0
    handoff_pending_count = 0
    flow_lines: list[str] = []
    for idx, row in enumerate(rows, start=1):
        fields = row.get("fields", {})
        process_seq = _normalize_process_seq(fields)
        stages = repo.get_process_stage_names(process_seq)
        if not stages:
            fallback_stage = str(fields.get("current_stage_name") or "").strip()
            stages = [fallback_stage] if fallback_stage else [f"Flow {idx}"]
        current_stage_index = _resolve_stage_index(fields, stages)
        status = str(fields.get("current_status") or fields.get("status") or "").strip()
        if _is_ms_row_completed_status(status):
            done_count += 1
        elif status == _MS_PENDING_CONFIRMATION:
            handoff_pending_count += 1
        elif _is_pending_ms_status(status):
            running_count += 1
        else:
            pending_count += 1

        process_label = repo.get_process_display_label(process_seq) or f"Flow {idx}"
        flow_tokens = _stage_chain_tokens(stages, current_stage_index, status)
        flow_lines.append(f"{idx}. {process_label}: " + " -> ".join(flow_tokens))

    lines = [
        "Batch Overview",
        f"Batch No: {batch_no or '-'}",
        f"Started: {_format_dt_short(started_at)}",
        f"Elapsed Days: {elapsed_days if elapsed_days is not None else '-'}",
        f"Expected Completion Date: {_format_dt_short(expected_completion)}",
        f"Flows: {len(rows)} | Complete: {done_count} | Running: {running_count} | Pending: {pending_count} | Hand-off Pending: {handoff_pending_count}",
        "Legend: ✅ Done | 🔄 Running | ⏳ Pending | 🤝 Hand-off Pending",
        "",
        "Flow Snapshot:",
    ]
    lines.extend(flow_lines)
    return "\n".join(lines)


def _build_ms_batch_overview_inline_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("View Flow Details", callback_data=_ms_batch_callback_data("sl", batch_id, "d", "0")),
                InlineKeyboardButton("View Timeline", callback_data=_ms_batch_callback_data("sl", batch_id, "t", "0")),
            ],
            [InlineKeyboardButton("Refresh", callback_data=_ms_batch_callback_data("ov", batch_id))],
        ]
    )


def _build_ms_batch_flow_selector_text(repo: ProductionRepo, rows: list[dict], mode: str, page: int, page_size: int) -> str:
    title = "Flow Details" if mode == "d" else "Flow Timelines"
    page_rows, start, _ = _paginate(rows, page, page_size)
    lines = [title, "Select a flow:", ""]
    for offset, row in enumerate(page_rows, start=1):
        row_index = start + offset
        fields = row.get("fields", {})
        process_label = repo.get_process_display_label(_normalize_process_seq(fields)) or f"Flow {row_index}"
        current_stage = str(fields.get("current_stage_name") or "-")
        status = str(fields.get("current_status") or fields.get("status") or "-")
        lines.append(f"{row_index}. {process_label} | Current: {current_stage} | Status: {status}")
    return "\n".join(lines)


def _build_ms_batch_flow_selector_keyboard(
    repo: ProductionRepo,
    batch_id: int,
    rows: list[dict],
    mode: str,
    page: int,
    page_size: int,
) -> InlineKeyboardMarkup:
    page_rows, start, end = _paginate(rows, page, page_size)
    keyboard: list[list[InlineKeyboardButton]] = []
    detail_action = "fd" if mode == "d" else "ft"
    for offset, row in enumerate(page_rows, start=1):
        row_index = start + offset
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        fields = row.get("fields", {})
        process_label = repo.get_process_display_label(_normalize_process_seq(fields)) or str(
            fields.get("current_stage_name") or f"Flow {row_index}"
        )
        label = f"{row_index}. {process_label}"
        keyboard.append(
            [
                InlineKeyboardButton(
                    label[:32],
                    callback_data=_ms_batch_callback_data(detail_action, batch_id, str(row_id), str(page)),
                )
            ]
        )
    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("Prev", callback_data=_ms_batch_callback_data("sl", batch_id, mode, str(page - 1))))
    nav_row.append(InlineKeyboardButton("Overview", callback_data=_ms_batch_callback_data("ov", batch_id)))
    if end < len(rows):
        nav_row.append(InlineKeyboardButton("Next", callback_data=_ms_batch_callback_data("sl", batch_id, mode, str(page + 1))))
    keyboard.append(nav_row)
    return InlineKeyboardMarkup(keyboard)


def _build_ms_batch_flow_detail_text(repo: ProductionRepo, batch_id: int, row: dict, row_number: int, total_rows: int) -> str:
    fields = row.get("fields", {})
    process_seq = _normalize_process_seq(fields)
    process_label = repo.get_process_display_label(process_seq) or f"Flow {row_number}"
    stages = repo.get_process_stage_names(process_seq)
    if not stages:
        current_stage = str(fields.get("current_stage_name") or "").strip()
        stages = [current_stage] if current_stage else ["-"]
    current_stage_index = _resolve_stage_index(fields, stages)
    current_stage = str(fields.get("current_stage_name") or stages[current_stage_index] or "-")
    next_stage = str(fields.get("next_stage_name") or _get_next_stage_name(stages, current_stage_index) or "-")
    status = str(fields.get("current_status") or fields.get("status") or "-")
    part_name = _resolve_ms_row_part_text(repo, fields) or "-"
    updated_at = _first_valid_datetime(fields.get("updated_at"), fields.get("created_at"))

    lines = [
        f"Flow Detail ({row_number}/{total_rows})",
        f"Batch No: {str((repo.get_master_by_id(batch_id) or {}).get('fields', {}).get('batch_no') or '-')}",
        f"Process: {process_label}",
        f"Part: {part_name}",
        f"Current Stage: {current_stage}",
        f"Next Stage: {next_stage}",
        f"Status: {status}",
        f"Last Updated: {_format_dt_short(updated_at)}",
        "",
        "Stage Path:",
    ]
    for stage_idx, stage in enumerate(stages, start=1):
        marker = "->" if stage_idx - 1 == current_stage_index else "  "
        lines.append(f"{marker} {stage_idx}. {stage}")
    return "\n".join(lines)


def _build_ms_batch_flow_timeline_text(repo: ProductionRepo, batch_id: int, row: dict, row_number: int, total_rows: int) -> str:
    row_id = row.get("id")
    fields = row.get("fields", {})
    process_seq = _normalize_process_seq(fields)
    process_label = repo.get_process_display_label(process_seq) or f"Flow {row_number}"
    stages = repo.get_process_stage_names(process_seq)
    if not stages:
        current_stage = str(fields.get("current_stage_name") or "").strip()
        stages = [current_stage] if current_stage else []
    current_stage = str(fields.get("current_stage_name") or "")
    history_rows = _build_stage_history_indexes(repo, batch_id).get(int(row_id), []) if isinstance(row_id, int) else []

    lines = [
        f"Flow Timeline ({row_number}/{total_rows})",
        f"Process: {process_label}",
    ]
    if not stages:
        lines.append("No stage sequence available.")
        return "\n".join(lines)

    for stage_index, stage in enumerate(stages):
        next_stage = stages[stage_index + 1] if stage_index + 1 < len(stages) else ""
        completed_at = _find_stage_completed_from_history(stage, next_stage, history_rows)
        started_at = _find_stage_start_from_history(stage, history_rows)
        if completed_at:
            lines.append(f"✅ {stage}: done on {_format_dt_short(completed_at)}")
        elif stage == current_stage:
            if str(fields.get('current_status') or fields.get('status') or '').strip() == _MS_PENDING_CONFIRMATION:
                lines.append(f"{_MS_HANDOFF_PENDING_ICON} {stage}: hand-off pending")
            else:
                lines.append(f"🔄 {stage}: running since {_format_dt_short(started_at)}")
        else:
            lines.append(f"⏳ {stage}: pending")
    return "\n".join(lines)


def _build_ms_batch_flow_action_keyboard(
    batch_id: int,
    row_id: int,
    page: int,
    mode: str,
    can_mark_done: bool,
    can_confirm: bool,
) -> InlineKeyboardMarkup:
    keyboard: list[list[InlineKeyboardButton]] = []
    if can_mark_done:
        keyboard.append([InlineKeyboardButton("Mark Stage Done", callback_data=_ms_batch_callback_data("dn", batch_id, str(row_id), str(page)))])
    if can_confirm:
        keyboard.append([InlineKeyboardButton("Confirm Hand-off", callback_data=_ms_batch_callback_data("cf", batch_id, str(row_id), str(page)))])
    keyboard.append(
        [
            InlineKeyboardButton("Back", callback_data=_ms_batch_callback_data("sl", batch_id, mode, str(page))),
            InlineKeyboardButton("Overview", callback_data=_ms_batch_callback_data("ov", batch_id)),
        ]
    )
    return InlineKeyboardMarkup(keyboard)


async def _show_ms_batch_tracker_overview(update, batch_id: int, batch_no: str) -> None:
    repo = ProductionRepo()
    text = _build_ms_batch_snapshot_overview_text(repo, batch_id, batch_no)
    await update.effective_message.reply_text(
        _format_menu_text(text),
        reply_markup=_build_ms_batch_overview_inline_keyboard(batch_id),
    )


def _find_ms_row_index(rows: list[dict], row_id: int) -> int:
    for idx, row in enumerate(rows):
        if row.get("id") == row_id:
            return idx
    return -1


def _resolve_batch_no(repo: ProductionRepo, batch_id: int) -> str:
    return str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")


async def _edit_ms_batch_tracker_message(query, text: str, reply_markup: InlineKeyboardMarkup) -> None:
    formatted = _format_menu_text(text)
    try:
        await query.edit_message_text(formatted, reply_markup=reply_markup)
    except Exception:
        await query.message.reply_text(formatted, reply_markup=reply_markup)


def _can_mark_stage_done(repo: ProductionRepo, row_fields: dict, user_role_name: str) -> bool:
    if not user_role_name:
        return False
    status = str(row_fields.get("current_status") or row_fields.get("status") or "").strip()
    if status == _MS_PENDING_CONFIRMATION or _is_ms_row_completed_status(status):
        return False
    process_seq = _normalize_process_seq(row_fields)
    current_stage = str(row_fields.get("current_stage_name") or "").strip()
    current_role = _resolve_supervisor_role_for_stage(repo, process_seq, current_stage)
    return user_role_name == current_role


def _can_confirm_handoff(repo: ProductionRepo, row_fields: dict, user_role_name: str) -> bool:
    if not user_role_name:
        return False
    status = str(row_fields.get("current_status") or row_fields.get("status") or "").strip()
    if status != _MS_PENDING_CONFIRMATION:
        return False
    process_seq = _normalize_process_seq(row_fields)
    next_stage = str(row_fields.get("next_stage_name") or "").strip()
    next_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage) if next_stage else ""
    return user_role_name == next_role


async def _render_ms_batch_flow_selector(query, repo: ProductionRepo, batch_id: int, mode: str, page: int) -> None:
    rows = _ordered_ms_rows_for_batch(repo, batch_id)
    if not rows:
        await _edit_ms_batch_tracker_message(
            query,
            f"Batch Overview\nBatch No: {_resolve_batch_no(repo, batch_id) or '-'}\nNo MS flows found for this batch.",
            _build_ms_batch_overview_inline_keyboard(batch_id),
        )
        return
    safe_mode = "d" if mode not in ("d", "t") else mode
    page_size = 8
    max_page = max((len(rows) - 1) // page_size, 0)
    safe_page = max(0, min(page, max_page))
    text = _build_ms_batch_flow_selector_text(repo, rows, safe_mode, safe_page, page_size)
    keyboard = _build_ms_batch_flow_selector_keyboard(repo, batch_id, rows, safe_mode, safe_page, page_size)
    await _edit_ms_batch_tracker_message(query, text, keyboard)


async def _render_ms_batch_flow_detail(query, context, repo: ProductionRepo, batch_id: int, row_id: int, page: int) -> None:
    rows = _ordered_ms_rows_for_batch(repo, batch_id)
    row_index = _find_ms_row_index(rows, row_id)
    if row_index < 0:
        await _render_ms_batch_flow_selector(query, repo, batch_id, "d", page)
        return
    row = rows[row_index]
    text = _build_ms_batch_flow_detail_text(repo, batch_id, row, row_index + 1, len(rows))
    row_fields = row.get("fields", {})
    user_role_name = _resolve_user_role_name(repo, context)
    keyboard = _build_ms_batch_flow_action_keyboard(
        batch_id,
        row_id,
        page,
        "d",
        can_mark_done=_can_mark_stage_done(repo, row_fields, user_role_name),
        can_confirm=_can_confirm_handoff(repo, row_fields, user_role_name),
    )
    await _edit_ms_batch_tracker_message(query, text, keyboard)


async def _render_ms_batch_flow_timeline(query, context, repo: ProductionRepo, batch_id: int, row_id: int, page: int) -> None:
    rows = _ordered_ms_rows_for_batch(repo, batch_id)
    row_index = _find_ms_row_index(rows, row_id)
    if row_index < 0:
        await _render_ms_batch_flow_selector(query, repo, batch_id, "t", page)
        return
    row = rows[row_index]
    text = _build_ms_batch_flow_timeline_text(repo, batch_id, row, row_index + 1, len(rows))
    row_fields = row.get("fields", {})
    user_role_name = _resolve_user_role_name(repo, context)
    keyboard = _build_ms_batch_flow_action_keyboard(
        batch_id,
        row_id,
        page,
        "t",
        can_mark_done=_can_mark_stage_done(repo, row_fields, user_role_name),
        can_confirm=_can_confirm_handoff(repo, row_fields, user_role_name),
    )
    await _edit_ms_batch_tracker_message(query, text, keyboard)


async def _show_my_ms_jobs_bulk_action_menu(update, context, selected_rows: list[dict]) -> None:
    count = len(selected_rows)
    lines = [
        f"Bulk action for {count} selected row(s).",
        "Choose one action:",
    ]
    context.user_data["menu_state"] = MY_MS_JOBS_BULK_ACTION_STATE
    await _reply(
        update,
        "\n".join(lines),
        [[_MS_BULK_ACTION_DONE], [_MS_BULK_ACTION_HOLD], [_MS_BULK_ACTION_REMARKS], [BACK_LABEL]],
    )


async def _show_my_ms_batch_action_menu(
    update,
    context,
    batch_id: int,
    batch_no: str,
    source_state: str = MY_MS_JOBS_SELECTION_STATE,
) -> None:
    context.user_data["my_ms_batch_action"] = {"batch_id": batch_id, "batch_no": batch_no, "source_state": source_state}
    context.user_data["menu_state"] = MY_MS_BATCH_ACTION_STATE
    await _reply(
        update,
        f"Batch Actions\nBatch No: {batch_no or '-'}\nChoose one action:",
        [[_MS_BATCH_ACTION_SUMMARY], [_MS_BATCH_ACTION_DONE], [_MS_BATCH_ACTION_HOLD], [_MS_BATCH_ACTION_REMARKS], [_MS_BATCH_ACTION_VIEW], [_MS_BATCH_ACTION_SCHEDULE], [BACK_LABEL]],
    )

def _extract_first_attachment_ref(attachments_value) -> tuple[int | None, str]:
    if attachments_value is None:
        return None, ""
    items = attachments_value
    if isinstance(items, dict):
        items = [items]
    elif not isinstance(items, list):
        items = [items]
    if items and items[0] == "L":
        items = items[1:]
    if not items:
        return None, ""

    item = items[0]
    if isinstance(item, dict):
        attachment_id = item.get("id") or item.get("attachmentId")
        file_name = str(item.get("fileName") or item.get("name") or "ms_cutlist.pdf")
        try:
            return int(attachment_id), file_name
        except (TypeError, ValueError):
            return None, file_name

    try:
        return int(item), "ms_cutlist.pdf"
    except (TypeError, ValueError):
        return None, "ms_cutlist.pdf"


def _download_attachment_bytes(repo: ProductionRepo, attachment_id: int) -> bytes:
    url = f"{repo.costing_client.server}/api/docs/{repo.costing_client.doc_id}/attachments/{attachment_id}/download"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {repo.costing_client.api_key}"},
        timeout=30,
    )
    response.raise_for_status()
    return response.content


def _build_ms_rows(repo: ProductionRepo, batch_id: int, part_ids: list[int], batch_qty: int, timestamp_iso: str, updated_by) -> list[dict]:
    grouped: dict[object, dict] = {}
    ms_columns = repo.get_ms_table_column_ids()
    product_part_col_type = repo.get_column_type("ProductBatchMS", "product_part")
    product_part_is_reflist = str(product_part_col_type).startswith("RefList:")

    for record in repo.get_ms_rows(part_ids):
        record_id = record.get("id")
        fields = record.get("fields", {})
        process_seq = _normalize_process_seq(fields)
        if process_seq in (None, "", 0):
            continue
        part_name = str(fields.get("ProductPartName_ProductPartName") or "").strip()
        total_qty = float(fields.get("QtyNos") or 0) * batch_qty
        if total_qty <= 0:
            continue
        bucket = grouped.setdefault(
            process_seq,
            {"total_qty": 0.0, "ms_refs": set(), "part_names": set()},
        )
        bucket["total_qty"] += total_qty
        if isinstance(record_id, int):
            bucket["ms_refs"].add(record_id)
        if part_name:
            bucket["part_names"].add(part_name)

    rows = []
    for process_seq, grouped_data in grouped.items():
        total_qty = float(grouped_data.get("total_qty") or 0.0)
        if total_qty <= 0:
            continue
        stages = repo.get_process_stage_names(process_seq)
        if not stages:
            continue
        first_stage = stages[0]
        next_stage = _get_next_stage_name(stages, 0)
        ms_refs = sorted(grouped_data.get("ms_refs") or [])
        part_names = sorted(grouped_data.get("part_names") or [])
        product_part_value = ["L", *ms_refs] if (product_part_is_reflist and ms_refs) else ", ".join(part_names)
        base_fields = {
            "batch_id": batch_id,
            "product_part": product_part_value,
            "process_seq": process_seq,
            "total_qty": total_qty,
            "current_stage_index": 0,
            "current_stage_name": first_stage,
            "next_stage_name": next_stage,
            "current_status": f"{first_stage} Pending",
            "created_at": timestamp_iso,
            "updated_at": timestamp_iso,
            "last_updated_by": updated_by,
            # Backward-compatible fallback for legacy status readers.
            "status": f"{first_stage} Pending",
        }
        if "required_qty" in ms_columns:
            base_fields["required_qty"] = total_qty
        rows.append(repo.filter_table_fields("ProductBatchMS", base_fields))

    return rows


def _build_ms_cutlist_sections(repo: ProductionRepo, part_ids: list[int], batch_qty: int) -> list[dict]:
    material_map = repo.get_material_name_map()
    grouped: dict[tuple[str, str, str, str, str], float] = {}
    for record in repo.get_ms_rows(part_ids):
        fields = record.get("fields", {})
        process_seq = _normalize_process_seq(fields)
        if process_seq in (None, "", 0):
            continue
        part_name = str(fields.get("ProductPartName_ProductPartName") or "")
        material_ref = _normalize_ref(fields.get("MaterialToCut"))
        material_name = material_map.get(material_ref, "") if isinstance(material_ref, int) else ""
        length_mm = _format_qty(float(fields.get("Length_mm") or 0))
        total_qty = float(fields.get("QtyNos") or 0) * batch_qty
        if total_qty <= 0:
            continue
        process_seq_label = repo.get_process_display_label(process_seq)
        stage_names = repo.get_process_stage_names(process_seq)
        next_stage = _get_next_stage_name(stage_names, 0)
        key = (process_seq_label, part_name, material_name, length_mm, next_stage)
        grouped[key] = grouped.get(key, 0.0) + total_qty

    sections: dict[str, list[dict]] = {}
    for (process_seq_label, part_name, material_name, length_mm, next_stage), total_qty in grouped.items():
        sections.setdefault(process_seq_label, []).append(
            {
                "product_part": part_name,
                "material_to_cut": material_name,
                "length_mm": length_mm,
                "total_qty": _format_qty(total_qty),
                "next_stage": next_stage,
            }
        )

    ordered_sections = []
    for process_seq in sorted(sections):
        rows = sorted(sections[process_seq], key=lambda row: (row["product_part"], row["material_to_cut"], row["length_mm"]))
        ordered_sections.append({"process_seq": process_seq, "rows": rows})
    return ordered_sections


def _build_ms_row_cutlist_map(repo: ProductionRepo, part_ids: list[int], batch_qty: int) -> dict[str, dict]:
    sections = _build_ms_cutlist_sections(repo, part_ids, batch_qty)
    payload: dict[str, dict] = {}
    for section in sections:
        process_label = str(section.get("process_seq") or "")
        if not process_label:
            continue
        payload[process_label] = {
            "process_seq": process_label,
            "rows": list(section.get("rows", [])),
        }
    return payload


def _resolve_supervisor_role_for_stage(repo: ProductionRepo, process_seq, stage_name: str) -> str:
    return repo.get_stage_role_for_process_stage(process_seq, stage_name)


async def _notify_stage_event(
    context,
    event_type: str,
    batch_id: int,
    message: str,
    supervisor_role: str = "",
    reply_markup=None,
) -> None:
    event_context = {"batch_id": batch_id}
    if supervisor_role:
        event_context["recipient_roles"] = [supervisor_role]
    await _notify_event(context.bot, event_type, message, context=event_context, reply_markup=reply_markup)


async def _notify_event(
    telegram_bot,
    event_type: str,
    message: str,
    context: dict | None = None,
    reply_markup=None,
    recipient_renderer=None,
) -> None:
    await dispatch_event(
        event_type,
        message,
        telegram_bot,
        context=context,
        reply_markup=reply_markup,
        recipient_renderer=recipient_renderer,
    )


def _approval_callback_data(action: str, batch_id: int) -> str:
    return f"{_APPROVAL_CB_PREFIX}:{action}:{batch_id}"


def _parse_approval_callback_data(data: str) -> tuple[str, int] | None:
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != _APPROVAL_CB_PREFIX:
        return None
    action = parts[1]
    if action not in ("open", "approve", "reject"):
        return None
    try:
        batch_id = int(parts[2])
    except ValueError:
        return None
    return action, batch_id


def _approval_open_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Yes", callback_data=_approval_callback_data("approve", batch_id)),
                InlineKeyboardButton("Reject", callback_data=_approval_callback_data("reject", batch_id)),
            ]
        ]
    )


def _supervisor_callback_data(action: str, record_id: int) -> str:
    return f"{_SUPV_CB_PREFIX}:{action}:{record_id}"


def _parse_supervisor_callback_data(data: str) -> tuple[str, int] | None:
    parts = data.split(":")
    if len(parts) != 3 or parts[0] != _SUPV_CB_PREFIX:
        return None
    action = str(parts[1] or "").strip()
    if action not in ("schedule", "done_row", "done_batch_stage", "complete_batch", "view_pdf", "confirm_row"):
        return None
    try:
        rec_id = int(parts[2])
    except ValueError:
        return None
    return action, rec_id


def _ms_batch_callback_data(action: str, batch_id: int, arg1: str = "", arg2: str = "") -> str:
    parts = [_MS_BATCH_CB_PREFIX, action, str(batch_id)]
    if arg1:
        parts.append(str(arg1))
    if arg2:
        parts.append(str(arg2))
    return ":".join(parts)


def _parse_ms_batch_callback_data(data: str) -> tuple[str, int, str, str] | None:
    parts = str(data or "").split(":")
    if len(parts) < 3 or parts[0] != _MS_BATCH_CB_PREFIX:
        return None
    action = str(parts[1] or "").strip()
    if action not in {"ov", "sl", "vd", "fd", "ft", "dn", "cf"}:
        return None
    try:
        batch_id = int(parts[2])
    except (TypeError, ValueError):
        return None
    arg1 = parts[3] if len(parts) >= 4 else ""
    arg2 = parts[4] if len(parts) >= 5 else ""
    return action, batch_id, arg1, arg2


def build_schedule_inline_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Schedule Batch",
                    callback_data=_supervisor_callback_data("schedule", batch_id),
                )
            ]
        ]
    )


def build_stage_inline_keyboard(batch_id: int, row_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Current Stage Done",
                    callback_data=_supervisor_callback_data("done_row", row_id),
                ),
                InlineKeyboardButton(
                    "Schedule Batch",
                    callback_data=_supervisor_callback_data("schedule", batch_id),
                ),
            ],
            [
                InlineKeyboardButton(
                    "View PDF",
                    callback_data=_supervisor_callback_data("view_pdf", row_id),
                ),
                InlineKeyboardButton(
                    "Done Full Batch Stage",
                    callback_data=_supervisor_callback_data("done_batch_stage", batch_id),
                )
            ],
        ]
    )


def build_stage_confirm_inline_keyboard(batch_id: int, row_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Confirm Stage Handover",
                    callback_data=_supervisor_callback_data("confirm_row", row_id),
                ),
                InlineKeyboardButton(
                    "Schedule Batch",
                    callback_data=_supervisor_callback_data("schedule", batch_id),
                ),
            ],
            [
                InlineKeyboardButton(
                    "View PDF",
                    callback_data=_supervisor_callback_data("view_pdf", row_id),
                ),
            ],
        ]
    )


def build_complete_batch_inline_keyboard(batch_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "Mark Batch Complete",
                    callback_data=_supervisor_callback_data("complete_batch", batch_id),
                )
            ]
        ]
    )


def _is_approval_actor_subscriber(recipient: dict) -> bool:
    role_id = str(recipient.get("role_id") or "").strip()
    role_name = str(recipient.get("role_name") or "").strip()
    return role_id in _APPROVER_ROLE_IDS or role_name in _APPROVER_ROLE_NAMES


def _batch_created_recipient_renderer(batch_id: int):
    approval_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("Click Here to Approve", callback_data=_approval_callback_data("open", batch_id))]]
    )

    def _render(recipient: dict) -> dict:
        if _is_approval_actor_subscriber(recipient):
            return {"reply_markup": approval_markup}
        return {"skip": True}

    return _render


def _target_return_state(context):
    nav_stack = context.user_data.setdefault("nav_stack", [MAIN_STATE])
    if nav_stack:
        return nav_stack[-1]
    return MAIN_STATE


def _set_paging(context, records: list, page: int = 0):
    context.user_data["production_paging"] = {
        "records": records,
        "page": page,
        "page_size": settings.MSCUTLIST_PAGE_SIZE,
    }


async def _move_model_selection(update, context):
    repo = ProductionRepo()
    model_codes = repo.get_product_models()
    if not model_codes:
        await _reply(update, "No product models found.")
        return
    _set_paging(context, model_codes, 0)
    context.user_data["menu_state"] = SELECTING_PRODUCT_MODEL_STATE
    await _show_model_page(update, context)


async def _move_part_selection(update, context):
    flow = _get_flow(context)
    model_code = flow.get("model_code")
    repo = ProductionRepo()
    parts = repo.get_product_parts_for_model(model_code)
    if not parts:
        await _reply(update, "No parts found for selected model.")
        return
    _set_paging(context, parts, 0)
    context.user_data["menu_state"] = SELECTING_PRODUCT_PARTS_STATE
    await _show_parts_page(update, context)


def _change_page(context, delta: int):
    paging = context.user_data.get("production_paging", {})
    records = paging.get("records", [])
    page_size = paging.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page = paging.get("page", 0) + delta
    max_page = max((len(records) - 1) // page_size, 0)
    paging["page"] = max(0, min(page, max_page))
    context.user_data["production_paging"] = paging


def _selection_index_from_text(text: str, page: int, page_size: int):
    if not text.isdigit():
        return None
    return page * page_size + int(text) - 1


def _parse_number_tokens(text: str) -> list[int]:
    values = []
    for token in text.split(","):
        token = token.strip()
        if not token:
            continue
        if not token.isdigit():
            return []
        values.append(int(token))
    return values


def _parse_prefixed_selection(text: str) -> tuple[str, int] | None:
    value = str(text or "").strip()
    if len(value) < 2:
        return None
    action = value[0].upper()
    if action not in {"D", "S", "P", "V", "R", "H", "B", "C", "N"}:
        return None
    token = value[1:].strip()
    if not token.isdigit():
        return None
    return action, int(token)


def _parse_bulk_prefixed_selection(text: str) -> list[int] | None:
    value = str(text or "").strip()
    if len(value) < 2:
        return None
    action = value[0].upper()
    if action != "X":
        return None
    tokens = _parse_number_tokens(value[1:].strip())
    if not tokens:
        return []
    return tokens


def _parse_schedule_date_text(text: str) -> str | None:
    value = str(text or "").strip()
    if not value:
        return None
    today = datetime.utcnow().date()
    if value == _TODAY:
        return datetime.combine(today, datetime.min.time()).isoformat()
    if value == _TOMORROW:
        return datetime.combine(today + timedelta(days=1), datetime.min.time()).isoformat()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None
    return datetime.combine(parsed.date(), datetime.min.time()).isoformat()


async def _prompt_schedule_date(update, context, batch_ids: list[int], return_state: str, title: str) -> None:
    context.user_data["schedule_date_context"] = {
        "batch_ids": [batch_id for batch_id in batch_ids if isinstance(batch_id, int)],
        "return_state": return_state,
    }
    context.user_data["menu_state"] = AWAITING_SCHEDULE_DATE_STATE
    await _reply(
        update,
        f"{title}\nSelect date or enter `YYYY-MM-DD`.",
        [[_TODAY], [_TOMORROW], [BACK_LABEL]],
    )


async def _notify_roles(context, batch_id: int, message: str, role_names: list[str]) -> None:
    normalized = [str(role or "").strip() for role in role_names if str(role or "").strip()]
    if not normalized:
        return
    await _notify_event(
        context.bot,
        "batch_status_changed",
        message,
        context={"batch_id": batch_id, "recipient_roles": normalized},
    )


def _resolve_selected_part_ids(repo: ProductionRepo, flow: dict) -> list[int]:
    selected_part_ids = flow.get("selected_part_ids", [])
    if flow.get("batch_mode") == _MODE_BY_MODEL:
        parts = repo.get_product_parts_for_model(flow["model_code"])
        selected_part_ids = [part["part_id"] for part in parts]
    return selected_part_ids


async def _create_batch_from_flow(update, context):
    flow = _get_flow(context)
    repo = ProductionRepo()
    user = context.user_data.get("user", {})
    creator_user_id = user.get("user_id", "")
    creator_user_ref = repo.get_costing_user_ref_by_user_id(creator_user_id)

    batch_type, include_ms, include_cnc, include_store = _resolve_type_flags(flow["batch_type"])
    batch_no = generate_batch_number(repo, flow["model_code"], include_ms, include_cnc, include_store)
    selected_part_ids = _resolve_selected_part_ids(repo, flow)

    created_date = _now_iso()
    master_id = repo.create_master_batch(
        {
            "batch_no": batch_no,
            "product_model": flow["model_code"],
            "qty": flow["batch_qty"],
            "batch_type": batch_type,
            "include_ms": include_ms,
            "include_cnc": include_cnc,
            "include_store": include_store,
            "created_by": creator_user_ref,
            "created_date": created_date,
            "start_date": None,
            "scheduled_date": None,
            "completion_date": None,
            "approval_status": "Pending Approval",
            "approval_date": None,
            "approved_by": "",
            "overall_status": "Pending Approval",
            "selected_part_ids": _to_int_list_csv(selected_part_ids),
        }
    )
    repo.add_lifecycle_history(master_id, "Batch Created", creator_user_ref, "Batch created and sent for approval")

    created_label_width = 12
    created_notification_message = (
        f"Batch created: {batch_no}\n"
        f"{'Model'.ljust(created_label_width)}: {flow['model_code']}\n"
        f"{'Qty'.ljust(created_label_width)}: {flow['batch_qty']}\n"
        f"{'Date Created'.ljust(created_label_width)}: {_format_notification_datetime(created_date)}\n"
        f"{'Approval'.ljust(created_label_width)}: Pending"
    )

    await _notify_event(
        context.bot,
        "production_batch_created",
        created_notification_message,
        context={"batch_id": master_id},
        recipient_renderer=_batch_created_recipient_renderer(master_id),
    )

    set_main_menu_state(context)
    await _reply(update, f"Batch created: {batch_no}\nStatus: Pending Approval")
    _clear_flow(context)

def _is_production_manager(context) -> bool:
    user = context.user_data.get("user", {})
    user_id = user.get("user_id", "")
    role_id = user.get("role", "")
    if role_id in ("R01", "R02"):
        return True

    repo = ProductionRepo()
    role_name = repo.get_role_name_by_user_id(user_id)
    return role_name in ("Production_Manager", "System_Admin")


async def start_pending_approvals(update, context) -> None:
    if not _is_production_manager(context):
        await _reply(update, "Only Production Manager or System Admin can approve batches.")
        return

    repo = ProductionRepo()
    pending = repo.list_pending_approvals()
    if not pending:
        set_main_menu_state(context)
        await _reply(update, "No pending approvals found.")
        return

    context.user_data["pending_approvals_selection"] = {
        "records": pending,
        "page": 0,
        "page_size": settings.MSCUTLIST_PAGE_SIZE,
    }
    context.user_data["menu_state"] = PENDING_APPROVALS_SELECTION_STATE
    await _show_pending_approvals_page(update, context)


async def _show_pending_approvals_page(update, context) -> None:
    selection = context.user_data.get("pending_approvals_selection", {})
    records = selection.get("records", [])
    page = selection.get("page", 0)
    page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_records, _, end = _paginate(records, page, page_size)

    lines = ["Production Batch Approval (Pending):", "Enter number(s) like 1 or 1,3"]
    lines.append("")

    for idx, record in enumerate(page_records, start=1):
        fields = record.get("fields", {})
        lines.append(
            f"{idx}. {fields.get('batch_no', '')} | Model: {fields.get('product_model', '')} | Qty: {fields.get('qty', '')}"
        )

    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(records):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


async def _show_pending_approval_confirmation(update, context) -> None:
    data = context.user_data.get("pending_approvals_confirm", {})
    selected_records = data.get("selected_records", [])

    lines = ["Confirm approval for selected batches:"]
    for record in selected_records:
        fields = record.get("fields", {})
        lines.append(f"- {fields.get('batch_no', '')}")
    lines.append("")
    lines.append("Choose action:")
    lines.append("- Yes = Approve")
    lines.append("- Reject = Reject selected batches")

    context.user_data["menu_state"] = PENDING_APPROVALS_CONFIRM_STATE
    await _reply(update, "\n".join(lines), [[_YES], [_REJECT], [_NO], [BACK_LABEL]])


def _build_rows_by_stage_role(repo: ProductionRepo, ms_rows: list[dict]) -> dict[str, list[dict]]:
    rows_by_role: dict[str, list[dict]] = {}
    for row in ms_rows:
        fields = row.get("fields", row)
        stage_name = str(fields.get("current_stage_name") or "").strip()
        if not stage_name:
            continue
        process_seq = _normalize_process_seq(fields)
        supervisor_role = _resolve_supervisor_role_for_stage(repo, process_seq, stage_name)
        if not supervisor_role:
            continue
        rows_by_role.setdefault(supervisor_role, []).append(row)
    return rows_by_role


async def _send_ms_batch_first_stage_detail_messages(
    query,
    context,
    batch_id: int,
) -> None:
    repo = ProductionRepo()
    batch = repo.get_master_by_id(batch_id)
    if not batch:
        await query.message.reply_text("Batch not found.")
        return

    batch_by_map = _get_batch_creator_name_map(repo, {batch_id})
    batch_by = batch_by_map.get(batch_id, "-")
    user = context.user_data.get("user", {})
    user_role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
    if not user_role_name:
        await query.message.reply_text("Unable to resolve your role for batch details.")
        return

    rows = _ordered_ms_rows_for_batch(repo, batch_id)
    batch_no = str(batch.get("fields", {}).get("batch_no") or "")
    sent = 0
    for row in rows:
        fields = row.get("fields", {})
        stage_name = str(fields.get("current_stage_name") or "").strip()
        process_seq = _normalize_process_seq(fields)
        row_role = _resolve_supervisor_role_for_stage(repo, process_seq, stage_name)
        if row_role != user_role_name:
            continue
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        part_name = _resolve_ms_row_part_text(repo, row)
        qty = _format_qty(float(fields.get("total_qty") or fields.get("required_qty") or 0))
        next_stage = _resolve_next_stage_for_row(repo, fields)
        message = _build_ms_stage_pending_message(
            batch_no=batch_no,
            batch_by=batch_by,
            part_name=part_name,
            current_stage=stage_name,
            next_stage=next_stage,
            qty=qty,
        )
        await query.message.reply_text(message, reply_markup=build_stage_inline_keyboard(batch_id, row_id))
        sent += 1

    if sent == 0:
        await query.message.reply_text("No batch flow details found for your role.")


async def _notify_ms_first_stage(repo: ProductionRepo, context, batch_id: int, ms_rows: list[dict], batch_no: str) -> None:
    batch = repo.get_master_by_id(batch_id)
    if not batch:
        return

    master_fields = batch.get("fields", {})
    status = str(master_fields.get("overall_status") or "Schedule Pending")
    approved_by = str(context.user_data.get("user", {}).get("name") or "-")
    batch_by_map = _get_batch_creator_name_map(repo, {batch_id})
    batch_by = batch_by_map.get(batch_id, "-")

    by_record_id, by_user_id = _build_costing_user_name_indexes(repo)
    batch_by = batch_by or _resolve_costing_user_name(master_fields.get("created_by"), by_record_id, by_user_id) or "-"
    rows_by_role = _build_rows_by_stage_role(repo, ms_rows)
    for row in ms_rows:
        fields = row.get("fields", row)
        stage_name = str(fields.get("current_stage_name") or "").strip()
        process_seq = _normalize_process_seq(fields)
        supervisor_role = _resolve_supervisor_role_for_stage(repo, process_seq, stage_name)
        if stage_name and not supervisor_role:
            await _notify_stage_event(
                context,
                "ms_stage_pending",
                batch_id,
                f"MS stage mapping missing for batch {batch_no}: Stage {stage_name}. Please configure ProcessStage role mapping.",
                supervisor_role="System_Admin",
            )

    if not rows_by_role:
        return

    for supervisor_role, role_rows in rows_by_role.items():
        message = _build_ms_batch_approval_summary_message(
            repo=repo,
            batch_no=batch_no,
            batch_by=batch_by,
            approved_by=approved_by,
            status=status,
            rows=role_rows,
        )
        await _notify_stage_event(
            context,
            "ms_stage_pending",
            batch_id,
            message,
            supervisor_role=supervisor_role,
            reply_markup=_build_ms_batch_view_detail_keyboard(batch_id),
        )


def _attach_ms_cutlist_pdf(repo: ProductionRepo, batch_id: int, batch_no: str, section_rows: list[dict]) -> None:
    if not section_rows:
        return
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = f"{temp_dir}\\ms_cut_list_{batch_no}.pdf"
        title = f"MS Cut List - {batch_no} ({_format_notification_datetime(_now_iso())})"
        write_grouped_ms_cutlist_pdf(section_rows, file_path, title=title)
        repo.attach_pdf_to_master(batch_id, file_path, field_name="ms_cutlist_pdf")


def _attach_ms_row_cutlist_pdfs(repo: ProductionRepo, batch_no: str, ms_rows: list[dict], row_cutlist_map: dict[str, dict]) -> None:
    if not ms_rows:
        return
    with tempfile.TemporaryDirectory() as temp_dir:
        for row in ms_rows:
            row_id = row.get("id")
            if not isinstance(row_id, int):
                continue
            part_name = _resolve_ms_row_part_text(repo, row)
            process_label = repo.get_process_display_label(row.get("process_seq"))
            payload = row_cutlist_map.get(process_label)
            if not payload:
                qty = _format_qty(float(row.get("total_qty") or row.get("required_qty") or 0))
                payload = {
                    "process_seq": process_label,
                    "rows": [
                        {
                            "product_part": part_name,
                            "material_to_cut": "",
                            "length_mm": "",
                            "total_qty": qty,
                            "next_stage": str(row.get("next_stage_name") or ""),
                        }
                    ],
                }
            title = f"MS Cut List - {batch_no} - Row {row_id}"
            file_path = f"{temp_dir}\\ms_cut_list_{batch_no}_{row_id}.pdf"
            write_grouped_ms_cutlist_pdf([payload], file_path, title=title)
            repo.attach_pdf_to_ms_row(row_id, file_path, field_name="row_cutlist_pdf")


def _get_batch_no_map(repo: ProductionRepo, batch_ids: set[int]) -> dict[int, str]:
    if not batch_ids:
        return {}
    if not hasattr(repo, "get_all_master_batches"):
        return {}
    result = {}
    for record in repo.get_all_master_batches():
        record_id = record.get("id")
        if record_id not in batch_ids:
            continue
        result[record_id] = str(record.get("fields", {}).get("batch_no") or "")
    return result


def _build_costing_user_name_indexes(repo: ProductionRepo) -> tuple[dict[int, str], dict[str, str]]:
    by_record_id: dict[int, str] = {}
    by_user_id: dict[str, str] = {}
    for record in repo.costing_client.get_records("Users"):
        rec_id = record.get("id")
        fields = record.get("fields", {})
        user_id = str(fields.get("User_ID") or "").strip()
        name = str(fields.get("Name") or fields.get("user_name") or user_id or rec_id or "").strip()
        if isinstance(rec_id, int):
            by_record_id[rec_id] = name
        if user_id:
            by_user_id[user_id] = name
    return by_record_id, by_user_id


def _resolve_costing_user_name(value, by_record_id: dict[int, str], by_user_id: dict[str, str]) -> str:
    normalized = _normalize_ref(value)
    if isinstance(normalized, int):
        return by_record_id.get(normalized, "")
    text = str(normalized or "").strip()
    if not text:
        return ""
    if text.isdigit():
        return by_record_id.get(int(text), "")
    return by_user_id.get(text, text)


def _get_batch_creator_name_map(repo: ProductionRepo, batch_ids: set[int]) -> dict[int, str]:
    if not batch_ids:
        return {}
    if not hasattr(repo, "get_all_master_batches"):
        return {}
    by_record_id, by_user_id = _build_costing_user_name_indexes(repo)
    creator_map: dict[int, str] = {}
    for master in repo.get_all_master_batches():
        batch_id = master.get("id")
        if not isinstance(batch_id, int) or batch_id not in batch_ids:
            continue
        created_by = master.get("fields", {}).get("created_by")
        creator_map[batch_id] = _resolve_costing_user_name(created_by, by_record_id, by_user_id)
    return creator_map


def _is_pending_ms_status(status: str) -> bool:
    value = str(status or "").strip()
    if not value:
        return False
    if value in ("Cutting Completed", _MS_PENDING_CONFIRMATION, "Done", "Completed"):
        return False
    return True


def _resolve_user_role_name(repo: ProductionRepo, context) -> str:
    user = context.user_data.get("user", {})
    return repo.get_role_name_by_user_id(user.get("user_id", ""))


def _is_batch_schedulable_for_role(repo: ProductionRepo, batch_id: int, role_name: str) -> bool:
    if not role_name:
        return False
    master = repo.get_master_by_id(batch_id)
    if not master:
        return False
    master_fields = master.get("fields", {})
    if master_fields.get("approval_status") != "Approved":
        return False
    if master_fields.get("scheduled_date"):
        return False

    for row in repo.list_ms_rows_for_batch(batch_id):
        fields = row.get("fields", {})
        status = str(fields.get("current_status") or fields.get("status") or "").strip()
        if status == "Cutting Completed":
            continue
        row_role = str(fields.get("current_stage_role_name") or "").strip()
        if not row_role:
            row_role = _resolve_supervisor_role_for_stage(repo, fields.get("process_seq"), fields.get("current_stage_name"))
        if row_role == role_name:
            return True
    return False


def _list_schedule_batches_for_user_role(repo: ProductionRepo, role_name: str) -> list[dict]:
    rows = repo.list_supervisor_schedule_pending_batches(0)
    eligible = [row for row in rows if role_name in row.get("roles", [])]
    eligible.sort(key=lambda row: (row.get("days_open", 0) * -1, row.get("batch_no", "")))
    return eligible


def _rows_for_batch_and_role(repo: ProductionRepo, batch_id: int, role_name: str) -> list[dict]:
    rows: list[dict] = []
    for row in repo.list_ms_rows_for_batch(batch_id):
        fields = row.get("fields", {})
        status = str(fields.get("current_status") or fields.get("status") or "").strip()
        if status == "Cutting Completed":
            continue
        stage_name = str(fields.get("current_stage_name") or "").strip()
        row_role = str(fields.get("current_stage_role_name") or "").strip()
        if not row_role:
            row_role = _resolve_supervisor_role_for_stage(repo, fields.get("process_seq"), stage_name)
        if row_role != role_name:
            continue
        rows.append(row)
    return rows


async def _mark_batch_stage_done(repo: ProductionRepo, context, batch_id: int, updated_by, role_name: str) -> int:
    done_count = 0
    for row in _rows_for_batch_and_role(repo, batch_id, role_name):
        row_id = row.get("id")
        if not isinstance(row_id, int):
            continue
        await _mark_ms_stage_done_pending_confirmation(repo, context, row_id, updated_by)
        done_count += 1
    return done_count


def _list_ms_jobs_for_user_role(repo: ProductionRepo, role_name: str) -> list[dict]:
    if not role_name:
        return []

    master_fields_by_id = {
        record.get("id"): record.get("fields", {})
        for record in repo.get_all_master_batches()
        if isinstance(record.get("id"), int)
    }
    rows = []
    batch_ids = set()
    for record in repo.costing_client.get_records("ProductBatchMS"):
        fields = record.get("fields", {})
        process_seq = _normalize_process_seq(fields)
        stage_name = str(fields.get("current_stage_name") or "").strip()
        current_status = str(fields.get("current_status") or fields.get("status") or "").strip()
        current_role = _resolve_supervisor_role_for_stage(repo, process_seq, stage_name)
        next_stage_name = str(fields.get("next_stage_name") or "").strip()
        next_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage_name) if next_stage_name else ""
        supervisor_role = str(fields.get("current_stage_role_name") or "").strip()
        if not supervisor_role:
            supervisor_role = current_role

        # Dual visibility rule for handoff pending:
        # - current supervisor sees it as "awaiting acceptance"
        # - next supervisor sees it to accept/reject
        if current_status == _MS_PENDING_CONFIRMATION:
            if role_name not in {current_role, next_role, supervisor_role}:
                continue
        else:
            if role_name != supervisor_role and role_name != current_role:
                continue
            if not _is_pending_ms_status(current_status):
                continue
        batch_id = _normalize_ref(fields.get("batch_id"))
        if not isinstance(batch_id, int):
            continue
        master_fields = master_fields_by_id.get(batch_id, {})
        if str(master_fields.get("approval_status") or "") != "Approved":
            continue
        batch_ids.add(batch_id)
        rows.append(record)

    batch_no_map = _get_batch_no_map(repo, batch_ids)
    rows.sort(
        key=lambda row: (
            batch_no_map.get(_normalize_ref(row.get("fields", {}).get("batch_id")), ""),
            _resolve_ms_row_part_text(repo, row),
            row.get("id", 0),
        )
    )
    return rows


def _list_all_ms_jobs_for_visibility(repo: ProductionRepo) -> list[dict]:
    master_fields_by_id = {
        record.get("id"): record.get("fields", {})
        for record in repo.get_all_master_batches()
        if isinstance(record.get("id"), int)
    }
    rows = []
    batch_ids = set()
    for record in repo.costing_client.get_records("ProductBatchMS"):
        fields = record.get("fields", {})
        batch_id = _normalize_ref(fields.get("batch_id"))
        if not isinstance(batch_id, int):
            continue
        master_fields = master_fields_by_id.get(batch_id, {})
        if str(master_fields.get("approval_status") or "") != "Approved":
            continue
        batch_ids.add(batch_id)
        rows.append(record)
    batch_no_map = _get_batch_no_map(repo, batch_ids)
    rows.sort(
        key=lambda row: (
            batch_no_map.get(_normalize_ref(row.get("fields", {}).get("batch_id")), ""),
            _resolve_ms_row_part_text(repo, row),
            row.get("id", 0),
        )
    )
    return rows


def _filter_ms_jobs(records: list[dict], mode: str) -> list[dict]:
    if mode == _MS_VIEW_BY_NEXT_STAGE:
        filtered = [
            row
            for row in records
            if str(row.get("fields", {}).get("next_stage_name") or "").strip()
        ]
        filtered.sort(key=lambda row: (str(row.get("fields", {}).get("next_stage_name") or ""), row.get("id", 0)))
        return filtered
    return list(records)


async def _show_my_ms_jobs_filter_menu(update, context) -> None:
    context.user_data["menu_state"] = MY_MS_JOBS_FILTER_STATE
    await _reply(
        update,
        "My MS Jobs\nChoose list view:",
        [[_MS_VIEW_ACTION_REQUIRED], [_MS_VIEW_ALL], [_MS_VIEW_BY_NEXT_STAGE], [_MS_VIEW_BY_CREATED_BY], [_MS_VIEW_BY_BATCH_NO], [BACK_LABEL]],
    )


def _get_my_ms_jobs_next_stage_options(records: list[dict]) -> list[str]:
    options = {
        str(row.get("fields", {}).get("next_stage_name") or "").strip()
        for row in records
        if str(row.get("fields", {}).get("next_stage_name") or "").strip()
    }
    return sorted(options)


def _get_my_ms_jobs_creator_options(records: list[dict], creator_by_batch: dict[int, str]) -> list[str]:
    options: set[str] = set()
    for row in records:
        batch_id = _normalize_ref(row.get("fields", {}).get("batch_id"))
        if not isinstance(batch_id, int):
            continue
        creator_name = str(creator_by_batch.get(batch_id) or "").strip()
        if creator_name:
            options.add(creator_name)
    return sorted(options)


def _get_my_ms_jobs_batch_options(records: list[dict], batch_no_by_id: dict[int, str]) -> list[str]:
    options: set[str] = set()
    for row in records:
        batch_id = _normalize_ref(row.get("fields", {}).get("batch_id"))
        if not isinstance(batch_id, int):
            continue
        batch_no = str(batch_no_by_id.get(batch_id) or "").strip()
        if batch_no:
            options.add(batch_no)
    return sorted(options)


async def _show_my_ms_jobs_next_stage_filter_page(update, context) -> None:
    selection = context.user_data.get("my_ms_jobs_next_stage_selection", {})
    options = selection.get("options", [])
    page = selection.get("page", 0)
    page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_options, _, end = _paginate(options, page, page_size)

    lines = ["Select Next Stage:", "Enter one number like 1", ""]
    for idx, stage_name in enumerate(page_options, start=1):
        lines.append(f"{idx}. {stage_name}")
    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(options):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


async def _show_my_ms_jobs_created_by_filter_page(update, context) -> None:
    selection = context.user_data.get("my_ms_jobs_created_by_selection", {})
    options = selection.get("options", [])
    page = selection.get("page", 0)
    page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_options, _, end = _paginate(options, page, page_size)

    lines = ["Select Batch Creator:", "Enter one number like 1", ""]
    for idx, creator_name in enumerate(page_options, start=1):
        lines.append(f"{idx}. {creator_name}")
    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(options):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


async def _show_my_ms_jobs_page(update, context) -> None:
    selection = context.user_data.get("my_ms_jobs_selection", {})
    records = selection.get("records", [])
    page = selection.get("page", 0)
    page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_records, _, end = _paginate(records, page, page_size)

    if not records:
        if context.user_data.get("my_ms_jobs_all_records"):
            if context.user_data.get("my_ms_jobs_filter") == _MS_VIEW_ACTION_REQUIRED:
                await _reply(update, "No pending handoff or pending completion jobs in your queue.")
                await _show_my_ms_jobs_filter_menu(update, context)
                return
            await _reply(update, "No MS jobs for selected view. Choose another view.")
            await _show_my_ms_jobs_filter_menu(update, context)
            return
        set_main_menu_state(context)
        await _reply(update, "No MS jobs in your current stage queue.")
        return

    repo = ProductionRepo()
    user = context.user_data.get("user", {})
    viewer_role = repo.get_role_name_by_user_id(user.get("user_id", ""))
    batch_ids = {_normalize_ref(row.get("fields", {}).get("batch_id")) for row in page_records}
    normalized_batch_ids = {bid for bid in batch_ids if isinstance(bid, int)}
    batch_no_map = _get_batch_no_map(repo, normalized_batch_ids)
    creator_by_batch = context.user_data.get("my_ms_jobs_creator_by_batch")
    if not isinstance(creator_by_batch, dict):
        creator_by_batch = _get_batch_creator_name_map(repo, normalized_batch_ids)
        context.user_data["my_ms_jobs_creator_by_batch"] = creator_by_batch

    mode = context.user_data.get("my_ms_jobs_filter", _MS_VIEW_ACTION_REQUIRED)
    lines = [
        "My MS Jobs",
        f"View: {selection.get('view_mode', _MS_VIEW_ACTION_REQUIRED)}",
        "Choose entries using: `1` or `1,3`",
        "Quick actions: `D1` Done | `C1` Accept | `N1` Reject | `R1` Remarks | `V1` View List | `H1` Hold | `B1` Batch Summary | `X1,3` Bulk Mode",
        "Optional: `S1` Schedule Batch",
        "",
    ]

    last_group_key = None
    for idx, row in enumerate(page_records, start=1):
        fields = row.get("fields", {})
        batch_id = _normalize_ref(fields.get("batch_id"))
        batch_no = batch_no_map.get(batch_id, str(batch_id or ""))
        batch_by = str(creator_by_batch.get(batch_id) or "")
        stage_name = str(fields.get("current_stage_name") or "")
        next_stage = str(fields.get("next_stage_name") or "")
        qty = fields.get("total_qty") if fields.get("total_qty") is not None else fields.get("required_qty")
        qty_text = _format_qty(float(qty or 0))
        part_label = _resolve_ms_row_part_text(repo, fields)
        status_text = str(fields.get("current_status") or fields.get("status") or "")
        if status_text == _MS_PENDING_CONFIRMATION:
            process_seq = _normalize_process_seq(fields)
            current_role = _resolve_supervisor_role_for_stage(repo, process_seq, stage_name)
            next_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage) if next_stage else ""
            if viewer_role == next_role:
                status_text = f"{_MS_PENDING_CONFIRMATION} (Action: Accept/Reject)"
            elif viewer_role == current_role:
                status_text = f"{_MS_PENDING_CONFIRMATION} (Waiting for {next_stage or 'next stage'})"

        if mode == _MS_VIEW_BY_NEXT_STAGE:
            group_key = (batch_no, stage_name, next_stage)
        elif mode == _MS_VIEW_BY_CREATED_BY:
            group_key = (batch_no, stage_name, batch_by)
        else:
            group_key = (batch_no, stage_name)

        if group_key != last_group_key:
            if last_group_key is not None:
                lines.append("")
            lines.extend(
                _build_ms_jobs_group_header(
                    mode=mode,
                    batch_no=batch_no,
                    current_stage=stage_name,
                    next_stage=next_stage,
                    created_by=batch_by,
                )
            )
            lines.append("")
            last_group_key = group_key

        lines.append(
            _build_ms_job_entry_text(
                index=idx,
                part_name=part_label,
                next_stage=next_stage,
                qty=qty_text,
                status=status_text,
                remarks=str(fields.get("supervisor_remarks") or ""),
                mode=mode,
            )
        )

    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(records):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


async def _show_my_ms_jobs_batch_filter_page(update, context) -> None:
    selection = context.user_data.get("my_ms_jobs_batch_selection", {})
    options = selection.get("options", [])
    page = selection.get("page", 0)
    page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_options, _, end = _paginate(options, page, page_size)

    lines = ["Select Batch No:", "Enter one number like 1", ""]
    for idx, batch_no in enumerate(page_options, start=1):
        lines.append(f"{idx}. {batch_no}")
    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(options):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


def _list_trackable_batches(repo: ProductionRepo) -> list[dict]:
    counts_by_batch: dict[int, int] = {}
    for row in repo.costing_client.get_records("ProductBatchMS"):
        batch_id = _normalize_ref(row.get("fields", {}).get("batch_id"))
        if not isinstance(batch_id, int):
            continue
        counts_by_batch[batch_id] = counts_by_batch.get(batch_id, 0) + 1

    records: list[dict] = []
    for master in repo.get_all_master_batches():
        batch_id = master.get("id")
        if not isinstance(batch_id, int):
            continue
        flow_count = counts_by_batch.get(batch_id, 0)
        if flow_count <= 0:
            continue
        fields = master.get("fields", {})
        records.append(
            {
                "batch_id": batch_id,
                "batch_no": str(fields.get("batch_no") or ""),
                "overall_status": str(fields.get("overall_status") or ""),
                "start_date": _format_dt_short(_parse_iso_datetime(fields.get("start_date"))),
                "flow_count": flow_count,
            }
        )
    records.sort(key=lambda row: row.get("batch_id", 0), reverse=True)
    return records


async def _show_view_batch_page(update, context) -> None:
    selection = context.user_data.get("view_batch_selection", {})
    records = selection.get("records", [])
    page = selection.get("page", 0)
    page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_records, _, end = _paginate(records, page, page_size)

    if not records:
        set_main_menu_state(context)
        await _reply(update, "No production batches found for tracking.")
        return

    lines = ["View Batch", "Select one batch number:", ""]
    for idx, row in enumerate(page_records, start=1):
        lines.append(
            f"{idx}. {row.get('batch_no') or '-'} | Start: {row.get('start_date') or '-'} | Status: {row.get('overall_status') or '-'} | Flows: {row.get('flow_count')}"
        )

    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(records):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


async def start_view_batch(update, context) -> None:
    repo = ProductionRepo()
    role_name = _resolve_user_role_name(repo, context)
    if role_name not in ("System_Admin", "Production_Manager"):
        await _reply(update, "Only System Admin and Production Manager can use View Batch.")
        return
    records = _list_trackable_batches(repo)
    if not records:
        set_main_menu_state(context)
        await _reply(update, "No production batches found for tracking.")
        return
    context.user_data["view_batch_selection"] = {
        "records": records,
        "page": 0,
        "page_size": settings.MSCUTLIST_PAGE_SIZE,
    }
    context.user_data["menu_state"] = VIEW_BATCH_SELECTION_STATE
    await _show_view_batch_page(update, context)


async def start_my_ms_jobs(update, context) -> None:
    repo = ProductionRepo()
    repo.ensure_ms_workflow_columns()
    user = context.user_data.get("user", {})
    role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
    all_records = _list_all_ms_jobs_for_visibility(repo)
    action_records = _list_ms_jobs_for_user_role(repo, role_name)
    if not all_records:
        set_main_menu_state(context)
        await _reply(update, "No approved MS jobs available.")
        return

    context.user_data["my_ms_jobs_all_records"] = all_records
    context.user_data["my_ms_jobs_action_records"] = action_records
    context.user_data["my_ms_jobs_filter"] = _MS_VIEW_ACTION_REQUIRED
    context.user_data["my_ms_jobs_filter_value"] = ""
    batch_ids = {
        _normalize_ref(row.get("fields", {}).get("batch_id"))
        for row in all_records
        if isinstance(_normalize_ref(row.get("fields", {}).get("batch_id")), int)
    }
    context.user_data["my_ms_jobs_creator_by_batch"] = _get_batch_creator_name_map(repo, batch_ids)
    context.user_data["my_ms_jobs_batch_no_by_id"] = _get_batch_no_map(repo, batch_ids)
    context.user_data["my_ms_jobs_selection"] = {
        "records": action_records,
        "page": 0,
        "page_size": settings.MSCUTLIST_PAGE_SIZE,
        "view_mode": _MS_VIEW_ACTION_REQUIRED,
    }
    await _show_my_ms_jobs_filter_menu(update, context)


async def _show_my_ms_schedule_page(update, context) -> None:
    selection = context.user_data.get("my_ms_schedule_selection", {})
    records = selection.get("records", [])
    page = selection.get("page", 0)
    page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_records, _, end = _paginate(records, page, page_size)

    if not records:
        set_main_menu_state(context)
        await _reply(update, "No batches pending schedule in your supervisor queue.")
        return

    lines = ["Schedule Batches:", "Enter number(s) like 1 or 1,3", ""]
    for idx, row in enumerate(page_records, start=1):
        roles = ", ".join(row.get("roles", []))
        lines.append(
            f"{idx}. Batch: {row.get('batch_no', '')} | Open Days: {row.get('days_open', 0)} | Roles: {roles}"
        )
    lines.append("")
    lines.append("After selection, choose schedule date (Today/Tomorrow or YYYY-MM-DD).")

    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(records):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


async def start_my_ms_schedule(update, context) -> None:
    repo = ProductionRepo()
    repo.ensure_ms_workflow_columns()
    role_name = _resolve_user_role_name(repo, context)
    records = _list_schedule_batches_for_user_role(repo, role_name)
    if not records:
        set_main_menu_state(context)
        await _reply(update, "No batches pending schedule in your queue.")
        return
    context.user_data["my_ms_schedule_selection"] = {
        "records": records,
        "page": 0,
        "page_size": settings.MSCUTLIST_PAGE_SIZE,
    }
    context.user_data["menu_state"] = MY_MS_SCHEDULE_SELECTION_STATE
    await _show_my_ms_schedule_page(update, context)


def _apply_my_ms_jobs_filter(
    records: list[dict],
    action_records: list[dict],
    mode: str,
    mode_value: str,
    creator_by_batch: dict[int, str],
    batch_no_by_id: dict[int, str],
) -> tuple[list[dict], str]:
    base_records = action_records if mode == _MS_VIEW_ACTION_REQUIRED else records
    filtered = _filter_ms_jobs(base_records, mode)
    if mode == _MS_VIEW_BY_NEXT_STAGE and mode_value:
        filtered = [
            row
            for row in base_records
            if str(row.get("fields", {}).get("next_stage_name") or "").strip() == mode_value
        ]
    if mode == _MS_VIEW_BY_CREATED_BY and mode_value:
        filtered = []
        for row in base_records:
            batch_id = _normalize_ref(row.get("fields", {}).get("batch_id"))
            if not isinstance(batch_id, int):
                continue
            if str(creator_by_batch.get(batch_id) or "").strip() == mode_value:
                filtered.append(row)
    if mode == _MS_VIEW_BY_BATCH_NO and mode_value:
        filtered = []
        for row in base_records:
            batch_id = _normalize_ref(row.get("fields", {}).get("batch_id"))
            if not isinstance(batch_id, int):
                continue
            if str(batch_no_by_id.get(batch_id) or "").strip() == mode_value:
                filtered.append(row)
    view_mode = f"{mode}: {mode_value}" if mode_value else mode
    return filtered, view_mode


def _refresh_my_ms_jobs_selection(context, repo: ProductionRepo, role_name: str) -> list[dict]:
    all_rows = _list_all_ms_jobs_for_visibility(repo)
    action_rows = _list_ms_jobs_for_user_role(repo, role_name)
    mode = context.user_data.get("my_ms_jobs_filter", _MS_VIEW_ACTION_REQUIRED)
    mode_value = str(context.user_data.get("my_ms_jobs_filter_value") or "").strip()
    batch_ids = {
        _normalize_ref(row.get("fields", {}).get("batch_id"))
        for row in all_rows
        if isinstance(_normalize_ref(row.get("fields", {}).get("batch_id")), int)
    }
    creator_by_batch = _get_batch_creator_name_map(repo, batch_ids)
    batch_no_by_id = _get_batch_no_map(repo, batch_ids)
    filtered_rows, view_mode = _apply_my_ms_jobs_filter(
        all_rows,
        action_rows,
        mode,
        mode_value,
        creator_by_batch,
        batch_no_by_id,
    )

    context.user_data["my_ms_jobs_all_records"] = all_rows
    context.user_data["my_ms_jobs_action_records"] = action_rows
    context.user_data["my_ms_jobs_creator_by_batch"] = creator_by_batch
    context.user_data["my_ms_jobs_batch_no_by_id"] = batch_no_by_id
    context.user_data["my_ms_jobs_selection"] = {
        "records": filtered_rows,
        "page": 0,
        "page_size": settings.MSCUTLIST_PAGE_SIZE,
        "view_mode": view_mode,
    }
    return action_rows


async def _show_ms_jobs_confirmation(update, context) -> None:
    data = context.user_data.get("my_ms_jobs_confirm", {})
    selected_rows = data.get("selected_rows", [])
    if not selected_rows:
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _show_my_ms_jobs_page(update, context)
        return

    lines = ["Mark selected MS jobs as Done?"]
    repo = ProductionRepo()
    for row in selected_rows:
        fields = row.get("fields", {})
        part_label = _resolve_ms_row_part_text(repo, fields)
        lines.append(
            f"- {part_label} | {fields.get('current_stage_name') or ''} | {fields.get('current_status') or fields.get('status') or ''}"
        )
    context.user_data["menu_state"] = MY_MS_JOBS_CONFIRM_STATE
    await _reply(update, "\n".join(lines), [[_YES], [_NO], [BACK_LABEL]])


async def _show_ms_job_action_menu(update, context, selected_record: dict) -> None:
    fields = selected_record.get("fields", {})
    repo = ProductionRepo()
    part_label = _resolve_ms_row_part_text(repo, fields)
    stage_name = str(fields.get("current_stage_name") or "")
    context.user_data["my_ms_jobs_action"] = {"selected_record": selected_record}
    context.user_data["menu_state"] = MY_MS_JOBS_ACTION_STATE
    await _reply(
        update,
        f"Selected: {part_label} | {stage_name}\nChoose action:",
        [[_MS_ACTION_DONE], [_MS_ACTION_CONFIRM_HANDOFF], [_MS_ACTION_REJECT_HANDOFF], [_MS_ACTION_REMARKS], [_MS_ACTION_VIEW_LIST], [_MS_ACTION_HOLD], [BACK_LABEL]],
    )


async def _reject_ms_handoff_with_remarks(
    repo: ProductionRepo,
    context,
    row_id: int,
    updated_by,
    user_role_name: str,
    remarks_text: str,
) -> bool:
    row = repo.get_ms_row_by_id(row_id)
    if not row:
        return False
    fields = row.get("fields", {})
    batch_id = _normalize_ref(fields.get("batch_id"))
    if not isinstance(batch_id, int):
        return False
    status = str(fields.get("current_status") or fields.get("status") or "").strip()
    if status != _MS_PENDING_CONFIRMATION:
        return False

    process_seq = _normalize_process_seq(fields)
    current_stage = str(fields.get("current_stage_name") or "").strip()
    next_stage = str(fields.get("next_stage_name") or "").strip()
    current_role = _resolve_supervisor_role_for_stage(repo, process_seq, current_stage)
    next_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage) if next_stage else ""
    if user_role_name != next_role:
        return False

    new_status = f"{current_stage} Pending" if current_stage else "Pending"
    now_iso = _now_iso()
    old_remarks = str(fields.get("supervisor_remarks") or "").strip()
    reject_note = f"Handoff rejected by {user_role_name}: {remarks_text}".strip()
    merged_remarks = reject_note if not old_remarks else f"{old_remarks}\n{reject_note}"

    repo.update_ms(
        row_id,
        repo.filter_table_fields(
            "ProductBatchMS",
            {
                "current_status": new_status,
                "status": new_status,
                "current_stage_role_name": current_role or "",
                "supervisor_remarks": merged_remarks,
                "updated_at": now_iso,
                "last_updated_by": updated_by,
            },
        ),
    )
    repo.add_status_history(
        batch_id,
        "MS",
        row_id,
        status,
        new_status,
        updated_by,
        reject_note,
    )

    part_name = _resolve_ms_row_part_text(repo, fields)
    batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")
    batch_by = _get_batch_creator_name_map(repo, {batch_id}).get(batch_id, "")
    if current_role:
        await _notify_stage_event(
            context,
            "ms_stage_pending",
            batch_id,
            _build_ms_stage_pending_message(
                batch_no=batch_no,
                batch_by=batch_by,
                part_name=part_name,
                current_stage=current_stage,
                next_stage=next_stage,
                qty=_format_qty(float(fields.get("total_qty") or fields.get("required_qty") or 0)),
                title="Handoff Rejected - Rework Required",
            )
            + f"\nRemarks: {remarks_text}",
            supervisor_role=current_role,
            reply_markup=build_stage_inline_keyboard(batch_id, row_id),
        )
    return True


async def _execute_ms_job_action(update, context, action_code: str, selected_record: dict) -> bool:
    row_id = selected_record.get("id")
    if not isinstance(row_id, int):
        await _reply(update, "Invalid row selected.")
        return True

    repo = ProductionRepo()
    user = context.user_data.get("user", {})
    user_role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
    updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))
    fields = selected_record.get("fields", {})
    batch_id = _normalize_ref(fields.get("batch_id"))

    if action_code == "B":
        if not isinstance(batch_id, int):
            await _reply(update, "Batch not found for selected row.")
            return True
        batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")
        await _show_ms_batch_tracker_overview(update, batch_id, batch_no)
        return True

    if action_code in ("P", "V"):
        try:
            sent = await _send_ms_row_pdf_for_chat(repo, context.bot, update.effective_chat.id, row_id)
        except Exception:
            await _reply(update, "Could not fetch PDF for this row.")
            return True
        if not sent:
            await _reply(update, "No PDF attachment found for this row/batch.")
            return True
        return True

    if action_code == "S":
        if not isinstance(batch_id, int):
            await _reply(update, "Batch not found for selected row.")
            return True
        if not _is_batch_schedulable_for_role(repo, batch_id, user_role_name):
            await _reply(update, "You are not authorized to schedule this batch or it is already scheduled.")
            return True
        await _prompt_schedule_date(
            update,
            context,
            [batch_id],
            return_state=MY_MS_JOBS_SELECTION_STATE,
            title="Schedule selected batch",
        )
        return True

    if action_code in ("D", "C", "N"):
        row = repo.get_ms_row_by_id(row_id)
        if not row:
            await _reply(update, "MS row not found.")
            return True
        row_fields = row.get("fields", {})
        row_status = str(row_fields.get("current_status") or row_fields.get("status") or "").strip()
        process_seq = _normalize_process_seq(row_fields)
        stage_name = str(row_fields.get("current_stage_name") or "").strip()
        next_stage = str(row_fields.get("next_stage_name") or "").strip()
        current_role = _resolve_supervisor_role_for_stage(repo, process_seq, stage_name)
        next_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage) if next_stage else ""
        if row_status == _MS_PENDING_CONFIRMATION:
            if action_code == "N":
                if user_role_name != next_role:
                    await _reply(update, "Only the next-stage supervisor can reject this handover.")
                    return True
                context.user_data["my_ms_jobs_handoff_reject_remarks"] = {"row_id": row_id, "batch_id": batch_id}
                context.user_data["menu_state"] = MY_MS_JOBS_HANDOFF_REJECT_REMARKS_STATE
                await _reply(update, "Enter rejection remarks for this handoff:", [[BACK_LABEL]])
                return True
            if user_role_name != next_role:
                await _reply(update, "Only the next-stage supervisor can confirm this handover.")
                return True
            try:
                await advance_ms_stage(repo, context, row_id, updated_by)
            except Exception:
                await _reply(update, "Could not confirm and advance this stage.")
                return True
            await _reply(update, "Stage handover confirmed.")
            return True

        if action_code in ("C", "N"):
            await _reply(update, "This row is not waiting for handoff confirmation.")
            return True

        if user_role_name != current_role:
            await _reply(update, "You are not authorized for this stage.")
            return True
        try:
            await _mark_ms_stage_done_pending_confirmation(repo, context, row_id, updated_by)
        except Exception:
            await _reply(update, "Could not update this stage.")
            return True
        await _reply(update, "Current stage marked done. Waiting for next-stage confirmation.")
        return True

    if action_code == "R":
        context.user_data["my_ms_jobs_remarks"] = {"row_id": row_id, "batch_id": batch_id}
        context.user_data["menu_state"] = MY_MS_JOBS_REMARKS_STATE
        await _reply(update, "Enter remarks text for selected row:", [[BACK_LABEL]])
        return True

    if action_code == "H":
        row = repo.get_ms_row_by_id(row_id)
        if not row:
            await _reply(update, "MS row not found.")
            return True
        row_fields = row.get("fields", {})
        old_status = str(row_fields.get("current_status") or row_fields.get("status") or "")
        hold_status = "On Hold"
        repo.update_ms(
            row_id,
            repo.filter_table_fields(
                "ProductBatchMS",
                {
                    "current_status": hold_status,
                    "status": hold_status,
                    "updated_at": _now_iso(),
                    "last_updated_by": updated_by,
                },
            ),
        )
        if isinstance(batch_id, int):
            repo.add_status_history(
                batch_id,
                "MS",
                row_id,
                old_status,
                hold_status,
                updated_by,
                "Marked as hold by supervisor.",
            )
            part_name = _resolve_ms_row_part_text(repo, row_fields)
            batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")
            await _notify_roles(
                context,
                batch_id,
                f"🔴 MS row put on hold.\n📦 Batch: {batch_no}\n🧩 Part: {part_name}\n🏷️ Status: {hold_status}",
                ["Production_Supervisor", "Production Supervisor", "System_Admin"],
            )
        await _reply(update, "Row marked as hold.")
        return True

    return False


async def _mark_ms_stage_done_pending_confirmation(repo: ProductionRepo, context, row_id: int, updated_by) -> dict:
    row = repo.get_ms_row_by_id(row_id)
    if not row:
        raise ValueError("MS row not found.")

    fields = row.get("fields", {})
    batch_id = _normalize_ref(fields.get("batch_id"))
    if not isinstance(batch_id, int):
        raise ValueError("Invalid batch reference in MS row.")
    process_seq = _normalize_process_seq(fields)
    stages = repo.get_process_stage_names(process_seq)
    if not stages:
        raise ValueError("Missing process sequence on MS row.")

    current_stage_index = int(fields.get("current_stage_index") or 0)
    current_stage_name = str(fields.get("current_stage_name") or stages[min(current_stage_index, len(stages) - 1)])
    next_stage = _get_next_stage_name(stages, current_stage_index)
    old_status = str(fields.get("current_status") or fields.get("status") or "")
    if old_status == _MS_PENDING_CONFIRMATION:
        return row

    if not next_stage:
        return await advance_ms_stage(repo, context, row_id, updated_by)

    now_iso = _now_iso()
    new_status = _MS_PENDING_CONFIRMATION
    next_stage_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage)
    update_fields = {
        "current_status": new_status,
        "status": new_status,
        "current_stage_role_name": next_stage_role or "",
        "updated_at": now_iso,
        "last_updated_by": updated_by,
    }
    safe_updates = repo.filter_table_fields("ProductBatchMS", update_fields)
    repo.update_ms(row_id, safe_updates)
    repo.add_status_history(
        batch_id,
        "MS",
        row_id,
        old_status,
        new_status,
        updated_by,
        f"Current stage marked done. Awaiting confirmation from {next_stage}.",
    )

    part_name = _resolve_ms_row_part_text(repo, fields)
    batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")
    batch_by = _get_batch_creator_name_map(repo, {batch_id}).get(batch_id, "")
    if next_stage_role:
        await _notify_stage_event(
            context,
            "ms_stage_pending",
            batch_id,
            _build_ms_stage_pending_message(
                batch_no=batch_no,
                batch_by=batch_by,
                part_name=part_name,
                current_stage=current_stage_name,
                next_stage=next_stage,
                qty=_format_qty(float(fields.get("total_qty") or fields.get("required_qty") or 0)),
                title="Stage Confirmation Required",
            ),
            supervisor_role=next_stage_role,
            reply_markup=build_stage_confirm_inline_keyboard(batch_id, row_id),
        )
    else:
        await _notify_stage_event(
            context,
            "ms_stage_pending",
            batch_id,
            f"MS stage mapping missing for batch {batch_no}: Stage {next_stage}. Please configure ProcessStage role mapping.",
            supervisor_role="System_Admin",
        )

    recalculate_master_overall_status(repo, batch_id, updated_by)
    return repo.get_ms_row_by_id(row_id) or row


async def _send_ms_row_pdf_for_chat(repo: ProductionRepo, bot, chat_id: int, row_id: int) -> bool:
    row = repo.get_ms_row_by_id(row_id)
    if not row:
        return False
    fields = row.get("fields", {})
    attachment_id, file_name = _extract_first_attachment_ref(fields.get("row_cutlist_pdf"))
    if not attachment_id:
        batch_id = _normalize_ref(fields.get("batch_id"))
        if isinstance(batch_id, int):
            master = repo.get_master_by_id(batch_id)
            master_fields = (master or {}).get("fields", {})
            attachment_id, file_name = _extract_first_attachment_ref(master_fields.get("ms_cutlist_pdf"))
    if not attachment_id:
        return False

    payload = _download_attachment_bytes(repo, attachment_id)
    document = BytesIO(payload)
    document.name = file_name or "ms_cutlist.pdf"
    await bot.send_document(chat_id=chat_id, document=document, filename=document.name)
    return True


async def advance_ms_stage(repo: ProductionRepo, context, row_id: int, updated_by) -> dict:
    row = repo.get_ms_row_by_id(row_id)
    if not row:
        raise ValueError("MS row not found.")
    fields = row.get("fields", {})
    batch_id = _normalize_ref(fields.get("batch_id"))
    if not isinstance(batch_id, int):
        raise ValueError("Invalid batch reference in MS row.")
    process_seq = _normalize_process_seq(fields)
    stages = repo.get_process_stage_names(process_seq)
    if not stages:
        raise ValueError("Missing process sequence on MS row.")

    current_stage_index = int(fields.get("current_stage_index") or 0)
    current_stage_name = str(fields.get("current_stage_name") or stages[min(current_stage_index, len(stages) - 1)])
    old_status = str(fields.get("current_status") or fields.get("status") or "")
    next_index = current_stage_index + 1
    next_stage = stages[next_index] if 0 <= next_index < len(stages) else None
    now_iso = _now_iso()

    current_stage_role = _resolve_supervisor_role_for_stage(repo, process_seq, current_stage_name)
    if next_stage is None:
        new_status = "Cutting Completed"
        history_remarks = "MS workflow completed without further stages."
        update_fields = {
            "current_stage_index": next_index,
            "next_stage_name": "",
            "current_status": new_status,
            "current_stage_role_name": "",
            "updated_at": now_iso,
            "last_updated_by": updated_by,
            "status": new_status,
        }
    elif next_stage == stages[-1]:
        new_status = f"In {next_stage}"
        history_remarks = f"Previous stages completed and handed over to {next_stage}."
        update_fields = {
            "current_stage_index": next_index,
            "current_stage_name": next_stage,
            "next_stage_name": _get_next_stage_name(stages, next_index),
            "current_stage_role_name": _resolve_supervisor_role_for_stage(repo, process_seq, next_stage) or "",
            "current_status": new_status,
            "updated_at": now_iso,
            "last_updated_by": updated_by,
            "status": new_status,
        }
    else:
        new_status = f"{next_stage} Pending"
        history_remarks = f"Advanced to next stage: {next_stage}."
        update_fields = {
            "current_stage_index": next_index,
            "current_stage_name": next_stage,
            "next_stage_name": _get_next_stage_name(stages, next_index),
            "current_stage_role_name": _resolve_supervisor_role_for_stage(repo, process_seq, next_stage) or "",
            "current_status": new_status,
            "updated_at": now_iso,
            "last_updated_by": updated_by,
            "status": new_status,
        }

    safe_updates = repo.filter_table_fields("ProductBatchMS", update_fields)
    repo.update_ms(row_id, safe_updates)
    repo.add_status_history(batch_id, "MS", row_id, old_status, new_status, updated_by, history_remarks)

    part_name = _resolve_ms_row_part_text(repo, fields)
    batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")
    batch_by = _get_batch_creator_name_map(repo, {batch_id}).get(batch_id, "")

    await _notify_stage_event(
        context,
        "ms_stage_completed",
        batch_id,
        f"MS stage completed for batch {batch_no}: {part_name} | Stage: {current_stage_name} | Status: {new_status}",
        supervisor_role=current_stage_role,
    )

    if next_stage:
        next_stage_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage)
        if not next_stage_role:
            await _notify_stage_event(
                context,
                "ms_stage_pending",
                batch_id,
                f"MS stage mapping missing for batch {batch_no}: Stage {next_stage}. Please configure ProcessStage role mapping.",
                supervisor_role="System_Admin",
            )
            return repo.get_ms_row_by_id(row_id) or row
        markup = build_stage_inline_keyboard(batch_id, row_id)
        await _notify_stage_event(
            context,
            "ms_stage_pending",
            batch_id,
            _build_ms_stage_pending_message(
                batch_no=batch_no,
                batch_by=batch_by,
                part_name=part_name,
                current_stage=next_stage,
                next_stage=_get_next_stage_name(stages, next_index),
                qty=_format_qty(float(fields.get("total_qty") or fields.get("required_qty") or 0)),
                title="MS Stage Task",
            ),
            supervisor_role=next_stage_role,
            reply_markup=markup,
        )
    recalculate_master_overall_status(repo, batch_id, updated_by)

    return repo.get_ms_row_by_id(row_id) or row


def recalculate_master_overall_status(repo: ProductionRepo, batch_id: int, updated_by) -> str:
    master = repo.get_master_by_id(batch_id)
    if not master:
        return ""

    fields = master.get("fields", {})
    old_status = fields.get("overall_status") or ""
    approval = fields.get("approval_status") or ""

    if approval == "Pending Approval":
        new_status = "Pending Approval"
    else:
        child_statuses = repo.list_child_statuses(batch_id)
        if child_statuses and all(status in ("Done", "Completed", "Cutting Completed") for status in child_statuses):
            new_status = "Completed"
        elif any(status == "In Progress" for status in child_statuses):
            new_status = "In Progress"
        elif any(status.startswith("In ") for status in child_statuses):
            new_status = "In Progress"
        elif child_statuses and all(status == "Schedule Pending" for status in child_statuses):
            new_status = "Schedule Pending"
        elif any(status == _MS_PENDING_CONFIRMATION for status in child_statuses):
            new_status = "In Progress"
        elif any(status.endswith("Pending") for status in child_statuses):
            new_status = "In Progress"
        else:
            new_status = old_status or "Schedule Pending"

    if new_status != old_status:
        updates = {"overall_status": new_status}
        if new_status == "Completed" and not fields.get("completion_date"):
            updates["completion_date"] = _now_iso()

        repo.update_master(batch_id, updates)
        repo.add_status_history(batch_id, "Master", batch_id, old_status, new_status, updated_by, "")

        if new_status == "Completed":
            repo.add_lifecycle_history(batch_id, "Completed", updated_by, "Master batch marked completed")

    return new_status


def _resolve_part_ids_for_master(repo: ProductionRepo, fields: dict) -> list[int]:
    selected_part_ids = _from_int_list_csv(str(fields.get("selected_part_ids") or ""))
    if selected_part_ids:
        return selected_part_ids

    model_code = fields.get("product_model")
    if not model_code:
        return []
    parts = repo.get_product_parts_for_model(model_code)
    return [part["part_id"] for part in parts]


def approve_batch_service(repo: ProductionRepo, batch_id: int, approved_by) -> dict:
    record = repo.get_master_by_id(batch_id)
    if not record:
        raise ValueError("Batch not found.")

    fields = record.get("fields", {})
    if fields.get("approval_status") != "Pending Approval":
        return {
            "master": record,
            "ms_rows": [],
            "ms_row_ids": [],
            "cutlist_sections": [],
            "row_cutlist_map": {},
        }

    now_iso = _now_iso()
    old_approval = fields.get("approval_status") or ""
    old_overall = fields.get("overall_status") or ""

    repo.update_master(
        batch_id,
        {
            "approval_status": "Approved",
            "approval_date": now_iso,
            "approved_by": approved_by,
            "start_date": now_iso,
            "overall_status": "Schedule Pending",
        },
    )
    repo.add_status_history(batch_id, "Master", batch_id, old_approval, "Approved", approved_by, "Batch approved")
    if old_overall != "Schedule Pending":
        repo.add_status_history(batch_id, "Master", batch_id, old_overall, "Schedule Pending", approved_by, "")
    repo.add_lifecycle_history(batch_id, "Batch Approved", approved_by, "Batch approved by manager/admin")

    include_ms = bool(fields.get("include_ms"))
    part_ids: list[int] = []
    ms_rows: list[dict] = []
    ms_row_ids: list[int] = []
    cutlist_sections: list[dict] = []
    row_cutlist_map: dict[str, dict] = {}
    if include_ms:
        repo.ensure_ms_workflow_columns()
        part_ids = _resolve_part_ids_for_master(repo, fields)
        batch_qty = int(fields.get("qty") or 0)
        ms_rows = _build_ms_rows(repo, batch_id, part_ids, batch_qty, timestamp_iso=now_iso, updated_by=approved_by)
        ms_row_ids = repo.create_ms_rows(ms_rows)
        for index, row_id in enumerate(ms_row_ids):
            if index < len(ms_rows):
                ms_rows[index]["id"] = row_id
        cutlist_sections = _build_ms_cutlist_sections(repo, part_ids, batch_qty)
        row_cutlist_map = _build_ms_row_cutlist_map(repo, part_ids, batch_qty)

    return {
        "master": repo.get_master_by_id(batch_id) or record,
        "ms_rows": ms_rows,
        "ms_row_ids": ms_row_ids,
        "cutlist_sections": cutlist_sections,
        "row_cutlist_map": row_cutlist_map,
    }


async def approve_batches_by_ids(update, context, batch_ids: list[int]) -> list[str]:
    if not _is_production_manager(context):
        await _reply(update, "Only Production Manager or System Admin can approve batches.")
        return []

    repo = ProductionRepo()
    user = context.user_data.get("user", {})
    approved_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))
    approved_batch_numbers = []

    for batch_id in batch_ids:
        updated = approve_batch_service(repo, batch_id, approved_by)
        master_record = updated.get("master", {})
        fields = master_record.get("fields", {})
        batch_no = fields.get("batch_no", "")
        if batch_no:
            approved_batch_numbers.append(batch_no)
            try:
                _attach_ms_cutlist_pdf(repo, batch_id, batch_no, updated.get("cutlist_sections", []))
            except Exception:
                pass
            try:
                _attach_ms_row_cutlist_pdfs(
                    repo,
                    batch_no,
                    updated.get("ms_rows", []),
                    updated.get("row_cutlist_map", {}),
                )
            except Exception:
                pass
            await _notify_ms_first_stage(repo, context, batch_id, updated.get("ms_rows", []), batch_no)
            await _notify_event(
                context.bot,
                "production_batch_approved",
                (
                    f"Batch approved: {batch_no} | "
                    f"Start Date: {_format_notification_datetime(fields.get('start_date'))} | "
                    "Status: Schedule Pending"
                ),
                context={"batch_id": batch_id},
            )

    return approved_batch_numbers


def reject_batch_service(repo: ProductionRepo, batch_id: int, rejected_by) -> dict:
    record = repo.get_master_by_id(batch_id)
    if not record:
        raise ValueError("Batch not found.")

    fields = record.get("fields", {})
    if fields.get("approval_status") != "Pending Approval":
        return record

    now_iso = _now_iso()
    old_approval = fields.get("approval_status") or ""
    old_overall = fields.get("overall_status") or ""

    repo.update_master(
        batch_id,
        {
            "approval_status": "Rejected",
            "approval_date": now_iso,
            "approved_by": rejected_by,
            "overall_status": "Batch Rejected",
        },
    )
    repo.add_status_history(batch_id, "Master", batch_id, old_approval, "Rejected", rejected_by, "Batch rejected")
    if old_overall != "Batch Rejected":
        repo.add_status_history(batch_id, "Master", batch_id, old_overall, "Batch Rejected", rejected_by, "")
    repo.add_lifecycle_history(batch_id, "Batch Rejected", rejected_by, "Batch rejected by manager/admin")

    return repo.get_master_by_id(batch_id) or record


async def reject_batches_by_ids(update, context, batch_ids: list[int]) -> list[str]:
    if not _is_production_manager(context):
        await _reply(update, "Only Production Manager or System Admin can reject batches.")
        return []

    repo = ProductionRepo()
    user = context.user_data.get("user", {})
    rejected_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))
    rejected_batch_numbers = []

    for batch_id in batch_ids:
        updated = reject_batch_service(repo, batch_id, rejected_by)
        fields = updated.get("fields", {})
        batch_no = fields.get("batch_no", "")
        if batch_no:
            rejected_batch_numbers.append(batch_no)
            await _notify_event(
                context.bot,
                "production_batch_rejected",
                f"Batch rejected: {batch_no} | Status: Batch Rejected",
                context={"batch_id": batch_id},
            )

    return rejected_batch_numbers


async def handle_production_callback(update, context) -> bool:
    query = getattr(update, "callback_query", None)
    if not query or not query.data:
        return False

    repo = ProductionRepo()
    user = context.user_data.get("user", {})
    user_role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
    updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))

    parsed_batch_view = _parse_ms_batch_callback_data(str(query.data))
    if parsed_batch_view:
        action, batch_id, arg1, arg2 = parsed_batch_view
        await query.answer()
        batch_no = _resolve_batch_no(repo, batch_id)
        if action == "ov":
            text = _build_ms_batch_snapshot_overview_text(repo, batch_id, batch_no)
            await _edit_ms_batch_tracker_message(query, text, _build_ms_batch_overview_inline_keyboard(batch_id))
            return True
        if action == "vd":
            await _send_ms_batch_first_stage_detail_messages(query, context, batch_id)
            return True
        if action == "sl":
            mode = "d" if arg1 not in ("d", "t") else arg1
            try:
                page = int(arg2 or 0)
            except (TypeError, ValueError):
                page = 0
            await _render_ms_batch_flow_selector(query, repo, batch_id, mode, page)
            return True
        if action in ("fd", "ft", "dn", "cf"):
            try:
                row_id = int(arg1)
            except (TypeError, ValueError):
                await query.message.reply_text("Invalid flow selection.")
                return True
            try:
                page = int(arg2 or 0)
            except (TypeError, ValueError):
                page = 0
            if action == "fd":
                await _render_ms_batch_flow_detail(query, context, repo, batch_id, row_id, page)
                return True
            if action == "ft":
                await _render_ms_batch_flow_timeline(query, context, repo, batch_id, row_id, page)
                return True

            row = repo.get_ms_row_by_id(row_id)
            if not row:
                await query.message.reply_text("MS row not found.")
                return True
            row_fields = row.get("fields", {})
            row_batch_id = _normalize_ref(row_fields.get("batch_id"))
            if row_batch_id != batch_id:
                await query.message.reply_text("Selected flow does not belong to this batch.")
                return True
            status = str(row_fields.get("current_status") or row_fields.get("status") or "").strip()
            process_seq = _normalize_process_seq(row_fields)
            current_stage = str(row_fields.get("current_stage_name") or "").strip()
            next_stage = str(row_fields.get("next_stage_name") or "").strip()
            current_role = _resolve_supervisor_role_for_stage(repo, process_seq, current_stage)
            next_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage) if next_stage else ""

            if action == "dn":
                if status == _MS_PENDING_CONFIRMATION:
                    await query.message.reply_text("This flow is waiting for next-stage hand-off confirmation.")
                    return True
                if user_role_name != current_role:
                    await query.message.reply_text("Only the current-stage supervisor can mark this stage done.")
                    return True
                try:
                    await _mark_ms_stage_done_pending_confirmation(repo, context, row_id, updated_by)
                except Exception:
                    await query.message.reply_text("Could not mark stage done.")
                    return True
                text = _build_ms_batch_snapshot_overview_text(repo, batch_id, batch_no)
                await _edit_ms_batch_tracker_message(query, text, _build_ms_batch_overview_inline_keyboard(batch_id))
                return True

            if action == "cf":
                if status != _MS_PENDING_CONFIRMATION:
                    await query.message.reply_text("This flow is not waiting for hand-off confirmation.")
                    return True
                if user_role_name != next_role:
                    await query.message.reply_text("Only the next-stage supervisor can confirm this hand-off.")
                    return True
                try:
                    await advance_ms_stage(repo, context, row_id, updated_by)
                except Exception:
                    await query.message.reply_text("Could not confirm and advance this flow.")
                    return True
                text = _build_ms_batch_snapshot_overview_text(repo, batch_id, batch_no)
                await _edit_ms_batch_tracker_message(query, text, _build_ms_batch_overview_inline_keyboard(batch_id))
                return True

    parsed_supervisor = _parse_supervisor_callback_data(str(query.data))
    if parsed_supervisor:
        action, record_id = parsed_supervisor
        await query.answer()

        if not user_role_name:
            await query.message.reply_text("Unable to resolve your role for this action.")
            return True

        if action == "schedule":
            if not _is_batch_schedulable_for_role(repo, record_id, user_role_name):
                await query.message.reply_text("You are not authorized to schedule this batch or it is already scheduled.")
                return True
            await _prompt_schedule_date(
                update,
                context,
                [record_id],
                return_state=_target_return_state(context),
                title="Schedule Batch",
            )
            return True

        if action == "done_row":
            row = repo.get_ms_row_by_id(record_id)
            if not row:
                await query.message.reply_text("MS row not found.")
                return True
            fields = row.get("fields", {})
            row_status = str(fields.get("current_status") or fields.get("status") or "").strip()
            process_seq = _normalize_process_seq(fields)
            stage_name = str(fields.get("current_stage_name") or "").strip()
            current_role = _resolve_supervisor_role_for_stage(repo, process_seq, stage_name)
            next_stage = str(fields.get("next_stage_name") or "").strip()
            next_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage) if next_stage else ""

            if row_status == _MS_PENDING_CONFIRMATION:
                if user_role_name != next_role:
                    await query.message.reply_text("Only the next-stage supervisor can confirm this handover.")
                    return True
                try:
                    await advance_ms_stage(repo, context, record_id, updated_by)
                except Exception:
                    await query.message.reply_text("Could not confirm and advance this stage.")
                    return True
                await query.message.reply_text("Stage handover confirmed.")
                return True
            try:
                if user_role_name != current_role:
                    await query.message.reply_text("You are not authorized for this stage.")
                    return True
                await _mark_ms_stage_done_pending_confirmation(repo, context, record_id, updated_by)
            except Exception:
                await query.message.reply_text("Could not update this stage.")
                return True
            await query.message.reply_text("Current stage marked done. Waiting for next-stage confirmation.")
            return True

        if action == "confirm_row":
            row = repo.get_ms_row_by_id(record_id)
            if not row:
                await query.message.reply_text("MS row not found.")
                return True
            fields = row.get("fields", {})
            process_seq = _normalize_process_seq(fields)
            next_stage = str(fields.get("next_stage_name") or "").strip()
            next_role = _resolve_supervisor_role_for_stage(repo, process_seq, next_stage) if next_stage else ""
            row_status = str(fields.get("current_status") or fields.get("status") or "").strip()
            if row_status != _MS_PENDING_CONFIRMATION:
                await query.message.reply_text("This row is not waiting for confirmation.")
                return True
            if user_role_name != next_role:
                await query.message.reply_text("Only the next-stage supervisor can confirm this handover.")
                return True
            try:
                await advance_ms_stage(repo, context, record_id, updated_by)
            except Exception:
                await query.message.reply_text("Could not confirm and advance this stage.")
                return True
            await query.message.reply_text("Stage handover confirmed.")
            return True

        if action == "view_pdf":
            try:
                sent = await _send_ms_row_pdf_for_chat(repo, context.bot, query.message.chat_id, record_id)
            except Exception:
                await query.message.reply_text("Could not fetch PDF for this row.")
                return True
            if not sent:
                await query.message.reply_text("No PDF attachment found for this row/batch.")
                return True
            return True

        if action == "done_batch_stage":
            batch_id = record_id
            done_count = 0
            try:
                done_count = await _mark_batch_stage_done(repo, context, batch_id, updated_by, user_role_name)
            except Exception:
                await query.message.reply_text("Could not complete batch stage action.")
                return True
            if done_count <= 0:
                await query.message.reply_text("No pending rows found in this batch for your current stage.")
                return True
            await query.message.reply_text(
                f"Marked {done_count} row(s) done. Waiting for next-stage confirmations."
            )
            return True

        if action == "complete_batch":
            batch_id = record_id
            statuses = repo.list_child_statuses(batch_id)
            if not statuses or not all(status in ("Done", "Completed", "Cutting Completed") for status in statuses):
                await query.message.reply_text("Batch is not ready for completion yet.")
                return True
            recalculate_master_overall_status(repo, batch_id, updated_by)
            await query.message.reply_text("Batch completion updated.")
            return True

        return True

    parsed = _parse_approval_callback_data(str(query.data))
    if not parsed:
        return False
    action, batch_id = parsed
    await query.answer()

    if not _is_production_manager(context):
        await query.message.reply_text("Only Production Manager or System Admin can approve/reject batches.")
        return True

    record = repo.get_master_by_id(batch_id)
    if not record:
        await query.message.reply_text("Batch not found.")
        return True

    fields = record.get("fields", {})
    batch_no = fields.get("batch_no", f"ID {batch_id}")
    approval_status = str(fields.get("approval_status") or "")

    if action == "open":
        if approval_status != "Pending Approval":
            await query.message.reply_text(f"Batch {batch_no} is already {approval_status}.")
            return True
        await query.message.reply_text(
            f"Batch {batch_no}\nStatus: Pending Approval\nChoose action:",
            reply_markup=_approval_open_keyboard(batch_id),
        )
        return True

    if approval_status != "Pending Approval":
        await query.message.reply_text(f"Batch {batch_no} is already {approval_status}.")
        return True

    if action == "approve":
        approved_batch_numbers = await approve_batches_by_ids(update, context, [batch_id])
        if approved_batch_numbers:
            # Notification event already confirms successful approval.
            return True
        else:
            await query.message.reply_text(f"Could not approve batch {batch_no}.")
        return True

    rejected_batch_numbers = await reject_batches_by_ids(update, context, [batch_id])
    if rejected_batch_numbers:
        await query.message.reply_text(f"Rejected: {', '.join(rejected_batch_numbers)}")
    else:
        await query.message.reply_text(f"Could not reject batch {batch_no}.")
    return True


async def set_master_scheduled_date(
    context,
    batch_id: int,
    scheduled_date_iso: str,
    updated_by,
    remarks: str = "",
) -> None:
    repo = ProductionRepo()
    master = repo.get_master_by_id(batch_id)
    if not master:
        raise ValueError("Batch not found.")

    old_date = master.get("fields", {}).get("scheduled_date")
    repo.update_master(batch_id, {"scheduled_date": scheduled_date_iso, "overall_status": "Scheduled"})
    repo.update_ms_for_batch(batch_id, {"scheduled_date": scheduled_date_iso})

    repo.add_status_history(batch_id, "Master", batch_id, str(old_date or ""), str(scheduled_date_iso), updated_by, remarks)
    repo.add_lifecycle_history(batch_id, "Scheduled", updated_by, remarks or "Master and MS rows scheduled")
    master = repo.get_master_by_id(batch_id)
    batch_no = str((master or {}).get("fields", {}).get("batch_no") or "")
    await _notify_event(
        context.bot,
        "production_batch_scheduled",
        f"Batch scheduled: {batch_no} | Scheduled Date: {_format_notification_datetime(scheduled_date_iso)}",
        context={"batch_id": batch_id},
    )


async def update_child_status(
    context,
    batch_id: int,
    entity_type: str,
    row_id: int,
    new_status: str,
    updated_by,
    remarks: str = "",
    extra_fields: dict | None = None,
) -> None:
    repo = ProductionRepo()
    master = repo.get_master_by_id(batch_id)
    if not master:
        raise ValueError("Batch not found.")
    if master.get("fields", {}).get("approval_status") != "Approved":
        raise ValueError("Scheduling/status updates are not allowed before approval.")

    table_map = {"MS": "ProductBatchMS", "CNC": "ProductBatchCNC", "Store": "ProductBatchStore"}
    table = table_map.get(entity_type)
    if not table:
        raise ValueError("Invalid entity type.")

    records = repo.costing_client.get_records(table)
    row = next((record for record in records if record.get("id") == row_id), None)
    if not row:
        raise ValueError("Child row not found.")
    old_status = row.get("fields", {}).get("status") or row.get("fields", {}).get("current_status") or ""

    updates = {"status": new_status}
    if extra_fields:
        updates.update(extra_fields)

    if entity_type == "MS":
        repo.update_ms(row_id, updates)
    elif entity_type == "CNC":
        repo.update_cnc(row_id, updates)
    else:
        repo.update_store(row_id, updates)

    repo.add_status_history(batch_id, entity_type, row_id, old_status, new_status, updated_by, remarks)
    new_master_status = recalculate_master_overall_status(repo, batch_id, updated_by)
    batch_no = master.get("fields", {}).get("batch_no", "")
    await _notify_event(
        context.bot,
        "batch_status_changed",
        f"{entity_type} status changed for batch {batch_no}: {old_status} -> {new_status}. Master: {new_master_status}",
        context={"batch_id": batch_id},
    )


async def handle_production_state_text(update, context, text: str) -> bool:
    state = context.user_data.get("menu_state")
    flow = _get_flow(context)

    if state == SELECTING_BATCH_MODE_STATE:
        if text == BACK_LABEL:
            _clear_flow(context)
            context.user_data["menu_state"] = _target_return_state(context)
            return True
        if text not in (_MODE_BY_MODEL, _MODE_BY_PART):
            await _reply(update, "Select a valid batch mode.")
            return True
        flow["batch_mode"] = text
        await _move_model_selection(update, context)
        return True

    if state == SELECTING_PRODUCT_MODEL_STATE:
        paging = context.user_data.get("production_paging", {})
        records = paging.get("records", [])
        page = paging.get("page", 0)
        page_size = paging.get("page_size", settings.MSCUTLIST_PAGE_SIZE)

        if text == _PAGE_PREV:
            _change_page(context, -1)
            await _show_model_page(update, context)
            return True
        if text == _PAGE_NEXT:
            _change_page(context, 1)
            await _show_model_page(update, context)
            return True
        if text == BACK_LABEL:
            context.user_data["menu_state"] = SELECTING_BATCH_MODE_STATE
            await start_new_production_batch(update, context)
            return True

        index = _selection_index_from_text(text, page, page_size)
        if index is None or index < 0 or index >= len(records):
            await _reply(update, "Enter a valid number from the list.")
            return True

        flow["model_code"] = records[index]
        if flow.get("batch_mode") == _MODE_BY_MODEL:
            context.user_data["menu_state"] = ENTERING_BATCH_QTY_STATE
            await _reply(update, "Enter Batch Quantity:")
            return True

        flow["selected_part_ids"] = []
        flow["selected_part_names"] = []
        flow["awaiting_more_parts_answer"] = False
        await _move_part_selection(update, context)
        return True

    if state == SELECTING_PRODUCT_PARTS_STATE:
        if flow.get("awaiting_more_parts_answer"):
            if text == _YES:
                flow["awaiting_more_parts_answer"] = False
                await _show_parts_page(update, context)
                return True
            if text == _NO:
                if not flow.get("selected_part_ids"):
                    await _reply(update, "Select at least one part.")
                    return True
                flow["awaiting_more_parts_answer"] = False
                context.user_data["menu_state"] = ENTERING_BATCH_QTY_STATE
                await _reply(update, "Enter Batch Quantity:")
                return True
            await _reply(update, "Select Yes or No.", [[_YES], [_NO], [BACK_LABEL]])
            return True

        paging = context.user_data.get("production_paging", {})
        records = paging.get("records", [])
        page = paging.get("page", 0)
        page_size = paging.get("page_size", settings.MSCUTLIST_PAGE_SIZE)

        if text == _PAGE_PREV:
            _change_page(context, -1)
            await _show_parts_page(update, context)
            return True
        if text == _PAGE_NEXT:
            _change_page(context, 1)
            await _show_parts_page(update, context)
            return True
        if text == BACK_LABEL:
            await _move_model_selection(update, context)
            return True

        selected_indices = _parse_number_tokens(text)
        if not selected_indices:
            await _reply(update, "Enter comma-separated numbers like: 1,3,4")
            return True

        chosen_ids = flow.get("selected_part_ids", [])
        chosen_names = flow.get("selected_part_names", [])
        page_records, _, _ = _paginate(records, page, page_size)
        for number in selected_indices:
            item_index = number - 1
            if item_index < 0 or item_index >= len(page_records):
                continue
            item = page_records[item_index]
            if item["part_id"] not in chosen_ids:
                chosen_ids.append(item["part_id"])
                chosen_names.append(item["part_name"])

        flow["selected_part_ids"] = chosen_ids
        flow["selected_part_names"] = chosen_names
        flow["awaiting_more_parts_answer"] = True
        await _reply(update, "Select more parts?", [[_YES], [_NO], [BACK_LABEL]])
        return True

    if state == ENTERING_BATCH_QTY_STATE:
        if text == BACK_LABEL:
            if flow.get("batch_mode") == _MODE_BY_PART:
                await _move_part_selection(update, context)
            else:
                await _move_model_selection(update, context)
            return True

        if not text.isdigit():
            await _reply(update, "Enter a valid numeric quantity.")
            return True

        qty = int(text)
        repo = ProductionRepo()
        min_qty, max_qty = _get_limits_from_config(repo)
        if qty < min_qty or qty > max_qty:
            await _reply(update, f"Batch quantity must be between {min_qty} and {max_qty}.")
            return True

        flow["batch_qty"] = qty
        await _show_batch_type_prompt(update, context)
        return True

    if state == CONFIRMING_BATCH_STATE:
        if text == BACK_LABEL:
            await _show_batch_type_prompt(update, context)
            return True
        if text == _NO:
            await start_new_production_batch(update, context)
            return True
        if text == _YES:
            await _create_batch_from_flow(update, context)
            return True
        await _reply(update, "Select Yes or No.", [[_YES], [_NO], [BACK_LABEL]])
        return True

    if state == SELECTING_BATCH_TYPE_STATE:
        if text == BACK_LABEL:
            context.user_data["menu_state"] = ENTERING_BATCH_QTY_STATE
            await _reply(update, "Enter Batch Quantity:")
            return True
        if text not in (_TYPE_COMPLETE, _TYPE_MS, _TYPE_CNC, _TYPE_STORE):
            await _reply(update, "Select a valid batch type.")
            return True

        flow["batch_type"] = text
        context.user_data["menu_state"] = CONFIRMING_BATCH_STATE
        await _reply(update, _batch_summary_text(flow), [[_YES], [_NO], [BACK_LABEL]])
        return True

    if state == PENDING_APPROVALS_SELECTION_STATE:
        selection = context.user_data.get("pending_approvals_selection", {})
        records = selection.get("records", [])
        page = selection.get("page", 0)
        page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)

        if text == _PAGE_PREV:
            selection["page"] = max(0, page - 1)
            await _show_pending_approvals_page(update, context)
            return True
        if text == _PAGE_NEXT:
            max_page = max((len(records) - 1) // page_size, 0)
            selection["page"] = min(max_page, page + 1)
            await _show_pending_approvals_page(update, context)
            return True
        if text == BACK_LABEL:
            context.user_data["menu_state"] = _target_return_state(context)
            context.user_data.pop("pending_approvals_selection", None)
            return True

        selected_numbers = _parse_number_tokens(text)
        if not selected_numbers:
            await _reply(update, "Enter one number or comma-separated values like 1,3")
            return True

        page_records, _, _ = _paginate(records, page, page_size)
        selected_records = []
        for number in selected_numbers:
            item_index = number - 1
            if item_index < 0 or item_index >= len(page_records):
                continue
            selected_records.append(page_records[item_index])

        if not selected_records:
            await _reply(update, "No valid selection on this page.")
            return True

        context.user_data["pending_approvals_confirm"] = {
            "selected_records": selected_records,
            "selected_ids": [record["id"] for record in selected_records],
        }
        await _show_pending_approval_confirmation(update, context)
        return True

    if state == PENDING_APPROVALS_CONFIRM_STATE:
        if text == BACK_LABEL or text == _NO:
            context.user_data.pop("pending_approvals_confirm", None)
            context.user_data["menu_state"] = PENDING_APPROVALS_SELECTION_STATE
            await _show_pending_approvals_page(update, context)
            return True
        if text not in (_YES, _REJECT):
            await _reply(update, "Select Yes, Reject, or No.", [[_YES], [_REJECT], [_NO], [BACK_LABEL]])
            return True

        confirm_data = context.user_data.get("pending_approvals_confirm", {})
        selected_ids = confirm_data.get("selected_ids", [])
        if text == _YES:
            approved_batch_numbers = await approve_batches_by_ids(update, context, selected_ids)
            rejected_batch_numbers = []
        else:
            rejected_batch_numbers = await reject_batches_by_ids(update, context, selected_ids)
            approved_batch_numbers = []

        context.user_data.pop("pending_approvals_confirm", None)
        selection = context.user_data.get("pending_approvals_selection", {})
        selection["records"] = ProductionRepo().list_pending_approvals()
        selection["page"] = 0

        if rejected_batch_numbers:
            await _reply(update, f"Rejected: {', '.join(rejected_batch_numbers)}")

        context.user_data.pop("pending_approvals_selection", None)
        set_main_menu_state(context)
        if not selection["records"]:
            await _reply(update, "No pending approvals remaining.")
            return True
        await _reply(update, "Approval updated. Returning to main menu.")
        return True

    if state == MY_MS_JOBS_FILTER_STATE:
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_jobs_selection", None)
            context.user_data.pop("my_ms_jobs_all_records", None)
            context.user_data.pop("my_ms_jobs_action_records", None)
            context.user_data.pop("my_ms_jobs_filter_value", None)
            context.user_data.pop("my_ms_jobs_creator_by_batch", None)
            context.user_data.pop("my_ms_jobs_batch_no_by_id", None)
            context.user_data.pop("my_ms_jobs_batch_selection", None)
            context.user_data.pop("my_ms_jobs_bulk_action", None)
            context.user_data.pop("my_ms_jobs_bulk_remarks", None)
            context.user_data.pop("my_ms_batch_action", None)
            context.user_data.pop("my_ms_batch_remarks", None)
            context.user_data["menu_state"] = _target_return_state(context)
            return True
        normalized_filter_map = {
            _normalize_menu_text(_MS_VIEW_ACTION_REQUIRED): _MS_VIEW_ACTION_REQUIRED,
            _normalize_menu_text(_MS_VIEW_ALL): _MS_VIEW_ALL,
            _normalize_menu_text(_MS_VIEW_BY_NEXT_STAGE): _MS_VIEW_BY_NEXT_STAGE,
            _normalize_menu_text(_MS_VIEW_BY_CREATED_BY): _MS_VIEW_BY_CREATED_BY,
            _normalize_menu_text(_MS_VIEW_BY_BATCH_NO): _MS_VIEW_BY_BATCH_NO,
        }
        selected_filter = normalized_filter_map.get(_normalize_menu_text(text))
        if not selected_filter:
            await _reply(update, "Choose a valid MS jobs view.")
            return True
        all_records = context.user_data.get("my_ms_jobs_all_records", [])
        action_records = context.user_data.get("my_ms_jobs_action_records", [])
        batch_no_by_id = context.user_data.get("my_ms_jobs_batch_no_by_id")
        if not isinstance(batch_no_by_id, dict):
            batch_ids = {
                _normalize_ref(row.get("fields", {}).get("batch_id"))
                for row in all_records
                if isinstance(_normalize_ref(row.get("fields", {}).get("batch_id")), int)
            }
            batch_no_by_id = _get_batch_no_map(ProductionRepo(), batch_ids)
            context.user_data["my_ms_jobs_batch_no_by_id"] = batch_no_by_id
        if selected_filter == _MS_VIEW_BY_NEXT_STAGE:
            options = _get_my_ms_jobs_next_stage_options(all_records)
            if not options:
                await _reply(update, "No next-stage entries available for your MS jobs.", [[BACK_LABEL]])
                return True
            context.user_data["my_ms_jobs_next_stage_selection"] = {
                "options": options,
                "page": 0,
                "page_size": settings.MSCUTLIST_PAGE_SIZE,
            }
            context.user_data["menu_state"] = MY_MS_JOBS_NEXT_STAGE_SELECTION_STATE
            await _show_my_ms_jobs_next_stage_filter_page(update, context)
            return True
        if selected_filter == _MS_VIEW_BY_CREATED_BY:
            creator_by_batch = context.user_data.get("my_ms_jobs_creator_by_batch")
            if not isinstance(creator_by_batch, dict):
                batch_ids = {
                    _normalize_ref(row.get("fields", {}).get("batch_id"))
                    for row in all_records
                    if isinstance(_normalize_ref(row.get("fields", {}).get("batch_id")), int)
                }
                creator_by_batch = _get_batch_creator_name_map(ProductionRepo(), batch_ids)
                context.user_data["my_ms_jobs_creator_by_batch"] = creator_by_batch
            options = _get_my_ms_jobs_creator_options(all_records, creator_by_batch)
            if not options:
                await _reply(update, "No creator entries available for your MS jobs.", [[BACK_LABEL]])
                return True
            context.user_data["my_ms_jobs_created_by_selection"] = {
                "options": options,
                "page": 0,
                "page_size": settings.MSCUTLIST_PAGE_SIZE,
            }
            context.user_data["menu_state"] = MY_MS_JOBS_CREATED_BY_SELECTION_STATE
            await _show_my_ms_jobs_created_by_filter_page(update, context)
            return True
        if selected_filter == _MS_VIEW_BY_BATCH_NO:
            options = _get_my_ms_jobs_batch_options(all_records, batch_no_by_id)
            if not options:
                await _reply(update, "No batch entries available for your MS jobs.", [[BACK_LABEL]])
                return True
            context.user_data["my_ms_jobs_batch_selection"] = {
                "options": options,
                "page": 0,
                "page_size": settings.MSCUTLIST_PAGE_SIZE,
            }
            context.user_data["menu_state"] = MY_MS_JOBS_BATCH_SELECTION_STATE
            await _show_my_ms_jobs_batch_filter_page(update, context)
            return True
        filtered, view_mode = _apply_my_ms_jobs_filter(
            all_records,
            action_records,
            selected_filter,
            "",
            context.user_data.get("my_ms_jobs_creator_by_batch", {}),
            batch_no_by_id if isinstance(batch_no_by_id, dict) else {},
        )
        if not filtered:
            await _reply(update, "No MS jobs found for selected view.", [[BACK_LABEL]])
            return True
        context.user_data["my_ms_jobs_filter"] = selected_filter
        context.user_data["my_ms_jobs_filter_value"] = ""
        context.user_data["my_ms_jobs_selection"] = {
            "records": filtered,
            "page": 0,
            "page_size": settings.MSCUTLIST_PAGE_SIZE,
            "view_mode": view_mode,
        }
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _show_my_ms_jobs_page(update, context)
        return True

    if state == MY_MS_JOBS_NEXT_STAGE_SELECTION_STATE:
        selection = context.user_data.get("my_ms_jobs_next_stage_selection", {})
        options = selection.get("options", [])
        page = selection.get("page", 0)
        page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)

        if text == _PAGE_PREV:
            selection["page"] = max(0, page - 1)
            await _show_my_ms_jobs_next_stage_filter_page(update, context)
            return True
        if text == _PAGE_NEXT:
            max_page = max((len(options) - 1) // page_size, 0)
            selection["page"] = min(max_page, page + 1)
            await _show_my_ms_jobs_next_stage_filter_page(update, context)
            return True
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_jobs_next_stage_selection", None)
            await _show_my_ms_jobs_filter_menu(update, context)
            return True

        selected_numbers = _parse_number_tokens(text)
        if len(selected_numbers) != 1:
            await _reply(update, "Select one option number.")
            return True
        page_options, _, _ = _paginate(options, page, page_size)
        option_index = selected_numbers[0] - 1
        if option_index < 0 or option_index >= len(page_options):
            await _reply(update, "No valid selection on this page.")
            return True
        selected_stage = page_options[option_index]
        all_records = context.user_data.get("my_ms_jobs_all_records", [])
        action_records = context.user_data.get("my_ms_jobs_action_records", [])
        filtered, view_mode = _apply_my_ms_jobs_filter(
            all_records,
            action_records,
            _MS_VIEW_BY_NEXT_STAGE,
            selected_stage,
            context.user_data.get("my_ms_jobs_creator_by_batch", {}),
            context.user_data.get("my_ms_jobs_batch_no_by_id", {}),
        )
        context.user_data.pop("my_ms_jobs_next_stage_selection", None)
        if not filtered:
            await _reply(update, "No MS jobs found for selected next stage.", [[BACK_LABEL]])
            return True
        context.user_data["my_ms_jobs_filter"] = _MS_VIEW_BY_NEXT_STAGE
        context.user_data["my_ms_jobs_filter_value"] = selected_stage
        context.user_data["my_ms_jobs_selection"] = {
            "records": filtered,
            "page": 0,
            "page_size": settings.MSCUTLIST_PAGE_SIZE,
            "view_mode": view_mode,
        }
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _show_my_ms_jobs_page(update, context)
        return True

    if state == MY_MS_JOBS_CREATED_BY_SELECTION_STATE:
        selection = context.user_data.get("my_ms_jobs_created_by_selection", {})
        options = selection.get("options", [])
        page = selection.get("page", 0)
        page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)

        if text == _PAGE_PREV:
            selection["page"] = max(0, page - 1)
            await _show_my_ms_jobs_created_by_filter_page(update, context)
            return True
        if text == _PAGE_NEXT:
            max_page = max((len(options) - 1) // page_size, 0)
            selection["page"] = min(max_page, page + 1)
            await _show_my_ms_jobs_created_by_filter_page(update, context)
            return True
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_jobs_created_by_selection", None)
            await _show_my_ms_jobs_filter_menu(update, context)
            return True

        selected_numbers = _parse_number_tokens(text)
        if len(selected_numbers) != 1:
            await _reply(update, "Select one option number.")
            return True
        page_options, _, _ = _paginate(options, page, page_size)
        option_index = selected_numbers[0] - 1
        if option_index < 0 or option_index >= len(page_options):
            await _reply(update, "No valid selection on this page.")
            return True
        selected_creator = page_options[option_index]
        all_records = context.user_data.get("my_ms_jobs_all_records", [])
        action_records = context.user_data.get("my_ms_jobs_action_records", [])
        filtered, view_mode = _apply_my_ms_jobs_filter(
            all_records,
            action_records,
            _MS_VIEW_BY_CREATED_BY,
            selected_creator,
            context.user_data.get("my_ms_jobs_creator_by_batch", {}),
            context.user_data.get("my_ms_jobs_batch_no_by_id", {}),
        )
        context.user_data.pop("my_ms_jobs_created_by_selection", None)
        if not filtered:
            await _reply(update, "No MS jobs found for selected creator.", [[BACK_LABEL]])
            return True
        context.user_data["my_ms_jobs_filter"] = _MS_VIEW_BY_CREATED_BY
        context.user_data["my_ms_jobs_filter_value"] = selected_creator
        context.user_data["my_ms_jobs_selection"] = {
            "records": filtered,
            "page": 0,
            "page_size": settings.MSCUTLIST_PAGE_SIZE,
            "view_mode": view_mode,
        }
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _show_my_ms_jobs_page(update, context)
        return True

    if state == MY_MS_JOBS_BATCH_SELECTION_STATE:
        selection = context.user_data.get("my_ms_jobs_batch_selection", {})
        options = selection.get("options", [])
        page = selection.get("page", 0)
        page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)

        if text == _PAGE_PREV:
            selection["page"] = max(0, page - 1)
            await _show_my_ms_jobs_batch_filter_page(update, context)
            return True
        if text == _PAGE_NEXT:
            max_page = max((len(options) - 1) // page_size, 0)
            selection["page"] = min(max_page, page + 1)
            await _show_my_ms_jobs_batch_filter_page(update, context)
            return True
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_jobs_batch_selection", None)
            await _show_my_ms_jobs_filter_menu(update, context)
            return True

        selected_numbers = _parse_number_tokens(text)
        if len(selected_numbers) != 1:
            await _reply(update, "Select one option number.")
            return True
        page_options, _, _ = _paginate(options, page, page_size)
        option_index = selected_numbers[0] - 1
        if option_index < 0 or option_index >= len(page_options):
            await _reply(update, "No valid selection on this page.")
            return True
        selected_batch_no = page_options[option_index]
        repo = ProductionRepo()
        master = repo.get_master_by_batch_no(selected_batch_no)
        batch_id = (master or {}).get("id")
        if not isinstance(batch_id, int):
            await _reply(update, "Batch not found.")
            return True
        await _show_ms_batch_tracker_overview(update, batch_id, selected_batch_no)
        context.user_data["my_ms_batch_action"] = {
            "batch_id": batch_id,
            "batch_no": selected_batch_no,
            "source_state": MY_MS_JOBS_BATCH_SELECTION_STATE,
        }
        context.user_data["menu_state"] = MY_MS_BATCH_ACTION_STATE
        return True

    if state == VIEW_BATCH_SELECTION_STATE:
        selection = context.user_data.get("view_batch_selection", {})
        records = selection.get("records", [])
        page = selection.get("page", 0)
        page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)

        if text == _PAGE_PREV:
            selection["page"] = max(0, page - 1)
            await _show_view_batch_page(update, context)
            return True
        if text == _PAGE_NEXT:
            max_page = max((len(records) - 1) // page_size, 0)
            selection["page"] = min(max_page, page + 1)
            await _show_view_batch_page(update, context)
            return True
        if text == BACK_LABEL:
            context.user_data.pop("view_batch_selection", None)
            context.user_data["menu_state"] = _target_return_state(context)
            return True

        selected_numbers = _parse_number_tokens(text)
        if len(selected_numbers) != 1:
            await _reply(update, "Select one option number.")
            return True
        page_records, _, _ = _paginate(records, page, page_size)
        option_index = selected_numbers[0] - 1
        if option_index < 0 or option_index >= len(page_records):
            await _reply(update, "No valid selection on this page.")
            return True
        selected = page_records[option_index]
        batch_id = selected.get("batch_id")
        batch_no = str(selected.get("batch_no") or "")
        if not isinstance(batch_id, int):
            await _reply(update, "Batch not found.")
            return True
        await _show_ms_batch_tracker_overview(update, batch_id, batch_no)
        context.user_data["my_ms_batch_action"] = {
            "batch_id": batch_id,
            "batch_no": batch_no,
            "source_state": VIEW_BATCH_SELECTION_STATE,
        }
        context.user_data["menu_state"] = MY_MS_BATCH_ACTION_STATE
        return True

    if state == MY_MS_SCHEDULE_SELECTION_STATE:
        selection = context.user_data.get("my_ms_schedule_selection", {})
        records = selection.get("records", [])
        page = selection.get("page", 0)
        page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)

        if text == _PAGE_PREV:
            selection["page"] = max(0, page - 1)
            await _show_my_ms_schedule_page(update, context)
            return True
        if text == _PAGE_NEXT:
            max_page = max((len(records) - 1) // page_size, 0)
            selection["page"] = min(max_page, page + 1)
            await _show_my_ms_schedule_page(update, context)
            return True
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_schedule_selection", None)
            context.user_data["menu_state"] = _target_return_state(context)
            return True

        selected_numbers = _parse_number_tokens(text)
        if not selected_numbers:
            await _reply(update, "Enter one number or comma-separated values like 1,3")
            return True

        page_records, _, _ = _paginate(records, page, page_size)
        selected_records = []
        for number in selected_numbers:
            item_index = number - 1
            if item_index < 0 or item_index >= len(page_records):
                continue
            selected_records.append(page_records[item_index])
        if not selected_records:
            await _reply(update, "No valid selection on this page.")
            return True

        context.user_data["my_ms_schedule_confirm"] = {
            "selected_batch_ids": [int(row.get("batch_id")) for row in selected_records if isinstance(row.get("batch_id"), int)],
            "selected_rows": selected_records,
        }
        context.user_data["menu_state"] = MY_MS_SCHEDULE_CONFIRM_STATE
        lines = ["Schedule selected batches now?"]
        for row in selected_records:
            lines.append(f"- {row.get('batch_no', '')}")
        await _reply(update, "\n".join(lines), [[_YES], [_NO], [BACK_LABEL]])
        return True

    if state == MY_MS_SCHEDULE_CONFIRM_STATE:
        if text == BACK_LABEL or text == _NO:
            context.user_data.pop("my_ms_schedule_confirm", None)
            context.user_data["menu_state"] = MY_MS_SCHEDULE_SELECTION_STATE
            await _show_my_ms_schedule_page(update, context)
            return True
        if text != _YES:
            await _reply(update, "Select Yes or No.", [[_YES], [_NO], [BACK_LABEL]])
            return True

        confirm_data = context.user_data.get("my_ms_schedule_confirm", {})
        selected_batch_ids = confirm_data.get("selected_batch_ids", [])
        context.user_data.pop("my_ms_schedule_confirm", None)
        await _prompt_schedule_date(
            update,
            context,
            selected_batch_ids,
            return_state=MY_MS_SCHEDULE_SELECTION_STATE,
            title="Schedule selected batches",
        )
        return True

    if state == MY_MS_JOBS_SELECTION_STATE:
        selection = context.user_data.get("my_ms_jobs_selection", {})
        records = selection.get("records", [])
        page = selection.get("page", 0)
        page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)

        if text == _PAGE_PREV:
            selection["page"] = max(0, page - 1)
            await _show_my_ms_jobs_page(update, context)
            return True
        if text == _PAGE_NEXT:
            max_page = max((len(records) - 1) // page_size, 0)
            selection["page"] = min(max_page, page + 1)
            await _show_my_ms_jobs_page(update, context)
            return True
        if text == BACK_LABEL:
            await _show_my_ms_jobs_filter_menu(update, context)
            return True

        bulk_prefixed = _parse_bulk_prefixed_selection(text)
        if bulk_prefixed is not None:
            if not bulk_prefixed:
                await _reply(update, "Use bulk mode like X1,3.")
                return True
            page_records, _, _ = _paginate(records, page, page_size)
            selected_records = []
            for number in bulk_prefixed:
                item_index = number - 1
                if item_index < 0 or item_index >= len(page_records):
                    continue
                selected_records.append(page_records[item_index])
            if not selected_records:
                await _reply(update, "No valid selection on this page.")
                return True
            if len(selected_records) == 1:
                row_fields = selected_records[0].get("fields", {})
                batch_id = _normalize_ref(row_fields.get("batch_id"))
                if not isinstance(batch_id, int):
                    await _reply(update, "Batch not found for selected row.")
                    return True
                repo = ProductionRepo()
                batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")
                await _reply(update, _build_ms_batch_summary_text(repo, batch_id, batch_no))
                await _show_my_ms_batch_action_menu(update, context, batch_id, batch_no)
                return True
            context.user_data["my_ms_jobs_bulk_action"] = {
                "selected_ids": [record.get("id") for record in selected_records if isinstance(record.get("id"), int)],
                "selected_rows": selected_records,
            }
            await _show_my_ms_jobs_bulk_action_menu(update, context, selected_records)
            return True

        prefixed = _parse_prefixed_selection(text)
        if prefixed:
            action_code, number = prefixed
            page_records, _, _ = _paginate(records, page, page_size)
            item_index = number - 1
            if item_index < 0 or item_index >= len(page_records):
                await _reply(update, "No valid selection on this page.")
                return True
            selected_record = page_records[item_index]
            handled = await _execute_ms_job_action(update, context, action_code, selected_record)
            if not handled:
                await _reply(update, "Unsupported action.")
                return True
            if context.user_data.get("menu_state") in (MY_MS_JOBS_REMARKS_STATE, AWAITING_SCHEDULE_DATE_STATE):
                return True
            repo = ProductionRepo()
            user = context.user_data.get("user", {})
            role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
            _refresh_my_ms_jobs_selection(context, repo, role_name)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True

        selected_numbers = _parse_number_tokens(text)
        if not selected_numbers:
            await _reply(update, "Use 1/1,3 or quick actions D1/C1/N1/R1/V1/H1/S1/B1/X1,3.")
            return True

        page_records, _, _ = _paginate(records, page, page_size)
        selected_records = []
        for number in selected_numbers:
            item_index = number - 1
            if item_index < 0 or item_index >= len(page_records):
                continue
            selected_records.append(page_records[item_index])
        if not selected_records:
            await _reply(update, "No valid selection on this page.")
            return True

        if len(selected_records) == 1:
            await _show_ms_job_action_menu(update, context, selected_records[0])
            return True

        context.user_data["my_ms_jobs_confirm"] = {
            "selected_ids": [record["id"] for record in selected_records],
            "selected_rows": selected_records,
        }
        await _show_ms_jobs_confirmation(update, context)
        return True

    if state == MY_MS_JOBS_ACTION_STATE:
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_jobs_action", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True
        action_map = {
            _MS_ACTION_DONE: "D",
            _MS_ACTION_CONFIRM_HANDOFF: "C",
            _MS_ACTION_REJECT_HANDOFF: "N",
            _MS_ACTION_REMARKS: "R",
            _MS_ACTION_VIEW_LIST: "V",
            _MS_ACTION_HOLD: "H",
        }
        action_code = action_map.get(text)
        if not action_code:
            await _reply(
                update,
                "Choose one action from menu.",
                [[_MS_ACTION_DONE], [_MS_ACTION_CONFIRM_HANDOFF], [_MS_ACTION_REJECT_HANDOFF], [_MS_ACTION_REMARKS], [_MS_ACTION_VIEW_LIST], [_MS_ACTION_HOLD], [BACK_LABEL]],
            )
            return True
        action_ctx = context.user_data.get("my_ms_jobs_action", {})
        selected_record = action_ctx.get("selected_record")
        if not selected_record:
            await _reply(update, "No selected row found.")
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True
        handled = await _execute_ms_job_action(update, context, action_code, selected_record)
        if not handled:
            await _reply(update, "Unsupported action.")
            return True
        context.user_data.pop("my_ms_jobs_action", None)
        if context.user_data.get("menu_state") in (MY_MS_JOBS_REMARKS_STATE, AWAITING_SCHEDULE_DATE_STATE):
            return True
        repo = ProductionRepo()
        user = context.user_data.get("user", {})
        role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
        _refresh_my_ms_jobs_selection(context, repo, role_name)
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _show_my_ms_jobs_page(update, context)
        return True

    if state == MY_MS_JOBS_CONFIRM_STATE:
        if text == BACK_LABEL or text == _NO:
            context.user_data.pop("my_ms_jobs_confirm", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True
        if text != _YES:
            await _reply(update, "Select Yes or No.", [[_YES], [_NO], [BACK_LABEL]])
            return True

        data = context.user_data.get("my_ms_jobs_confirm", {})
        selected_ids = data.get("selected_ids", [])
        repo = ProductionRepo()
        user = context.user_data.get("user", {})
        updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))
        done_count = 0
        for row_id in selected_ids:
            try:
                await _mark_ms_stage_done_pending_confirmation(repo, context, row_id, updated_by)
                done_count += 1
            except Exception:
                continue

        context.user_data.pop("my_ms_jobs_confirm", None)
        role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
        refreshed = _refresh_my_ms_jobs_selection(context, repo, role_name)
        if not refreshed:
            context.user_data.pop("my_ms_jobs_selection", None)
            set_main_menu_state(context)
            await _reply(
                update,
                f"Marked {done_count} job(s) done. No pending MS jobs in your queue.",
            )
            return True
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _reply(update, f"Marked {done_count} job(s) done. Waiting for next-stage confirmations.")
        await _show_my_ms_jobs_page(update, context)
        return True

    if state == MY_MS_JOBS_BULK_ACTION_STATE:
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_jobs_bulk_action", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True
        data = context.user_data.get("my_ms_jobs_bulk_action", {})
        selected_ids = [row_id for row_id in data.get("selected_ids", []) if isinstance(row_id, int)]
        selected_rows = data.get("selected_rows", [])
        if not selected_ids:
            context.user_data.pop("my_ms_jobs_bulk_action", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _reply(update, "No selected rows for bulk action.")
            await _show_my_ms_jobs_page(update, context)
            return True

        repo = ProductionRepo()
        user = context.user_data.get("user", {})
        updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))

        if text == _MS_BULK_ACTION_REMARKS:
            context.user_data["my_ms_jobs_bulk_remarks"] = {"selected_ids": selected_ids, "selected_rows": selected_rows}
            context.user_data["menu_state"] = MY_MS_JOBS_BULK_REMARKS_STATE
            await _reply(update, "Enter remarks text for selected rows:", [[BACK_LABEL]])
            return True

        if text == _MS_BULK_ACTION_DONE:
            done_count = 0
            for row_id in selected_ids:
                try:
                    await _mark_ms_stage_done_pending_confirmation(repo, context, row_id, updated_by)
                    done_count += 1
                except Exception:
                    continue
            context.user_data.pop("my_ms_jobs_bulk_action", None)
            role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
            refreshed = _refresh_my_ms_jobs_selection(context, repo, role_name)
            if not refreshed:
                context.user_data.pop("my_ms_jobs_selection", None)
                set_main_menu_state(context)
                await _reply(update, f"Marked {done_count}/{len(selected_ids)} selected row(s) done. No pending MS jobs in your queue.")
                return True
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _reply(update, f"Marked {done_count}/{len(selected_ids)} selected row(s) done.")
            await _show_my_ms_jobs_page(update, context)
            return True

        if text == _MS_BULK_ACTION_HOLD:
            hold_count = 0
            for selected in selected_rows:
                row_id = selected.get("id")
                if not isinstance(row_id, int):
                    continue
                row = repo.get_ms_row_by_id(row_id)
                if not row:
                    continue
                row_fields = row.get("fields", {})
                batch_id = _normalize_ref(row_fields.get("batch_id"))
                old_status = str(row_fields.get("current_status") or row_fields.get("status") or "")
                hold_status = "On Hold"
                try:
                    repo.update_ms(
                        row_id,
                        repo.filter_table_fields(
                            "ProductBatchMS",
                            {
                                "current_status": hold_status,
                                "status": hold_status,
                                "updated_at": _now_iso(),
                                "last_updated_by": updated_by,
                            },
                        ),
                    )
                    hold_count += 1
                except Exception:
                    continue
                if isinstance(batch_id, int):
                    repo.add_status_history(
                        batch_id,
                        "MS",
                        row_id,
                        old_status,
                        hold_status,
                        updated_by,
                        "Marked as hold by supervisor (bulk action).",
                    )
                    part_name = _resolve_ms_row_part_text(repo, row_fields)
                    batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")
                    await _notify_roles(
                        context,
                        batch_id,
                        f"MS row put on hold (bulk).\nBatch: {batch_no}\nPart: {part_name}\nStatus: {hold_status}",
                        ["Production_Supervisor", "Production Supervisor", "System_Admin"],
                    )
            context.user_data.pop("my_ms_jobs_bulk_action", None)
            role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
            _refresh_my_ms_jobs_selection(context, repo, role_name)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _reply(update, f"Marked {hold_count}/{len(selected_ids)} selected row(s) hold.")
            await _show_my_ms_jobs_page(update, context)
            return True

        await _reply(
            update,
            "Choose one bulk action from menu.",
            [[_MS_BULK_ACTION_DONE], [_MS_BULK_ACTION_HOLD], [_MS_BULK_ACTION_REMARKS], [BACK_LABEL]],
        )
        return True

    if state == MY_MS_JOBS_BULK_REMARKS_STATE:
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_jobs_bulk_remarks", None)
            data = context.user_data.get("my_ms_jobs_bulk_action", {})
            await _show_my_ms_jobs_bulk_action_menu(update, context, data.get("selected_rows", []))
            return True
        remarks_text = str(text or "").strip()
        if not remarks_text:
            await _reply(update, "Remarks cannot be empty. Enter remarks text:", [[BACK_LABEL]])
            return True
        data = context.user_data.get("my_ms_jobs_bulk_remarks", {})
        selected_ids = [row_id for row_id in data.get("selected_ids", []) if isinstance(row_id, int)]
        if not selected_ids:
            context.user_data.pop("my_ms_jobs_bulk_remarks", None)
            context.user_data.pop("my_ms_jobs_bulk_action", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _reply(update, "No selected rows for bulk remarks.")
            await _show_my_ms_jobs_page(update, context)
            return True

        repo = ProductionRepo()
        user = context.user_data.get("user", {})
        updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))
        updated_count = 0
        for row_id in selected_ids:
            row = repo.get_ms_row_by_id(row_id)
            if not row:
                continue
            row_fields = row.get("fields", {})
            batch_id = _normalize_ref(row_fields.get("batch_id"))
            old_remarks = str(row_fields.get("supervisor_remarks") or "")
            try:
                repo.update_ms(
                    row_id,
                    repo.filter_table_fields(
                        "ProductBatchMS",
                        {
                            "supervisor_remarks": remarks_text,
                            "updated_at": _now_iso(),
                            "last_updated_by": updated_by,
                        },
                    ),
                )
                updated_count += 1
            except Exception:
                continue

            if isinstance(batch_id, int):
                repo.add_status_history(
                    batch_id,
                    "MS",
                    row_id,
                    old_remarks,
                    remarks_text,
                    updated_by,
                    "Supervisor remarks updated (bulk action).",
                )
                part_name = _resolve_ms_row_part_text(repo, row_fields)
                batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")
                await _notify_roles(
                    context,
                    batch_id,
                    f"Supervisor Remarks Added (bulk)\nBatch: {batch_no}\nPart: {part_name}\n{remarks_text}",
                    ["Production_Supervisor", "Production Supervisor", "Production_Manager"],
                )

        context.user_data.pop("my_ms_jobs_bulk_remarks", None)
        context.user_data.pop("my_ms_jobs_bulk_action", None)
        role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
        _refresh_my_ms_jobs_selection(context, repo, role_name)
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _reply(update, f"Remarks updated for {updated_count}/{len(selected_ids)} selected row(s).")
        await _show_my_ms_jobs_page(update, context)
        return True

    if state == MY_MS_BATCH_ACTION_STATE:
        if text == BACK_LABEL:
            data = context.user_data.pop("my_ms_batch_action", {})
            source_state = str(data.get("source_state") or MY_MS_JOBS_SELECTION_STATE)
            if source_state == MY_MS_JOBS_BATCH_SELECTION_STATE and context.user_data.get("my_ms_jobs_batch_selection"):
                context.user_data["menu_state"] = MY_MS_JOBS_BATCH_SELECTION_STATE
                await _show_my_ms_jobs_batch_filter_page(update, context)
                return True
            if source_state == VIEW_BATCH_SELECTION_STATE and context.user_data.get("view_batch_selection"):
                context.user_data["menu_state"] = VIEW_BATCH_SELECTION_STATE
                await _show_view_batch_page(update, context)
                return True
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True

        data = context.user_data.get("my_ms_batch_action", {})
        batch_id = data.get("batch_id")
        batch_no = str(data.get("batch_no") or "")
        source_state = str(data.get("source_state") or MY_MS_JOBS_SELECTION_STATE)
        if not isinstance(batch_id, int):
            context.user_data.pop("my_ms_batch_action", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _reply(update, "Batch context is missing.")
            await _show_my_ms_jobs_page(update, context)
            return True

        repo = ProductionRepo()
        user = context.user_data.get("user", {})
        user_role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
        updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))

        if text == _MS_BATCH_ACTION_SUMMARY:
            await _show_ms_batch_tracker_overview(update, batch_id, batch_no)
            await _show_my_ms_batch_action_menu(update, context, batch_id, batch_no, source_state=source_state)
            return True

        if text == _MS_BATCH_ACTION_VIEW:
            all_records = context.user_data.get("my_ms_jobs_all_records", [])
            action_records = context.user_data.get("my_ms_jobs_action_records", [])
            batch_no_by_id = context.user_data.get("my_ms_jobs_batch_no_by_id", {})
            filtered, view_mode = _apply_my_ms_jobs_filter(
                all_records,
                action_records,
                _MS_VIEW_BY_BATCH_NO,
                batch_no,
                context.user_data.get("my_ms_jobs_creator_by_batch", {}),
                batch_no_by_id if isinstance(batch_no_by_id, dict) else {},
            )
            context.user_data["my_ms_jobs_filter"] = _MS_VIEW_BY_BATCH_NO
            context.user_data["my_ms_jobs_filter_value"] = batch_no
            context.user_data["my_ms_jobs_selection"] = {
                "records": filtered,
                "page": 0,
                "page_size": settings.MSCUTLIST_PAGE_SIZE,
                "view_mode": view_mode,
            }
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True

        if text == _MS_BATCH_ACTION_SCHEDULE:
            if not _is_batch_schedulable_for_role(repo, batch_id, user_role_name):
                await _reply(update, "You are not authorized to schedule this batch or it is already scheduled.")
                return True
            await _prompt_schedule_date(
                update,
                context,
                [batch_id],
                return_state=MY_MS_BATCH_ACTION_STATE,
                title=f"Schedule Batch {batch_no or ''}",
            )
            return True

        if text == _MS_BATCH_ACTION_DONE:
            done_count = 0
            try:
                done_count = await _mark_batch_stage_done(repo, context, batch_id, updated_by, user_role_name)
            except Exception:
                await _reply(update, "Could not complete batch stage action.")
                return True
            role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
            _refresh_my_ms_jobs_selection(context, repo, role_name)
            await _reply(update, f"Marked {done_count} row(s) done for batch {batch_no or '-'}")
            await _show_my_ms_batch_action_menu(update, context, batch_id, batch_no, source_state=source_state)
            return True

        if text == _MS_BATCH_ACTION_HOLD:
            hold_count = 0
            for row in _rows_for_batch_and_role(repo, batch_id, user_role_name):
                row_id = row.get("id")
                if not isinstance(row_id, int):
                    continue
                row_fields = row.get("fields", {})
                old_status = str(row_fields.get("current_status") or row_fields.get("status") or "")
                hold_status = "On Hold"
                try:
                    repo.update_ms(
                        row_id,
                        repo.filter_table_fields(
                            "ProductBatchMS",
                            {
                                "current_status": hold_status,
                                "status": hold_status,
                                "updated_at": _now_iso(),
                                "last_updated_by": updated_by,
                            },
                        ),
                    )
                    hold_count += 1
                except Exception:
                    continue
                repo.add_status_history(
                    batch_id,
                    "MS",
                    row_id,
                    old_status,
                    hold_status,
                    updated_by,
                    "Marked as hold by supervisor (batch action).",
                )
            role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
            _refresh_my_ms_jobs_selection(context, repo, role_name)
            await _reply(update, f"Marked {hold_count} row(s) hold for batch {batch_no or '-'}")
            await _show_my_ms_batch_action_menu(update, context, batch_id, batch_no, source_state=source_state)
            return True

        if text == _MS_BATCH_ACTION_REMARKS:
            context.user_data["my_ms_batch_remarks"] = {
                "batch_id": batch_id,
                "batch_no": batch_no,
                "source_state": source_state,
            }
            context.user_data["menu_state"] = MY_MS_BATCH_REMARKS_STATE
            await _reply(update, f"Enter remarks text for batch {batch_no or '-'}:", [[BACK_LABEL]])
            return True

        await _reply(
            update,
            "Choose one batch action from menu.",
            [[_MS_BATCH_ACTION_SUMMARY], [_MS_BATCH_ACTION_DONE], [_MS_BATCH_ACTION_HOLD], [_MS_BATCH_ACTION_REMARKS], [_MS_BATCH_ACTION_VIEW], [_MS_BATCH_ACTION_SCHEDULE], [BACK_LABEL]],
        )
        return True

    if state == MY_MS_BATCH_REMARKS_STATE:
        if text == BACK_LABEL:
            remarks_ctx = context.user_data.get("my_ms_batch_remarks", {})
            context.user_data.pop("my_ms_batch_remarks", None)
            batch_id = remarks_ctx.get("batch_id")
            batch_no = str(remarks_ctx.get("batch_no") or "")
            source_state = str(remarks_ctx.get("source_state") or MY_MS_JOBS_SELECTION_STATE)
            if not isinstance(batch_id, int):
                context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
                await _show_my_ms_jobs_page(update, context)
                return True
            await _show_my_ms_batch_action_menu(update, context, batch_id, batch_no, source_state=source_state)
            return True
        remarks_text = str(text or "").strip()
        if not remarks_text:
            await _reply(update, "Remarks cannot be empty. Enter remarks text:", [[BACK_LABEL]])
            return True
        remarks_ctx = context.user_data.get("my_ms_batch_remarks", {})
        batch_id = remarks_ctx.get("batch_id")
        batch_no = str(remarks_ctx.get("batch_no") or "")
        source_state = str(remarks_ctx.get("source_state") or MY_MS_JOBS_SELECTION_STATE)
        if not isinstance(batch_id, int):
            context.user_data.pop("my_ms_batch_remarks", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _reply(update, "Invalid batch for remarks.")
            await _show_my_ms_jobs_page(update, context)
            return True

        repo = ProductionRepo()
        user = context.user_data.get("user", {})
        user_role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
        updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))
        updated_count = 0
        for row in _rows_for_batch_and_role(repo, batch_id, user_role_name):
            row_id = row.get("id")
            if not isinstance(row_id, int):
                continue
            row_fields = row.get("fields", {})
            old_remarks = str(row_fields.get("supervisor_remarks") or "")
            try:
                repo.update_ms(
                    row_id,
                    repo.filter_table_fields(
                        "ProductBatchMS",
                        {
                            "supervisor_remarks": remarks_text,
                            "updated_at": _now_iso(),
                            "last_updated_by": updated_by,
                        },
                    ),
                )
                updated_count += 1
            except Exception:
                continue
            repo.add_status_history(
                batch_id,
                "MS",
                row_id,
                old_remarks,
                remarks_text,
                updated_by,
                "Supervisor remarks updated (batch action).",
            )
        context.user_data.pop("my_ms_batch_remarks", None)
        role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
        _refresh_my_ms_jobs_selection(context, repo, role_name)
        await _reply(update, f"Updated remarks for {updated_count} row(s) in batch {batch_no or '-'}")
        await _show_my_ms_batch_action_menu(update, context, batch_id, batch_no, source_state=source_state)
        return True

    if state == MY_MS_JOBS_REMARKS_STATE:
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_jobs_remarks", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True
        remarks_text = str(text or "").strip()
        if not remarks_text:
            await _reply(update, "Remarks cannot be empty. Enter remarks text:", [[BACK_LABEL]])
            return True
        data = context.user_data.get("my_ms_jobs_remarks", {})
        row_id = data.get("row_id")
        batch_id = data.get("batch_id")
        if not isinstance(row_id, int):
            await _reply(update, "Invalid row for remarks.")
            return True
        repo = ProductionRepo()
        user = context.user_data.get("user", {})
        updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))
        row = repo.get_ms_row_by_id(row_id)
        if not row:
            await _reply(update, "MS row not found.")
            return True
        old_remarks = str(row.get("fields", {}).get("supervisor_remarks") or "")
        repo.update_ms(
            row_id,
            repo.filter_table_fields(
                "ProductBatchMS",
                {
                    "supervisor_remarks": remarks_text,
                    "updated_at": _now_iso(),
                    "last_updated_by": updated_by,
                },
            ),
        )
        if isinstance(batch_id, int):
            repo.add_status_history(
                batch_id,
                "MS",
                row_id,
                old_remarks,
                remarks_text,
                updated_by,
                "Supervisor remarks updated.",
            )
            part_name = _resolve_ms_row_part_text(repo, row.get("fields", {}))
            batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")
            await _notify_roles(
                context,
                batch_id,
                f"📝 Supervisor Remarks Added\n📦 Batch: {batch_no}\n🧩 Part: {part_name}\n💬 {remarks_text}",
                ["Production_Supervisor", "Production Supervisor", "Production_Manager"],
            )
        context.user_data.pop("my_ms_jobs_remarks", None)
        role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
        _refresh_my_ms_jobs_selection(context, repo, role_name)
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _reply(update, "Remarks added and notification sent.")
        await _show_my_ms_jobs_page(update, context)
        return True

    if state == MY_MS_JOBS_HANDOFF_REJECT_REMARKS_STATE:
        if text == BACK_LABEL:
            context.user_data.pop("my_ms_jobs_handoff_reject_remarks", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True
        remarks_text = str(text or "").strip()
        if not remarks_text:
            await _reply(update, "Rejection remarks cannot be empty. Enter remarks text:", [[BACK_LABEL]])
            return True
        data = context.user_data.get("my_ms_jobs_handoff_reject_remarks", {})
        row_id = data.get("row_id")
        if not isinstance(row_id, int):
            context.user_data.pop("my_ms_jobs_handoff_reject_remarks", None)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _reply(update, "Invalid row for handoff rejection.")
            await _show_my_ms_jobs_page(update, context)
            return True

        repo = ProductionRepo()
        user = context.user_data.get("user", {})
        user_role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
        updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))
        try:
            rejected = await _reject_ms_handoff_with_remarks(
                repo,
                context,
                row_id,
                updated_by,
                user_role_name,
                remarks_text,
            )
        except Exception:
            rejected = False
        context.user_data.pop("my_ms_jobs_handoff_reject_remarks", None)
        _refresh_my_ms_jobs_selection(context, repo, user_role_name)
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        if not rejected:
            await _reply(update, "Could not reject handoff. Check stage ownership/status and try again.")
            await _show_my_ms_jobs_page(update, context)
            return True
        await _reply(update, "Handoff rejected and sent back to previous supervisor with remarks.")
        await _show_my_ms_jobs_page(update, context)
        return True

    if state == AWAITING_SCHEDULE_DATE_STATE:
        if text == BACK_LABEL:
            schedule_ctx = context.user_data.get("schedule_date_context", {})
            return_state = schedule_ctx.get("return_state") or _target_return_state(context)
            context.user_data.pop("schedule_date_context", None)
            context.user_data["menu_state"] = return_state
            if return_state == MY_MS_JOBS_SELECTION_STATE:
                await _show_my_ms_jobs_page(update, context)
                return True
            if return_state == MY_MS_SCHEDULE_SELECTION_STATE:
                await _show_my_ms_schedule_page(update, context)
                return True
            return True

        scheduled_date_iso = _parse_schedule_date_text(text)
        if not scheduled_date_iso:
            await _reply(update, "Enter a valid date as YYYY-MM-DD or choose Today/Tomorrow.", [[_TODAY], [_TOMORROW], [BACK_LABEL]])
            return True

        schedule_ctx = context.user_data.get("schedule_date_context", {})
        batch_ids = schedule_ctx.get("batch_ids", [])
        return_state = schedule_ctx.get("return_state") or _target_return_state(context)
        repo = ProductionRepo()
        user = context.user_data.get("user", {})
        role_name = _resolve_user_role_name(repo, context)
        updated_by = repo.get_costing_user_ref_by_user_id(user.get("user_id", ""))
        scheduled = 0
        for batch_id in batch_ids:
            if not isinstance(batch_id, int):
                continue
            if not _is_batch_schedulable_for_role(repo, batch_id, role_name):
                continue
            await set_master_scheduled_date(
                context,
                batch_id,
                scheduled_date_iso,
                updated_by,
                remarks=f"Scheduled by {user.get('user_id', '')} via supervisor flow",
            )
            scheduled += 1
        context.user_data.pop("schedule_date_context", None)
        await _reply(update, f"Scheduled {scheduled} batch(es) for {scheduled_date_iso[:10]}.")

        if return_state == MY_MS_SCHEDULE_SELECTION_STATE:
            refreshed = _list_schedule_batches_for_user_role(repo, role_name)
            if not refreshed:
                context.user_data.pop("my_ms_schedule_selection", None)
                set_main_menu_state(context)
                await _reply(update, "No pending schedule batches in your queue.")
                return True
            context.user_data["my_ms_schedule_selection"] = {
                "records": refreshed,
                "page": 0,
                "page_size": settings.MSCUTLIST_PAGE_SIZE,
            }
            context.user_data["menu_state"] = MY_MS_SCHEDULE_SELECTION_STATE
            await _show_my_ms_schedule_page(update, context)
            return True

        if return_state == MY_MS_JOBS_SELECTION_STATE:
            _refresh_my_ms_jobs_selection(context, repo, role_name)
            context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
            await _show_my_ms_jobs_page(update, context)
            return True

        context.user_data["menu_state"] = return_state
        return True

    if state == AWAITING_APPROVAL_STATE:
        context.user_data["menu_state"] = _target_return_state(context)
        return False

    return False

