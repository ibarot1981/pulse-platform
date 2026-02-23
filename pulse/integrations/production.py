from __future__ import annotations

from datetime import datetime
import tempfile

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup

from pulse.data.production_repo import ProductionRepo
from pulse.integrations.workflow_routing import next_stage_from_index, parse_process_seq
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
MY_MS_JOBS_CONFIRM_STATE = "my_ms_jobs_confirm"

ACTION_NEW_PRODUCTION_BATCH = "NEW_PRODUCTION_BATCH"
ACTION_PENDING_APPROVALS = "PRODUCTION_PENDING_APPROVALS"
ACTION_MY_MS_JOBS = "MY_MS_JOBS"

_PAGE_PREV = "Prev"
_PAGE_NEXT = "Next"
_MODE_BY_MODEL = "By Product Model"
_MODE_BY_PART = "By Product Part"
_YES = "Yes"
_NO = "No"
_REJECT = "Reject"

_TYPE_COMPLETE = "New Complete Batch (M-C-S)"
_TYPE_MS = "MS Only"
_TYPE_CNC = "CNC Only"
_TYPE_STORE = "Store Only"
_APPROVAL_CB_PREFIX = "prodappr"


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


def _build_keyboard(rows: list[list[str]]):
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


async def _reply(update, text: str, rows: list[list[str]] | None = None):
    if rows:
        has_main_menu = any(row and row[0] == MAIN_MENU_LABEL for row in rows)
        if not has_main_menu:
            rows = rows + [[MAIN_MENU_LABEL]]
    markup = _build_keyboard(rows) if rows else None
    await update.effective_message.reply_text(text, reply_markup=markup)


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


def _normalize_process_seq(fields: dict) -> str:
    value = fields.get("process_seq")
    if not value:
        value = fields.get("Process_Seq")
    return str(value or "").strip()


def _format_qty(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return f"{value:g}"


def _build_ms_rows(repo: ProductionRepo, batch_id: int, part_ids: list[int], batch_qty: int, timestamp_iso: str, updated_by) -> list[dict]:
    grouped: dict[tuple[str, str], float] = {}
    ms_columns = repo.get_ms_table_column_ids()

    for record in repo.get_ms_rows(part_ids):
        fields = record.get("fields", {})
        process_seq = _normalize_process_seq(fields)
        if not process_seq:
            continue
        part_name = str(fields.get("ProductPartName_ProductPartName") or "")
        total_qty = float(fields.get("QtyNos") or 0) * batch_qty
        if total_qty <= 0:
            continue
        group_key = (part_name, process_seq)
        grouped[group_key] = grouped.get(group_key, 0.0) + total_qty

    rows = []
    for (part_name, process_seq), total_qty in grouped.items():
        stages = parse_process_seq(process_seq)
        if not stages:
            continue
        first_stage = stages[0]
        base_fields = {
            "batch_id": batch_id,
            "product_part": part_name,
            "process_seq": process_seq,
            "total_qty": total_qty,
            "current_stage_index": 0,
            "current_stage_name": first_stage,
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
    grouped: dict[tuple[str, str, str, str], float] = {}
    for record in repo.get_ms_rows(part_ids):
        fields = record.get("fields", {})
        process_seq = _normalize_process_seq(fields)
        if not process_seq:
            continue
        part_name = str(fields.get("ProductPartName_ProductPartName") or "")
        material_ref = _normalize_ref(fields.get("MaterialToCut"))
        material_name = material_map.get(material_ref, "") if isinstance(material_ref, int) else ""
        length_mm = _format_qty(float(fields.get("Length_mm") or 0))
        total_qty = float(fields.get("QtyNos") or 0) * batch_qty
        if total_qty <= 0:
            continue
        key = (process_seq, part_name, material_name, length_mm)
        grouped[key] = grouped.get(key, 0.0) + total_qty

    sections: dict[str, list[dict]] = {}
    for (process_seq, part_name, material_name, length_mm), total_qty in grouped.items():
        sections.setdefault(process_seq, []).append(
            {
                "product_part": part_name,
                "material_to_cut": material_name,
                "length_mm": length_mm,
                "total_qty": _format_qty(total_qty),
            }
        )

    ordered_sections = []
    for process_seq in sorted(sections):
        rows = sorted(sections[process_seq], key=lambda row: (row["product_part"], row["material_to_cut"], row["length_mm"]))
        ordered_sections.append({"process_seq": process_seq, "rows": rows})
    return ordered_sections


def _resolve_supervisor_role_for_stage(repo: ProductionRepo, stage_name: str) -> str:
    mapping = repo.get_process_stage_mapping()
    details = mapping.get(stage_name, {})
    return str(details.get("supervisor_role") or "").strip()


async def _notify_stage_event(
    context,
    event_type: str,
    batch_id: int,
    message: str,
    supervisor_role: str = "",
) -> None:
    event_context = {"batch_id": batch_id}
    if supervisor_role:
        event_context["recipient_roles"] = [supervisor_role]
    await _notify_event(context.bot, event_type, message, context=event_context)


async def _notify_event(
    telegram_bot,
    event_type: str,
    message: str,
    context: dict | None = None,
    reply_markup=None,
) -> None:
    await dispatch_event(event_type, message, telegram_bot, context=context, reply_markup=reply_markup)


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

    await _notify_event(
        context.bot,
        "production_batch_created",
        f"Batch created: {batch_no} | Model: {flow['model_code']} | Qty: {flow['batch_qty']} | Approval: Pending",
        context={"batch_id": master_id},
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Click Here to Approve", callback_data=_approval_callback_data("open", master_id))]]
        ),
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


async def _notify_ms_first_stage(repo: ProductionRepo, context, batch_id: int, ms_rows: list[dict], batch_no: str) -> None:
    notified_pairs = set()
    for row in ms_rows:
        stage_name = str(row.get("current_stage_name") or "").strip()
        if not stage_name:
            continue
        supervisor_role = _resolve_supervisor_role_for_stage(repo, stage_name)
        if not supervisor_role:
            continue
        notify_key = (stage_name, supervisor_role)
        if notify_key in notified_pairs:
            continue
        notified_pairs.add(notify_key)
        await _notify_stage_event(
            context,
            "ms_stage_pending",
            batch_id,
            f"MS stage pending for batch {batch_no}: {stage_name}",
            supervisor_role=supervisor_role,
        )


def _attach_ms_cutlist_pdf(repo: ProductionRepo, batch_id: int, batch_no: str, section_rows: list[dict]) -> None:
    if not section_rows:
        return
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = f"{temp_dir}\\ms_cut_list_{batch_no}.pdf"
        title = f"MS Cut List - {batch_no} ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')})"
        write_grouped_ms_cutlist_pdf(section_rows, file_path, title=title)
        repo.attach_pdf_to_master(batch_id, file_path, field_name="ms_cutlist_pdf")


def _get_batch_no_map(repo: ProductionRepo, batch_ids: set[int]) -> dict[int, str]:
    if not batch_ids:
        return {}
    result = {}
    for record in repo.get_all_master_batches():
        record_id = record.get("id")
        if record_id not in batch_ids:
            continue
        result[record_id] = str(record.get("fields", {}).get("batch_no") or "")
    return result


def _list_ms_jobs_for_user_role(repo: ProductionRepo, role_name: str) -> list[dict]:
    if not role_name:
        return []
    stage_mapping = repo.get_process_stage_mapping()
    stage_names = {stage for stage, details in stage_mapping.items() if details.get("supervisor_role") == role_name}
    if not stage_names:
        return []

    rows = []
    batch_ids = set()
    for record in repo.costing_client.get_records("ProductBatchMS"):
        fields = record.get("fields", {})
        stage_name = str(fields.get("current_stage_name") or "").strip()
        current_status = str(fields.get("current_status") or fields.get("status") or "").strip()
        if stage_name not in stage_names:
            continue
        if current_status.startswith("In ") or current_status == "Cutting Completed":
            continue
        batch_id = _normalize_ref(fields.get("batch_id"))
        if not isinstance(batch_id, int):
            continue
        batch_ids.add(batch_id)
        rows.append(record)

    batch_no_map = _get_batch_no_map(repo, batch_ids)
    rows.sort(
        key=lambda row: (
            batch_no_map.get(_normalize_ref(row.get("fields", {}).get("batch_id")), ""),
            str(row.get("fields", {}).get("product_part") or ""),
            row.get("id", 0),
        )
    )
    return rows


async def _show_my_ms_jobs_page(update, context) -> None:
    selection = context.user_data.get("my_ms_jobs_selection", {})
    records = selection.get("records", [])
    page = selection.get("page", 0)
    page_size = selection.get("page_size", settings.MSCUTLIST_PAGE_SIZE)
    page_records, _, end = _paginate(records, page, page_size)

    if not records:
        set_main_menu_state(context)
        await _reply(update, "No MS jobs in your current stage queue.")
        return

    repo = ProductionRepo()
    batch_ids = {_normalize_ref(row.get("fields", {}).get("batch_id")) for row in page_records}
    batch_no_map = _get_batch_no_map(repo, {bid for bid in batch_ids if isinstance(bid, int)})

    lines = ["My MS Jobs:", "Enter number(s) like 1 or 1,3", ""]
    for idx, row in enumerate(page_records, start=1):
        fields = row.get("fields", {})
        batch_id = _normalize_ref(fields.get("batch_id"))
        batch_no = batch_no_map.get(batch_id, str(batch_id or ""))
        qty = fields.get("total_qty") if fields.get("total_qty") is not None else fields.get("required_qty")
        qty_text = _format_qty(float(qty or 0))
        lines.append(
            f"{idx}. Batch: {batch_no} | Part: {fields.get('product_part', '')} | Qty: {qty_text} | Status: {fields.get('current_status') or fields.get('status') or ''}"
        )

    rows = []
    if page > 0:
        rows.append([_PAGE_PREV])
    if end < len(records):
        rows.append([_PAGE_NEXT])
    rows.append([BACK_LABEL])
    await _reply(update, "\n".join(lines), rows)


async def start_my_ms_jobs(update, context) -> None:
    repo = ProductionRepo()
    user = context.user_data.get("user", {})
    role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
    records = _list_ms_jobs_for_user_role(repo, role_name)
    if not records:
        set_main_menu_state(context)
        await _reply(update, "No MS jobs in your queue.")
        return

    context.user_data["my_ms_jobs_selection"] = {
        "records": records,
        "page": 0,
        "page_size": settings.MSCUTLIST_PAGE_SIZE,
    }
    context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
    await _show_my_ms_jobs_page(update, context)


async def _show_ms_jobs_confirmation(update, context) -> None:
    data = context.user_data.get("my_ms_jobs_confirm", {})
    selected_rows = data.get("selected_rows", [])
    if not selected_rows:
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _show_my_ms_jobs_page(update, context)
        return

    lines = ["Mark selected MS jobs as Done?"]
    for row in selected_rows:
        fields = row.get("fields", {})
        lines.append(
            f"- {fields.get('product_part', '')} | {fields.get('current_stage_name') or ''} | {fields.get('current_status') or fields.get('status') or ''}"
        )
    context.user_data["menu_state"] = MY_MS_JOBS_CONFIRM_STATE
    await _reply(update, "\n".join(lines), [[_YES], [_NO], [BACK_LABEL]])


async def advance_ms_stage(repo: ProductionRepo, context, row_id: int, updated_by) -> dict:
    row = repo.get_ms_row_by_id(row_id)
    if not row:
        raise ValueError("MS row not found.")
    fields = row.get("fields", {})
    batch_id = _normalize_ref(fields.get("batch_id"))
    if not isinstance(batch_id, int):
        raise ValueError("Invalid batch reference in MS row.")
    process_seq = str(fields.get("process_seq") or "").strip()
    stages = parse_process_seq(process_seq)
    if not stages:
        raise ValueError("Missing process sequence on MS row.")

    current_stage_index = int(fields.get("current_stage_index") or 0)
    current_stage_name = str(fields.get("current_stage_name") or stages[min(current_stage_index, len(stages) - 1)])
    old_status = str(fields.get("current_status") or fields.get("status") or "")
    next_index, next_stage = next_stage_from_index(stages, current_stage_index)
    now_iso = _now_iso()

    current_stage_role = _resolve_supervisor_role_for_stage(repo, current_stage_name)
    if next_stage is None:
        new_status = "Cutting Completed"
        history_remarks = "MS workflow completed without further stages."
        update_fields = {
            "current_stage_index": next_index,
            "current_status": new_status,
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
            "current_status": new_status,
            "updated_at": now_iso,
            "last_updated_by": updated_by,
            "status": new_status,
        }

    safe_updates = repo.filter_table_fields("ProductBatchMS", update_fields)
    repo.update_ms(row_id, safe_updates)
    repo.add_status_history(batch_id, "MS", row_id, old_status, new_status, updated_by, history_remarks)

    part_name = str(fields.get("product_part") or "")
    batch_no = str((repo.get_master_by_id(batch_id) or {}).get("fields", {}).get("batch_no") or "")

    await _notify_stage_event(
        context,
        "ms_stage_completed",
        batch_id,
        f"MS stage completed for batch {batch_no}: {part_name} | Stage: {current_stage_name} | Status: {new_status}",
        supervisor_role=current_stage_role,
    )

    if next_stage:
        next_stage_role = _resolve_supervisor_role_for_stage(repo, next_stage)
        await _notify_stage_event(
            context,
            "ms_stage_pending",
            batch_id,
            f"MS stage pending for batch {batch_no}: {part_name} | Stage: {next_stage} | Status: {new_status}",
            supervisor_role=next_stage_role,
        )

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
        if child_statuses and all(status in ("Done", "Completed", "Cutting Completed") or status.startswith("In ") for status in child_statuses):
            new_status = "Completed"
        elif any(status == "In Progress" for status in child_statuses):
            new_status = "In Progress"
        elif child_statuses and all(status == "Schedule Pending" for status in child_statuses):
            new_status = "Schedule Pending"
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
    if include_ms:
        part_ids = _resolve_part_ids_for_master(repo, fields)
        batch_qty = int(fields.get("qty") or 0)
        ms_rows = _build_ms_rows(repo, batch_id, part_ids, batch_qty, timestamp_iso=now_iso, updated_by=approved_by)
        ms_row_ids = repo.create_ms_rows(ms_rows)
        cutlist_sections = _build_ms_cutlist_sections(repo, part_ids, batch_qty)

    return {
        "master": repo.get_master_by_id(batch_id) or record,
        "ms_rows": ms_rows,
        "ms_row_ids": ms_row_ids,
        "cutlist_sections": cutlist_sections,
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
            await _notify_ms_first_stage(repo, context, batch_id, updated.get("ms_rows", []), batch_no)
            await _notify_event(
                context.bot,
                "production_batch_approved",
                f"Batch approved: {batch_no} | Start Date: {fields.get('start_date', '')} | Status: Schedule Pending",
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

    parsed = _parse_approval_callback_data(str(query.data))
    if not parsed:
        return False
    action, batch_id = parsed
    await query.answer()

    if not _is_production_manager(context):
        await query.message.reply_text("Only Production Manager or System Admin can approve/reject batches.")
        return True

    repo = ProductionRepo()
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
            await query.message.reply_text(f"Approved: {', '.join(approved_batch_numbers)}")
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

        if approved_batch_numbers:
            await _reply(update, f"Approved: {', '.join(approved_batch_numbers)}")
        if rejected_batch_numbers:
            await _reply(update, f"Rejected: {', '.join(rejected_batch_numbers)}")

        context.user_data.pop("pending_approvals_selection", None)
        set_main_menu_state(context)
        if not selection["records"]:
            await _reply(update, "No pending approvals remaining.")
            return True
        await _reply(update, "Approval updated. Returning to main menu.")
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
            context.user_data.pop("my_ms_jobs_selection", None)
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

        context.user_data["my_ms_jobs_confirm"] = {
            "selected_ids": [record["id"] for record in selected_records],
            "selected_rows": selected_records,
        }
        await _show_ms_jobs_confirmation(update, context)
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
                await advance_ms_stage(repo, context, row_id, updated_by)
                done_count += 1
            except Exception:
                continue

        context.user_data.pop("my_ms_jobs_confirm", None)
        role_name = repo.get_role_name_by_user_id(user.get("user_id", ""))
        refreshed = _list_ms_jobs_for_user_role(repo, role_name)
        if not refreshed:
            context.user_data.pop("my_ms_jobs_selection", None)
            set_main_menu_state(context)
            await _reply(update, f"Updated {done_count} job(s). No pending MS jobs in your queue.")
            return True
        context.user_data["my_ms_jobs_selection"] = {
            "records": refreshed,
            "page": 0,
            "page_size": settings.MSCUTLIST_PAGE_SIZE,
        }
        context.user_data["menu_state"] = MY_MS_JOBS_SELECTION_STATE
        await _reply(update, f"Updated {done_count} job(s).")
        await _show_my_ms_jobs_page(update, context)
        return True

    if state == AWAITING_APPROVAL_STATE:
        context.user_data["menu_state"] = _target_return_state(context)
        return False

    return False
