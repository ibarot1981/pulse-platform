from __future__ import annotations

from datetime import datetime

from telegram import ReplyKeyboardMarkup

from pulse.data.production_repo import ProductionRepo
from pulse.menu.submenu import BACK_LABEL, MAIN_STATE
from pulse.notifications.dispatcher import dispatch_event
from pulse.settings import settings

SELECTING_BATCH_MODE_STATE = "selecting_batch_mode"
SELECTING_PRODUCT_MODEL_STATE = "selecting_product_model"
SELECTING_PRODUCT_PARTS_STATE = "selecting_product_parts"
ENTERING_BATCH_QTY_STATE = "entering_batch_qty"
SELECTING_BATCH_TYPE_STATE = "selecting_batch_type"
CONFIRMING_BATCH_STATE = "confirming_batch"
AWAITING_APPROVAL_STATE = "awaiting_approval"
PENDING_APPROVALS_SELECTION_STATE = "pending_approvals_selection"

ACTION_NEW_PRODUCTION_BATCH = "NEW_PRODUCTION_BATCH"
ACTION_PENDING_APPROVALS = "PRODUCTION_PENDING_APPROVALS"

_PAGE_PREV = "⬅ Prev"
_PAGE_NEXT = "➡ Next"
_MODE_BY_MODEL = "By Product Model"
_MODE_BY_PART = "By Product Part"
_YES = "Yes"
_NO = "No"

_TYPE_COMPLETE = "New Complete Batch (M-C-S)"
_TYPE_MS = "MS Only"
_TYPE_CNC = "CNC Only"
_TYPE_STORE = "Store Only"


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
    context.user_data.pop("selected_pending_batch_id", None)


def _build_keyboard(rows: list[list[str]]):
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


async def _reply(update, text: str, rows: list[list[str]] | None = None):
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


def _build_master_notification_users(repo: ProductionRepo, creator_user_id: str) -> str:
    users = repo.get_users()
    role_users = []
    role_ids = {r["id"]: r["fields"].get("Role_Name") for r in repo.get_roles()}
    for user in users:
        fields = user.get("fields", {})
        if not fields.get("Active"):
            continue
        role_ref = _normalize_ref(fields.get("Role"))
        role_name = role_ids.get(role_ref)
        if role_name in ("Production_Manager", "System_Admin"):
            user_id = fields.get("User_ID")
            if user_id:
                role_users.append(user_id)

    all_users = [creator_user_id] + role_users
    dedup = []
    for user_id in all_users:
        if user_id and user_id not in dedup:
            dedup.append(user_id)
    return ",".join(dedup)


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


def _build_ms_rows(repo: ProductionRepo, batch_id: int, part_ids: list[int], batch_qty: int) -> list[dict]:
    material_map = repo.get_material_name_map()
    rows = []
    for record in repo.get_ms_rows(part_ids):
        fields = record.get("fields", {})
        part_name = fields.get("ProductPartName_ProductPartName") or ""
        material_ref = _normalize_ref(fields.get("MaterialToCut"))
        required_qty = float(fields.get("QtyNos") or 0) * batch_qty
        rows.append(
            {
                "batch_id": batch_id,
                "product_part": part_name,
                "material_to_cut": material_map.get(material_ref, ""),
                "required_qty": required_qty,
                "status": "Schedule Pending",
                "scheduled_date": None,
                "expected_completion_date": None,
                "remarks": "",
            }
        )
    return rows


def _build_cnc_rows(repo: ProductionRepo, batch_id: int, part_ids: list[int], batch_qty: int) -> list[dict]:
    gauge_map = repo.get_cnc_sheet_gauge_map()
    rows = []
    for record in repo.get_cnc_rows(part_ids):
        fields = record.get("fields", {})
        part_name = fields.get("ProductPartName_ProductPartName2") or ""
        cnc_part_ref = _normalize_ref(fields.get("CNC_Part_File_Name"))
        required_qty = float(fields.get("QtyNos") or 0) * batch_qty
        rows.append(
            {
                "batch_id": batch_id,
                "product_part": part_name,
                "sheet_gauge": gauge_map.get(cnc_part_ref, ""),
                "sheet_size": "",
                "required_qty": required_qty,
                "status": "Schedule Pending",
                "nest_status": "Nest Pending",
                "scheduled_date": None,
                "expected_completion_date": None,
                "remarks": "",
            }
        )
    return rows


def _build_store_rows(repo: ProductionRepo, batch_id: int, model_code: str, batch_qty: int) -> list[dict]:
    issue_slips = repo.get_store_issue_slip_ids_for_model(model_code)
    rows = []
    for record in repo.get_store_issue_items(issue_slips):
        fields = record.get("fields", {})
        required_qty = float(fields.get("Qty") or 0) * batch_qty
        rows.append(
            {
                "batch_id": batch_id,
                "item_name": fields.get("Product_Part_Name") or "",
                "source_type": "",
                "required_qty": required_qty,
                "status": "Schedule Pending",
                "scheduled_date": None,
                "expected_completion_date": None,
                "remarks": "",
            }
        )
    return rows


async def _notify_batch(
    repo: ProductionRepo,
    telegram_bot,
    event_type: str,
    message: str,
    creator_user_id: str,
) -> None:
    await dispatch_event(event_type, message, telegram_bot)
    creator_telegram = repo.get_telegram_by_user_id(creator_user_id)
    if creator_telegram:
        try:
            await telegram_bot.send_message(chat_id=creator_telegram, text=message)
        except Exception:
            pass


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


async def _create_batch_from_flow(update, context):
    flow = _get_flow(context)
    repo = ProductionRepo()
    user = context.user_data.get("user", {})
    creator_user_id = user.get("user_id", "")

    batch_type, include_ms, include_cnc, include_store = _resolve_type_flags(flow["batch_type"])
    batch_no = generate_batch_number(repo, flow["model_code"], include_ms, include_cnc, include_store)

    master_id = repo.create_master_batch(
        {
            "batch_no": batch_no,
            "product_model": flow["model_code"],
            "qty": flow["batch_qty"],
            "batch_type": batch_type,
            "include_ms": include_ms,
            "include_cnc": include_cnc,
            "include_store": include_store,
            "created_by": creator_user_id,
            "created_date": _now_iso(),
            "approval_status": "Pending Approval",
            "overall_status": "Pending Approval",
            "notification_users": _build_master_notification_users(repo, creator_user_id),
        }
    )

    selected_part_ids = flow.get("selected_part_ids", [])
    if flow.get("batch_mode") == _MODE_BY_MODEL:
        parts = repo.get_product_parts_for_model(flow["model_code"])
        selected_part_ids = [part["part_id"] for part in parts]

    if include_ms:
        repo.create_ms_rows(_build_ms_rows(repo, master_id, selected_part_ids, flow["batch_qty"]))
    if include_cnc:
        repo.create_cnc_rows(_build_cnc_rows(repo, master_id, selected_part_ids, flow["batch_qty"]))
    if include_store:
        repo.create_store_rows(_build_store_rows(repo, master_id, flow["model_code"], flow["batch_qty"]))

    context.user_data["menu_state"] = AWAITING_APPROVAL_STATE
    await _reply(update, f"Batch created: {batch_no}\nStatus: Pending Approval")
    _clear_flow(context)


def _is_production_manager(context) -> bool:
    user = context.user_data.get("user", {})
    return user.get("role") == "R02"


async def start_pending_approvals(update, context) -> None:
    if not _is_production_manager(context):
        await _reply(update, "Only Production Manager can approve batches.")
        return

    repo = ProductionRepo()
    pending = repo.list_pending_approvals()
    if not pending:
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

    lines = ["Pending Approvals:\n"]
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


def recalculate_master_overall_status(repo: ProductionRepo, batch_id: int, updated_by: str) -> str:
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
        if child_statuses and all(status in ("Done", "Completed") for status in child_statuses):
            new_status = "Completed"
        elif any(status == "In Progress" for status in child_statuses):
            new_status = "In Progress"
        elif child_statuses and all(status == "Schedule Pending" for status in child_statuses):
            new_status = "Schedule Pending"
        else:
            new_status = old_status or "Schedule Pending"

    if new_status != old_status:
        repo.update_master(batch_id, {"overall_status": new_status})
        repo.add_status_history(batch_id, "Master", batch_id, old_status, new_status, updated_by, "")
    return new_status


async def approve_batch_by_id(update, context, batch_id: int) -> None:
    if not _is_production_manager(context):
        await _reply(update, "Only Production Manager can approve batches.")
        return

    repo = ProductionRepo()
    record = repo.get_master_by_id(batch_id)
    if not record:
        await _reply(update, "Batch not found.")
        return

    fields = record.get("fields", {})
    old_approval = fields.get("approval_status") or ""
    old_overall = fields.get("overall_status") or ""
    user = context.user_data.get("user", {})
    updated_by = user.get("user_id", "")

    repo.update_master(batch_id, {"approval_status": "Approved", "overall_status": "Schedule Pending"})
    repo.add_status_history(batch_id, "Master", batch_id, old_approval, "Approved", updated_by, "Batch approved")
    if old_overall != "Schedule Pending":
        repo.add_status_history(
            batch_id,
            "Master",
            batch_id,
            old_overall,
            "Schedule Pending",
            updated_by,
            "Approval transition",
        )

    await _notify_batch(
        repo,
        context.bot,
        "batch_approved",
        f"Batch {fields.get('batch_no')} approved. Overall status: Schedule Pending.",
        fields.get("created_by", ""),
    )
    await _reply(update, f"Approved batch: {fields.get('batch_no')}")


async def update_child_status(
    context,
    batch_id: int,
    entity_type: str,
    row_id: int,
    new_status: str,
    updated_by: str,
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
    old_status = row.get("fields", {}).get("status") or ""

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
    await _notify_batch(
        repo,
        context.bot,
        "batch_status_changed",
        f"{entity_type} status changed for batch {batch_no}: {old_status} -> {new_status}. Master: {new_master_status}",
        master.get("fields", {}).get("created_by", ""),
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

        selected_indices = []
        try:
            tokens = [item.strip() for item in text.split(",")]
            for token in tokens:
                if token:
                    selected_indices.append(int(token))
        except ValueError:
            await _reply(update, "Enter comma-separated numbers like: 1,3,4")
            return True

        if not selected_indices:
            await _reply(update, "Select at least one part number.")
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
        context.user_data["menu_state"] = CONFIRMING_BATCH_STATE
        await _reply(update, _batch_summary_text(flow), [[_YES], [_NO], [BACK_LABEL]])
        return True

    if state == CONFIRMING_BATCH_STATE:
        if text == BACK_LABEL:
            context.user_data["menu_state"] = ENTERING_BATCH_QTY_STATE
            await _reply(update, "Enter Batch Quantity:")
            return True
        if text == _NO:
            await start_new_production_batch(update, context)
            return True
        if text == _YES:
            await _show_batch_type_prompt(update, context)
            return True
        await _reply(update, "Select Yes or No.", [[_YES], [_NO], [BACK_LABEL]])
        return True

    if state == SELECTING_BATCH_TYPE_STATE:
        if text == BACK_LABEL:
            context.user_data["menu_state"] = CONFIRMING_BATCH_STATE
            await _reply(update, _batch_summary_text(flow), [[_YES], [_NO], [BACK_LABEL]])
            return True
        if text not in (_TYPE_COMPLETE, _TYPE_MS, _TYPE_CNC, _TYPE_STORE):
            await _reply(update, "Select a valid batch type.")
            return True

        flow["batch_type"] = text
        await _create_batch_from_flow(update, context)
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

        index = _selection_index_from_text(text, page, page_size)
        if index is None or index < 0 or index >= len(records):
            await _reply(update, "Enter a valid number from the list.")
            return True

        record = records[index]
        await approve_batch_by_id(update, context, record["id"])
        selection["records"] = ProductionRepo().list_pending_approvals()
        if not selection["records"]:
            context.user_data["menu_state"] = _target_return_state(context)
            context.user_data.pop("pending_approvals_selection", None)
            await _reply(update, "No pending approvals remaining.")
            return True
        await _show_pending_approvals_page(update, context)
        return True

    if state == AWAITING_APPROVAL_STATE:
        context.user_data["menu_state"] = _target_return_state(context)
        return False

    return False
