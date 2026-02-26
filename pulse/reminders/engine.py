from __future__ import annotations

from pulse.data.production_repo import ProductionRepo
from pulse.notifications.dispatcher import dispatch_event
from pulse.integrations.production import build_schedule_inline_keyboard, build_stage_inline_keyboard

PRODUCTION_NOT_SCHEDULED_RULE = "production_batch_not_scheduled_reminder"
SUPERVISOR_BATCH_SCHEDULE_RULE = "supervisor_batch_schedule_reminder"
MS_STAGE_PENDING_RULE = "ms_stage_pending_reminder"


def _parse_int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _resolve_threshold_days(rule_fields: dict) -> int:
    return _parse_int(
        rule_fields.get("Threshold_Days")
        or rule_fields.get("Delay_Days")
        or rule_fields.get("Days")
        or rule_fields.get("Trigger_After_Days"),
        default=2,
    )


def _is_rule_enabled(rule_fields: dict) -> bool:
    enabled = rule_fields.get("Enabled")
    if enabled is None:
        return True
    return bool(enabled)


async def run_production_batch_reminder_checks(telegram_bot) -> int:
    repo = ProductionRepo()
    rule_fields = repo.get_reminder_rule(PRODUCTION_NOT_SCHEDULED_RULE)
    if rule_fields and not _is_rule_enabled(rule_fields):
        return 0

    threshold_days = _resolve_threshold_days(rule_fields)
    pending_batches = repo.list_batches_pending_schedule_reminder(threshold_days)

    for record in pending_batches:
        fields = record.get("fields", {})
        batch_no = fields.get("batch_no", "")
        start_date = fields.get("start_date", "")
        await dispatch_event(
            PRODUCTION_NOT_SCHEDULED_RULE,
            f"Reminder: Batch {batch_no} is still not scheduled. Start Date: {start_date}",
            telegram_bot,
            context={"batch_id": record.get("id")},
        )

    return len(pending_batches)


async def run_supervisor_batch_schedule_reminders(telegram_bot) -> int:
    repo = ProductionRepo()
    rule_fields = repo.get_reminder_rule(SUPERVISOR_BATCH_SCHEDULE_RULE)
    if rule_fields and not _is_rule_enabled(rule_fields):
        return 0
    threshold_days = _resolve_threshold_days(rule_fields)
    pending_batches = repo.list_supervisor_schedule_pending_batches(threshold_days)
    for batch in pending_batches:
        roles = batch.get("roles", [])
        batch_id = int(batch.get("batch_id"))
        batch_no = batch.get("batch_no", "")
        days_open = int(batch.get("days_open", 0))
        await dispatch_event(
            SUPERVISOR_BATCH_SCHEDULE_RULE,
            f"Reminder: Batch {batch_no} is not scheduled for {days_open} day(s). Please schedule now.",
            telegram_bot,
            context={"batch_id": batch_id, "recipient_roles": roles},
            reply_markup=build_schedule_inline_keyboard(batch_id),
        )
    return len(pending_batches)


async def run_ms_stage_pending_reminders(telegram_bot) -> int:
    repo = ProductionRepo()
    rule_fields = repo.get_reminder_rule(MS_STAGE_PENDING_RULE)
    if rule_fields and not _is_rule_enabled(rule_fields):
        return 0
    threshold_days = _resolve_threshold_days(rule_fields)
    pending_rows = repo.list_stage_rows_pending_reminder(threshold_days)
    for row in pending_rows:
        row_id = int(row.get("row_id"))
        batch_id = int(row.get("batch_id"))
        role_name = str(row.get("role_name") or "").strip()
        if not role_name:
            continue
        await dispatch_event(
            MS_STAGE_PENDING_RULE,
            (
                f"Reminder: Batch {row.get('batch_id')} | Part {row.get('product_part')} | "
                f"Process {row.get('process_seq')} | Stage {row.get('current_stage_name')} "
                f"is pending for {row.get('days_waiting')} day(s)."
            ),
            telegram_bot,
            context={"batch_id": batch_id, "recipient_roles": [role_name]},
            reply_markup=build_stage_inline_keyboard(batch_id, row_id),
        )
    return len(pending_rows)


async def run_all_reminder_checks(telegram_bot) -> int:
    total = 0
    total += await run_production_batch_reminder_checks(telegram_bot)
    total += await run_supervisor_batch_schedule_reminders(telegram_bot)
    total += await run_ms_stage_pending_reminders(telegram_bot)
    return total
