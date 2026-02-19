from __future__ import annotations

from pulse.data.production_repo import ProductionRepo
from pulse.notifications.dispatcher import dispatch_event

PRODUCTION_NOT_SCHEDULED_RULE = "production_batch_not_scheduled_reminder"


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
