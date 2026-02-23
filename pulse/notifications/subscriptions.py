from pulse.core.grist_client import GristClient
from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER


pulse_client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)
costing_client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)

RECIPIENT_MODE_OWNER_ONLY = "OWNER_ONLY"
RECIPIENT_MODE_OWNER_PLUS_SUBSCRIBERS = "OWNER_PLUS_SUBSCRIBERS"
RECIPIENT_MODE_SUBSCRIBERS_ONLY = "SUBSCRIBERS_ONLY"


def _normalize_ref_value(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _to_str(value):
    if value is None:
        return ""
    return str(value)


def _build_users_index(users: list[dict]) -> tuple[dict[str, dict], dict[int, dict]]:
    by_user_id = {}
    by_record_id = {}
    for user in users:
        fields = user.get("fields", {})
        user_id = fields.get("User_ID")
        if user_id:
            by_user_id[str(user_id)] = user
        rec_id = user.get("id")
        if isinstance(rec_id, int):
            by_record_id[rec_id] = user
    return by_user_id, by_record_id


def _find_event_record(events: list[dict], event_type: str) -> dict | None:
    for row in events:
        if _to_str(row.get("fields", {}).get("Event_ID")) == event_type:
            return row
    return None


def _get_event_recipient_mode(event_record: dict | None) -> str:
    if not event_record:
        return RECIPIENT_MODE_SUBSCRIBERS_ONLY
    mode = _to_str(event_record.get("fields", {}).get("Recipient_Mode")).strip().upper()
    if mode in (
        RECIPIENT_MODE_OWNER_ONLY,
        RECIPIENT_MODE_OWNER_PLUS_SUBSCRIBERS,
        RECIPIENT_MODE_SUBSCRIBERS_ONLY,
    ):
        return mode
    return RECIPIENT_MODE_SUBSCRIBERS_ONLY


def _resolve_owner_user_id(context: dict | None) -> str:
    if not context:
        return ""
    batch_id = context.get("batch_id")
    if not isinstance(batch_id, int):
        return ""

    masters = costing_client.get_records("ProductBatchMaster")
    costing_users = costing_client.get_records("Users")
    costing_user_id_by_rec_id = {
        row.get("id"): _to_str(row.get("fields", {}).get("User_ID"))
        for row in costing_users
        if isinstance(row.get("id"), int)
    }
    for record in masters:
        if record.get("id") != batch_id:
            continue
        created_by_value = _normalize_ref_value(record.get("fields", {}).get("created_by"))
        if isinstance(created_by_value, int):
            return costing_user_id_by_rec_id.get(created_by_value, "")
        created_by_text = _to_str(created_by_value).strip()
        if created_by_text.isdigit():
            return costing_user_id_by_rec_id.get(int(created_by_text), "")
        return created_by_text
    return ""


def _add_user_if_valid(
    user: dict | None,
    recipients: list[dict],
    seen_telegram_ids: set[str],
) -> None:
    if not user:
        return
    fields = user.get("fields", {})
    if not fields.get("Active"):
        return
    telegram_id = _to_str(fields.get("Telegram_ID")).strip()
    user_id = _to_str(fields.get("User_ID")).strip()
    if not telegram_id or not user_id or telegram_id in seen_telegram_ids:
        return
    recipients.append({"user_id": user_id, "telegram_id": telegram_id})
    seen_telegram_ids.add(telegram_id)


def _get_subscription_recipients(
    event_type: str,
    event_record: dict | None,
    users: list[dict],
) -> list[dict]:
    subs = pulse_client.get_records("Notification_Subscriptions")
    users_by_user_id, users_by_record_id = _build_users_index(users)

    event_row_id = event_record.get("id") if event_record else None
    recipients = []
    seen_telegram_ids: set[str] = set()

    for sub in subs:
        fields = sub.get("fields", {})
        if not fields.get("Enabled"):
            continue

        event_value = _normalize_ref_value(fields.get("Event"))
        is_match = False
        if isinstance(event_value, int):
            is_match = bool(event_row_id) and event_value == event_row_id
        else:
            is_match = _to_str(event_value) == event_type
        if not is_match:
            continue

        user_value = _normalize_ref_value(fields.get("User"))
        role_value = _normalize_ref_value(fields.get("Role"))

        explicit_user = None
        if isinstance(user_value, int):
            explicit_user = users_by_record_id.get(user_value)
        elif user_value not in (None, "", 0, "0"):
            explicit_user = users_by_user_id.get(_to_str(user_value))
        if explicit_user:
            _add_user_if_valid(explicit_user, recipients, seen_telegram_ids)
            continue

        if not isinstance(role_value, int):
            continue

        for user in users:
            user_fields = user.get("fields", {})
            user_role = _normalize_ref_value(user_fields.get("Role"))
            if user_role != role_value:
                continue
            _add_user_if_valid(user, recipients, seen_telegram_ids)

    return recipients


def _get_context_role_recipients(context: dict | None, users: list[dict]) -> list[dict]:
    if not context:
        return []
    role_names = context.get("recipient_roles")
    if not isinstance(role_names, list) or not role_names:
        return []
    normalized = {str(name).strip() for name in role_names if str(name).strip()}
    if not normalized:
        return []

    roles = pulse_client.get_records("Roles")
    role_ids = {
        row.get("id")
        for row in roles
        if str(row.get("fields", {}).get("Role_Name") or "").strip() in normalized
    }
    recipients = []
    seen_telegram_ids: set[str] = set()
    for user in users:
        user_fields = user.get("fields", {})
        user_role = _normalize_ref_value(user_fields.get("Role"))
        if user_role not in role_ids:
            continue
        _add_user_if_valid(user, recipients, seen_telegram_ids)
    return recipients


def get_subscribers(event_type: str, context: dict | None = None) -> list[dict]:
    events = pulse_client.get_records("Notification_Events")
    users = pulse_client.get_records("Users")
    users_by_user_id, _ = _build_users_index(users)
    event_record = _find_event_record(events, event_type)
    recipient_mode = _get_event_recipient_mode(event_record)

    recipients = []
    seen_telegram_ids: set[str] = set()

    if recipient_mode in (RECIPIENT_MODE_OWNER_ONLY, RECIPIENT_MODE_OWNER_PLUS_SUBSCRIBERS):
        owner_user_id = _resolve_owner_user_id(context)
        owner_user = users_by_user_id.get(owner_user_id)
        _add_user_if_valid(owner_user, recipients, seen_telegram_ids)

    if recipient_mode in (RECIPIENT_MODE_SUBSCRIBERS_ONLY, RECIPIENT_MODE_OWNER_PLUS_SUBSCRIBERS):
        subscription_recipients = _get_subscription_recipients(event_type, event_record, users)
        for user in subscription_recipients:
            telegram_id = _to_str(user.get("telegram_id"))
            if not telegram_id or telegram_id in seen_telegram_ids:
                continue
            recipients.append(user)
            seen_telegram_ids.add(telegram_id)

    context_role_recipients = _get_context_role_recipients(context, users)
    for user in context_role_recipients:
        telegram_id = _to_str(user.get("telegram_id"))
        if not telegram_id or telegram_id in seen_telegram_ids:
            continue
        recipients.append(user)
        seen_telegram_ids.add(telegram_id)

    return recipients
