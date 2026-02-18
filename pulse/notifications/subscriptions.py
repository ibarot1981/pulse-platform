from pulse.core.grist_client import GristClient
from pulse.config import PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY


client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)


def _normalize_ref_value(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def get_subscribers(event_type):

    events = client.get_records("Notification_Events")
    subs = client.get_records("Notification_Subscriptions")
    users = client.get_records("Users")
    event_ref_to_id = {row["id"]: row["fields"].get("Event_ID") for row in events}

    result = []
    seen_telegram_ids = set()

    for s in subs:
        f = s["fields"]
        if not f.get("Enabled"):
            continue

        event_value = _normalize_ref_value(f.get("Event"))
        if isinstance(event_value, int):
            resolved_event = event_ref_to_id.get(event_value)
        else:
            resolved_event = event_value

        if resolved_event != event_type:
            continue

        role = _normalize_ref_value(f.get("Role"))
        user = f.get("User")

        for u in users:
            uf = u["fields"]
            telegram_id = uf.get("Telegram_ID")
            if not telegram_id or telegram_id in seen_telegram_ids:
                continue

            if user and uf.get("User_ID") == user:
                result.append({
                    "user_id": uf.get("User_ID"),
                    "telegram_id": telegram_id
                })
                seen_telegram_ids.add(telegram_id)
            elif role and _normalize_ref_value(uf.get("Role")) == role:
                result.append({
                    "user_id": uf.get("User_ID"),
                    "telegram_id": telegram_id
                })
                seen_telegram_ids.add(telegram_id)

    return result
