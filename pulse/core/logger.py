from pulse.core.grist_client import GristClient
from pulse.config import PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY
from datetime import datetime


client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)


def _resolve_user_ref(user_value):
    if user_value is None:
        return None
    if isinstance(user_value, int):
        return user_value

    user_id = str(user_value).strip()
    if not user_id:
        return None

    try:
        users = client.get_records("Users")
    except Exception:
        return None

    for user in users:
        if str(user.get("fields", {}).get("User_ID") or "") == user_id:
            return user.get("id")
    return None


def log_event(user_id, action, result):
    user_ref = _resolve_user_ref(user_id)
    payload = {
        "Timestamp": datetime.utcnow().isoformat(),
        "User": user_ref,
        "Action": action,
        "Result": result,
    }
    try:
        client.add_records("Activity_Log", [payload])
    except Exception:
        # Activity logging must never break workflow execution.
        pass
