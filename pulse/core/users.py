from pulse.core.grist_client import GristClient
from pulse.config import PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY


client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)


def _normalize_ref_value(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _build_roles_lookup():
    roles = client.get_records("Roles")
    return {r["id"]: r["fields"] for r in roles}


def get_user_by_telegram(telegram_id):
    users = client.get_records("Users")
    roles_lookup = _build_roles_lookup()

    for u in users:
        fields = u["fields"]
        if str(fields.get("Telegram_ID")) == str(telegram_id) and fields.get("Active"):
            role_ref = _normalize_ref_value(fields.get("Role"))
            print("DEBUG USER role_ref:", role_ref) # temp
            role_fields = roles_lookup.get(role_ref, {})
            print("DEBUG USER role_fields:", role_fields) # temp
            role_id = role_fields.get("Role_ID")
            print("DEBUG USER role_id:", role_id) # temp
            return {
                "record_id": u["id"],
                "user_id": fields.get("User_ID"),
                "name": fields.get("Name"),
                "role_id": role_id,
                "role": role_id,
                "role_ref_id": role_ref,
                "reports_to": fields.get("Reports_To")
            }

    return None
