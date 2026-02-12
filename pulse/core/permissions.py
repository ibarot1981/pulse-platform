from pulse.core.grist_client import GristClient
from pulse.config import PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY


client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)


def get_permissions_for_role(role_id):
    records = client.get_records("Role_Permissions")

    perms = []

    for r in records:
        f = r["fields"]
        if f.get("Role") == role_id and f.get("Active"):
            perms.append(f.get("Permission"))

    return perms


def has_permission(user_permissions, permission):
    return permission in user_permissions
