from pulse.core.grist_client import GristClient
from pulse.config import PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY


client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)


def _normalize_ref_value(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _build_role_index():
    roles = client.get_records("Roles")
    return {r["fields"].get("Role_ID"): r["id"] for r in roles}


def get_permissions_for_role(role_id):
    records = client.get_records("Role_Permissions")
    role_id = _normalize_ref_value(role_id)
    role_id_to_ref = _build_role_index()
    role_ref_id = role_id_to_ref.get(role_id)
    print("DEBUG PERMS input role_id:", role_id)
    print("DEBUG PERMS role_ref_id:", role_ref_id)

    perms = []

    for r in records:
        f = r["fields"]
        role_value = _normalize_ref_value(f.get("Role"))
        print("DEBUG PERMS record role_value:", role_value)
        if f.get("Active") and (role_value == role_id or role_value == role_ref_id):
            print("DEBUG PERMS MATCH FOUND")
            perms.append(f.get("Permission"))

    return perms


def has_permission(user_permissions, permission):
    return permission in user_permissions
