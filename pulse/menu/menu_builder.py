from __future__ import annotations

from functools import lru_cache
from typing import Iterable

from telegram import ReplyKeyboardMarkup

from pulse.config import PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient
from pulse.core.permissions import get_permissions_for_role
from pulse.core.users import get_user_by_telegram
from pulse.settings import settings

client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)

# Central source of truth for permission -> menu label mapping.
# Kept for compatibility with existing handlers in main.py.
PERMISSION_MENU_MAP = {
    "production_view": "View Production Jobs",
    "production_complete": "Mark Job Completed",
    "sales_view": "View Sales Data",
    "sales_update": "Update Sales Data",
    "task_assign_main": "Assign Task",
    "task_assign_usercontext": "Assign Task",
    "task_close": "My Tasks",
    "user_manage": "Manage Users",
    "reminder_manage": "Reminder Rules",
}

# Feature flags that can hide menu items even when permission exists.
PERMISSION_FEATURE_FLAGS = {
    "task_assign_main": "ENABLE_TASKS",
    "task_assign_usercontext": "ENABLE_TASKS",
    "task_close": "ENABLE_TASKS",
    "reminder_manage": "ENABLE_REMINDERS",
}


def _normalize_ref_value(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


@lru_cache(maxsize=1)
def _permissions_metadata():
    records = client.get_records("Permissions")
    metadata = {}

    for record in records:
        row_id = record["id"]
        fields = record["fields"]
        permission_id = fields.get("Permission_ID")
        menu_label = (
            fields.get("Menu_Label")
            or fields.get("Menu_Label_Text")
            or fields.get("Menu")
            or fields.get("Label")
            or fields.get("Name")
            or PERMISSION_MENU_MAP.get(permission_id)
        )
        metadata[row_id] = {
            "permission_id": permission_id,
            "menu_label": menu_label,
            "menu_parent": fields.get("Menu_Parent") or "MAIN",
            "action_type": fields.get("Action_Type") or "RUN_STUB",
            "action_target": fields.get("Action_Target"),
        }

    return metadata


def _is_permission_enabled(permission_id: str | None) -> bool:
    if not permission_id:
        return True
    flag_name = PERMISSION_FEATURE_FLAGS.get(permission_id)
    if not flag_name:
        return True
    return bool(getattr(settings, flag_name, False))


def get_menu_labels_for_permissions(
    permissions: Iterable[object],
    menu_parent: str = "MAIN",
) -> list[str]:
    actions = get_menu_actions_for_permissions(permissions, menu_parent=menu_parent)
    return list(actions.keys())


def get_menu_actions_for_permissions(
    permissions: Iterable[object],
    menu_parent: str = "MAIN",
) -> dict[str, dict[str, str | None]]:
    metadata = _permissions_metadata()
    permission_row_ids = []
    seen_row_ids = set()
    for permission in permissions:
        row_id = _normalize_ref_value(permission)
        if row_id is None or row_id in seen_row_ids:
            continue
        permission_row_ids.append(row_id)
        seen_row_ids.add(row_id)
    actions: dict[str, dict[str, str | None]] = {}

    for row_id in permission_row_ids:
        details = metadata.get(row_id)
        if not details:
            continue

        menu_label = details.get("menu_label")
        permission_id = details.get("permission_id")
        permission_menu_parent = details.get("menu_parent") or "MAIN"
        action_type = details.get("action_type") or "RUN_STUB"
        action_target = details.get("action_target")

        if not menu_label:
            continue
        if permission_menu_parent != menu_parent:
            continue
        if not _is_permission_enabled(permission_id):
            continue
        if menu_label in actions:
            continue

        actions[menu_label] = {
            "permission_id": permission_id,
            "action_type": action_type,
            "action_target": action_target,
        }

    return actions


def get_enabled_permission_ids(permissions: Iterable[object]) -> set[str]:
    metadata = _permissions_metadata()
    permission_ids: set[str] = set()

    for permission in permissions:
        row_id = _normalize_ref_value(permission)
        if row_id is None:
            continue

        details = metadata.get(row_id)
        if not details:
            continue

        permission_id = details.get("permission_id")
        if not permission_id:
            continue
        if not _is_permission_enabled(permission_id):
            continue

        permission_ids.add(permission_id)

    return permission_ids


def get_menu_labels_for_user(telegram_id: int | str) -> list[str]:
    user = get_user_by_telegram(telegram_id)
    if not user:
        return []

    permissions = get_permissions_for_role(user["role"])
    return get_menu_labels_for_permissions(permissions)


def build_menu_markup(menu_labels: list[str]) -> ReplyKeyboardMarkup:
    keyboard = [[label] for label in menu_labels]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)
