from __future__ import annotations

from pulse.notifications import subscriptions


class _FakeClient:
    def __init__(self, tables: dict[str, list[dict]]):
        self.tables = tables

    def get_records(self, table: str) -> list[dict]:
        return list(self.tables.get(table, []))


def test_get_subscribers_includes_owner_creator_and_notifiers_and_dedup(monkeypatch):
    pulse_tables = {
        "Roles": [
            {"id": 1, "fields": {"Role_ID": "R01", "Role_Name": "Production_Manager"}},
            {"id": 2, "fields": {"Role_ID": "R03", "Role_Name": "Production_Supervisor"}},
        ],
        "Users": [
            {"id": 1, "fields": {"User_ID": "U_CREATOR", "Telegram_ID": "1001", "Role": 2, "Active": True}},
            {"id": 2, "fields": {"User_ID": "U_OWNER", "Telegram_ID": "1002", "Role": 2, "Active": True}},
            {"id": 3, "fields": {"User_ID": "U_NOTIFIER", "Telegram_ID": "1003", "Role": 2, "Active": True}},
        ],
        "Notification_Events": [
            {"id": 1, "fields": {"Event_ID": "batch_status_changed", "Recipient_Mode": "SUBSCRIBERS_ONLY"}},
        ],
        "Notification_Subscriptions": [],
        "UserRoleAssignment": [],
    }
    costing_tables = {
        "Users": [
            {"id": 11, "fields": {"User_ID": "U_CREATOR"}},
            {"id": 12, "fields": {"User_ID": "U_OWNER"}},
            {"id": 13, "fields": {"User_ID": "U_NOTIFIER"}},
        ],
        "ProductBatchMaster": [
            {
                "id": 501,
                "fields": {
                    "created_by": 11,
                    "owner_user": 12,
                    "notifier_users": ["L", 13, 12],  # owner duplicated as notifier
                },
            }
        ],
    }
    monkeypatch.setattr(subscriptions, "pulse_client", _FakeClient(pulse_tables))
    monkeypatch.setattr(subscriptions, "costing_client", _FakeClient(costing_tables))

    recipients = subscriptions.get_subscribers("batch_status_changed", context={"batch_id": 501})
    user_ids = {row.get("user_id") for row in recipients}
    telegram_ids = [row.get("telegram_id") for row in recipients]

    assert user_ids == {"U_CREATOR", "U_OWNER", "U_NOTIFIER"}
    assert len(telegram_ids) == len(set(telegram_ids))


def test_get_subscribers_uses_user_role_assignment_for_context_roles(monkeypatch):
    pulse_tables = {
        "Roles": [
            {"id": 1, "fields": {"Role_ID": "R01", "Role_Name": "Production_Manager"}},
            {"id": 2, "fields": {"Role_ID": "R03", "Role_Name": "Production_Supervisor"}},
        ],
        "Users": [
            {"id": 1, "fields": {"User_ID": "U_MULTI", "Telegram_ID": "1001", "Role": 2, "Active": True}},
        ],
        "Notification_Events": [
            {"id": 1, "fields": {"Event_ID": "batch_status_changed", "Recipient_Mode": "SUBSCRIBERS_ONLY"}},
        ],
        "Notification_Subscriptions": [],
        "UserRoleAssignment": [
            {"id": 1, "fields": {"User": 1, "Role": 1, "Active": True}},
            {"id": 2, "fields": {"User": 1, "Role": 2, "Active": True}},
        ],
    }
    costing_tables = {"Users": [], "ProductBatchMaster": []}
    monkeypatch.setattr(subscriptions, "pulse_client", _FakeClient(pulse_tables))
    monkeypatch.setattr(subscriptions, "costing_client", _FakeClient(costing_tables))

    recipients = subscriptions.get_subscribers(
        "batch_status_changed",
        context={"recipient_roles": ["Production_Manager"]},
    )
    assert [row.get("user_id") for row in recipients] == ["U_MULTI"]
