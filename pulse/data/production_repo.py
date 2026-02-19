from __future__ import annotations

from datetime import datetime

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


class ProductionRepo:
    PRODUCT_BATCH_MASTER_SCHEMA = {
        "batch_no": "Text",
        "product_model": "Text",
        "qty": "Numeric",
        "batch_type": "Text",
        "include_ms": "Bool",
        "include_cnc": "Bool",
        "include_store": "Bool",
        "created_by": "Ref:Users",
        "created_date": "DateTime",
        "start_date": "DateTime",
        "scheduled_date": "DateTime",
        "completion_date": "DateTime",
        "approval_status": "Text",
        "approval_date": "DateTime",
        "approved_by": "Ref:Users",
        "overall_status": "Text",
        "selected_part_ids": "Text",
        "notification_users": "Text",
    }

    PRODUCT_BATCH_MS_SCHEMA = {
        "batch_id": "Reference:ProductBatchMaster",
        "product_part": "Text",
        "material_to_cut": "Text",
        "post_process": "Text",
        "required_qty": "Numeric",
        "status": "Text",
        "start_date": "DateTime",
        "scheduled_date": "DateTime",
        "expected_completion_date": "DateTime",
        "remarks": "Text",
    }

    BATCH_STATUS_HISTORY_SCHEMA = {
        "batch_id": "Reference:ProductBatchMaster",
        "entity_type": "Text",
        "entity_id": "Numeric",
        "old_status": "Text",
        "new_status": "Text",
        "updated_by": "Ref:Users",
        "timestamp": "DateTime",
        "remarks": "Text",
    }

    def __init__(self):
        self.costing_client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)
        self.pulse_client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)

    @staticmethod
    def _normalize_ref(value):
        if isinstance(value, list):
            return value[0] if value else None
        return value

    @staticmethod
    def _to_number(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def get_production_config(self) -> dict:
        records = self.costing_client.get_records("ProductionConfig")
        if not records:
            return {}
        return records[0].get("fields", {})

    def get_product_models(self) -> list[str]:
        records = self.costing_client.get_records("ProductModelConfig_summary_ProductModelCode")
        seen = set()
        result = []
        for record in records:
            code = record.get("fields", {}).get("ProductModelCode_ProductModelCode")
            if not code or code in seen:
                continue
            seen.add(code)
            result.append(code)
        return result

    def get_product_parts_for_model(self, model_code: str) -> list[dict]:
        records = self.costing_client.get_records("ProductModelConfig")
        seen = set()
        parts = []
        for record in records:
            fields = record.get("fields", {})
            if fields.get("ProductModelCode_ProductModelCode2") != model_code:
                continue
            part_id = self._normalize_ref(fields.get("ProductPartName"))
            part_name = fields.get("ProductPartName_ProductPartName")
            if not part_id or part_id in seen:
                continue
            seen.add(part_id)
            parts.append({"part_id": part_id, "part_name": part_name or str(part_id)})
        parts.sort(key=lambda row: row["part_name"])
        return parts

    def get_material_name_map(self) -> dict[int, str]:
        records = self.costing_client.get_records("MasterMaterial")
        material_map = {}
        for record in records:
            rec_id = record.get("id")
            if isinstance(rec_id, int):
                material_map[rec_id] = str(record.get("fields", {}).get("MasterMaterial") or "")
        return material_map

    def get_cnc_sheet_gauge_map(self) -> dict[int, str]:
        records = self.costing_client.get_records("CNCPartsMaster")
        gauge_map = {}
        for record in records:
            rec_id = record.get("id")
            if isinstance(rec_id, int):
                gauge_map[rec_id] = str(record.get("fields", {}).get("Thickness") or "")
        return gauge_map

    def get_ms_rows(self, part_ids: list[int]) -> list[dict]:
        records = self.costing_client.get_records("ProductPartMSList")
        result = []
        allowed = set(part_ids)
        for record in records:
            fields = record.get("fields", {})
            part_id = self._normalize_ref(fields.get("ProductPartName"))
            if part_id not in allowed:
                continue
            result.append(record)
        return result

    def get_cnc_rows(self, part_ids: list[int]) -> list[dict]:
        records = self.costing_client.get_records("ProductPartCNCList")
        result = []
        allowed = set(part_ids)
        for record in records:
            fields = record.get("fields", {})
            part_id = self._normalize_ref(fields.get("ProductPartName"))
            if part_id not in allowed:
                continue
            result.append(record)
        return result

    def get_store_issue_slip_ids_for_model(self, model_code: str) -> set[int]:
        records = self.costing_client.get_records("ProductPartStoresList")
        result = set()
        for record in records:
            fields = record.get("fields", {})
            if fields.get("ProductModelCode_ProductModelCode") != model_code:
                continue
            slip_ref = self._normalize_ref(fields.get("StoreIssueSlipMaster"))
            if isinstance(slip_ref, int):
                result.add(slip_ref)
        return result

    def get_store_issue_items(self, issue_slip_ids: set[int]) -> list[dict]:
        records = self.costing_client.get_records("StoresIssueSlipMasterLog")
        result = []
        for record in records:
            fields = record.get("fields", {})
            issue_slip_ref = self._normalize_ref(fields.get("IssueSlipNumber"))
            if issue_slip_ref not in issue_slip_ids:
                continue
            result.append(record)
        return result

    def get_existing_batch_numbers(self) -> list[str]:
        records = self.costing_client.get_records("ProductBatchMaster")
        batch_numbers = []
        for record in records:
            number = record.get("fields", {}).get("batch_no")
            if number:
                batch_numbers.append(str(number))
        return batch_numbers

    def create_master_batch(self, fields: dict) -> int:
        response = self.costing_client.add_records("ProductBatchMaster", [fields])
        records = response.get("records", [])
        return records[0]["id"]

    def create_ms_rows(self, rows: list[dict]) -> None:
        if rows:
            self.costing_client.add_records("ProductBatchMS", rows)

    def create_cnc_rows(self, rows: list[dict]) -> None:
        if rows:
            self.costing_client.add_records("ProductBatchCNC", rows)

    def create_store_rows(self, rows: list[dict]) -> None:
        if rows:
            self.costing_client.add_records("ProductBatchStore", rows)

    def add_status_history(
        self,
        batch_id: int,
        entity_type: str,
        entity_id: int,
        old_status: str,
        new_status: str,
        updated_by,
        remarks: str = "",
    ) -> None:
        self.costing_client.add_records(
            "BatchStatusHistory",
            [
                {
                    "batch_id": batch_id,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "old_status": old_status or "",
                    "new_status": new_status or "",
                    "updated_by": updated_by,
                    "timestamp": datetime.utcnow().isoformat(),
                    "remarks": remarks or "",
                }
            ],
        )

    def get_master_by_id(self, batch_id: int) -> dict | None:
        records = self.costing_client.get_records("ProductBatchMaster")
        for record in records:
            if record.get("id") == batch_id:
                return record
        return None

    def get_master_by_batch_no(self, batch_no: str) -> dict | None:
        records = self.costing_client.get_records("ProductBatchMaster")
        for record in records:
            if record.get("fields", {}).get("batch_no") == batch_no:
                return record
        return None

    def get_all_master_batches(self) -> list[dict]:
        return self.costing_client.get_records("ProductBatchMaster")

    def list_pending_approvals(self) -> list[dict]:
        records = self.costing_client.get_records("ProductBatchMaster")
        pending = []
        for record in records:
            fields = record.get("fields", {})
            if fields.get("approval_status") == "Pending Approval":
                pending.append(record)
        pending.sort(key=lambda r: r.get("id", 0))
        return pending

    def update_master(self, batch_id: int, fields: dict) -> None:
        self.costing_client.patch_record("ProductBatchMaster", batch_id, fields)

    def update_master_by_ids(self, batch_ids: list[int], fields: dict) -> None:
        for batch_id in batch_ids:
            self.update_master(batch_id, fields)

    def update_ms(self, row_id: int, fields: dict) -> None:
        self.costing_client.patch_record("ProductBatchMS", row_id, fields)

    def update_ms_for_batch(self, batch_id: int, fields: dict) -> None:
        records = self.costing_client.get_records("ProductBatchMS")
        for record in records:
            row_batch = self._normalize_ref(record.get("fields", {}).get("batch_id"))
            if row_batch != batch_id:
                continue
            self.update_ms(record.get("id"), fields)

    def update_cnc(self, row_id: int, fields: dict) -> None:
        self.costing_client.patch_record("ProductBatchCNC", row_id, fields)

    def update_store(self, row_id: int, fields: dict) -> None:
        self.costing_client.patch_record("ProductBatchStore", row_id, fields)

    def list_child_statuses(self, batch_id: int) -> list[str]:
        all_statuses = []
        for table in ("ProductBatchMS", "ProductBatchCNC", "ProductBatchStore"):
            records = self.costing_client.get_records(table)
            for record in records:
                fields = record.get("fields", {})
                if self._normalize_ref(fields.get("batch_id")) != batch_id:
                    continue
                status = fields.get("status")
                if status:
                    all_statuses.append(str(status))
        return all_statuses

    def get_users(self) -> list[dict]:
        return self.pulse_client.get_records("Users")

    def get_roles(self) -> list[dict]:
        return self.pulse_client.get_records("Roles")

    def get_telegram_by_user_id(self, user_id: str) -> str | None:
        users = self.get_users()
        for user in users:
            fields = user.get("fields", {})
            if fields.get("User_ID") == user_id and fields.get("Active"):
                return str(fields.get("Telegram_ID"))
        return None

    def get_role_user_telegrams(self, role_names: list[str]) -> list[str]:
        roles = self.get_roles()
        role_ids = {r.get("id") for r in roles if r.get("fields", {}).get("Role_Name") in role_names}

        users = self.get_users()
        telegram_ids = []
        for user in users:
            fields = user.get("fields", {})
            if not fields.get("Active"):
                continue
            role_ref = self._normalize_ref(fields.get("Role"))
            if role_ref in role_ids and fields.get("Telegram_ID"):
                telegram_ids.append(str(fields.get("Telegram_ID")))
        return telegram_ids

    def get_role_name_by_user_id(self, user_id: str) -> str:
        users = self.get_users()
        roles = self.get_roles()
        role_name_by_id = {r.get("id"): r.get("fields", {}).get("Role_Name") for r in roles}

        for user in users:
            fields = user.get("fields", {})
            if fields.get("User_ID") != user_id:
                continue
            role_ref = self._normalize_ref(fields.get("Role"))
            if not role_ref:
                return ""
            return str(role_name_by_id.get(role_ref) or "")
        return ""

    def add_lifecycle_history(self, batch_id: int, stage: str, updated_by, remarks: str = "") -> None:
        self.add_status_history(batch_id, "Master", batch_id, "", stage, updated_by, remarks)

    def get_costing_user_ref_by_user_id(self, user_id: str) -> int | None:
        if not user_id:
            return None
        records = self.costing_client.get_records("Users")
        for record in records:
            fields = record.get("fields", {})
            if str(fields.get("User_ID") or "") == str(user_id):
                rec_id = record.get("id")
                if isinstance(rec_id, int):
                    return rec_id
        return None

    def list_batches_pending_schedule_reminder(self, threshold_days: int) -> list[dict]:
        now = datetime.utcnow()
        records = self.get_all_master_batches()
        pending = []

        for record in records:
            fields = record.get("fields", {})
            if fields.get("approval_status") != "Approved":
                continue
            if fields.get("scheduled_date"):
                continue

            start_raw = fields.get("start_date")
            if not start_raw:
                continue

            try:
                start_dt = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
            except ValueError:
                continue

            if start_dt.tzinfo is not None:
                start_dt = start_dt.replace(tzinfo=None)

            days_open = (now - start_dt).days
            if days_open >= threshold_days:
                pending.append(record)

        return pending

    def get_reminder_rule(self, rule_event_id: str) -> dict:
        records = self.pulse_client.get_records("Reminder_Rules")
        for record in records:
            fields = record.get("fields", {})
            event_id = (
                fields.get("Rule_ID")
                or fields.get("Reminder_ID")
                or fields.get("Event_ID")
                or fields.get("Rule_Event")
            )
            if event_id != rule_event_id:
                continue
            return fields
        return {}
