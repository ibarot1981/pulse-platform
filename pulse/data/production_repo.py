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
        "owner_user": "Ref:Users",
        "notifier_users": "RefList:Users",
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
        "ms_cutlist_pdf": "Attachments",
        "cnc_cutlist_pdf": "Attachments",
    }

    PRODUCT_BATCH_MS_SCHEMA = {
        "batch_id": "Reference:ProductBatchMaster",
        "product_part": "RefList:ProductPartMSList",
        "process_seq": "Ref:ProcessMaster",
        "total_qty": "Numeric",
        "current_stage_index": "Int",
        "current_stage_name": "Text",
        "next_stage_name": "Text",
        "current_stage_role_name": "Text",
        "current_stage_supervisors": "Text",
        "current_status": "Text",
        "supervisor_remarks": "Text",
        "scheduled_date": "DateTime",
        "stage_due_date": "DateTime",
        "row_cutlist_pdf": "Attachments",
        "created_at": "DateTime",
        "updated_at": "DateTime",
        "last_updated_by": "Ref:Users",
    }

    PROCESS_STAGE_MAPPING_SCHEMA = {
        "stage_name": "Text",
        "supervisor_role": "Text",
        "stage_order_priority": "Int",
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
        self._product_partms_index_cache: dict[int, dict] | None = None

    def ensure_ms_workflow_columns(self) -> None:
        required_columns = {
            "next_stage_name": "Text",
            "current_stage_role_name": "Text",
            "row_cutlist_pdf": "Attachments",
            "supervisor_remarks": "Text",
        }
        existing = self.get_table_columns("ProductBatchMS")
        for column_id, column_type in required_columns.items():
            if column_id in existing:
                continue
            try:
                self.costing_client.add_column("ProductBatchMS", column_id, column_type)
            except Exception:
                continue

    @staticmethod
    def _normalize_ref(value):
        if isinstance(value, list):
            return value[0] if value else None
        return value

    @staticmethod
    def _normalize_reflist(value) -> list[int]:
        if value is None:
            return []
        raw_items = value
        if isinstance(value, list):
            # Grist RefList values are encoded as ["L", id1, id2, ...].
            if value and value[0] == "L":
                raw_items = value[1:]
        else:
            raw_items = [value]

        result: list[int] = []
        for item in raw_items:
            try:
                item_id = int(item)
            except (TypeError, ValueError):
                continue
            result.append(item_id)
        return result

    @staticmethod
    def _to_number(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _safe_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

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

    def _get_product_partms_index(self) -> dict[int, dict]:
        if self._product_partms_index_cache is not None:
            return self._product_partms_index_cache
        index: dict[int, dict] = {}
        for record in self.costing_client.get_records("ProductPartMSList"):
            rec_id = record.get("id")
            if not isinstance(rec_id, int):
                continue
            index[rec_id] = record.get("fields", {})
        self._product_partms_index_cache = index
        return index

    def get_product_part_names_from_field(self, product_part_value) -> list[str]:
        refs = self._normalize_reflist(product_part_value)
        if refs:
            names: list[str] = []
            seen: set[str] = set()
            ms_index = self._get_product_partms_index()
            for ref_id in refs:
                fields = ms_index.get(ref_id, {})
                part_name = str(fields.get("ProductPartName_ProductPartName") or "").strip()
                if not part_name or part_name in seen:
                    continue
                seen.add(part_name)
                names.append(part_name)
            if names:
                return names

        raw_text = str(product_part_value or "").strip()
        return [raw_text] if raw_text else []

    def format_product_parts(self, product_part_value) -> str:
        names = self.get_product_part_names_from_field(product_part_value)
        return ", ".join(names)

    def get_table_columns(self, table: str) -> set[str]:
        columns = self.costing_client.get_columns(table)
        column_ids = set()
        for column in columns:
            col_id = column.get("id")
            if col_id:
                column_ids.add(str(col_id))
        return column_ids

    def get_column_type(self, table: str, column_id: str) -> str:
        for column in self.costing_client.get_columns(table):
            if str(column.get("id") or "") != str(column_id):
                continue
            return str(column.get("fields", {}).get("type") or "")
        return ""

    def get_writable_table_columns(self, table: str) -> set[str]:
        columns = self.costing_client.get_columns(table)
        writable_ids = set()
        for column in columns:
            col_id = column.get("id")
            if not col_id:
                continue
            fields = column.get("fields", {})
            if fields.get("isFormula"):
                continue
            writable_ids.add(str(col_id))
        return writable_ids

    def filter_table_fields(self, table: str, fields: dict) -> dict:
        columns = self.get_writable_table_columns(table)
        return {key: value for key, value in fields.items() if key in columns}

    def get_ms_table_column_ids(self) -> set[str]:
        return self.get_table_columns("ProductBatchMS")

    def get_process_stage_mapping(self) -> dict[str, dict]:
        # Phase-1 source: ProcessStage + ProcessMaster definitions.
        try:
            stage_rows = self.costing_client.get_records("ProcessStage")
        except Exception:
            stage_rows = []
        mapping = {}
        for record in stage_rows:
            fields = record.get("fields", {})
            stage_name = str(fields.get("stage_name") or "").strip()
            supervisor_role = str(fields.get("resolved_role_name") or fields.get("supervisor_role") or "").strip()
            if not stage_name or not supervisor_role:
                continue
            if stage_name in mapping:
                continue
            mapping[stage_name] = {
                "supervisor_role": supervisor_role,
                "stage_order_priority": fields.get("seq_no") or fields.get("stage_order_priority"),
            }
        return mapping

    def _process_master_indexes(self) -> tuple[dict[int, dict], dict[str, int]]:
        by_id: dict[int, dict] = {}
        by_legacy_text: dict[str, int] = {}
        try:
            records = self.costing_client.get_records("ProcessMaster")
        except Exception:
            return by_id, by_legacy_text

        for record in records:
            rec_id = record.get("id")
            if not isinstance(rec_id, int):
                continue
            fields = record.get("fields", {})
            by_id[rec_id] = fields
            legacy_text = str(fields.get("legacy_process_seq_text") or "").strip()
            if legacy_text and legacy_text not in by_legacy_text:
                by_legacy_text[legacy_text] = rec_id
        return by_id, by_legacy_text

    def get_process_seq_ref_id(self, process_seq_value) -> int | None:
        normalized = self._normalize_ref(process_seq_value)
        seq_id = self._safe_int(normalized)
        if seq_id is not None:
            return seq_id

        legacy_text = str(normalized or "").strip()
        if not legacy_text:
            return None
        _, by_legacy_text = self._process_master_indexes()
        return by_legacy_text.get(legacy_text)

    def get_process_display_label(self, process_seq_value) -> str:
        seq_id = self.get_process_seq_ref_id(process_seq_value)
        if seq_id is not None:
            by_id, _ = self._process_master_indexes()
            fields = by_id.get(seq_id, {})
            label = str(fields.get("display_label") or "").strip()
            if label:
                return label
            name = str(fields.get("process_name") or "").strip()
            if name:
                return name
        return str(process_seq_value or "").strip()

    def get_process_stage_names(self, process_seq_value) -> list[str]:
        seq_id = self.get_process_seq_ref_id(process_seq_value)
        if seq_id is not None:
            try:
                records = self.costing_client.get_records("ProcessStage")
            except Exception:
                records = []
            rows: list[tuple[int, int, str]] = []
            for record in records:
                fields = record.get("fields", {})
                row_seq = self._safe_int(self._normalize_ref(fields.get("process_seq_id")))
                if row_seq != seq_id:
                    continue
                stage_name = str(fields.get("stage_name") or "").strip()
                if not stage_name:
                    continue
                seq_no = self._safe_int(fields.get("seq_no")) or 0
                rows.append((seq_no, int(record.get("id") or 0), stage_name))
            if rows:
                rows.sort(key=lambda item: (item[0], item[1]))
                return [row[2] for row in rows]

        legacy = str(process_seq_value or "").strip()
        if not legacy:
            return []
        return [token.strip() for token in legacy.split(" - ") if token.strip()]

    def get_stage_role_for_process_stage(self, process_seq_value, stage_name: str) -> str:
        stage_name_clean = str(stage_name or "").strip()
        if not stage_name_clean:
            return ""

        seq_id = self.get_process_seq_ref_id(process_seq_value)
        if seq_id is not None:
            try:
                records = self.costing_client.get_records("ProcessStage")
            except Exception:
                records = []
            for record in records:
                fields = record.get("fields", {})
                row_seq = self._safe_int(self._normalize_ref(fields.get("process_seq_id")))
                if row_seq != seq_id:
                    continue
                row_stage = str(fields.get("stage_name") or "").strip()
                if row_stage != stage_name_clean:
                    continue
                role = str(fields.get("resolved_role_name") or fields.get("supervisor_role") or "").strip()
                if role:
                    return role

        mapping = self.get_process_stage_mapping()
        details = mapping.get(stage_name_clean, {})
        return str(details.get("supervisor_role") or "").strip()

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

    def create_ms_rows(self, rows: list[dict]) -> list[int]:
        if not rows:
            return []
        response = self.costing_client.add_records("ProductBatchMS", rows)
        return [record.get("id") for record in response.get("records", []) if isinstance(record.get("id"), int)]

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

    def list_ms_rows_for_batch(self, batch_id: int) -> list[dict]:
        records = self.costing_client.get_records("ProductBatchMS")
        result = []
        for record in records:
            fields = record.get("fields", {})
            row_batch = self._normalize_ref(fields.get("batch_id"))
            if row_batch == batch_id:
                result.append(record)
        return result

    def get_ms_row_by_id(self, row_id: int) -> dict | None:
        records = self.costing_client.get_records("ProductBatchMS")
        for record in records:
            if record.get("id") == row_id:
                return record
        return None

    def attach_pdf_to_master(self, batch_id: int, file_path: str, field_name: str = "ms_cutlist_pdf") -> None:
        attachment_id = self.costing_client.upload_attachment(file_path)
        self.update_master(batch_id, {field_name: ["L", attachment_id]})

    def attach_pdf_to_ms_row(self, row_id: int, file_path: str, field_name: str = "row_cutlist_pdf") -> None:
        attachment_id = self.costing_client.upload_attachment(file_path)
        self.update_ms(row_id, {field_name: ["L", attachment_id]})

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
                status = fields.get("status") or fields.get("current_status")
                if status:
                    all_statuses.append(str(status))
        return all_statuses

    def get_users(self) -> list[dict]:
        return self.pulse_client.get_records("Users")

    def get_roles(self) -> list[dict]:
        return self.pulse_client.get_records("Roles")

    def get_user_role_assignments(self) -> list[dict]:
        try:
            return self.pulse_client.get_records("UserRoleAssignment")
        except Exception:
            return []

    def _role_name_by_id(self) -> dict[int, str]:
        return {
            row.get("id"): str(row.get("fields", {}).get("Role_Name") or "").strip()
            for row in self.get_roles()
            if isinstance(row.get("id"), int)
        }

    def _user_record_by_user_id(self) -> dict[str, dict]:
        mapping: dict[str, dict] = {}
        for user in self.get_users():
            fields = user.get("fields", {})
            user_id = str(fields.get("User_ID") or "").strip()
            if user_id:
                mapping[user_id] = user
            telegram_id = str(fields.get("Telegram_ID") or "").strip()
            if telegram_id and telegram_id not in mapping:
                mapping[telegram_id] = user
        return mapping

    def get_role_names_by_user_id(self, user_id: str) -> list[str]:
        target = str(user_id or "").strip()
        if not target:
            return []

        role_name_by_id = self._role_name_by_id()
        user_by_user_id = self._user_record_by_user_id()
        target_user = user_by_user_id.get(target)
        if not target_user:
            return []

        role_names: list[str] = []
        seen: set[str] = set()

        # Default/legacy single-role source from Users.Role.
        user_role = self._normalize_ref(target_user.get("fields", {}).get("Role"))
        if isinstance(user_role, int):
            role_name = str(role_name_by_id.get(user_role) or "").strip()
            if role_name and role_name not in seen:
                seen.add(role_name)
                role_names.append(role_name)

        # Preferred multi-role source (when present).
        user_rec_id = target_user.get("id")
        for row in self.get_user_role_assignments():
            fields = row.get("fields", {})
            if not bool(fields.get("Active", True)):
                continue
            user_ref = self._normalize_ref(fields.get("User"))
            if isinstance(user_ref, int) and isinstance(user_rec_id, int):
                if user_ref != user_rec_id:
                    continue
            else:
                user_ref_text = str(user_ref or "").strip()
                if user_ref_text not in {target, str(user_rec_id or "").strip()}:
                    continue
            role_ref = self._normalize_ref(fields.get("Role"))
            if not isinstance(role_ref, int):
                continue
            role_name = str(role_name_by_id.get(role_ref) or "").strip()
            if role_name and role_name not in seen:
                seen.add(role_name)
                role_names.append(role_name)

        return role_names

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

    def get_active_users_by_role_names(self, role_names: list[str]) -> list[dict]:
        if not role_names:
            return []
        target = {str(name or "").strip() for name in role_names if str(name or "").strip()}
        if not target:
            return []
        users = self.get_users()
        result = []
        for user in users:
            fields = user.get("fields", {})
            if not fields.get("Active"):
                continue
            user_id = str(fields.get("User_ID") or "").strip()
            user_roles = set(self.get_role_names_by_user_id(user_id))
            if not (user_roles & target):
                continue
            result.append(user)
        return result

    def get_role_name_by_user_id(self, user_id: str) -> str:
        roles = self.get_role_names_by_user_id(user_id)
        return "|".join(roles)

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

    def list_supervisor_schedule_pending_batches(self, threshold_days: int = 0) -> list[dict]:
        now = datetime.utcnow()
        masters = self.get_all_master_batches()
        ms_rows = self.costing_client.get_records("ProductBatchMS")

        current_roles_by_batch: dict[int, set[str]] = {}
        for row in ms_rows:
            fields = row.get("fields", {})
            batch_id = self._normalize_ref(fields.get("batch_id"))
            if not isinstance(batch_id, int):
                continue
            status = str(fields.get("current_status") or fields.get("status") or "")
            if status == "Cutting Completed":
                continue
            role = str(fields.get("current_stage_role_name") or "").strip()
            if not role:
                stage_name = str(fields.get("current_stage_name") or "").strip()
                role = self.get_stage_role_for_process_stage(fields.get("process_seq"), stage_name)
            if not role:
                continue
            current_roles_by_batch.setdefault(batch_id, set()).add(role)

        pending = []
        for record in masters:
            batch_id = record.get("id")
            if not isinstance(batch_id, int):
                continue
            fields = record.get("fields", {})
            if fields.get("approval_status") != "Approved":
                continue
            if fields.get("scheduled_date"):
                continue
            roles = sorted(current_roles_by_batch.get(batch_id, set()))
            if not roles:
                continue
            start_raw = fields.get("start_date")
            days_open = 0
            if start_raw:
                try:
                    start_dt = datetime.fromisoformat(str(start_raw).replace("Z", "+00:00"))
                    if start_dt.tzinfo is not None:
                        start_dt = start_dt.replace(tzinfo=None)
                    days_open = (now - start_dt).days
                except ValueError:
                    days_open = 0
            if days_open < threshold_days:
                continue
            pending.append(
                {
                    "batch_id": batch_id,
                    "batch_no": str(fields.get("batch_no") or ""),
                    "roles": roles,
                    "days_open": days_open,
                }
            )
        return pending

    def list_stage_rows_pending_reminder(self, threshold_days: int) -> list[dict]:
        now = datetime.utcnow()
        rows = self.costing_client.get_records("ProductBatchMS")
        pending: list[dict] = []
        for record in rows:
            fields = record.get("fields", {})
            status = str(fields.get("current_status") or fields.get("status") or "").strip()
            if not status or status == "Cutting Completed":
                continue
            role_name = str(fields.get("current_stage_role_name") or "").strip()
            if not role_name:
                stage_name = str(fields.get("current_stage_name") or "").strip()
                role_name = self.get_stage_role_for_process_stage(fields.get("process_seq"), stage_name)
            if not role_name:
                continue
            updated_raw = fields.get("updated_at") or fields.get("created_at")
            if not updated_raw:
                continue
            try:
                updated_dt = datetime.fromisoformat(str(updated_raw).replace("Z", "+00:00"))
                if updated_dt.tzinfo is not None:
                    updated_dt = updated_dt.replace(tzinfo=None)
            except ValueError:
                continue
            days_waiting = (now - updated_dt).days
            if days_waiting < threshold_days:
                continue
            batch_id = self._normalize_ref(fields.get("batch_id"))
            if not isinstance(batch_id, int):
                continue
            pending.append(
                {
                    "row_id": record.get("id"),
                    "batch_id": batch_id,
                    "product_part": self.format_product_parts(fields.get("product_part")),
                    "process_seq": self.get_process_display_label(fields.get("process_seq")),
                    "current_stage_name": str(fields.get("current_stage_name") or ""),
                    "current_status": status,
                    "role_name": role_name,
                    "days_waiting": days_waiting,
                }
            )
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
