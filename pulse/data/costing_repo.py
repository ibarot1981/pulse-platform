from __future__ import annotations

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


class CostingRepo:
    """Repository facade for Costing doc lookups (MS cut list flow)."""

    def __init__(self):
        self.client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)

    def get_products(self) -> list[dict]:
        """Return paginatable product rows for the Costing flow."""
        # TODO: implement with actual Costing table name/columns.
        return []

    def get_parts_for_product(self, product_key: str) -> list[dict]:
        """Return parts for a selected product."""
        # TODO: implement with actual Costing table relation keys.
        return []

    def get_ms_cut_list(self, product_key: str, part_key: str) -> list[dict]:
        """Return final MS cut list rows for a selected product/part."""
        # TODO: implement with actual Costing table schema.
        return []

    def get_product_model_codes(self) -> list[str]:
        """Return unique product model codes from the summary table."""
        records = self.client.get_records("ProductModelConfig_summary_ProductModelCode")
        model_codes: list[str] = []
        seen: set[str] = set()

        for record in records:
            fields = record.get("fields", {})
            model_code = fields.get("ProductModelCode_ProductModelCode")
            if not model_code or model_code in seen:
                continue
            seen.add(model_code)
            model_codes.append(model_code)

        return model_codes

    def get_full_ms_list_for_product_model(self, model_code: str) -> list[dict]:
        """Return ProductPartMSList rows for all parts configured in a model code."""
        config_records = self.client.get_records("ProductModelConfig")
        part_ids: set[int] = set()

        for record in config_records:
            fields = record.get("fields", {})
            row_model_code = fields.get("ProductModelCode_ProductModelCode2")
            if row_model_code != model_code:
                continue
            part_id = fields.get("ProductPartName")
            if isinstance(part_id, int):
                part_ids.add(part_id)

        if not part_ids:
            return []

        ms_records = self.client.get_records("ProductPartMSList")
        filtered: list[dict] = []

        for record in ms_records:
            fields = record.get("fields", {})
            part_id = fields.get("ProductPartName")
            if part_id not in part_ids:
                continue
            filtered.append(record)

        filtered.sort(
            key=lambda record: (
                str(record.get("fields", {}).get("ProductPartName_ProductPartName", "")),
                int(record.get("id", 0)),
            )
        )
        return filtered

    @staticmethod
    def _normalize_ref_value(value):
        if isinstance(value, list):
            return value[0] if value else None
        return value

    @staticmethod
    def _is_nonzero_qty(value) -> bool:
        if value is None:
            return False
        if isinstance(value, str) and not value.strip():
            return False
        try:
            return float(value) != 0
        except (TypeError, ValueError):
            return bool(str(value).strip())

    @staticmethod
    def _format_number(value):
        if value is None:
            return ""
        try:
            number = float(value)
        except (TypeError, ValueError):
            return str(value)
        if number.is_integer():
            return str(int(number))
        return f"{number:g}"

    def get_full_ms_table_rows_for_product_model(self, model_code: str) -> list[dict]:
        """
        Return curated rows for PDF table:
        No., Part Name, MaterialToCut, Length (mm), Qty, Remarks, OptionGroup1_TEMP.
        Filters out rows where Qty is blank or 0.
        """
        base_rows = self.get_full_ms_list_for_product_model(model_code)
        material_records = self.client.get_records("MasterMaterial")
        material_map: dict[int, str] = {}

        for record in material_records:
            material_id = record.get("id")
            if not isinstance(material_id, int):
                continue
            material_name = record.get("fields", {}).get("MasterMaterial")
            material_map[material_id] = str(material_name or "")

        table_rows: list[dict] = []
        sequence = 1

        for row in base_rows:
            fields = row.get("fields", {})
            qty = fields.get("QtyNos")
            if not self._is_nonzero_qty(qty):
                continue

            material_ref = self._normalize_ref_value(fields.get("MaterialToCut"))
            material_value = material_map.get(material_ref, "") if isinstance(material_ref, int) else ""

            table_rows.append(
                {
                    "No.": str(sequence),
                    "Part Name": str(fields.get("ProductPartName_ProductPartName") or ""),
                    "MaterialToCut": material_value,
                    "Length (mm)": self._format_number(fields.get("Length_mm")),
                    "Qty": self._format_number(qty),
                    "Remarks": str(fields.get("Remarks") or ""),
                    "OptionGroup1_TEMP": str(fields.get("OptionGroup1_TEMP") or ""),
                }
            )
            sequence += 1

        return table_rows
