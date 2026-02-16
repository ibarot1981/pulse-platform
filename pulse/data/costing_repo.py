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

