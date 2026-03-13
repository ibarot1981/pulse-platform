from __future__ import annotations

import argparse
import requests

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient


def _base_url() -> str:
    return str(PULSE_GRIST_SERVER).rstrip("/")


def _apply(actions: list) -> None:
    url = f"{_base_url()}/api/docs/{COSTING_DOC_ID}/apply"
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {COSTING_API_KEY}", "Content-Type": "application/json"},
        json=actions,
        timeout=60,
    )
    response.raise_for_status()


def _table_exists(client: GristClient, table_name: str) -> bool:
    return table_name in {str(t.get("id")) for t in client.list_tables()}


def _find_remaining_refs(client: GristClient) -> list[str]:
    issues: list[str] = []
    for table in client.list_tables():
        table_name = str(table.get("id") or "")
        if not table_name:
            continue
        for col in client.get_columns(table_name):
            col_id = str(col.get("id") or "")
            col_type = str(col.get("fields", {}).get("type") or "")
            if "UserMaster_Mirror" in col_type:
                issues.append(f"{table_name}.{col_id} -> {col_type}")
    return issues


def main() -> None:
    parser = argparse.ArgumentParser(description="Deprecate/remove legacy UserMaster_Mirror in Costing doc.")
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Actually remove UserMaster_Mirror table after reference checks.",
    )
    args = parser.parse_args()

    client = GristClient(PULSE_GRIST_SERVER, COSTING_DOC_ID, COSTING_API_KEY)
    if not _table_exists(client, "UserMaster_Mirror"):
        print("UserMaster_Mirror does not exist. Nothing to do.")
        return

    remaining_refs = _find_remaining_refs(client)
    if remaining_refs:
        print("Cannot deprecate UserMaster_Mirror. Remaining column references found:")
        for issue in remaining_refs:
            print(f"- {issue}")
        raise SystemExit(1)

    if not args.drop:
        print("Validation passed. Run with --drop to remove UserMaster_Mirror.")
        return

    _apply([["RemoveTable", "UserMaster_Mirror"]])
    print("Removed UserMaster_Mirror table.")


if __name__ == "__main__":
    main()
