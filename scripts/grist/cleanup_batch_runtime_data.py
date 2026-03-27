from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import requests
from dotenv import load_dotenv

if __package__ in (None, ""):
    repo_root = str(Path(__file__).resolve().parents[2])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

from pulse.config import COSTING_API_KEY, COSTING_DOC_ID, PULSE_API_KEY, PULSE_DOC_ID, PULSE_GRIST_SERVER
from pulse.core.grist_client import GristClient
from pulse.runtime import test_api_key, test_doc_id


REPO_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(REPO_ROOT / ".env")


@dataclass(frozen=True)
class TableSpec:
    label: str
    names: tuple[str, ...]


@dataclass(frozen=True)
class CleanupScope:
    label: str
    doc_id: str
    api_key: str
    tables: tuple[TableSpec, ...]


COSTING_TABLES = (
    TableSpec("ProductBatchMS", ("ProductBatchMS",)),
    TableSpec("BatchStatusHistory", ("BatchStatusHistory", "Batch StatusHistory")),
    TableSpec("ProductBatchMaster", ("ProductBatchMaster",)),
)

PULSE_TABLES = (
    TableSpec("Activity_Log", ("Activity_Log",)),
    TableSpec("Reminder_Log", ("Reminder_Log",)),
)

RUNTIME_TABLES = (
    TableSpec("Test_Inbox", ("Test_Inbox",)),
    TableSpec("Test_Outbox", ("Test_Outbox",)),
    TableSpec("Test_UserContext", ("Test_UserContext",)),
    TableSpec("Test_RunLog", ("Test_RunLog",)),
    TableSpec("Test_Attachments", ("Test_Attachments",)),
)


def _default_log_path() -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return REPO_ROOT / "artifacts" / "logs" / f"cleanup_batch_runtime_data_{stamp}.log"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Cleanup utility for batch/runtime simulation data. "
            "Deletes rows only from the fixed allow-list of tables."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Execute deletion. If omitted, script runs in dry-run mode.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=200,
        help="Number of record ids per DELETE request (default: 200).",
    )
    parser.add_argument(
        "--runtime-doc-id",
        default="",
        help="Override runtime doc id. Defaults to PULSE_TEST_DOC_ID.",
    )
    parser.add_argument(
        "--runtime-api-key",
        default="",
        help="Override runtime API key. Defaults to PULSE_TEST_API_KEY (or PULSE_API_KEY).",
    )
    parser.add_argument(
        "--allow-missing-table",
        action="store_true",
        help="Skip missing allow-listed tables instead of failing.",
    )
    parser.add_argument(
        "--log-file",
        default="",
        help="Optional path for detailed cleanup log output.",
    )
    return parser.parse_args()


def _normalize_table_names(raw_tables: list) -> set[str]:
    names: set[str] = set()
    for row in raw_tables:
        if isinstance(row, str):
            value = row
        elif isinstance(row, dict):
            value = row.get("id")
        else:
            value = None
        text = str(value or "").strip()
        if text:
            names.add(text)
    return names


def _normalize_ref(value):
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _build_client(doc_id: str, api_key: str, label: str) -> GristClient:
    server = str(PULSE_GRIST_SERVER or "").rstrip("/")
    doc = str(doc_id or "").strip()
    key = str(api_key or "").strip()
    if not server or not doc or not key:
        raise ValueError(
            f"Missing configuration for {label}. Required: PULSE_GRIST_SERVER, doc id, and API key."
        )
    return GristClient(server, doc, key)


def _resolve_table_name(available: set[str], spec: TableSpec, allow_missing: bool) -> str | None:
    for candidate in spec.names:
        if candidate in available:
            return candidate
    if allow_missing:
        return None
    raise ValueError(
        f"Required table not found for {spec.label}. Tried: {', '.join(spec.names)}"
    )


def _record_ids(client: GristClient, table: str) -> list[int]:
    rows = client.get_records(table)
    ids: list[int] = []
    for row in rows:
        rec_id = row.get("id")
        if isinstance(rec_id, int):
            ids.append(rec_id)
    return ids


def _rows_by_id(client: GristClient, table: str) -> dict[int, dict]:
    rows = client.get_records(table)
    mapped: dict[int, dict] = {}
    for row in rows:
        rec_id = row.get("id")
        if not isinstance(rec_id, int):
            continue
        fields = row.get("fields", {})
        mapped[rec_id] = fields if isinstance(fields, dict) else {}
    return mapped


def _format_delete_line(
    table: str,
    rec_id: int,
    fields: dict,
    batch_no_by_id: dict[int, str],
    batch_summary: dict[str, dict[str, int]],
) -> str:
    if table == "ProductBatchMaster":
        batch_no = str(fields.get("batch_no") or "").strip() or f"<id:{rec_id}>"
        batch_summary[batch_no][table] += 1
        return f"{table} : entry deleted (record_id={rec_id}, batch_no={batch_no})"

    if table in {"ProductBatchMS", "BatchStatusHistory", "Batch StatusHistory"}:
        raw_batch_id = _normalize_ref(fields.get("batch_id"))
        try:
            batch_id = int(raw_batch_id)
        except (TypeError, ValueError):
            batch_id = None
        batch_no = batch_no_by_id.get(batch_id or -1, "")
        if not batch_no and batch_id is not None:
            batch_no = f"<batch_id:{batch_id}>"
        if not batch_no:
            batch_no = "<batch_unknown>"
        batch_summary[batch_no][table] += 1
        return f"{table} : entry deleted (record_id={rec_id}, batch_no={batch_no})"

    return f"{table} : entry deleted (record_id={rec_id})"


def _accumulate_batch_summary(
    table: str,
    rec_id: int,
    fields: dict,
    batch_no_by_id: dict[int, str],
    batch_summary: dict[str, dict[str, int]],
) -> None:
    if table == "ProductBatchMaster":
        batch_no = str(fields.get("batch_no") or "").strip() or f"<id:{rec_id}>"
        batch_summary[batch_no][table] += 1
        return

    if table in {"ProductBatchMS", "BatchStatusHistory", "Batch StatusHistory"}:
        raw_batch_id = _normalize_ref(fields.get("batch_id"))
        try:
            batch_id = int(raw_batch_id)
        except (TypeError, ValueError):
            batch_id = None
        batch_no = batch_no_by_id.get(batch_id or -1, "")
        if not batch_no and batch_id is not None:
            batch_no = f"<batch_id:{batch_id}>"
        if not batch_no:
            batch_no = "<batch_unknown>"
        batch_summary[batch_no][table] += 1


def _delete_records(
    client: GristClient,
    table: str,
    ids: list[int],
    chunk_size: int,
    rows_by_id: dict[int, dict],
    log_lines: list[str],
    batch_no_by_id: dict[int, str],
    batch_summary: dict[str, dict[str, int]],
) -> int:
    if not ids:
        return 0
    if chunk_size <= 0:
        raise ValueError("--chunk-size must be > 0")

    base = client.server.rstrip("/")
    url = f"{base}/api/docs/{client.doc_id}/tables/{table}/records"
    apply_url = f"{base}/api/docs/{client.doc_id}/apply"
    headers = {"Authorization": f"Bearer {client.api_key}"}
    delete_supported: bool | None = None
    fallback_noted = False

    def _bulk_remove(chunk_ids: list[int]) -> None:
        payload = [["BulkRemoveRecord", table, chunk_ids]]
        response = requests.post(
            apply_url,
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=60,
        )
        response.raise_for_status()

    deleted = 0
    for start in range(0, len(ids), chunk_size):
        chunk = ids[start : start + chunk_size]
        if delete_supported is False:
            _bulk_remove(chunk)
        else:
            payload = ",".join(str(x) for x in chunk)
            response = requests.delete(url, headers=headers, params={"records": payload}, timeout=60)
            if response.status_code == 404:
                delete_supported = False
                if not fallback_noted:
                    msg = (
                        f"{table}: DELETE endpoint unavailable on this server; "
                        "using /apply BulkRemoveRecord fallback."
                    )
                    print(f"- {msg}")
                    log_lines.append(msg)
                    fallback_noted = True
                _bulk_remove(chunk)
            else:
                response.raise_for_status()
                delete_supported = True
        deleted += len(chunk)
        for rec_id in chunk:
            fields = rows_by_id.get(rec_id, {})
            log_lines.append(_format_delete_line(table, rec_id, fields, batch_no_by_id, batch_summary))
    return deleted


def _run_scope(
    scope: CleanupScope,
    *,
    apply: bool,
    chunk_size: int,
    allow_missing_table: bool,
    log_lines: list[str],
    batch_summary: dict[str, dict[str, int]],
) -> dict[str, int]:
    client = _build_client(scope.doc_id, scope.api_key, scope.label)
    available_tables = _normalize_table_names(client.list_tables())
    results: dict[str, int] = {}
    resolved_tables: dict[str, str | None] = {}

    print(f"\n[{scope.label}] doc={client.doc_id}")
    log_lines.append("")
    log_lines.append(f"[{scope.label}] doc={client.doc_id}")
    for spec in scope.tables:
        resolved_tables[spec.label] = _resolve_table_name(available_tables, spec, allow_missing=allow_missing_table)

    batch_no_by_id: dict[int, str] = {}
    if scope.label == "COSTING":
        batch_master_table = resolved_tables.get("ProductBatchMaster")
        if isinstance(batch_master_table, str) and batch_master_table:
            master_rows = client.get_records(batch_master_table)
            for row in master_rows:
                rec_id = row.get("id")
                if not isinstance(rec_id, int):
                    continue
                fields = row.get("fields", {})
                if not isinstance(fields, dict):
                    continue
                batch_no = str(fields.get("batch_no") or "").strip()
                if batch_no:
                    batch_no_by_id[rec_id] = batch_no

    for spec in scope.tables:
        table = resolved_tables.get(spec.label)
        if not table:
            print(f"- {spec.label}: missing, skipped (--allow-missing-table).")
            log_lines.append(f"{spec.label}: missing, skipped (--allow-missing-table).")
            continue
        rows_map = _rows_by_id(client, table)
        ids = list(rows_map.keys())
        planned = len(ids)
        if not apply:
            print(f"- {table}: would delete {planned} row(s).")
            log_lines.append(f"{table}: would delete {planned} row(s).")
            if scope.label == "COSTING":
                for rec_id, fields in rows_map.items():
                    _accumulate_batch_summary(table, rec_id, fields, batch_no_by_id, batch_summary)
            results[table] = planned
            continue
        deleted = _delete_records(
            client,
            table,
            ids,
            chunk_size,
            rows_map,
            log_lines,
            batch_no_by_id,
            batch_summary,
        )
        remaining = len(_record_ids(client, table))
        print(f"- {table}: deleted {deleted} row(s); remaining {remaining}.")
        log_lines.append(f"{table}: deleted {deleted} row(s); remaining {remaining}.")
        results[table] = deleted
    return results


def _write_log(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _batch_summary_lines(batch_summary: dict[str, dict[str, int]], *, apply: bool) -> list[str]:
    lines: list[str] = []
    if not batch_summary:
        return ["No batch entries deleted." if apply else "No batch entries would be deleted."]
    verb = "deleted" if apply else "would delete"
    for batch_no in sorted(batch_summary.keys()):
        per_table = batch_summary[batch_no]
        parts = ", ".join(f"{table}={count}" for table, count in sorted(per_table.items()))
        lines.append(f"{batch_no} - {verb} ({parts})")
    return lines


def main() -> None:
    args = _parse_args()
    runtime_doc = str(args.runtime_doc_id or "").strip() or str(test_doc_id() or "").strip()
    runtime_key = str(args.runtime_api_key or "").strip() or str(test_api_key() or "").strip()
    log_path = Path(str(args.log_file or "").strip()) if str(args.log_file or "").strip() else _default_log_path()

    scopes = (
        CleanupScope("COSTING", str(COSTING_DOC_ID or "").strip(), str(COSTING_API_KEY or "").strip(), COSTING_TABLES),
        CleanupScope("PULSE", str(PULSE_DOC_ID or "").strip(), str(PULSE_API_KEY or "").strip(), PULSE_TABLES),
        CleanupScope("RUNTIME", runtime_doc, runtime_key, RUNTIME_TABLES),
    )

    mode = "APPLY" if args.apply else "DRY-RUN"
    log_lines: list[str] = [
        f"Cleanup Run Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Mode: {mode}",
        "Scope lock: deletes rows only from the fixed allow-listed tables in this script.",
    ]
    batch_summary: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    print(f"Mode: {mode}")
    print("Scope lock: deletes rows only from the fixed allow-listed tables in this script.")
    print(f"Detailed log path: {log_path}")

    total = 0
    for scope in scopes:
        scope_result = _run_scope(
            scope,
            apply=args.apply,
            chunk_size=args.chunk_size,
            allow_missing_table=args.allow_missing_table,
            log_lines=log_lines,
            batch_summary=batch_summary,
        )
        total += sum(scope_result.values())

    summary_lines = _batch_summary_lines(batch_summary, apply=args.apply)
    print("\nBatch Summary")
    for line in summary_lines:
        print(f"- {line}")

    log_lines.append("")
    log_lines.append("Batch Summary")
    log_lines.extend(summary_lines)

    if args.apply:
        print(f"\nDone. Deleted {total} row(s) from allow-listed tables.")
        log_lines.append("")
        log_lines.append(f"Done. Deleted {total} row(s) from allow-listed tables.")
    else:
        print(f"\nDry-run complete. {total} row(s) would be deleted from allow-listed tables.")
        print("Re-run with --apply to execute.")
        log_lines.append("")
        log_lines.append(f"Dry-run complete. {total} row(s) would be deleted from allow-listed tables.")
        log_lines.append("Re-run with --apply to execute.")

    _write_log(log_path, log_lines)
    print(f"Log written: {log_path}")


if __name__ == "__main__":
    main()
