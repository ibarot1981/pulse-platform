# Change Note: Grist Batch Reference Display

Date: 2026-02-21

## Summary

Updated Grist child/history `batch_id` columns to behave as true references to `ProductBatchMaster` while displaying `batch_no` in grid cells.

Tables covered:

- `ProductBatchMS`
- `ProductBatchCNC`
- `ProductBatchStore`
- `BatchStatusHistory`

## What was changed

For each table above:

- Set `batch_id` type to `Ref:ProductBatchMaster`
- Set `batch_id.visibleCol` to `batch_no`
- Added/updated helper formula column `batch_no_display = $batch_id.batch_no`
- Set `batch_id.displayCol` to `batch_no_display`

## Why

`visibleCol` alone did not consistently render batch numbers in the active Grist document. Setting `displayCol` to a formula-based helper column forces friendly display while preserving stable ID-based reference integrity.

## Script

The migration is captured in:

- `scripts/grist/ensure_batch_batchno_display_refs.py`

Run with:

```powershell
python scripts/grist/ensure_batch_batchno_display_refs.py
```

The script is idempotent and safe to re-run.
