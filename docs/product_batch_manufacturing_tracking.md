# Product Batch Manufacturing Tracking (Telegram + Grist)

This document summarizes the implemented workflow for:

- `Manage Production` -> `New Production Batch`
- `Manage Production` -> `Pending Approvals`

## 1. Menu + Permission Integration

Added in Pulse doc `Permissions`:

- `production_new_batch`
  - `Menu_Label`: `New Production Batch`
  - `Menu_Parent`: `MANAGE_PRODUCTION`
  - `Action_Type`: `RUN_STUB`
  - `Action_Target`: `NEW_PRODUCTION_BATCH`
- `production_pending_approvals`
  - `Menu_Label`: `Pending Approvals`
  - `Menu_Parent`: `MANAGE_PRODUCTION`
  - `Action_Type`: `RUN_STUB`
  - `Action_Target`: `PRODUCTION_PENDING_APPROVALS`

Role mappings added in `Role_Permissions`:

- New Batch: `Production_Supervisor`, `Production_Manager`, `System_Admin`
- Pending Approvals: `Production_Manager`, `System_Admin`

## 2. FSM States Added

Implemented in `pulse/integrations/production.py`:

- `selecting_batch_mode`
- `selecting_product_model`
- `selecting_product_parts`
- `entering_batch_qty`
- `selecting_batch_type`
- `confirming_batch`
- `awaiting_approval`

Additional runtime list state:

- `pending_approvals_selection`

## 3. Flow Implemented

### A) Batch creation by Product Model

1. Select mode `By Product Model`.
2. Select model from paginated list.
3. Enter quantity (validated against `ProductionConfig.min_batch_qty` / `max_batch_qty`).
4. Confirm summary.
5. Select batch type (`M-C-S`, `MS`, `CNC`, `STORE`).
6. Master + child rows are created.

### B) Batch creation by Product Part

1. Select mode `By Product Part`.
2. Select model.
3. Select parts (comma-separated multi-select, repeatable).
4. Enter quantity (config-validated).
5. Confirm summary.
6. Select batch type.
7. Master + child rows are created for selected parts only.

## 4. Batch Number Generation

Implemented format:

- `MMMYY-MODELCODE-MCS-XXX`

Rules:

- month-year from current UTC time (e.g. `FEB26`)
- process code from include flags (`M`, `C`, `S`)
- running sequence per month from existing `ProductBatchMaster.batch_no`

## 5. Costing Grist Tables Created

In Costing doc (`COSTING_DOC_ID`), created:

- `ProductionConfig`
  - `min_batch_qty` (Numeric)
  - `max_batch_qty` (Numeric)
- `ProductBatchMaster`
  - `batch_no`, `product_model`, `qty`, `batch_type`
  - `include_ms`, `include_cnc`, `include_store`
  - `created_by`, `created_date`
  - `approval_status`, `overall_status`
  - `notification_users`
- `ProductBatchMS`
  - `batch_id`, `product_part`, `material_to_cut`, `required_qty`
  - `status`, `scheduled_date`, `expected_completion_date`, `remarks`
- `ProductBatchCNC`
  - `batch_id`, `product_part`, `sheet_gauge`, `sheet_size`, `required_qty`
  - `status`, `nest_status`, `scheduled_date`, `expected_completion_date`, `remarks`
- `ProductBatchStore`
  - `batch_id`, `item_name`, `source_type`, `required_qty`
  - `status`, `scheduled_date`, `expected_completion_date`, `remarks`
- `BatchStatusHistory`
  - `batch_id`, `entity_type`, `entity_id`
  - `old_status`, `new_status`, `updated_by`, `timestamp`, `remarks`

Seeded config:

- `min_batch_qty=1`
- `max_batch_qty=1000`

## 6. Child Population Logic

Implemented in `pulse/integrations/production.py` + `pulse/data/production_repo.py`.

- MS:
  - Source: `ProductPartMSList`
  - Grouping key represented as row-per-source record
  - `required_qty = QtyNos * batch_qty`
  - `material_to_cut` resolved from `MasterMaterial`
- CNC:
  - Source: `ProductPartCNCList`
  - `sheet_gauge` from `CNCPartsMaster.Thickness`
  - `sheet_size` kept blank for now (pending future logic)
  - `required_qty = QtyNos * batch_qty`
- Store:
  - Source: model-linked store slips (`ProductPartStoresList` -> `StoresIssueSlipMasterLog`)
  - `required_qty = Qty * batch_qty`
  - `source_type` kept blank for now (pending future logic)

## 7. Approval + Status Handling

Approval:

- Only `Production_Manager` (`R02`) can approve in handler logic.
- On approval:
  - `approval_status = Approved`
  - `overall_status = Schedule Pending`
  - history entries inserted in `BatchStatusHistory`

Master auto recalculation (`recalculate_master_overall_status`):

- If approval pending -> `Pending Approval`
- Else:
  - all child statuses in `Done/Completed` -> `Completed`
  - any child in `In Progress` -> `In Progress`
  - all child in `Schedule Pending` -> `Schedule Pending`

Child update service (`update_child_status`):

- blocks updates before approval
- updates child row
- writes history
- recalculates master status
- sends notifications

## 8. Notification Integration

Added events in Pulse doc `Notification_Events`:

- `batch_approved`
- `batch_status_changed`

Subscriptions added for roles:

- `Production_Manager`
- `System_Admin`

Also sends direct notification to batch creator (lookup from Pulse `Users` table).

## 9. Files Added/Updated

- Added: `pulse/data/production_repo.py`
- Updated: `pulse/integrations/production.py`
- Updated: `pulse/main.py`
- Updated: `pulse/core/grist_client.py`
- Updated: `pulse/notifications/subscriptions.py`

