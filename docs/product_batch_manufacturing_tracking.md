# Product Batch Manufacturing Tracking (Phase 1)

This document reflects the approval-driven implementation for:

- `Manage Production -> New Production Batch`
- `Manage Production -> Approvals -> Production Batch Approval`

## 1. Master Table Contract (`ProductBatchMaster`)

Required fields:

- `created_date`
- `start_date`
- `scheduled_date`
- `completion_date`
- `approval_status`
- `approval_date`
- `approved_by`

Additional persisted context for delayed child population:

- `selected_part_ids` (CSV list of selected part IDs)

Batch creation behavior:

- `created_date` is set at creation time.
- `approval_status = Pending Approval`
- `overall_status = Pending Approval`
- No MS/CNC/Store child rows are created at this stage.

## 2. Approval FSM + Selection

Implemented states in `pulse/integrations/production.py`:

- `pending_approvals_selection`
- `pending_approvals_confirm`

Approval menu behavior:

- Lists only `approval_status = Pending Approval` batches.
- Supports single value (`2`) and comma-separated selection (`1,3`).
- Requires explicit confirmation (`Yes`/`No`) before approval.

## 3. Approval Service Behavior

On approval confirmation:

- `approval_status = Approved`
- `approval_date = now`
- `start_date = now`
- `approved_by = approver user_id`
- `overall_status = Schedule Pending`

History log entries are added in `BatchStatusHistory` for:

- `Batch Approved`
- status transitions (`Pending Approval -> Approved`, `... -> Schedule Pending`)

## 4. Child Population Rule (Phase 1: MS Only)

Child rows are created only after approval.

MS row generation:

- Source: `ProductPartMSList`
- Scope: selected parts captured in `selected_part_ids` (or full model fallback)
- Grouping key: `Batch + Product Part + Material To Cut + Post Process`
- `required_qty = QtyNos * batch_qty` (aggregated by grouping key)
- `start_date` inherited from approved master `start_date`
- `status = Schedule Pending`

No CNC/Store child population occurs in Phase 1 approval flow.

## 5. Scheduling Propagation (Master -> MS)

Service added:

- `set_master_scheduled_date(context, batch_id, scheduled_date_iso, updated_by, remarks="")`

Behavior:

- Updates `ProductBatchMaster.scheduled_date`
- Sets master `overall_status = Scheduled`
- Propagates `scheduled_date` to all `ProductBatchMS` rows for that batch
- Writes lifecycle/status history entries (`Scheduled`)

## 6. Notification Events

Implemented event triggers:

- `production_batch_created`
  - Trigger: batch creation
  - Uses `Notification_Subscriptions` via dispatcher
- `production_batch_approved`
  - Trigger: batch approval
  - Uses `Notification_Subscriptions` via dispatcher

No users are hardcoded in code paths.

## 7. Reminder Integration Hook

Implemented in `pulse/reminders/engine.py`:

- `run_production_batch_reminder_checks(telegram_bot)`

Rule:

- `scheduled_date is NULL`
- `approval_status = Approved`
- `current_date - start_date >= threshold_days`

Event fired:

- `production_batch_not_scheduled_reminder`

Threshold source:

- `Reminder_Rules` lookup (`production_batch_not_scheduled_reminder`)
- falls back to `2` days if not configured

## 8. Lifecycle Tracking + Durations

Lifecycle events now logged in status history:

- `Batch Created`
- `Batch Approved`
- `Scheduled`
- `Completed` (when master recalculates to completed)

Date derivations now available from master table:

- `planning_duration = start_date - created_date`
- `production_duration = completion_date - start_date`
- `total_duration = completion_date - created_date`

## 9. Files Updated

- `pulse/data/production_repo.py`
- `pulse/integrations/production.py`
- `pulse/reminders/engine.py`
- `pulse/main.py`
- `docs/product_batch_manufacturing_tracking.md`
