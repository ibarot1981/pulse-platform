# Pulse Usage Playbook (Living Document)

## Purpose

This is the single maintenance doc for operating and extending Pulse safely.

Use this for:

- adding roles, permissions, users
- adding notification events and subscriptions
- adding reminder rules
- documenting each new business scenario with repeatable steps

Update this file whenever a new scenario is implemented.

## System Model (Quick)

- Identity and access: `Roles`, `Permissions`, `Role_Permissions`, `Users` (Pulse doc)
- Notifications config: `Notification_Events`, `Notification_Subscriptions` (Pulse doc)
- Reminders config: `Reminder_Rules` (Pulse doc)
- Production data: `ProductBatchMaster`, `BatchStatusHistory`, child tables (Costing doc)
- MS routing config (phase 1): `ProcessMaster`, `ProcessStage`, `StageMaster`, `RoleMaster_Mirror`, `UserRoleAssignment_Mirror` (Costing doc)

## Prerequisites

1. Confirm `.env` points to correct Grist docs:
   - `PULSE_DOC_ID`
   - `COSTING_DOC_ID`
2. Confirm target user has valid `Telegram_ID`.
3. Confirm role and permission naming follows existing convention (`Rxx`, snake_case permission IDs).

## A) Add New Role, Permissions, Users

### A1. Add a Role

1. Open Pulse table `Roles`.
2. Add row:
   - `Role_ID` (example: `R07`)
   - `Role_Name` (example: `Planning_Supervisor`)
   - `Department`, `Level`, `Active=True`
3. Save and note Role record ID (internal row id).

### A2. Add Permission(s)

1. Open Pulse table `Permissions`.
2. Add row(s):
   - `Permission_ID` (stable id, no spaces)
   - `Menu_Label`
   - `Menu_Parent` (`MAIN` or submenu state key)
   - `Action_Type` (`OPEN_SUBMENU`, `OPEN_USER_PICKER`, `RUN_STUB`)
   - `Action_Target` (handler target id when needed)
3. Keep `Permission_ID` immutable after release.

### A3. Map Role to Permission(s)

1. Open Pulse table `Role_Permissions`.
2. Add row(s):
   - `Role` (ref to `Roles`)
   - `Permission` (ref to `Permissions`)
   - `Active=True`
3. Avoid duplicate active mappings for same `(Role, Permission)`.

### A4. Add User

1. Open Pulse table `Users`.
2. Add row:
   - `User_ID` (example: `U12`)
   - `Name`
   - `Telegram_ID`
   - `Role` (ref)
   - `Department`, `Reports_To`, `Active=True`
3. Validate by running `/start` with that Telegram account.

### A5. If feature writes Ref:Users in Costing

When Costing tables use `Ref:Users` (for example `created_by`, `approved_by`, `updated_by`), ensure `Users` table in Costing is synced by `User_ID`.

## B) Add Notification Event + Subscriptions

### B1. Add Event

1. Open Pulse table `Notification_Events`.
2. Add row:
   - `Event_ID` (example: `production_batch_scheduled`)
   - `Description`
   - `Domain` (example: `production`)
   - `Active=True`
   - `Recipient_Mode` one of:
     - `OWNER_ONLY`
     - `OWNER_PLUS_SUBSCRIBERS`
     - `SUBSCRIBERS_ONLY`

### B2. Decide Recipient Strategy

- Use `OWNER_ONLY` for personal batch events.
- Use `OWNER_PLUS_SUBSCRIBERS` for owner + optional manager/admin visibility.
- Use `SUBSCRIBERS_ONLY` for global alerts, reminders, operational broadcast.

### B3. Add Subscriptions

1. Open Pulse table `Notification_Subscriptions`.
2. Add row(s):
   - `Event` (ref to event)
   - either `Role` or `User`
   - `Enabled=True`
3. For all-manager visibility, subscribe `Production_Manager` and `System_Admin` roles.
4. Avoid duplicate active rows for same `(Event, Role/User)`.

### B4. Verify

1. Trigger the event from workflow.
2. Check `Activity_Log`:
   - `notification_sent:<event_id>`
   - `notification_failed:<event_id>`
3. Fix missing recipients by checking `Recipient_Mode`, `Subscriptions`, `Users.Telegram_ID`.

## C) Add Reminder Rule

### C1. Add Reminder Rule Row

1. Open Pulse table `Reminder_Rules`.
2. Add row:
   - `Rule_ID` (same as reminder event id)
   - `Applies_To`
   - `Condition_Type`
   - `Target_Domain`
   - `Escalation_Level`
   - `Frequency`
   - `Active=True`
   - optional threshold field used by code (`Threshold_Days` / equivalent)

### C2. Add/Verify Event

1. Ensure same event exists in `Notification_Events`.
2. Set suitable `Recipient_Mode` (usually `SUBSCRIBERS_ONLY`).
3. Add role/user subscriptions in `Notification_Subscriptions`.

### C3. Code Hook

1. Add evaluator in `pulse/reminders/engine.py`.
2. Dispatch event using `dispatch_event(...)`.
3. Pass context (`batch_id`) when owner-aware behavior is needed.

### C4. Verify

1. Create controlled test data that should trigger condition.
2. Run reminder cycle.
3. Confirm delivery and `Activity_Log` entries.

## D) New Scenario Template (Append Below Each Time)

Copy this section and fill it for every new use case.

### Scenario: `<name>`

- Date:
- Owner:
- Business goal:
- Tables touched:
- Events added/changed:
- Reminder rules added/changed:
- Roles/permissions/users changes:
- Code files changed:
- Test steps:
- Rollback plan:
- Notes:

## E) Change Checklist (Before Marking Done)

1. Access control tested with real role accounts.
2. Event IDs documented and stable.
3. `Recipient_Mode` explicitly set for new events.
4. Subscriptions verified for intended audience.
5. Reminder rule has clear threshold and frequency.
6. Activity logs show successful sends for test case.
7. This document updated with the new scenario section.

## F) Permission IDs In Use (Code Reference)

These are the permission IDs currently referenced directly by code.

- `production_view`: legacy stub entry for viewing production jobs.
- `production_complete`: legacy stub entry for marking jobs complete.
- `sales_view`: legacy stub entry for viewing sales data.
- `sales_update`: legacy stub entry for updating sales data.
- `task_assign_main`: main menu action to open task assignment flow.
- `task_assign_usercontext`: user-context action to assign task to selected user.
- `task_close`: "My Tasks" action.
- `user_manage`: opens Manage Users flow.
- `reminder_manage`: opens Reminder Rules stub.

Related action targets used by menu routing:

- `FULL_PRODUCT_MS_LIST`: opens full product MS list PDF flow.
- `NEW_PRODUCTION_BATCH`: opens new production batch flow.
- `PRODUCTION_PENDING_APPROVALS`: opens production batch approval flow.
- `MY_MS_JOBS`: opens stage-based MS job queue for current supervisor role.

## G) Event IDs In Use (Code Reference)

These are event IDs currently dispatched by application workflows/reminders.

- `production_batch_created`: emitted when a new production batch is created.
- `production_batch_approved`: emitted after manager/admin approves a batch.
- `production_batch_rejected`: emitted after manager/admin rejects a batch.
- `batch_status_changed`: emitted on child row status updates via generic updater.
- `ms_stage_pending`: emitted when an MS item enters a pending stage; recipients resolved from `ProcessStage.resolved_role_name` for the row's `process_seq` and stage.
- `ms_stage_completed`: emitted when a supervisor marks an MS stage as done.
- `production_batch_not_scheduled_reminder`: reminder event from reminder engine for approved but unscheduled batches.

## H) MS Process Routing Notes

- Source route field: `ProductPartMSList.Process_Seq` (`Ref:ProcessMaster`).
- Source route remarks field: `ProductPartMSList.Process_Seq_Remarks` (`Ref:ProcessMaster`, formula `$Process_Seq`).
- Process definition tables:
  - `ProcessMaster` (header, display label/summary, version/status)
  - `ProcessStage` (ordered stage rows, resolved role per stage)
  - `StageMaster` (default role per stage name)
- Role/user mirror tables (synced from Pulse):
  - `RoleMaster_Mirror`
  - `UserMaster_Mirror`
  - `UserRoleAssignment_Mirror`
- Batch-level cut list attachment fields (master table):
  - `ProductBatchMaster.ms_cutlist_pdf`
  - `ProductBatchMaster.cnc_cutlist_pdf`
- MS workflow tracking fields (child table): `process_seq` (`Ref:ProcessMaster`), `total_qty`, `current_stage_index`, `current_stage_name`, `current_status`, `created_at`, `updated_at`, `last_updated_by`.
- MS progression behavior:
  - Intermediate stage transitions set status to `<Next Stage> Pending`.
  - Transition into final stage sets status to `In <Final Stage>`.
  - Completing final stage sets status to `Cutting Completed`.
- Legacy compatibility: if no stage rows are found for a process ref, code falls back to parsing legacy text sequence.

## I) Phase 1 Migration Registry

- Date: 2026-02-24
- Scope: Process sequence routing migrated from text/choice to master-reference model.
- Schema migration script: `scripts/grist/apply_process_seq_phase1.py`
- Bot files updated:
  - `pulse/data/production_repo.py`
  - `pulse/integrations/production.py`
- Constraints applied:
  - No new columns added to `ProductPartMSList`.
  - Only `ProductPartMSList.Process_Seq` and `ProductPartMSList.Process_Seq_Remarks` types changed to references.

## J) Phase 1 Table Registry + Data Entry

Tables created in Costing for this requirement:

1. `ProcessMaster`
- Purpose: Process sequence header/version.
- Enter manually:
  - `process_code` (immutable key)
  - `process_name`
  - `version`
  - `status` (`Active`/`Retired`)
  - `legacy_process_seq_text` (for migration traceability)
- Formula/auto:
  - `active`, `stage_count`, `display_label`, `display_summary`

2. `ProcessStage`
- Purpose: Ordered stages for each process and resolved stage role.
- Enter manually:
  - `process_seq_id` (ref to `ProcessMaster`)
  - `seq_no` (10,20,30...)
  - `stage_id` (ref to `StageMaster`)
  - `stage_level` (use `1` for current phase)
  - `parent_stage_id` (optional, keep blank in phase 1)
  - `role_override_id` (optional, only if stage role differs from default)
- Formula/auto:
  - `stage_name`, `resolved_role_id`, `resolved_role_name`, `suggested_supervisors`, `stage_label`

3. `StageMaster`
- Purpose: Canonical stage list + default role mapping.
- Enter manually:
  - `stage_code` (immutable key)
  - `stage_name` (canonical text used across processes)
  - `default_role_id` (ref to `RoleMaster_Mirror`)
  - `active`
- Formula/auto:
  - `default_role_name`

4. `RoleMaster_Mirror`
- Purpose: Mirror of Pulse `Roles`.
- Data source: sync script (`scripts/grist/apply_process_seq_phase1.py`).
- Do not maintain manually except emergency correction.

5. `UserMaster_Mirror`
- Purpose: Mirror of Pulse `Users`.
- Data source: sync script (`scripts/grist/apply_process_seq_phase1.py`).
- Do not maintain manually except emergency correction.

6. `UserRoleAssignment_Mirror`
- Purpose: User-role mapping used for supervisor suggestions.
- Data source in phase 1: derived from Pulse `Users.Role` (single role per user).
- Enter manually only if temporary override is needed before Pulse-side update.

7. `ProcessMaster.process_remarks` (column)
- Purpose: Remarks shown in `ProductPartMSList.Process_Seq_Remarks`.
- Enter manually in `ProcessMaster`:
  - `process_remarks`
- Behavior:
  - `ProductPartMSList.Process_Seq_Remarks` auto-follows `Process_Seq`.
  - UI displays `ProcessMaster.process_remarks` for that selected process.

Existing tables changed for this requirement:

1. `ProductPartMSList`
- `Process_Seq` -> `Ref:ProcessMaster` (manual selection by user)
- `Process_Seq_Remarks` -> `Ref:ProcessMaster` (formula-managed, auto-follows `Process_Seq`)

2. `ProductBatchMS`
- `process_seq` -> `Ref:ProcessMaster` (auto-filled by workflow from selected process)

## Suggestions

1. Keep `Event_ID` immutable; deprecate by `Active=False` instead of renaming.
2. Prefer role subscriptions for manager/admin visibility, user subscription only for exceptions.
3. Add one smoke-test scenario per release (create -> approve -> status change -> reminder).
4. Review this file at every PR merge that touches roles, notifications, or reminders.
