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

### Scenario: Batch Overview + Drill-down Tracker (Telegram)

- Date: 2026-02-28
- Owner: Engineering
- Business goal: Reduce overload in batch summary messages by showing compact flow snapshot with drill-down menus.
- Tables touched:
  - Pulse: `Permissions`, `Role_Permissions`
  - Costing (read path): `ProductBatchMaster`, `ProductBatchMS`, `BatchStatusHistory`, `ProcessMaster`, `ProcessStage`
- Events added/changed: None
- Reminder rules added/changed: None
- Roles/permissions/users changes:
  - Added `Permissions` row:
    - `Permission_ID=production_view_batch`
    - `Menu_Label=View Batch`
    - `Menu_Parent=MANAGE_PRODUCTION`
    - `Action_Type=RUN_STUB`
    - `Action_Target=VIEW_BATCH`
  - Added `Role_Permissions` mapping:
    - `Production_Manager -> production_view_batch`
    - `System_Admin -> production_view_batch`
- Code files changed:
  - `pulse/integrations/production.py`
  - `pulse/main.py`
- Test steps:
  - Supervisor path: `My MS Jobs -> View By Batch No -> select batch` shows overview + inline `View Flow Details | View Timeline`.
  - Supervisor action checks:
    - Current-stage supervisor can use `Mark Stage Done`.
    - Next-stage supervisor can use `Confirm Hand-off`.
    - Next-stage supervisor can reject handoff with remarks (`Reject Handoff`).
  - My Jobs queue behavior:
    - Default view is `My Pending Actions` (handoff pending + current pending completion only).
    - `View All` shows all approved batches and flows.
    - Handoff-pending rows are visible to both current and next-stage supervisors with role-specific action context.
  - Admin/PM path: `Manage Production -> View Batch -> select batch` opens same tracker.
- Rollback plan:
  - Set `production_view_batch` inactive or remove role mappings in `Role_Permissions`.
  - Revert code changes in `pulse/integrations/production.py` and `pulse/main.py`.
- Notes:
  - Flow snapshot legend uses status icons for done, running, pending, and handoff pending.
  - Tracker uses inline keyboard callbacks for drill-down; no Telegram color styling dependency.

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
- `production_my_ms_jobs`: opens supervisor MS stage queue.
- `production_my_ms_schedule`: opens supervisor batch scheduling queue.
- `production_view_batch`: opens batch tracker list under `Manage Production` for `System_Admin` and `Production_Manager`.

Related action targets used by menu routing:

- `FULL_PRODUCT_MS_LIST`: opens full product MS list PDF flow.
- `NEW_PRODUCTION_BATCH`: opens new production batch flow.
- `PRODUCTION_PENDING_APPROVALS`: opens production batch approval flow.
- `MY_MS_JOBS`: opens stage-based MS job queue for current supervisor role.
- `MY_MS_SCHEDULE`: opens supervisor schedule queue and propagates schedule date to all MS rows in selected batch.
- `VIEW_BATCH`: opens batch tracker list and batch overview+drill-down screens.

## G) Event IDs In Use (Code Reference)

These are event IDs currently dispatched by application workflows/reminders.

- `production_batch_created`: emitted when a new production batch is created.
- `production_batch_approved`: emitted after manager/admin approves a batch.
- `production_batch_rejected`: emitted after manager/admin rejects a batch.
- `batch_status_changed`: emitted on child row status updates via generic updater.
- `ms_stage_pending`: emitted when an MS item enters a pending stage; recipients resolved from `ProcessStage.resolved_role_name` for the row's `process_seq` and stage.
- `ms_stage_completed`: emitted when a supervisor marks an MS stage as done.
- `production_batch_not_scheduled_reminder`: reminder event from reminder engine for approved but unscheduled batches.
- `production_batch_scheduled`: emitted when a batch is scheduled from supervisor quick action/menu flow.
- `supervisor_batch_schedule_reminder`: reminder for supervisors to schedule batches pending in their stage queue.
- `ms_stage_pending_reminder`: reminder for supervisors to update pending stage actions.

## H) MS Process Routing Notes

- Source route field: `ProductPartMSList.Process_Seq` (`Ref:ProcessMaster`).
- Source route remarks field: `ProductPartMSList.Process_Seq_Remarks` (`Ref:ProcessMaster`, formula `$Process_Seq`).
- Process definition tables:
  - `ProcessMaster` (header, display label/summary, version/status)
  - `ProcessStage` (ordered stage rows, resolved role per stage)
  - `StageMaster` (default role per stage name)
- Role/user mirror tables (synced from Pulse):
  - `RoleMaster_Mirror`
  - `Users` (Costing mirror of Pulse users)
  - `UserRoleAssignment_Mirror`
- Batch-level cut list attachment fields (master table):
  - `ProductBatchMaster.ms_cutlist_pdf`
  - `ProductBatchMaster.cnc_cutlist_pdf`
- MS workflow tracking fields (child table): `process_seq` (`Ref:ProcessMaster`), `total_qty`, `current_stage_index`, `current_stage_name`, `current_status`, `created_at`, `updated_at`, `last_updated_by`.
- MS progression behavior:
  - Intermediate stage transitions set status to `<Next Stage> Pending`.
  - Transition into final stage sets status to `In <Final Stage>`.
  - Completing final stage sets status to `Cutting Completed`.
  - Marking done before handoff acceptance sets status to `Done - Pending Confirmation`.
  - Next-stage supervisor can accept handoff (advances flow) or reject with remarks (returns row to previous stage pending).
  - Default `My Pending Actions` queue shows only actionable rows for logged-in supervisor; `View All` is full visibility.
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

5. `UserRoleAssignment_Mirror`
- Purpose: User-role mapping used for supervisor suggestions.
- Source of truth: Pulse `UserRoleAssignment` (multi-role capable).
- Sync target: Costing `UserRoleAssignment_Mirror`.
- Sync script: `scripts/grist/sync_pulse_user_role_assignments_to_costing.py`
- Non-destructive behavior:
  - Upsert by `assignment_key` only.
  - No deletes.
  - Existing rows without `assignment_key` are untouched.

### J1) Safe Sync Command (UserRoleAssignment -> Mirror)

Use this when a user is updated in Pulse with additional role assignments.

Dry-run first (recommended):

```powershell
$env:PYTHONPATH='.'
python scripts/grist/sync_pulse_user_role_assignments_to_costing.py --user-id U02 --dry-run
```

Apply for just that user:

```powershell
$env:PYTHONPATH='.'
python scripts/grist/sync_pulse_user_role_assignments_to_costing.py --user-id U02
```

Apply for all users (still upsert-only; no deletes):

```powershell
$env:PYTHONPATH='.'
python scripts/grist/sync_pulse_user_role_assignments_to_costing.py
```

6. `ProcessMaster.process_remarks` (column)
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

## K) TEST Mode + Preview Utilities

### K1) Required Environment Variables

Set at runtime:

- `PULSE_RUNTIME_MODE=TEST`
- `PULSE_TEST_DOC_ID=<test-grist-doc-id>`
- `PULSE_TEST_API_KEY=<api-key-for-test-doc>`
- `PULSE_DOC_ID=<live-pulse-doc-id>`
- `PULSE_API_KEY=<live-pulse-api-key>`
- `PULSE_GRIST_SERVER=<grist-server-url>`

Optional:

- `PULSE_TEST_SESSION_ID=sim-e2e-001` (default)
- `PULSE_TEST_SUPERVISOR_USER_ID=<telegram_id>` (optional, auto-resolved if available)
- `PULSE_TEST_MANAGER_USER_ID=<telegram_id>` (optional, auto-resolved if available)
- `PULSE_TEST_SAMPLE_ACTOR_USER_ID=<telegram_id>` (for optional sample inbox seed)
- `PULSE_TEST_PREVIEW_PATH=artifacts/test_preview/outbox_preview.html`

### K2) One-time TEST document schema setup

```powershell
$env:PULSE_RUNTIME_MODE="TEST"
$env:PYTHONPATH="."
python scripts/grist/setup_test_runtime_doc.py
```

What it creates/ensures in `PULSE_TEST_DOC_ID`:

- `Test_Inbox`
- `Test_Outbox`
- `Test_UserContext`
- `Test_Attachments`
- `Test_RunLog`

### K3) Run full supervisor -> manager approval flow in one command

Recommended for quick regression checks:

```powershell
$env:PYTHONPATH='.'
.\venv\Scripts\python.exe scripts/grist/run_e2e_batch_approval.py --refresh-session --render
```

Override actors explicitly if auto-detect fails:

```powershell
.\venv\Scripts\python.exe scripts/grist/run_e2e_batch_approval.py --supervisor 900000003 --manager 900000004 --session sim-e2e-001 --render
```

This script performs:

1. Supervisor path creation (`/start` -> batch flow -> batch creation).
2. Detects the new `ProductBatchMaster` row id automatically.
3. Manager approval callback flow (`prodappr:open` then `prodappr:approve`).
4. Optional preview render.

### K4) Manual TEST actions (legacy style)

You can still push individual rows directly.

```powershell
python scripts/grist/push_test_inbox.py --session sim-e2e-001 --actor 900000003 --text "/start" --process-now --render
python scripts/grist/push_test_inbox.py --session sim-e2e-001 --actor 900000003 --callback "prodappr:approve:123" --process-now --render
```

### K5) Quick My MS Jobs runner (recommended)

Use this to simulate:

- `/start -> Manage Production -> My MS Jobs`
- Optional immediate view selection (for example `View By Batch No`)
- Optional batch auto-selection by batch number (`--batch-no`) when using `View By Batch No`
- Optional render refresh

Examples:

```powershell
.\venv\Scripts\python.exe scripts/grist/run_my_ms_jobs_view.py --actor 8492411029 --refresh-session --render
.\venv\Scripts\python.exe scripts/grist/run_my_ms_jobs_view.py --actor 8492411029 --refresh-session --view "View By Batch No" --render
.\venv\Scripts\python.exe scripts/grist/run_my_ms_jobs_view.py --actor 8492411029 --refresh-session --batch-no "MAR26-S1KHFL-BASE-MCS-001" --render
.\venv\Scripts\python.exe scripts/grist/run_my_ms_jobs_view.py --actor 8492411029 --refresh-session --view "View Created By" --render
.\venv\Scripts\python.exe scripts/grist/run_my_ms_jobs_view.py --actor 8492411029 --refresh-session --view "View By Next Stage" --render
```

Notes:

- If `--batch-no` is passed without `--view`, the runner automatically uses `View By Batch No`.
- The runner reads the `Select Batch No:` options and submits the matching serial number automatically (including moving to next pages if needed).

### K6) Preview + outbox filtering

```powershell
.\scripts\grist\render_preview.cmd
```

Open:

- `artifacts/test_preview/outbox_preview.html`

Preview updates included:

- Toolbar user filter (All users / User ID).
- Message cards show source and parse-mode metadata.
- Button tooltip shows callback payload (hover to inspect).

### K7) Test Inbox rows you can inject

- Approve a batch:

```json
{
  "session_id": "sim-e2e-001",
  "actor_user_id": "900000004",
  "actor_role": "Production_Manager",
  "input_type": "callback",
  "payload": "prodappr:approve:123",
  "processed": false
}
```

- Open a supervisor stage detail flow (replace batch/row context):

```json
{
  "session_id": "sim-e2e-001",
  "actor_user_id": "900000003",
  "actor_role": "Production_Supervisor",
  "input_type": "callback",
  "payload": "msbatch:vd:123",
  "processed": false
}
```

- Schedule batch action:

```json
{
  "session_id": "sim-e2e-001",
  "actor_user_id": "900000003",
  "actor_role": "Production_Supervisor",
  "input_type": "callback",
  "payload": "prodsv:schedule:123",
  "processed": false
}
```

### K8) Dual-Role Owner E2E Runner

Use this for end-to-end regression of:
- batch create
- approval
- stage done + handoff confirm across supervisors
- final completion
- notification fanout checks

Script:
- `scripts/grist/run_e2e_dual_role_owner_flow.py`
- Interaction model:
  - menu-driven simulation (text menus + action buttons)
  - no hardcoded stage/approval callback payloads
  - manager approval uses submenu when available, else approval inline button from notification

Recommended strict run (creator=owner):
- This validates whether the owner can create from menu with current primary role permissions.

```powershell
$env:PYTHONPATH='.'
$env:PULSE_RUNTIME_MODE='TEST'
python scripts/grist/run_e2e_dual_role_owner_flow.py --session sim-e2e-dual-role-strict --owner-telegram 8492411029 --creator-telegram 8492411029 --render
```

Fallback full-task run (creator as admin, owner dual-role user):
- Use when owner create menu is blocked by current primary-role menu permissions.
- Still validates owner-stage actions + handoffs + completion + notifications.

```powershell
$env:PYTHONPATH='.'
$env:PULSE_RUNTIME_MODE='TEST'
python scripts/grist/run_e2e_dual_role_owner_flow.py --session sim-e2e-dual-role-admin --creator-telegram 820565883 --owner-telegram 8492411029 --manager-telegram 900000004 --machine-telegram 900000006 --render
```

Key validations performed by script:
- all `ProductBatchMS` rows for the batch reach completion (`Cutting Completed/Done/Completed`)
- handoff confirmations are executed by next-stage supervisor actor
- owner actor receives approval/completion notifications
- machine-stage actor receives handoff/pending task notifications
- only rows for the test-created batch are acted on during stage progression
