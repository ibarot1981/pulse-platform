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

## Suggestions

1. Keep `Event_ID` immutable; deprecate by `Active=False` instead of renaming.
2. Prefer role subscriptions for manager/admin visibility, user subscription only for exceptions.
3. Add one smoke-test scenario per release (create -> approve -> status change -> reminder).
4. Review this file at every PR merge that touches roles, notifications, or reminders.
