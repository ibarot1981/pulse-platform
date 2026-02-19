# Notifications and Reminders: Implementation and Maintenance Guide

## Overview

This project uses a table-driven notification system in Pulse Grist and a reminder evaluator in code.

Core principle:

- event producers in business flows emit event IDs
- recipient resolution is data-driven from Grist
- message dispatch is centralized

## Key Components

### 1. Event Dispatch

Code entrypoint:

- `pulse/notifications/dispatcher.py`
- `dispatch_event(event_type, message, telegram_bot, context=None)`

Responsibilities:

- resolve recipients using `get_subscribers(...)`
- send Telegram messages
- write success/failure activity logs

### 2. Recipient Resolution

Code entrypoint:

- `pulse/notifications/subscriptions.py`
- `get_subscribers(event_type, context=None)`

Supported recipient modes (from `Notification_Events.Recipient_Mode`):

- `OWNER_ONLY`
- `OWNER_PLUS_SUBSCRIBERS`
- `SUBSCRIBERS_ONLY`

Owner resolution:

- uses `context["batch_id"]`
- loads `ProductBatchMaster.created_by` from Costing doc
- maps it to Pulse user by `User_ID`

Subscriber resolution:

- uses `Notification_Subscriptions` (`Event`, `Role`, optional `User`, `Enabled`)
- role-based and user-based subscriptions are supported
- recipients deduped by Telegram ID

### 3. Reminder Engine

Code entrypoint:

- `pulse/reminders/engine.py`
- `run_production_batch_reminder_checks(telegram_bot)`

Current implemented reminder:

- `production_batch_not_scheduled_reminder`
- trigger condition:
  - batch approved
  - `scheduled_date` is null
  - age from `start_date` >= threshold days (default 2, configurable via `Reminder_Rules`)

## Grist Tables and Their Roles

### Pulse doc tables

- `Notification_Events`
  - defines event IDs and recipient mode
- `Notification_Subscriptions`
  - maps event -> role/user subscription entries
- `Users`
  - source of Telegram IDs and role references
- `Roles`
  - role references for role-based subscriptions
- `Reminder_Rules`
  - rule metadata for reminder behavior
- `Activity_Log`
  - notification send/fail logs

### Costing doc tables

- `ProductBatchMaster`
  - batch lifecycle source (`created_by`, `approved_by`, dates, status)
- `BatchStatusHistory`
  - status transitions and actor (`updated_by`)
- `Users`
  - local ref target table for `Ref:Users` fields in Costing

## Important Data Contracts

### Notification_Events

Required columns:

- `Event_ID`
- `Active`
- `Recipient_Mode` (Choice)

Recommended values:

- `production_batch_created` -> `SUBSCRIBERS_ONLY`
- `production_batch_approved` -> `OWNER_PLUS_SUBSCRIBERS`
- `batch_status_changed` -> `OWNER_PLUS_SUBSCRIBERS`
- `production_batch_not_scheduled_reminder` -> `SUBSCRIBERS_ONLY`

### Reference Fields in Costing

These are now `Ref:Users` (not plain text):

- `ProductBatchMaster.created_by`
- `ProductBatchMaster.approved_by`
- `BatchStatusHistory.updated_by`

When writing these fields from code, use Costing `Users` record IDs.

## How to Maintain

### Add a new notification event

1. Add row in `Notification_Events` with:
   - `Event_ID`
   - `Active=True`
   - correct `Recipient_Mode`
2. Add subscription rows in `Notification_Subscriptions` as needed.
3. Emit the event via `dispatch_event(...)` from service code.
4. Pass context data (for owner mode), especially `batch_id`.

### Add a new reminder

1. Add event row in `Notification_Events`.
2. Add rule row in `Reminder_Rules`.
3. Implement evaluator in `pulse/reminders/engine.py`.
4. Dispatch reminder event via `dispatch_event(...)`.

### Keep Costing Users in Sync

Because Costing batch tables use `Ref:Users`, ensure Costing `Users` contains the same `User_ID` values as Pulse `Users`.

Maintenance recommendation:

- when a user is created/updated in Pulse, sync/create the same user in Costing `Users`

## Troubleshooting

### Event triggered but no one receives message

Check:

1. `Notification_Events` contains the exact `Event_ID`.
2. `Active=True` and correct `Recipient_Mode`.
3. For owner mode: event call includes `context={"batch_id": ...}`.
4. `Notification_Subscriptions` has enabled rows for subscriber modes.
5. `Users.Telegram_ID` is valid and account is active.
6. `Activity_Log` entries for `notification_failed:*`.

### Owner notifications not reaching creator

Check:

1. `ProductBatchMaster.created_by` contains valid `Ref:Users`.
2. That user maps to a valid Pulse `User_ID` and active Telegram ID.
3. Costing `Users` and Pulse `Users` are synchronized by `User_ID`.

### Reminder not firing

Check:

1. `Reminder_Rules` entry exists and is active.
2. Batch has `approval_status=Approved`.
3. `scheduled_date` is empty.
4. `start_date` is populated and older than threshold.

