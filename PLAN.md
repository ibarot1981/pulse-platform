## Requirement
So I need an implementation plan for the following design intent:
For each batch, clearly assign Batch Creator, Batch Owner and Batch Notifier
- Batch Owner should always be notified for every event in the batch. He can also be Batch Creator.
- Batch Creator can also be System Admin or Production manager. If Batch Creator is System Admin or Production Manager, then during batch creation, they should get a list of production supervisors to designate the owner of the batch. Batch Creator also gets all notification. But if batch creator and Batch owner are same users, then duplicate messages and notifications should always be avoided.

- Approval process for Batch remains as-is. i.e. Production Manager or System Admin can approve the batch.

- We need to design and maintain a system where who receives notification messages in the batch can be configured easily via grist tables and not hardcoded in the code.

- It is also possible where one user can have multiple roles. In that case notifications and tasks for both the roles needs to be sent to him. How do we design this.

- Production Supervisors are a special role. They are the initiators of a Batch and all the stages of the batch end at them. So assigning owner and creator defines who gets the messages. It is possible we can have multiple notifiers. So Batch Owner can assign from a list of Production Supervisors users - multiple users as Batch Notifiers. If batch notifiers are assigned, they also get all updates and can perform hand-off acceptance or rejection.

- Other Roles such as Cutting Supervisor, Machine Shop Supervisors are mostly one person each but they can have sub roles within them. so delegation of tasks should be allowed.
So is it a good design to have a role - sub role hierarchy as optional. so if a role and sub role is not defined, then everything works as descried above. 
So a Cutting Supervisor may have a CNC Laser Cutter role under him and a MS Cutting role under him.
While at a stage level cutting supervisors gets the task, he should be able to delegate this to sub roles under him.
So a Angle cutting job in one flow can be delegated to MS Cutting user and then he gets the action to complete the task. In this scenario since the task is delegated, cutting supervisor himself can action as well as the delegated user can also action on the task wherein he can either mark it as done or submit a remark.


should we design stage level users. E.g. if Press Job stage is defined - we assign multiple users to this stage and mark who gets notified and who gets actionable tasks peretaining to that specific stage only. Currently we have this at a role level. So can we design it such that if role is only defined then all users in the role gets it but if user level is defined then only users specified gets it.
Validate this against current tables like stage master,  process master,  process stage and its values and suggest a good design. can we do this without changing much.
Go through Role Master, User master, UserroleAssignment and user tables as well. We have mirrored thsi from pulse to costing as well.

Give me your ideas and do not implement this as of now. Only if the design intent is correct, we will  go ahead and implement this.

##Plan
## Batch Responsibility + Notification Routing Plan (Config-Driven, Minimal-Change)

### Summary
- Keep approval flow unchanged: only `Production_Manager`/`System_Admin` approve batches.
- Introduce explicit batch actors: `Batch Creator`, `Batch Owner`, and `Batch Notifiers` with no hardcoded recipients.
- Preserve table-driven notification design via Grist and extend current resolver (already dedupes by Telegram ID).
- Validate current state findings:
  - `Users.Role` is single-role today, so true multi-role needs a separate assignment model.
  - `ProductBatchMaster` already has `notification_users` (unused), so this is a low-impact extension point.
  - `ProcessStage` already has `stage_level`/`parent_stage_id`/`role_override_id`, so stage-level user overrides can be added without replacing current role model.

### Key Implementation Changes
- Data model additions (Pulse source-of-truth, mirrored to Costing):
  - Add `UserRoleAssignment` table in Pulse (one user, many roles), keep `Users.Role` as default/display role for backward compatibility.
  - Add `RoleHierarchy` table in Pulse (optional parent/child role mapping for delegation scopes).
  - Mirror both into Costing (`UserRoleAssignment_Mirror` already exists; add/extend sync to use Pulse table as source).
- Batch actor fields (Costing `ProductBatchMaster`):
  - Add `owner_user` (`Ref:Users`).
  - Add `notifier_users` (`RefList:Users`) and keep legacy `notification_users` for backward compatibility/read migration.
  - Keep `created_by` as creator.
- Stage assignment configurability:
  - Add `ProcessStageUserAssignment` table (per stage template): `process_stage_id`, `user_id`, `can_notify`, `can_act`, `active`.
  - Resolution rule: if stage-specific users exist and active, they override role-level recipients; otherwise fallback to role-level (`resolved_role_name`).
- Runtime delegation:
  - Add `BatchMSDelegation` table (per batch/stage row): `batch_ms_id`, `delegated_to_user`, `delegated_by_user`, `can_notify`, `can_act`, `active`, `remarks`, timestamps.
  - Delegation candidates are constrained by optional `RoleHierarchy`; if no hierarchy rows exist, current behavior remains role-based.
- Notification/task resolver changes:
  - Build recipients from union of creator + owner + notifiers + stage recipients + configured subscriptions.
  - Deduplicate by `User_ID` then `Telegram_ID` before send/action rendering.
  - Multi-role support uses `UserRoleAssignment` memberships (not single `Users.Role`) for routing.
- Batch creation UX behavior:
  - If creator is `System_Admin`/`Production_Manager`: owner selection from Production Supervisors is mandatory.
  - Otherwise owner defaults to creator.
  - Notifiers are multi-select Production Supervisors and can perform full stage actions (per your choice).
  - If creator == owner, only one notification/task surface is shown (deduped).

### Public Interfaces / Behavior Contracts
- Notification context/resolution contract:
  - Continue supporting existing `Recipient_Mode` and `Notification_Subscriptions`.
  - Extend resolver to accept explicit batch actor fields (`owner_user`, `notifier_users`) plus stage-user/delegation sources.
- Authorization contract for stage actions:
  - Actionable users = stage override users (if present) else stage role users, plus active delegated users, plus notifiers (full actions).
  - Supervisor and delegated user can both action delegated tasks.
- Backward compatibility:
  - Existing role-only stage configuration continues unchanged where no stage-user overrides are defined.
  - Existing events/subscriptions remain valid.

### Test Plan
- Batch actor assignment:
  - Admin/PM creator must pick owner from Production Supervisors.
  - Supervisor creator defaults to self as owner.
  - Creator=Owner receives single notification (no duplicates).
- Notification fan-out:
  - Owner always receives all batch events.
  - Notifiers receive all updates and can execute stage actions.
  - Multi-role user receives combined visibility/actions once (deduped).
- Stage routing precedence:
  - With `ProcessStageUserAssignment`: only configured users get notify/action.
  - Without it: all users in resolved role get notify/action.
- Delegation:
  - Cutting Supervisor delegates to child role user; both can mark done/add remarks.
  - Delegation deactivation immediately removes delegated actor rights.
- Approval regression:
  - Approval/rejection remains restricted to PM/System Admin.
- Mirror/sync:
  - Pulse `UserRoleAssignment` changes are reflected correctly in Costing mirror and used by runtime resolution.

### Assumptions and Defaults
- Chosen defaults from this planning session:
  - Owner assignment: creator default; Admin/PM must explicitly choose owner.
  - Notifier action scope: full stage actions.
  - Stage precedence: user-level override over role-level.
  - Multi-role model: new Pulse `UserRoleAssignment` table as source-of-truth, mirrored to Costing.
- Minimal-change principle:
  - Keep existing event model, approval flow, and role-based fallback behavior intact.
  - Additive schema changes preferred over breaking field-type changes.

