# Menu + Feature Walkthrough (Grist-Driven)

This guide explains how to add new menus/submenus and wire feature behavior in this project.

It follows the same pattern used for:
- `Manage Production` -> `View MS Cut List`
- `MSCUTLIST_MENU` -> `View Full Product MS List` / `By Product-Part MS List`

## 1. Add Menu Rows In Grist (`Permissions`)

Each clickable menu item is one row in `Permissions`.

Required columns:
- `Permission_ID` (unique key, stable)
- `Menu_Label` (button text)
- `Menu_Parent` (where it appears)
- `Action_Type` (`OPEN_SUBMENU`, `OPEN_USER_PICKER`, `RUN_STUB`)
- `Action_Target` (state key or action key)
- `Active` (`true`/`false`)

Example:
- Parent menu item:
  - `Permission_ID=production_manage`
  - `Menu_Parent=MAIN`
  - `Action_Type=OPEN_SUBMENU`
  - `Action_Target=MANAGE_PRODUCTION`
- Child menu item:
  - `Permission_ID=production_view_mscutlist`
  - `Menu_Parent=MANAGE_PRODUCTION`
  - `Action_Type=OPEN_SUBMENU`
  - `Action_Target=MSCUTLIST_MENU`
- Final action item:
  - `Permission_ID=production_view_bypart_ms`
  - `Menu_Parent=MSCUTLIST_MENU`
  - `Action_Type=RUN_STUB`
  - `Action_Target=BY_PRODUCT_PART_MS_LIST`

## 2. Grant Access In Grist (`Role_Permissions`)

Menu visibility is role-based. If menu is missing, usually role mapping is missing.

For every new `Permission_ID`, add `Role_Permissions` rows:
- `Role=<role ref>`
- `Permission=<permission ref>`
- `Active=true`

## 3. Add Environment Placeholders (If New Data Source)

Update `.env`:
- `COSTING_DOC_ID=`
- `COSTING_API_KEY=`
- `MSCUTLIST_PAGE_SIZE=12`

Update `pulse/config.py`:
- add `COSTING_DOC_ID`
- add `COSTING_API_KEY`

Update `pulse/settings.py`:
- add dataclass fields for the above
- add init parsing via `get_env` / `get_int`

## 4. Add Data Repository Stub

Create `pulse/data/<feature>_repo.py` (example: `pulse/data/costing_repo.py`).

Start with minimal methods:
- `get_products()`
- `get_parts_for_product(product_key)`
- `get_ms_cut_list(product_key, part_key)`

Return placeholders first, then implement with real Costing table schema.

## 5. How Menu Routing Works In Code

### `pulse/menu/menu_builder.py`
- Loads `Permissions` metadata.
- Reads:
  - `Menu_Parent` (default `MAIN`)
  - `Action_Type` (default `RUN_STUB`)
  - `Action_Target`
- Exposes:
  - `get_menu_labels_for_permissions(...)`
  - `get_menu_actions_for_permissions(...)`
  - `get_enabled_permission_ids(...)`

### `pulse/main.py`
- Loads role permissions for current user.
- Resolves visible actions by current `menu_state`.
- Uses `_execute_menu_action(...)`:
  - `OPEN_SUBMENU` -> switch state and render submenu
  - `OPEN_USER_PICKER` -> launch picker flow
  - `RUN_STUB` -> standard "Feature under development"

### `pulse/menu/submenu.py`
- Handles shared submenu UI and navigation (`Back`, pagination).
- `show_dynamic_submenu(...)` renders any state-driven submenu without hardcoding labels.
- Existing user picker flow is reused for action types that need selection.

## 6. Adding A New Menu Feature (Checklist)

1. Add new rows in `Permissions` with correct `Menu_Parent`, `Action_Type`, `Action_Target`.
2. Add role mappings in `Role_Permissions`.
3. If external data needed, add `.env` keys + `config.py` + `settings.py`.
4. Add/extend repo under `pulse/data/`.
5. If custom flow is needed (beyond `RUN_STUB`/`OPEN_SUBMENU`/`OPEN_USER_PICKER`), add handling in `main.py` and/or `submenu.py`.
6. Compile check:
   - `python -m compileall pulse/main.py pulse/menu/menu_builder.py pulse/menu/submenu.py`
7. Manual bot test:
   - `/start` -> parent menu visible
   - submenu visible
   - action click triggers expected behavior

## 7. Common Failure Points

- Parent menu visible but submenu empty:
  - child permissions not assigned in `Role_Permissions`
- Menu item not visible at all:
  - wrong `Menu_Parent`, inactive row, or missing role mapping
- Click does nothing useful:
  - `Action_Type`/`Action_Target` mismatch
- Changes in Grist not reflected:
  - restart bot or clear metadata cache (`_permissions_metadata`)

## 8. Current Supported Action Types

- `OPEN_SUBMENU`
- `OPEN_USER_PICKER`
- `RUN_STUB`

If you add a new `Action_Type`, update `_execute_menu_action(...)` in `pulse/main.py`.
