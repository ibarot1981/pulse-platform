from telegram import ReplyKeyboardRemove, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

from pulse.config import BOT_TOKEN
from pulse.core.permissions import get_permissions_for_role
from pulse.core.users import get_user_by_telegram
from pulse.menu.menu_builder import (
    build_menu_markup,
    get_enabled_permission_ids,
    get_menu_actions_for_permissions,
    get_menu_labels_for_permissions,
)
from pulse.menu.submenu import MAIN_STATE, MANAGE_USERS_STATE, USER_CONTEXT_STATE, USER_SELECTION_STATE
from pulse.menu.submenu import BACK_LABEL, show_dynamic_submenu, show_main_menu, show_manage_users_menu, show_user_context_menu

DENY_MESSAGE = "You are not registered in Pulse. Please contact administrator."
UNAUTHORIZED_MESSAGE = "You do not have access to this action."
STUB_MESSAGE = "Feature under development"
ACTION_OPEN_SUBMENU = "OPEN_SUBMENU"
ACTION_OPEN_USER_PICKER = "OPEN_USER_PICKER"
ACTION_RUN_STUB = "RUN_STUB"


def _get_telegram_id(update: Update) -> int | None:
    if not update.effective_user:
        return None
    return update.effective_user.id


async def _reply_text(update: Update, text: str, reply_markup=None) -> None:
    message = update.effective_message
    if not message:
        return
    await message.reply_text(text, reply_markup=reply_markup)


async def load_user_access(update: Update, context: ContextTypes.DEFAULT_TYPE, refresh: bool = False) -> bool:
    telegram_id = _get_telegram_id(update)
    if telegram_id is None:
        return False

    cached_telegram_id = context.user_data.get("telegram_id")
    if not refresh and context.user_data.get("access_loaded") and cached_telegram_id == telegram_id:
        return bool(context.user_data.get("is_registered"))

    user = get_user_by_telegram(telegram_id)
    if not user:
        context.user_data.clear()
        context.user_data["access_loaded"] = True
        context.user_data["is_registered"] = False
        context.user_data["telegram_id"] = telegram_id
        await _reply_text(update, DENY_MESSAGE, reply_markup=ReplyKeyboardRemove())
        return False

    permissions = get_permissions_for_role(user["role"])
    menu_labels = get_menu_labels_for_permissions(permissions, menu_parent=MAIN_STATE)

    context.user_data["access_loaded"] = True
    context.user_data["is_registered"] = True
    context.user_data["telegram_id"] = telegram_id
    context.user_data["user"] = user
    context.user_data["permissions"] = permissions
    context.user_data["menu_labels"] = menu_labels

    return True


def _submenu_labels(context: ContextTypes.DEFAULT_TYPE, menu_parent: str) -> list[str]:
    permissions = context.user_data.get("permissions", [])
    return get_menu_labels_for_permissions(permissions, menu_parent=menu_parent)


def _menu_actions(context: ContextTypes.DEFAULT_TYPE, menu_parent: str) -> dict[str, dict[str, str | None]]:
    permissions = context.user_data.get("permissions", [])
    return get_menu_actions_for_permissions(permissions, menu_parent=menu_parent)


async def _handle_stub_action(update: Update, context: ContextTypes.DEFAULT_TYPE, permission_key: str) -> None:
    if not await load_user_access(update, context):
        return

    permissions = context.user_data.get("permissions", [])
    enabled_permission_ids = get_enabled_permission_ids(permissions)
    if permission_key not in enabled_permission_ids:
        await _reply_text(update, UNAUTHORIZED_MESSAGE)
        return

    await _reply_text(update, STUB_MESSAGE)


async def _execute_menu_action(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    action: dict[str, str | None],
) -> bool:
    permission_key = action.get("permission_id")
    action_type = action.get("action_type") or ACTION_RUN_STUB
    action_target = action.get("action_target")

    if not permission_key:
        return False

    if action_type == ACTION_OPEN_SUBMENU:
        target_state = action_target or MAIN_STATE
        context.user_data["menu_state"] = target_state
        await _show_menu_for_state(update, context)
        return True

    if action_type == ACTION_OPEN_USER_PICKER:
        from pulse.menu.submenu import start_user_selection

        await start_user_selection(update, context)
        return True

    await _handle_stub_action(update, context, permission_key)
    return True


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    print("Telegram ID:", update.message.chat.id)
    if not await load_user_access(update, context, refresh=True):
        return

    permissions = context.user_data.get("permissions", [])
    menu_labels = get_menu_labels_for_permissions(permissions, menu_parent=MAIN_STATE)
    context.user_data["menu_labels"] = menu_labels

    await show_main_menu(update, context, menu_labels, build_menu_markup)


async def view_production_jobs(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_stub_action(update, context, "production_view")


async def mark_job_completed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_stub_action(update, context, "production_complete")


async def view_sales_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_stub_action(update, context, "sales_view")


async def update_sales_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_stub_action(update, context, "sales_update")


async def assign_task(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await load_user_access(update, context):
        return

    main_actions = _menu_actions(context, MAIN_STATE)
    action = None
    for menu_action in main_actions.values():
        if menu_action.get("permission_id") == "task_assign_main":
            action = menu_action
            break
    if not action:
        await _reply_text(update, UNAUTHORIZED_MESSAGE)
        return

    await _execute_menu_action(update, context, action)


async def my_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_stub_action(update, context, "task_close")


async def manage_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await load_user_access(update, context):
        return

    action = None
    for menu_action in _menu_actions(context, MAIN_STATE).values():
        if menu_action.get("permission_id") == "user_manage":
            action = menu_action
            break
    if not action:
        await _reply_text(update, UNAUTHORIZED_MESSAGE)
        return

    await _execute_menu_action(update, context, action)


async def reminder_rules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_stub_action(update, context, "reminder_manage")


async def _show_menu_for_state(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    state = context.user_data.get("menu_state", MAIN_STATE)
    if state == MAIN_STATE:
        menu_labels = context.user_data.get("menu_labels", [])
        await show_main_menu(update, context, menu_labels, build_menu_markup)
        return
    if state == MANAGE_USERS_STATE:
        manage_users_labels = _submenu_labels(context, MANAGE_USERS_STATE)
        await show_manage_users_menu(update, context, manage_users_labels)
        return
    if state == USER_CONTEXT_STATE:
        user_context_labels = _submenu_labels(context, USER_CONTEXT_STATE)
        await show_user_context_menu(update, context, user_context_labels)
        return

    menu_labels = _submenu_labels(context, state)
    await show_dynamic_submenu(update, context, state, menu_labels)


async def fallback_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await load_user_access(update, context):
        return

    state = context.user_data.get("menu_state", MAIN_STATE)
    text = update.effective_message.text

    # MANAGE USERS / USER SELECTION STATE
    if state in (MANAGE_USERS_STATE, USER_SELECTION_STATE):
        # Handle paginated selection first
        from pulse.menu.submenu import handle_user_selection

        handled = await handle_user_selection(update, context, text)

        if handled:
            if context.user_data.get("menu_state") != USER_SELECTION_STATE:
                await _show_menu_for_state(update, context)
            return

        if state == USER_SELECTION_STATE:
            await _reply_text(update, "Please use the menu buttons.")
            return

        if text == BACK_LABEL:
            nav_stack = context.user_data.setdefault("nav_stack", [MAIN_STATE])
            if nav_stack and nav_stack[-1] == MANAGE_USERS_STATE:
                nav_stack.pop()
            context.user_data["menu_state"] = nav_stack[-1] if nav_stack else MAIN_STATE
            await _show_menu_for_state(update, context)
            return

        manage_actions = _menu_actions(context, MANAGE_USERS_STATE)
        action = manage_actions.get(text)
        if action:
            await _execute_menu_action(update, context, action)
            return

        await _reply_text(update, "Please use the menu buttons.")
        return

    elif state == USER_CONTEXT_STATE:

        if text == BACK_LABEL:
            context.user_data.pop("selected_user", None)
            return_state = context.user_data.pop("user_context_return_state", MANAGE_USERS_STATE)
            nav_stack = context.user_data.setdefault("nav_stack", [MAIN_STATE])
            if nav_stack and nav_stack[-1] == USER_CONTEXT_STATE:
                nav_stack.pop()
            context.user_data["menu_state"] = return_state
            await _show_menu_for_state(update, context)
            return

        user_context_actions = _menu_actions(context, USER_CONTEXT_STATE)
        action = user_context_actions.get(text)
        if action:
            await _execute_menu_action(update, context, action)
            return

        await _reply_text(update, "Please use the menu buttons.")
        return
    elif state not in (MAIN_STATE, USER_SELECTION_STATE):
        if text == BACK_LABEL:
            nav_stack = context.user_data.setdefault("nav_stack", [MAIN_STATE])
            if nav_stack and nav_stack[-1] == state:
                nav_stack.pop()
            context.user_data["menu_state"] = nav_stack[-1] if nav_stack else MAIN_STATE
            await _show_menu_for_state(update, context)
            return

        actions = _menu_actions(context, state)
        action = actions.get(text)
        if action:
            await _execute_menu_action(update, context, action)
            return

        await _reply_text(update, "Please use the menu buttons.")
        return
    elif state == MAIN_STATE:
        main_actions = _menu_actions(context, MAIN_STATE)
        action = main_actions.get(text)
        if action:
            await _execute_menu_action(update, context, action)
            return
    # DEFAULT MAIN STATE FALLBACK
    await _reply_text(update, "Use the menu buttons or /start to refresh your menu.")


async def fallback_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await load_user_access(update, context):
        return
    await _reply_text(update, "Unknown command. Use /start to open your menu.")


def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    app.add_handler(MessageHandler(filters.COMMAND, fallback_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_text))

    print("Pulse running...")
    app.run_polling()


if __name__ == "__main__":
    main()
