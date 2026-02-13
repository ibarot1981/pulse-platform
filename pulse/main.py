import re

from telegram import ReplyKeyboardRemove, Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

from pulse.config import BOT_TOKEN
from pulse.core.permissions import get_permissions_for_role
from pulse.core.users import get_user_by_telegram
from pulse.menu.menu_builder import PERMISSION_MENU_MAP, build_menu_markup, get_menu_labels_for_permissions
from pulse.menu.submenu import show_main_menu, show_manage_users_menu, MAIN_STATE, MANAGE_USERS_STATE

DENY_MESSAGE = "You are not registered in Pulse. Please contact administrator."
UNAUTHORIZED_MESSAGE = "You do not have access to this action."
STUB_MESSAGE = "Feature under development"


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
    menu_labels = get_menu_labels_for_permissions(permissions)

    context.user_data["access_loaded"] = True
    context.user_data["is_registered"] = True
    context.user_data["telegram_id"] = telegram_id
    context.user_data["user"] = user
    context.user_data["permissions"] = permissions
    context.user_data["menu_labels"] = menu_labels

    return True


async def _handle_stub_action(update: Update, context: ContextTypes.DEFAULT_TYPE, permission_key: str) -> None:
    if not await load_user_access(update, context):
        return

    allowed_labels = set(context.user_data.get("menu_labels", []))
    required_label = PERMISSION_MENU_MAP[permission_key]
    if required_label not in allowed_labels:
        await _reply_text(update, UNAUTHORIZED_MESSAGE)
        return

    await _reply_text(update, STUB_MESSAGE)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    print("Telegram ID:", update.message.chat.id)
    if not await load_user_access(update, context, refresh=True):
        return

    menu_labels = context.user_data.get("menu_labels", [])

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
    await _handle_stub_action(update, context, "task_assign")


async def my_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_stub_action(update, context, "task_close")


async def manage_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await load_user_access(update, context):
        return

    allowed_labels = set(context.user_data.get("menu_labels", []))
    required_label = PERMISSION_MENU_MAP["user_manage"]

    if required_label not in allowed_labels:
        await _reply_text(update, UNAUTHORIZED_MESSAGE)
        return

    await show_manage_users_menu(update, context)



async def reminder_rules(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_stub_action(update, context, "reminder_manage")


async def fallback_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await load_user_access(update, context):
        return

    state = context.user_data.get("menu_state", MAIN_STATE)
    text = update.effective_message.text

    # MANAGE USERS STATE
    if state == MANAGE_USERS_STATE:

        if text == "🔙 Back":
            menu_labels = context.user_data.get("menu_labels", [])
            await show_main_menu(update, context, menu_labels, build_menu_markup)
            return

        if text == "View All Users":
            from pulse.menu.submenu import show_all_users
            await show_all_users(update, context)
            return

        if text == "Assign Task to User":
            await _reply_text(update, "Assign Task - Coming Soon")
            return

        if text == "View Tasks of User":
            await _reply_text(update, "View Tasks - Coming Soon")
            return

        await _reply_text(update, "Please use the menu buttons.")
        return

    # DEFAULT MAIN STATE FALLBACK
    await _reply_text(update, "Use the menu buttons or /start to refresh your menu.")



async def fallback_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await load_user_access(update, context):
        return
    await _reply_text(update, "Unknown command. Use /start to open your menu.")


def _menu_handler(pattern: str, callback):
    return MessageHandler(filters.Regex(f"^{re.escape(pattern)}$"), callback)


def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))

    app.add_handler(_menu_handler(PERMISSION_MENU_MAP["production_view"], view_production_jobs))
    app.add_handler(_menu_handler(PERMISSION_MENU_MAP["production_complete"], mark_job_completed))
    app.add_handler(_menu_handler(PERMISSION_MENU_MAP["sales_view"], view_sales_data))
    app.add_handler(_menu_handler(PERMISSION_MENU_MAP["sales_update"], update_sales_data))
    app.add_handler(_menu_handler(PERMISSION_MENU_MAP["task_assign"], assign_task))
    app.add_handler(_menu_handler(PERMISSION_MENU_MAP["task_close"], my_tasks))
    app.add_handler(_menu_handler(PERMISSION_MENU_MAP["user_manage"], manage_users))
    app.add_handler(_menu_handler(PERMISSION_MENU_MAP["reminder_manage"], reminder_rules))

    app.add_handler(MessageHandler(filters.COMMAND, fallback_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, fallback_text))

    print("Pulse running...")
    app.run_polling()


if __name__ == "__main__":
    main()
