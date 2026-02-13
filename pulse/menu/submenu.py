from telegram import ReplyKeyboardMarkup
from pulse.data.pulse_repo import get_all_users

MAIN_STATE = "MAIN"
MANAGE_USERS_STATE = "MANAGE_USERS"


async def show_main_menu(update, context, menu_labels, build_menu_markup):
    context.user_data["menu_state"] = MAIN_STATE

    if not menu_labels:
        await update.effective_message.reply_text(
            "No actions available for your role."
        )
        return

    await update.effective_message.reply_text(
        "Welcome to Pulse. Choose an action:",
        reply_markup=build_menu_markup(menu_labels),
    )


async def show_manage_users_menu(update, context):
    context.user_data["menu_state"] = MANAGE_USERS_STATE

    keyboard = ReplyKeyboardMarkup(
        [
            ["View All Users"],
            ["Assign Task to User"],
            ["View Tasks of User"],
            ["🔙 Back"],
        ],
        resize_keyboard=True,
    )

    await update.effective_message.reply_text(
        "Manage Users:",
        reply_markup=keyboard,
    )

async def show_all_users(update, context):

    users = get_all_users()

    if not users:
        await update.effective_message.reply_text("No users found.")
        return

    message_lines = ["👥 All Users:\n"]

    count = 1
    for u in users:
        fields = u["fields"]

        name = fields.get("Name", "Unknown")
        user_id = fields.get("User_ID", "")
        active = "🟢" if fields.get("Active") else "🔴"

        message_lines.append(f"{count}. {name} ({user_id}) {active}")
        count += 1

    await update.effective_message.reply_text("\n".join(message_lines))
