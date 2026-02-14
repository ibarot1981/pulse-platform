from telegram import ReplyKeyboardMarkup
from pulse.data.pulse_repo import get_all_users

MAIN_STATE = "MAIN"
MANAGE_USERS_STATE = "MANAGE_USERS"
USER_CONTEXT_STATE = "USER_CONTEXT"
USER_SELECTION_STATE = "USER_SELECTION"



async def show_main_menu(update, context, menu_labels, build_menu_markup):
    context.user_data["menu_state"] = MAIN_STATE
    context.user_data["nav_stack"] = [MAIN_STATE]

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
    nav_stack = context.user_data.setdefault("nav_stack", [MAIN_STATE])
    if not nav_stack:
        nav_stack.append(MAIN_STATE)
    if nav_stack[-1] != MANAGE_USERS_STATE:
        nav_stack.append(MANAGE_USERS_STATE)

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

PAGE_SIZE = 5


async def start_user_selection(update, context):

    users = get_all_users()

    if not users:
        await update.effective_message.reply_text("No users found.")
        return

    context.user_data["selection_context"] = {
        "type": "users",
        "records": users,
        "page": 0,
        "page_size": PAGE_SIZE,
    }
    context.user_data["menu_state"] = USER_SELECTION_STATE
    nav_stack = context.user_data.setdefault("nav_stack", [MAIN_STATE])
    if not nav_stack:
        nav_stack.append(MAIN_STATE)
    if nav_stack[-1] != USER_SELECTION_STATE:
        nav_stack.append(USER_SELECTION_STATE)

    await show_user_page(update, context)


async def show_user_page(update, context):

    selection = context.user_data.get("selection_context")

    if not selection:
        return

    records = selection["records"]
    page = selection["page"]
    page_size = selection["page_size"]

    start = page * page_size
    end = start + page_size

    page_records = records[start:end]

    if not page_records:
        await update.effective_message.reply_text("No records.")
        return

    message_lines = ["👥 Select a User:\n"]

    for idx, record in enumerate(page_records, start=1):
        fields = record["fields"]
        name = fields.get("Name", "Unknown")
        message_lines.append(f"{idx}. {name}")

    keyboard_rows = []

    if page > 0:
        keyboard_rows.append(["⬅ Prev"])

    if end < len(records):
        keyboard_rows.append(["➡ Next"])

    keyboard_rows.append(["🔙 Back"])

    keyboard = ReplyKeyboardMarkup(keyboard_rows, resize_keyboard=True)

    await update.effective_message.reply_text(
        "\n".join(message_lines),
        reply_markup=keyboard,
    )


async def handle_user_selection(update, context, text):

    selection = context.user_data.get("selection_context")

    if not selection:
        return False

    records = selection["records"]
    page = selection["page"]
    page_size = selection["page_size"]

    # Navigation
    if text == "⬅ Prev":
        selection["page"] -= 1
        await show_user_page(update, context)
        return True

    if text == "➡ Next":
        selection["page"] += 1
        await show_user_page(update, context)
        return True

    if text == "🔙 Back":
        context.user_data.pop("selection_context", None)
        nav_stack = context.user_data.setdefault("nav_stack", [MAIN_STATE])
        if nav_stack and nav_stack[-1] == USER_SELECTION_STATE:
            nav_stack.pop()
        context.user_data["menu_state"] = nav_stack[-1] if nav_stack else MAIN_STATE
        return True

    # Number selection
    if text.isdigit():
        choice = int(text)
        start = page * page_size
        index = start + choice - 1

        if 0 <= index < len(records):
            selected_user = records[index]

            context.user_data["selected_user"] = selected_user

            await show_user_context_menu(update, context)

            return True

    return False

async def show_user_context_menu(update, context):

    selected_user = context.user_data.get("selected_user")

    if not selected_user:
        return

    context.user_data["menu_state"] = USER_CONTEXT_STATE

    name = selected_user["fields"].get("Name", "Unknown")

    keyboard = ReplyKeyboardMarkup(
        [
            ["Assign Task"],
            ["View Tasks"],
            ["🔙 Back"],
        ],
        resize_keyboard=True,
    )

    await update.effective_message.reply_text(
        f"User: {name}\n\nChoose an action:",
        reply_markup=keyboard,
    )

