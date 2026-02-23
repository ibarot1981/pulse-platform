from pulse.notifications.subscriptions import get_subscribers
from pulse.core.logger import log_event


async def dispatch_event(
    event_type,
    message,
    telegram_bot,
    context=None,
    reply_markup=None,
    recipient_renderer=None,
):

    subscribers = get_subscribers(event_type, context=context)

    for user in subscribers:
        try:
            rendered_message = message
            rendered_markup = reply_markup
            if recipient_renderer:
                rendered = recipient_renderer(user) or {}
                if rendered.get("skip"):
                    continue
                if "message" in rendered:
                    rendered_message = rendered["message"]
                if "reply_markup" in rendered:
                    rendered_markup = rendered["reply_markup"]

            await telegram_bot.send_message(
                chat_id=user["telegram_id"],
                text=rendered_message,
                reply_markup=rendered_markup,
            )

            log_event(
                user["user_id"],
                f"notification_sent:{event_type}",
                "Success"
            )

        except Exception as e:
            log_event(
                user["user_id"],
                f"notification_failed:{event_type}",
                str(e)
            )
