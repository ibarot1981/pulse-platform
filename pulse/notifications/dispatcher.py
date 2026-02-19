from pulse.notifications.subscriptions import get_subscribers
from pulse.core.logger import log_event


async def dispatch_event(event_type, message, telegram_bot, context=None):

    subscribers = get_subscribers(event_type, context=context)

    for user in subscribers:
        try:
            await telegram_bot.send_message(
                chat_id=user["telegram_id"],
                text=message
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
