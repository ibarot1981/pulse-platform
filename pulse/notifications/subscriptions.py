from pulse.core.grist_client import GristClient
from pulse.config import PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY


client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)


def get_subscribers(event_type):

    subs = client.get_records("Notification_Subscriptions")
    users = client.get_records("Users")

    result = []

    for s in subs:
        f = s["fields"]

        if f.get("Enabled") and f.get("Event") == event_type:

            role = f.get("Role")
            user = f.get("User")

            for u in users:
                uf = u["fields"]

                if user and uf.get("User_ID") == user:
                    result.append({
                        "user_id": uf.get("User_ID"),
                        "telegram_id": uf.get("Telegram_ID")
                    })

                elif role and uf.get("Role") == role:
                    result.append({
                        "user_id": uf.get("User_ID"),
                        "telegram_id": uf.get("Telegram_ID")
                    })

    return result
