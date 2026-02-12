from pulse.core.grist_client import GristClient
from pulse.config import PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY


client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)


def get_user_by_telegram(telegram_id):
    users = client.get_records("Users")

    for u in users:
        fields = u["fields"]
        if str(fields.get("Telegram_ID")) == str(telegram_id) and fields.get("Active"):
            return {
                "record_id": u["id"],
                "user_id": fields.get("User_ID"),
                "name": fields.get("Name"),
                "role": fields.get("Role"),
                "reports_to": fields.get("Reports_To")
            }

    return None
