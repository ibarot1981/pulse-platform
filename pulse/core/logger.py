from pulse.core.grist_client import GristClient
from pulse.config import PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY
from datetime import datetime


client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)


def log_event(user_id, action, result):

    client.patch_record(
        "Activity_Log",
        None,
        {
            "Timestamp": datetime.utcnow().isoformat(),
            "User": user_id,
            "Action": action,
            "Result": result
        }
    )
