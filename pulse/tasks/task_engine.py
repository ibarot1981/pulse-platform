from pulse.core.grist_client import GristClient
from pulse.config import PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY


client = GristClient(PULSE_GRIST_SERVER, PULSE_DOC_ID, PULSE_API_KEY)


def get_tasks_for_user(user_id):
    tasks = client.get_records("Tasks")

    return [
        t for t in tasks
        if t["fields"].get("Assigned_To") == user_id
    ]
