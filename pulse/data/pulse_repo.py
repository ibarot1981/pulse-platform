from pulse.core.grist_client import GristClient
from pulse.settings import settings


_client = GristClient(
    settings.PULSE_GRIST_SERVER,
    settings.PULSE_DOC_ID,
    settings.PULSE_API_KEY,
)


def get_all_users():
    return _client.get_records("Users")
