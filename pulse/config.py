import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Pulse control doc
PULSE_GRIST_SERVER = os.getenv("PULSE_GRIST_SERVER")
PULSE_DOC_ID = os.getenv("PULSE_DOC_ID")
PULSE_API_KEY = os.getenv("PULSE_API_KEY")

# External docs (example)
PRODUCTION_DOC_ID = os.getenv("PRODUCTION_DOC_ID")
SALES_DOC_ID = os.getenv("SALES_DOC_ID")
COSTING_DOC_ID = os.getenv("COSTING_DOC_ID")
COSTING_API_KEY = os.getenv("COSTING_API_KEY")

PULSE_RUNTIME_MODE = os.getenv("PULSE_RUNTIME_MODE", "LIVE")
PULSE_TEST_DOC_ID = os.getenv("PULSE_TEST_DOC_ID", "")
PULSE_TEST_API_KEY = os.getenv("PULSE_TEST_API_KEY", "")
PULSE_TEST_POLL_INTERVAL_SECONDS = int(os.getenv("PULSE_TEST_POLL_INTERVAL_SECONDS", "30"))
PULSE_TEST_ALLOW_PROD_WRITES = os.getenv("PULSE_TEST_ALLOW_PROD_WRITES", "").lower() in ("1", "true", "yes")

# Display timezone and format used for date/time text in notifications.
NOTIFICATION_TIMEZONE = os.getenv("NOTIFICATION_TIMEZONE", "Asia/Calcutta")
NOTIFICATION_DATETIME_FORMAT = os.getenv("NOTIFICATION_DATETIME_FORMAT", "%d-%m-%Y %H:%M:%S %Z")
