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
