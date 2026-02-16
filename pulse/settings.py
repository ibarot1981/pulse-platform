import os
from dotenv import load_dotenv
from dataclasses import dataclass


# -------------------------------------------------
# Load environment file
# -------------------------------------------------
load_dotenv()


# -------------------------------------------------
# Helper Functions
# -------------------------------------------------

def get_env(name: str, required: bool = True, default=None):
    value = os.getenv(name, default)

    if required and not value:
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def get_bool(name: str, default: bool = False):
    value = os.getenv(name)

    if value is None:
        return default

    return value.lower() in ["true", "1", "yes"]


def get_int(name: str, default: int = 0):
    value = os.getenv(name)

    if value is None:
        return default

    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Environment variable {name} must be an integer")


# -------------------------------------------------
# Settings Dataclass
# -------------------------------------------------

@dataclass
class Settings:

    # Telegram
    BOT_TOKEN: str
    ADMIN_TELEGRAM_ID: str

    # Pulse Control Plane
    PULSE_GRIST_SERVER: str
    PULSE_DOC_ID: str
    PULSE_API_KEY: str

    # External Docs
    PRODUCTION_DOC_ID: str
    PRODUCTION_API_KEY: str

    SALES_DOC_ID: str
    SALES_API_KEY: str

    COSTING_DOC_ID: str
    COSTING_API_KEY: str

    ACCOUNTS_DOC_ID: str
    ACCOUNTS_API_KEY: str

    LOGISTICS_DOC_ID: str
    LOGISTICS_API_KEY: str

    # Runtime
    REMINDER_INTERVAL: int
    EVENT_POLL_INTERVAL: int
    TIMEZONE: str
    MSCUTLIST_PAGE_SIZE: int

    # Logging
    LOG_LEVEL: str
    ENABLE_ACTIVITY_LOG: bool
    ENABLE_DEBUG_PRINT: bool

    # Feature Flags
    ENABLE_REMINDERS: bool
    ENABLE_TASKS: bool
    ENABLE_NOTIFICATIONS: bool
    ENABLE_ESCALATION: bool


# -------------------------------------------------
# Instantiate Settings
# -------------------------------------------------

settings = Settings(

    BOT_TOKEN=get_env("BOT_TOKEN"),
    ADMIN_TELEGRAM_ID=get_env("ADMIN_TELEGRAM_ID", required=False, default=""),

    PULSE_GRIST_SERVER=get_env("PULSE_GRIST_SERVER"),
    PULSE_DOC_ID=get_env("PULSE_DOC_ID"),
    PULSE_API_KEY=get_env("PULSE_API_KEY"),

    PRODUCTION_DOC_ID=get_env("PRODUCTION_DOC_ID"),
    PRODUCTION_API_KEY=get_env("PRODUCTION_API_KEY"),

    SALES_DOC_ID=get_env("SALES_DOC_ID"),
    SALES_API_KEY=get_env("SALES_API_KEY"),

    COSTING_DOC_ID=get_env("COSTING_DOC_ID", required=False, default=""),
    COSTING_API_KEY=get_env("COSTING_API_KEY", required=False, default=""),

    ACCOUNTS_DOC_ID=get_env("ACCOUNTS_DOC_ID"),
    ACCOUNTS_API_KEY=get_env("ACCOUNTS_API_KEY"),

    LOGISTICS_DOC_ID=get_env("LOGISTICS_DOC_ID"),
    LOGISTICS_API_KEY=get_env("LOGISTICS_API_KEY"),

    REMINDER_INTERVAL=get_int("REMINDER_INTERVAL", 300),
    EVENT_POLL_INTERVAL=get_int("EVENT_POLL_INTERVAL", 60),
    TIMEZONE=get_env("TIMEZONE", required=False, default="Asia/Kolkata"),
    MSCUTLIST_PAGE_SIZE=get_int("MSCUTLIST_PAGE_SIZE", 12),

    LOG_LEVEL=get_env("LOG_LEVEL", required=False, default="INFO"),
    ENABLE_ACTIVITY_LOG=get_bool("ENABLE_ACTIVITY_LOG", True),
    ENABLE_DEBUG_PRINT=get_bool("ENABLE_DEBUG_PRINT", True),

    ENABLE_REMINDERS=get_bool("ENABLE_REMINDERS", True),
    ENABLE_TASKS=get_bool("ENABLE_TASKS", True),
    ENABLE_NOTIFICATIONS=get_bool("ENABLE_NOTIFICATIONS", True),
    ENABLE_ESCALATION=get_bool("ENABLE_ESCALATION", True),
)


print("Pulse Settings Loaded Successfully.")
