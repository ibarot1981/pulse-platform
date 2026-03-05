from __future__ import annotations

import os

from dotenv import load_dotenv


load_dotenv()


RUNTIME_MODE_LIVE = "LIVE"
RUNTIME_MODE_TEST = "TEST"


def runtime_mode() -> str:
    value = str(os.getenv("PULSE_RUNTIME_MODE", RUNTIME_MODE_LIVE)).strip().upper()
    if value not in {RUNTIME_MODE_LIVE, RUNTIME_MODE_TEST}:
        return RUNTIME_MODE_LIVE
    return value


def is_test_mode() -> bool:
    return runtime_mode() == RUNTIME_MODE_TEST


def test_doc_id() -> str:
    return str(os.getenv("PULSE_TEST_DOC_ID", "")).strip()


def test_api_key() -> str:
    value = str(os.getenv("PULSE_TEST_API_KEY", "")).strip()
    if value:
        return value
    return str(os.getenv("PULSE_API_KEY", "")).strip()


def test_poll_interval_seconds() -> int:
    raw = str(os.getenv("PULSE_TEST_POLL_INTERVAL_SECONDS", "30")).strip()
    try:
        value = int(raw)
    except ValueError:
        return 30
    return max(1, value)


def allow_prod_writes_in_test() -> bool:
    return str(os.getenv("PULSE_TEST_ALLOW_PROD_WRITES", "")).strip().lower() in {"1", "true", "yes"}
