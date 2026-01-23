import json
from datetime import datetime
from typing import Any, Dict

from app.config import MONITOR_STATE_FILE

monitor_state: Dict[str, Any] = {
    "is_running": False,
    "check_interval_minutes": 2,
    "last_check_time": None,
    "total_checks": 0,
    "total_files_extracted": 0,
    "last_extraction_result": None,
    "filters": {
        "sender_email": None,
        "subject_contains": None,
        "unread_only": True
    }
}


def save_monitor_state():
    MONITOR_STATE_FILE.write_text(json.dumps(monitor_state, indent=2))


def load_monitor_state():
    global monitor_state
    if MONITOR_STATE_FILE.exists():
        monitor_state.update(json.loads(MONITOR_STATE_FILE.read_text()))
