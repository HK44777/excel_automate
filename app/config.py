from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

DOWNLOADS_DIR = Path(os.getenv("DOWNLOADS_DIR", "downloads"))
LOGS_DIR = Path(os.getenv("LOGS_DIR", "logs"))

TOKEN_FILE = Path(os.getenv("TOKEN_FILE", "token.json"))
CREDENTIALS_FILE = Path(os.getenv("CREDENTIALS_FILE", "credentials.json"))
MONITOR_STATE_FILE = Path(os.getenv("MONITOR_STATE_FILE", "monitor_state.json"))

DOWNLOADS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

EXCEL_EXTENSIONS = {'.xlsx', '.xls', '.xlsm', '.csv'}

REDIRECT_URI = os.getenv("REDIRECT_URI", "http://localhost:8000/oauth2callback")
