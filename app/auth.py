import logging
from typing import Optional, Tuple

from fastapi import HTTPException
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

from app.config import TOKEN_FILE, CREDENTIALS_FILE, SCOPES, REDIRECT_URI

logger = logging.getLogger(__name__)


class GmailAuthenticator:
    """Handles Gmail OAuth2 authentication"""

    @staticmethod
    def get_credentials() -> Optional[Credentials]:
        creds = None

        if TOKEN_FILE.exists():
            creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

        # Refresh if expired
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                TOKEN_FILE.write_text(creds.to_json())
                logger.info("Credentials refreshed successfully")
            except Exception as e:
                logger.error(f"Failed to refresh credentials: {e}")
                return None

        return creds if creds and creds.valid else None

    @staticmethod
    def initiate_oauth_flow() -> Tuple[InstalledAppFlow, str]:
        if not CREDENTIALS_FILE.exists():
            raise HTTPException(
                status_code=500,
                detail="credentials.json not found. Please download from Google Cloud Console"
            )

        flow = InstalledAppFlow.from_client_secrets_file(
            str(CREDENTIALS_FILE),
            SCOPES,
            redirect_uri=REDIRECT_URI
        )

        auth_url, _ = flow.authorization_url(prompt='consent')
        return flow, auth_url

    @staticmethod
    def complete_oauth_flow(flow: InstalledAppFlow, auth_response: str) -> Credentials:
        flow.fetch_token(authorization_response=auth_response)
        creds = flow.credentials
        TOKEN_FILE.write_text(creds.to_json())
        logger.info("Credentials saved successfully")
        return creds
