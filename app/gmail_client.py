import base64
import json
import logging
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from fastapi import HTTPException
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from app.auth import GmailAuthenticator
from app.company_router import resolve_company_from_sender
from app.config import DOWNLOADS_DIR, EXCEL_EXTENSIONS
from app.schemas import EmailFilter, ExtractionResult, ExtractedFile
from app import formatting

logger = logging.getLogger(__name__)

# ✅ Resolve company.json safely (no CWD problems)
COMPANY_JSON_PATH = Path(__file__).resolve().parent / "company.json"


class GmailExcelExtractor:
    """Extracts Excel attachments from Gmail"""

    def __init__(self):
        self.creds = GmailAuthenticator.get_credentials()
        if not self.creds:
            raise HTTPException(
                status_code=401,
                detail="Not authenticated. Please visit /authorize first"
            )
        self.service = build("gmail", "v1", credentials=self.creds)

    # ----------------------------
    # Utilities
    # ----------------------------
    def _load_company_db(self) -> dict:
        if not COMPANY_JSON_PATH.exists():
            raise FileNotFoundError(f"company.json not found at {COMPANY_JSON_PATH}")

        with COMPANY_JSON_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _send_error_report_email(self, to_email: str, subject: str, report: dict) -> None:
        """
        Send validation error report email.
        Uses same OAuth creds as extractor.
        """
        try:
            email_message = EmailMessage()
            email_message["To"] = to_email
            email_message["From"] = "me"
            email_message["Subject"] = subject
            email_message.set_content(json.dumps(report, indent=4))

            encoded_message = base64.urlsafe_b64encode(email_message.as_bytes()).decode()
            create_message = {"raw": encoded_message}

            self.service.users().messages().send(userId="me", body=create_message).execute()
            logger.info(f"Sent validation error report email to {to_email}")

        except HttpError as error:
            logger.error(f"Failed sending error report email: {error}")

    # ----------------------------
    # Gmail Query Builder
    # ----------------------------
    def build_query(self, filters: EmailFilter) -> str:
        query_parts = []

        if filters.unread_only:
            query_parts.append("is:unread")

        if filters.sender_email:
            query_parts.append(f"from:{filters.sender_email}")

        if filters.subject_contains:
            query_parts.append(f"subject:{filters.subject_contains}")

        query_parts.append("has:attachment")

        excel_query = " OR ".join([f"filename:{ext}" for ext in EXCEL_EXTENSIONS])
        query_parts.append(f"({excel_query})")

        return " ".join(query_parts)

    # ----------------------------
    # Fetch Gmail Messages
    # ----------------------------
    def fetch_emails(self, filters: EmailFilter) -> List[dict]:
        try:
            query = self.build_query(filters)
            logger.info(f"Gmail query: {query}")

            results = self.service.users().messages().list(
                userId="me",
                q=query,
                maxResults=filters.max_results
            ).execute()

            messages = results.get("messages", [])
            logger.info(f"Found {len(messages)} matching emails")
            return messages

        except HttpError as error:
            logger.error(f"Gmail API error: {error}")
            raise HTTPException(status_code=500, detail=f"Gmail API error: {error}")

    # ----------------------------
    # Fetch Full Email Details
    # ----------------------------
    def get_email_details(self, msg_id: str) -> Optional[dict]:
        try:
            return self.service.users().messages().get(
                userId="me",
                id=msg_id,
                format="full"
            ).execute()
        except HttpError as error:
            logger.error(f"Error fetching email {msg_id}: {error}")
            return None

    # ----------------------------
    # Extract Email Metadata
    # ----------------------------
    def extract_metadata(self, message: dict) -> dict:
        headers = message["payload"]["headers"]
        metadata = {"subject": "", "from": "", "to": "", "date": ""}

        for header in headers:
            name = header["name"].lower()
            if name == "subject":
                metadata["subject"] = header["value"]
            elif name == "from":
                metadata["from"] = header["value"]
            elif name == "to":
                metadata["to"] = header["value"]
            elif name == "date":
                metadata["date"] = header["value"]

        return metadata

    # ----------------------------
    # Extract Attachments (recursive)
    # ----------------------------
    def get_attachments(self, message: dict) -> List[dict]:
        attachments = []

        def process_parts(parts):
            for part in parts:
                if "parts" in part:
                    process_parts(part["parts"])

                filename = part.get("filename", "")
                if filename:
                    file_ext = Path(filename).suffix.lower()
                    if file_ext in EXCEL_EXTENSIONS:
                        attachments.append({
                            "filename": filename,
                            "mimeType": part.get("mimeType", ""),
                            "attachmentId": part["body"].get("attachmentId"),
                            "size": part["body"].get("size", 0)
                        })

        if "parts" in message["payload"]:
            process_parts(message["payload"]["parts"])

        return attachments

    # ----------------------------
    # Download Attachment
    # ----------------------------
    def download_attachment(self, msg_id: str, attachment_id: str, filename: str, company_code: str) -> Path:
        """
        Download attachment from Gmail and save to:
            downloads/{company_code}/{timestamp}_{filename}
        """
        attachment = self.service.users().messages().attachments().get(
            userId="me",
            messageId=msg_id,
            id=attachment_id
        ).execute()

        file_data = base64.urlsafe_b64decode(attachment["data"])

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = f"{timestamp}_{filename}"

        company_dir = DOWNLOADS_DIR / company_code
        company_dir.mkdir(parents=True, exist_ok=True)

        filepath = company_dir / safe_filename
        filepath.write_bytes(file_data)

        logger.info(f"Downloaded: {company_code}/{safe_filename} ({len(file_data)} bytes)")
        return filepath

    # ----------------------------
    # Mark Email as Read
    # ----------------------------
    def mark_as_read(self, msg_id: str):
        try:
            self.service.users().messages().modify(
                userId="me",
                id=msg_id,
                body={"removeLabelIds": ["UNREAD"]}
            ).execute()
            logger.info(f"Marked email {msg_id} as read")
        except HttpError as error:
            logger.error(f"Error marking email as read: {error}")

    # ----------------------------
    # Main Extraction Method
    # ----------------------------
    def extract_all(self, filters: EmailFilter) -> ExtractionResult:
        extracted_files: List[ExtractedFile] = []
        errors: List[str] = []

        company_db = self._load_company_db()

        messages = self.fetch_emails(filters)

        for msg_summary in messages:
            msg_id = msg_summary["id"]

            try:
                message = self.get_email_details(msg_id)
                if not message:
                    errors.append(f"Could not fetch email {msg_id}")
                    continue

                metadata = self.extract_metadata(message)

                # ✅ sender-first routing
                company_ctx = resolve_company_from_sender(metadata.get("from", ""))

                if not company_ctx:
                    logger.warning(f"QUARANTINE: Unknown sender. from={metadata.get('from')} msg_id={msg_id}")
                    continue

                attachments = self.get_attachments(message)
                if not attachments:
                    logger.info(f"No Excel attachments in email: {metadata['subject']}")
                    continue

                any_success = False

                for attachment in attachments:
                    try:
                        attachment_id = attachment.get("attachmentId")
                        if not attachment_id:
                            logger.warning(
                                f"Skipping attachment without attachmentId. msg_id={msg_id} filename={attachment.get('filename')}"
                            )
                            continue

                        # 1) Download
                        filepath = self.download_attachment(
                            msg_id=msg_id,
                            attachment_id=attachment_id,
                            filename=attachment["filename"],
                            company_code=company_ctx.company_code
                        )

                        # 2) Validate + Clean
                        df = pd.read_excel(filepath, engine="openpyxl")
                        report, cleaned_df = formatting.process_and_validate_excel(
                            df=df,
                            company_key=company_ctx.company_name,   # company_key in json
                            json_db=company_db
                        )

                        if report:
                            logger.warning(f"Validation failed for {filepath.name}. Sending report to sender.")
                            # send error report back to sender
                            self._send_error_report_email(
                                to_email=metadata.get("from", ""),
                                subject="Excel Validation Failed",
                                report=report
                            )
                            continue

                        # 3) Save cleaned file
                        cleaned_path = filepath.parent / f"{filepath.stem}_cleaned.xlsx"
                        cleaned_df.to_excel(cleaned_path, index=False)
                        logger.info(f"Cleaned file saved: {cleaned_path}")

                        extracted_files.append(ExtractedFile(
                            filename=attachment["filename"],
                            filepath=str(cleaned_path),   # return cleaned file path
                            size_bytes=attachment["size"],
                            email_subject=metadata["subject"],
                            email_from=metadata["from"],
                            email_date=metadata["date"],
                            email_id=msg_id,
                            attachment_id=attachment_id,
                            company_code=company_ctx.company_code,
                            company_name=company_ctx.company_name
                        ))

                        any_success = True

                    except Exception as e:
                        error_msg = f"Failed processing {attachment.get('filename')}: {str(e)}"
                        errors.append(error_msg)
                        logger.error(error_msg)

                # ✅ only mark read if at least one attachment succeeded
                if any_success:
                    self.mark_as_read(msg_id)

            except Exception as e:
                error_msg = f"Error processing email {msg_id}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)

        return ExtractionResult(
            success=len(errors) == 0,
            files_extracted=extracted_files,
            emails_processed=len(messages),
            errors=errors,
            timestamp=datetime.now().isoformat()
        )
