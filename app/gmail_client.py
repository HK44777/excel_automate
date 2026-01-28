import base64
import json
import logging
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import List, Optional

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

COMPANY_JSON_PATH = Path(__file__).resolve().parent / "company.json"
FAILED_LABEL = "VALIDATION_FAILED"


# ======================================================
# Gmail Excel Extractor
# ======================================================

class GmailExcelExtractor:

    def __init__(self):
        self.creds = GmailAuthenticator.get_credentials()
        if not self.creds:
            raise HTTPException(status_code=401, detail="Not authenticated")

        self.service = build("gmail", "v1", credentials=self.creds)

    # ======================================================
    # Label Utilities
    # ======================================================

    def _get_or_create_label(self, label_name: str) -> str:
        labels = self.service.users().labels().list(userId="me").execute().get("labels", [])

        for lbl in labels:
            if lbl["name"] == label_name:
                return lbl["id"]

        label = self.service.users().labels().create(
            userId="me",
            body={
                "name": label_name,
                "labelListVisibility": "labelShow",
                "messageListVisibility": "show"
            }
        ).execute()

        return label["id"]

    def mark_failed(self, msg_id: str):
        label_id = self._get_or_create_label(FAILED_LABEL)

        self.service.users().messages().modify(
            userId="me",
            id=msg_id,
            body={
                "addLabelIds": [label_id],
                "removeLabelIds": ["UNREAD"]
            }
        ).execute()

    def mark_read(self, msg_id: str):
        self.service.users().messages().modify(
            userId="me",
            id=msg_id,
            body={"removeLabelIds": ["UNREAD"]}
        ).execute()

    # ======================================================
    # Email Sender
    # ======================================================

    def send_error_email(self, to_email: str, report: dict):
        msg = EmailMessage()
        msg["To"] = to_email
        msg["From"] = "me"
        msg["Subject"] = "Excel Validation Failed"
        msg.set_content(json.dumps(report, indent=4))

        encoded = base64.urlsafe_b64encode(msg.as_bytes()).decode()

        self.service.users().messages().send(
            userId="me",
            body={"raw": encoded}
        ).execute()

    # ======================================================
    # Query Builder
    # ======================================================

    def build_query(self, filters: EmailFilter) -> str:
        parts = []

        if filters.unread_only:
            parts.append("is:unread")

        parts.append("-label:VALIDATION_FAILED")
        parts.append("has:attachment")

        if filters.sender_email:
            parts.append(f"from:{filters.sender_email}")

        if filters.subject_contains:
            parts.append(f"subject:{filters.subject_contains}")

        excel_query = " OR ".join([f"filename:{e}" for e in EXCEL_EXTENSIONS])
        parts.append(f"({excel_query})")

        return " ".join(parts)

    # ======================================================
    # Gmail Fetching
    # ======================================================

    def fetch_emails(self, filters):
        res = self.service.users().messages().list(
            userId="me",
            q=self.build_query(filters),
            maxResults=filters.max_results
        ).execute()

        return res.get("messages", [])

    def get_message(self, msg_id):
        return self.service.users().messages().get(
            userId="me",
            id=msg_id,
            format="full"
        ).execute()

    def extract_metadata(self, msg):
        data = {"subject": "", "from": "", "date": ""}

        for h in msg["payload"]["headers"]:
            n = h["name"].lower()
            if n == "subject":
                data["subject"] = h["value"]
            if n == "from":
                data["from"] = h["value"]
            if n == "date":
                data["date"] = h["value"]

        return data

    # ======================================================
    # Attachment Handling
    # ======================================================

    def get_attachments(self, msg):
        out = []

        def walk(parts):
            for p in parts:
                if "parts" in p:
                    walk(p["parts"])

                name = p.get("filename", "")
                if name and Path(name).suffix.lower() in EXCEL_EXTENSIONS:
                    out.append({
                        "filename": name,
                        "id": p["body"].get("attachmentId"),
                        "size": p["body"].get("size", 0)
                    })

        walk(msg["payload"].get("parts", []))
        return out

    def download(self, msg_id, att_id, filename, company_code):
        data = self.service.users().messages().attachments().get(
            userId="me",
            messageId=msg_id,
            id=att_id
        ).execute()

        raw = base64.urlsafe_b64decode(data["data"])

        name = f"{datetime.now():%Y%m%d_%H%M%S}_{filename}"
        folder = DOWNLOADS_DIR / company_code
        folder.mkdir(parents=True, exist_ok=True)

        path = folder / name
        path.write_bytes(raw)
        return path

    # ======================================================
    # Main Pipeline
    # ======================================================

    def extract_all(self, filters: EmailFilter) -> ExtractionResult:

        with open(COMPANY_JSON_PATH) as f:
            company_db = json.load(f)

        extracted = []
        errors = []

        for msg_stub in self.fetch_emails(filters):

            msg_id = msg_stub["id"]

            try:
                msg = self.get_message(msg_id)
                meta = self.extract_metadata(msg)

                company = resolve_company_from_sender(meta["from"])
                if not company:
                    self.mark_read(msg_id)
                    continue

                attachments = self.get_attachments(msg)
                success_for_email = False

                for att in attachments:

                    file_path = self.download(
                        msg_id,
                        att["id"],
                        att["filename"],
                        company.company_code
                    )

                    df = pd.read_excel(file_path)

                    report, cleaned_df = formatting.process_and_validate_excel(
                        df,
                        company.company_name,
                        company_db
                    )

                    # ---------- FAILED ----------
                    if report:
                        self.send_error_email(meta["from"], report)
                        self.mark_failed(msg_id)
                        continue

                    # ---------- SUCCESS ----------
                    cleaned_path = file_path.with_name(
                        f"{file_path.stem}_cleaned.xlsx"
                    )
                    cleaned_df.to_excel(cleaned_path, index=False)

                    extracted.append(ExtractedFile(
                        filename=att["filename"],
                        filepath=str(cleaned_path),
                        size_bytes=att["size"],
                        email_subject=meta["subject"],
                        email_from=meta["from"],
                        email_date=meta["date"],
                        email_id=msg_id,
                        attachment_id=att["id"],
                        company_code=company.company_code,
                        company_name=company.company_name
                    ))

                    success_for_email = True

                if success_for_email:
                    self.mark_read(msg_id)

            except Exception as e:
                errors.append(str(e))
                logger.exception("Processing error")

        return ExtractionResult(
            success=len(errors) == 0,
            files_extracted=extracted,
            emails_processed=len(extracted),
            errors=errors,
            timestamp=datetime.now().isoformat()
        )
