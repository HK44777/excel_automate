from pydantic import BaseModel, EmailStr
from typing import List, Optional


class EmailFilter(BaseModel):
    sender_email: Optional[EmailStr] = None
    subject_contains: Optional[str] = None
    unread_only: bool = True
    max_results: int = 10


class MonitorConfig(BaseModel):
    check_interval_minutes: int = 2
    filters: EmailFilter = EmailFilter()


class ExtractedFile(BaseModel):
    filename: str
    filepath: str
    size_bytes: int
    email_subject: str
    email_from: str
    email_date: str
    email_id: str
    attachment_id: str
    company_code: str
    company_name: str




class ExtractionResult(BaseModel):
    success: bool
    files_extracted: List[ExtractedFile]
    emails_processed: int
    errors: List[str]
    timestamp: str


class MonitorStatus(BaseModel):
    is_running: bool
    check_interval_minutes: int
    last_check_time: Optional[str]
    total_checks: int
    total_files_extracted: int
    next_check_in_seconds: Optional[int]
    last_extraction_result: Optional[ExtractionResult]
