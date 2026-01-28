import re
from dataclasses import dataclass
from typing import Optional, Dict

# -------------------------------------------------------
# Sender-first routing (DEV / TEST)
# Later replace with DB tables
# -------------------------------------------------------

@dataclass(frozen=True)
class CompanyContext:
    company_code: str
    company_name: str


# ✅ 1) Map sender email -> company context
SENDER_TO_COMPANY: Dict[str, CompanyContext] = {
    # dev sender -> mapped company
    "ghimirekumudraj@gmail.com": CompanyContext(company_code="hnm", company_name="HopNMove Pvt Ltd"),
    "hemanthkumar.r2005@gmail.com": CompanyContext(company_code="nec", company_name="Nebula Corp"),
    # add more later
    # "hr.xyz@gmail.com": CompanyContext(company_code="xyz", company_name="XYZ Industries"),
}


# optional: support extracting email inside angle brackets
EMAIL_EXTRACT_REGEX = re.compile(r"<([^>]+)>")

def extract_email_address(raw_header: str) -> str:
    """
    Convert:
      'Finance Team <reports@abc.com>' -> 'reports@abc.com'
      'reports@abc.com' -> 'reports@abc.com'
    """
    raw_header = (raw_header or "").strip()
    match = EMAIL_EXTRACT_REGEX.search(raw_header)
    if match:
        return match.group(1).strip().lower()
    return raw_header.lower()


def resolve_company_from_sender(from_header: str) -> Optional[CompanyContext]:
    """
    Main resolver:
    - Parse sender email from From header
    - Lookup in sender->company registry
    """
    sender_email = extract_email_address(from_header)
    return SENDER_TO_COMPANY.get(sender_email)
