import logging
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from app.auth import GmailAuthenticator
from app.gmail_client import GmailExcelExtractor
from app.schemas import EmailFilter
from app.state import monitor_state, save_monitor_state

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler()


def auto_extract_job():
    logger.info("🔍 Auto-extraction job started")

    try:
        creds = GmailAuthenticator.get_credentials()
        if not creds:
            logger.warning("⚠️ No valid credentials. Skipping auto-extraction.")
            return

        extractor = GmailExcelExtractor()
        filters = EmailFilter(**monitor_state["filters"])
        result = extractor.extract_all(filters)

        monitor_state["last_check_time"] = datetime.now().isoformat()
        monitor_state["total_checks"] += 1
        monitor_state["total_files_extracted"] += len(result.files_extracted)
        monitor_state["last_extraction_result"] = result.dict()
        save_monitor_state()

        if result.files_extracted:
            logger.info(f"✅ Extracted {len(result.files_extracted)} Excel files")
        else:
            logger.info("ℹ️ No new Excel files found")

        if result.errors:
            logger.error(f"❌ Errors during extraction: {result.errors}")

    except Exception as e:
        logger.error(f"❌ Auto-extraction job failed: {str(e)}")


def start_job(check_interval_minutes: int):
    if scheduler.get_job('auto_extract_job'):
        scheduler.remove_job('auto_extract_job')

    scheduler.add_job(
        auto_extract_job,
        trigger=IntervalTrigger(minutes=check_interval_minutes),
        id='auto_extract_job',
        name='Auto Extract Excel from Gmail',
        replace_existing=True
    )


def start_scheduler():
    if not scheduler.running:
        scheduler.start()


def stop_scheduler():
    scheduler.shutdown()
