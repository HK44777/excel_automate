import logging
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse

from app.auth import GmailAuthenticator
from app.schemas import EmailFilter, MonitorConfig, ExtractionResult, MonitorStatus
from app.gmail_client import GmailExcelExtractor
from app.scheduler import start_job, auto_extract_job, scheduler, start_scheduler
from app.state import monitor_state, save_monitor_state, load_monitor_state
from app.config import DOWNLOADS_DIR, REDIRECT_URI

app = FastAPI(title="Gmail Excel Auto-Extractor")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

oauth_flows = {}


@app.on_event("startup")
def startup_event():
    load_monitor_state()
    start_scheduler()
    logger.info("Scheduler ready")


@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()
    logger.info("Scheduler shut down")


@app.get("/")
def root():
    return {
        "message": "Gmail Excel Auto-Extractor API",
        "status": "running",
        "endpoints": {
            "authorize": "/authorize",
            "auth_status": "/auth-status",
            "extract_now": "POST /extract",
            "monitor_start": "POST /monitor/start",
            "monitor_stop": "POST /monitor/stop",
            "monitor_status": "/monitor/status",
            "downloads": "/downloads"
        }
    }


@app.get("/authorize")
def authorize():
    try:
        flow, auth_url = GmailAuthenticator.initiate_oauth_flow()
        oauth_flows['current'] = flow
        return RedirectResponse(url=auth_url)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/oauth2callback")
def oauth2callback(code: str = None, state: str = None):
    if not code:
        raise HTTPException(status_code=400, detail="No authorization code received")

    try:
        flow = oauth_flows.get('current')
        if not flow:
            raise HTTPException(status_code=400, detail="No active OAuth flow")

        auth_response = f"{REDIRECT_URI}?code={code}&state={state}"
        GmailAuthenticator.complete_oauth_flow(flow, auth_response)

        oauth_flows.pop('current', None)

        return {
            "success": True,
            "message": "Authentication successful! You can now start the monitor with POST /monitor/start"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Authentication failed: {str(e)}")


@app.get("/auth-status")
def auth_status():
    creds = GmailAuthenticator.get_credentials()
    if creds and creds.valid:
        return {"authenticated": True, "message": "Ready to extract emails and start monitoring"}
    return {"authenticated": False, "message": "Please visit /authorize to authenticate"}


@app.post("/extract", response_model=ExtractionResult)
def extract_excel_files(filters: EmailFilter = EmailFilter()):
    try:
        extractor = GmailExcelExtractor()
        return extractor.extract_all(filters)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Extraction failed: {str(e)}")


@app.post("/monitor/start")
def start_monitor(config: MonitorConfig = MonitorConfig()):
    creds = GmailAuthenticator.get_credentials()
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated. Please visit /authorize first")

    if monitor_state["is_running"]:
        return {
            "status": "already_running",
            "message": f"Monitor is already running (checks every {monitor_state['check_interval_minutes']} minutes)"
        }

    monitor_state["is_running"] = True
    monitor_state["check_interval_minutes"] = config.check_interval_minutes
    monitor_state["filters"] = config.filters.dict()
    save_monitor_state()

    start_job(config.check_interval_minutes)

    logger.info(f"✅ Monitor started - checking every {config.check_interval_minutes} minutes")

    auto_extract_job()

    return {
        "status": "started",
        "message": f"Monitor started successfully. Checking Gmail every {config.check_interval_minutes} minutes.",
        "config": config.dict()
    }


@app.post("/monitor/stop")
def stop_monitor():
    if not monitor_state["is_running"]:
        return {"status": "not_running", "message": "Monitor is not running"}

    if scheduler.get_job('auto_extract_job'):
        scheduler.remove_job('auto_extract_job')

    monitor_state["is_running"] = False
    save_monitor_state()

    logger.info("⏸️ Monitor stopped")
    return {"status": "stopped", "message": "Monitor stopped successfully"}


@app.get("/monitor/status", response_model=MonitorStatus)
def get_monitor_status():
    next_check = None
    if monitor_state["is_running"]:
        job = scheduler.get_job('auto_extract_job')
        if job and job.next_run_time:
            next_check = int((job.next_run_time - datetime.now(job.next_run_time.tzinfo)).total_seconds())

    return MonitorStatus(
        is_running=monitor_state["is_running"],
        check_interval_minutes=monitor_state["check_interval_minutes"],
        last_check_time=monitor_state["last_check_time"],
        total_checks=monitor_state["total_checks"],
        total_files_extracted=monitor_state["total_files_extracted"],
        next_check_in_seconds=next_check,
        last_extraction_result=monitor_state.get("last_extraction_result")
    )


@app.get("/downloads")
def list_downloads():
    files = []
    for filepath in DOWNLOADS_DIR.glob("*"):
        if filepath.is_file():
            files.append({
                "filename": filepath.name,
                "size_bytes": filepath.stat().st_size,
                "modified": datetime.fromtimestamp(filepath.stat().st_mtime).isoformat()
            })
    return {"files": files, "total": len(files)}


@app.delete("/downloads/{filename}")
def delete_download(filename: str):
    filepath = DOWNLOADS_DIR / filename
    if filepath.exists():
        filepath.unlink()
        return {"success": True, "message": f"Deleted {filename}"}
    raise HTTPException(status_code=404, detail="File not found")
