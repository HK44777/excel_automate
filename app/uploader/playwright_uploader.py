import os
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError

# ---------------- CONFIG ----------------
HISSA_LOGIN_URL = "https://home.hissa.com/login"
ESOP_URL = "https://esop.hissa.com"

BASE_DIR = Path(__file__).resolve().parent
SESSION_FILE = BASE_DIR / "session_state.json"

# Example file (replace dynamically later)
DOWNLOADS_DIR = Path("downloads")
EXCEL_FILE_PATH = DOWNLOADS_DIR / "hnm" / "20260128_224716_HopNMoveTest.xlsx"


class HissaSessionManager:
    """
    Handles:
    - Login
    - Session persistence
    - Navigation
    - Excel upload (headless-safe)
    """

    def __init__(self):
        self.email = os.getenv("HISSA_EMAIL")
        self.password = os.getenv("HISSA_PASSWORD")

        if not self.email or not self.password:
            raise RuntimeError(
                "HISSA_EMAIL and HISSA_PASSWORD must be set in environment variables"
            )

    # --------------------------------------------------
    # PUBLIC ENTRY
    # --------------------------------------------------
    def run(self):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = (
                browser.new_context(storage_state=str(SESSION_FILE))
                if SESSION_FILE.exists()
                else browser.new_context()
            )

            page = context.new_page()

            if not self._session_is_valid(page):
                self._perform_login(page)
                context.storage_state(path=str(SESSION_FILE))

            self.navigate_to_export_import(page)
            self.open_import_from_excel(page)
            self.upload_excel(page, EXCEL_FILE_PATH)

            print("🟢 Upload completed. Browser will remain open.")
            input("Press ENTER to close browser...")

    # --------------------------------------------------
    # LOGIN HELPERS
    # --------------------------------------------------
    def _session_is_valid(self, page) -> bool:
        page.goto(HISSA_LOGIN_URL, wait_until="domcontentloaded")
        try:
            page.wait_for_selector("input[name='email']", timeout=3000)
            return False
        except TimeoutError:
            return True

    def _perform_login(self, page):
        page.goto(HISSA_LOGIN_URL, wait_until="domcontentloaded")

        page.fill("input[name='email']", self.email)
        page.click("button:has-text('Continue')")

        page.wait_for_selector("input[name='password']", timeout=10000)
        page.fill("input[name='password']", self.password)
        page.click("button:has-text('Sign in')")

        page.wait_for_load_state("networkidle")

        if not self._session_is_valid(page):
            raise RuntimeError("Login failed — check credentials or UI changes")

    # --------------------------------------------------
    # NAVIGATION
    # --------------------------------------------------
    def navigate_to_export_import(self, page):
        """
        home → esop → Plans & Grants → Export/Import
        """

        # STEP 0: Go to ESOP
        page.goto(ESOP_URL, wait_until="domcontentloaded")

        # Wait for sidebar/icons
        page.wait_for_selector(
            "div.cursor-pointer:has(svg)",
            timeout=20000
        )

        # STEP 1: Plans & Grants
        page.wait_for_selector(
            "div.cursor-pointer:has(svg.iconify--icon-park-outline)",
            timeout=15000
        )
        page.click(
            "div.cursor-pointer:has(svg.iconify--icon-park-outline)"
        )

        page.wait_for_timeout(600)

        # STEP 2: Export / Import
        page.wait_for_selector(
            "button:has(span:has-text('Export/Import'))",
            timeout=15000
        )
        page.click(
            "button:has(span:has-text('Export/Import'))"
        )

        page.wait_for_load_state("networkidle")

    def open_import_from_excel(self, page):
        page.wait_for_selector(
            "a:has-text('Import From Excel')",
            timeout=15000
        )
        page.click("a:has-text('Import From Excel')")

    # --------------------------------------------------
    # FILE UPLOAD
    # --------------------------------------------------
    def upload_excel(self, page, excel_path: Path):
        if not excel_path.exists():
            raise FileNotFoundError(f"Excel file not found: {excel_path}")

        # -------------------------------
        # Upload file
        # -------------------------------
        page.wait_for_selector("input[type='file']", timeout=15000)
        page.set_input_files("input[type='file']", str(excel_path))
        print(f"📤 Excel attached: {excel_path.name}")

        # -------------------------------
        # WAIT FOR TOAST
        # -------------------------------
        try:
            toast = page.wait_for_selector(
                "[role='alert'], [aria-live='assertive'], .toast, .snackbar",
                timeout=15000
            )
        except TimeoutError:
            raise RuntimeError(
                "No toast notification received after upload — upload status unknown"
            )

        toast_text = toast.inner_text().strip()
        print(f"🔔 Toast: {toast_text}")

        # -------------------------------
        # CLASSIFY TOAST
        # -------------------------------
        error_keywords = [
            "error",
            "invalid",
            "failed",
            "row",
            "missing",
            "not allowed",
        ]

        success_keywords = [
            "success",
            "uploaded",
            "imported",
            "completed",
        ]

        toast_lower = toast_text.lower()

        if any(word in toast_lower for word in error_keywords):
            raise RuntimeError(
                f"❌ Platform rejected Excel:\n{toast_text}"
            )

        if any(word in toast_lower for word in success_keywords):
            print("✅ Platform accepted Excel")
            return

        # -------------------------------
        # UNKNOWN TOAST (FAIL SAFE)
        # -------------------------------
        raise RuntimeError(
            f"⚠️ Unrecognized toast message:\n{toast_text}"
        )



# --------------------------------------------------
# RUN
# --------------------------------------------------
if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    manager = HissaSessionManager()
    manager.run()
