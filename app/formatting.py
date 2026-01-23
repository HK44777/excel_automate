import pandas as pd
from datetime import datetime
from dateutil import parser
import json
import os
from pathlib import Path


# -------------------------------------------------------
# 1) CONFIGURATION
# -------------------------------------------------------

# ✅ Always resolve company.json relative to this file location
# app/formatting.py  -> app/company.json
COMPANY_JSON_PATH = Path(__file__).resolve().parent / "company.json"

if not COMPANY_JSON_PATH.exists():
    raise FileNotFoundError(
        f"company.json not found at: {COMPANY_JSON_PATH}. "
        "Please ensure app/company.json exists."
    )

with COMPANY_JSON_PATH.open("r", encoding="utf-8") as f:
    json_db = json.load(f)

MANDATORY_HEADERS = [
    "Employee Id",
    "Options Granted",
    "Plan Name",
    "Date Of Grant",
    "Grant Price",
    "Vesting Template",
    "Vesting Date Type"
]

VESTING_DATE_TYPE_OPTS = ["GrantDate", "CustomDate", "EmployeeJoiningDate"]
ACTUAL_VESTING_DAY_OPTS = ["SAME_DAY", "NEXT_DAY", "PREVIOUS_DAY", "STARTING_OF_MONTH", "END_OF_MONTH"]


# -------------------------------------------------------
# 2) HELPER FUNCTIONS
# -------------------------------------------------------

def clean_and_convert_date(date_val):
    if pd.isna(date_val) or str(date_val).strip() == "":
        raise ValueError("Date is empty")

    dt_obj = None

    if isinstance(date_val, (int, float)):
        # Excel numeric date support
        dt_obj = pd.to_datetime(date_val, unit="D", origin="1899-12-30")

    elif isinstance(date_val, (pd.Timestamp, datetime)):
        dt_obj = date_val

    elif isinstance(date_val, str):
        date_str = str(date_val).strip().replace(" ", "-").replace(".", "-").replace("/", "-")
        try:
            dt_obj = pd.to_datetime(date_str, dayfirst=True)
        except Exception:
            try:
                dt_obj = parser.parse(date_str, dayfirst=True)
            except Exception:
                raise ValueError(f"Invalid date format: '{date_val}'")

    if dt_obj is None or pd.isna(dt_obj):
        raise ValueError("Unknown date error")

    # Handle short year (ex: 24 -> 2024)
    if dt_obj.year < 100:
        dt_obj = dt_obj.replace(year=dt_obj.year + 2000)

    return dt_obj.strftime("%d-%m-%Y")


# -------------------------------------------------------
# 3) MAIN LOGIC
# -------------------------------------------------------

def process_and_validate_excel(df, company_key, json_db):
    error_report = {
        "file_status": "Valid",
        "file_errors": [],
        "row_errors": {}
    }

    # Clean header names
    df.columns = [str(c).strip() for c in df.columns]
    existing_columns = df.columns.tolist()

    # --- A. CHECK MANDATORY HEADERS ---
    missing_cols = [col for col in MANDATORY_HEADERS if col not in existing_columns]
    if missing_cols:
        error_report["file_status"] = "Has Errors"
        error_report["file_errors"] = [f"Missing mandatory column header: '{col}'" for col in missing_cols]

    # Load company config
    try:
        valid_plans = set(json_db[company_key]["plan_names"])
        valid_templates = set(json_db[company_key]["vesting_templates"])
    except KeyError:
        return (
            {
                "file_status": "Fatal Error",
                "file_errors": ["Company Key not found in JSON DB"],
                "row_errors": {}
            },
            None
        )

    # --- B. ROW VALIDATION & CLEANING ---
    for index, row in df.iterrows():
        excel_row_num = index + 2
        row_issues = {}

        # 1. Employee Id
        if "Employee Id" in existing_columns:
            emp_id = str(row.get("Employee Id", "")).strip()
            if not emp_id or emp_id.lower() == "nan":
                row_issues["Employee Id"] = "Field is empty."

        # 2. Options Granted
        if "Options Granted" in existing_columns:
            opt_grant = row.get("Options Granted")
            if pd.isna(opt_grant) or str(opt_grant).strip() == "":
                row_issues["Options Granted"] = "Field is empty."
            else:
                try:
                    val = float(opt_grant)
                    if val <= 0:
                        row_issues["Options Granted"] = "Must be > 0."
                    elif not val.is_integer():
                        row_issues["Options Granted"] = "Must be a whole number."
                except Exception:
                    row_issues["Options Granted"] = "Invalid number."

        # 3. Plan Name
        if "Plan Name" in existing_columns:
            plan = str(row.get("Plan Name", "")).strip()
            if not plan or plan.lower() == "nan":
                row_issues["Plan Name"] = "Field is empty."
            elif plan not in valid_plans:
                row_issues["Plan Name"] = f"Invalid Plan. Allowed: {list(valid_plans)}"

        # 4. Date Of Grant
        if "Date Of Grant" in existing_columns:
            grant_date_raw = row.get("Date Of Grant")
            try:
                clean_date = clean_and_convert_date(grant_date_raw)
                df.at[index, "Date Of Grant"] = clean_date
            except ValueError as e:
                row_issues["Date Of Grant"] = str(e)

        # 5. Grant Price
        if "Grant Price" in existing_columns:
            grant_price = row.get("Grant Price")
            if pd.isna(grant_price) or str(grant_price).strip() == "":
                row_issues["Grant Price"] = "Field is empty."
            else:
                try:
                    float(grant_price)
                except Exception:
                    row_issues["Grant Price"] = "Must be a number."

        # 6. Vesting Template
        if "Vesting Template" in existing_columns:
            template = str(row.get("Vesting Template", "")).strip()
            if not template or template.lower() == "nan":
                row_issues["Vesting Template"] = "Field is empty."
            elif template not in valid_templates:
                row_issues["Vesting Template"] = "Invalid Template Name."

        # 7. Vesting Date Type
        if "Vesting Date Type" in existing_columns:
            v_type = str(row.get("Vesting Date Type", "")).strip()

            if not v_type or v_type.lower() == "nan":
                row_issues["Vesting Date Type"] = "Field is empty."
            elif v_type not in VESTING_DATE_TYPE_OPTS:
                row_issues["Vesting Date Type"] = f"Invalid Type. Must be {VESTING_DATE_TYPE_OPTS}"

            # If CustomDate, 'Vesting Date' is mandatory
            if v_type == "CustomDate":
                if "Vesting Date" not in existing_columns:
                    row_issues["Vesting Date"] = "Column missing but required for 'CustomDate'."
                else:
                    v_date_raw = row.get("Vesting Date")
                    try:
                        clean_v_date = clean_and_convert_date(v_date_raw)
                        df.at[index, "Vesting Date"] = clean_v_date
                    except ValueError:
                        row_issues["Vesting Date"] = "Invalid/Empty Date (Required for CustomDate)."

            elif "Vesting Date" in existing_columns:
                # If vesting date provided optionally, validate it
                v_date_raw = row.get("Vesting Date")
                if not pd.isna(v_date_raw) and str(v_date_raw).strip() != "":
                    try:
                        clean_v_date = clean_and_convert_date(v_date_raw)
                        df.at[index, "Vesting Date"] = clean_v_date
                    except ValueError:
                        row_issues["Vesting Date"] = "Invalid Date format provided."

        # 8. Actual Vesting Day
        if "Actual Vesting Day" in existing_columns:
            act_vest_day = str(row.get("Actual Vesting Day", "")).strip()
            if act_vest_day and act_vest_day.lower() != "nan":
                if act_vest_day not in ACTUAL_VESTING_DAY_OPTS:
                    row_issues["Actual Vesting Day"] = "Invalid Option"

        if row_issues:
            error_report["row_errors"][str(excel_row_num)] = row_issues
            error_report["file_status"] = "Has Errors"

    # --- C. FINAL DECISION ---
    if error_report["file_errors"] or error_report["row_errors"]:
        return error_report, None
    else:
        return None, df


# -------------------------------------------------------
# 4) EXECUTION (for local testing)
# -------------------------------------------------------
if __name__ == "__main__":
    file_path = "data_dummy.xlsx"
    df = pd.read_excel(file_path, engine="openpyxl")

    print("--- PROCESSING ---")
    report, cleaned_df = process_and_validate_excel(df, "Company_Y", json_db)

    if report:
        print("VALIDATION FAILED:")
        print(json.dumps(report, indent=4))
    else:
        print("SUCCESS! File cleaned.")

        base_name = os.path.splitext(file_path)[0]
        new_file_path = f"{base_name}_cleaned.xlsx"

        cleaned_df.to_excel(new_file_path, index=False)
        print(f"New file saved as: {new_file_path}")
