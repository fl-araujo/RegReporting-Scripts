import pandas as pd
import snowflake.connector
from datetime import datetime
from tabulate import tabulate

# ---------------- PARAMETERS ----------------
REFERENCE_DATE = "2025-07-31"  # Now in ISO format (YYYY-MM-DD)
SNAPSHOT_DATE = "2025-09-01"
EXCEL_FILE = "mec_validations/input_files/Jul-25 Manual subledger Accounting.xlsx"
EXCEL_SHEET = "Manual"
OUTPUT_FILE = "mec_validations/output_files/validation_output.txt"

# ---------------- HELPER FUNCTION ----------------
def clean_ids(series):
    return (
        series.dropna()
        .apply(lambda x: str(int(x)) if isinstance(x, (int, float)) and not pd.isna(x) else str(x))
        .str.strip()
        .unique()
        .tolist()
    )

# ---------------- HELPER FUNCTION FOR FORMATTING IDS ----------------
def format_id_pair(gl_account, sap_id):
    """Format G/L Account and SAP ID pair, avoiding scientific notation"""
    # Convert to string and handle potential float formatting
    gl_str = str(int(float(gl_account))) if pd.notna(gl_account) else ""
    sap_str = str(int(float(sap_id))) if pd.notna(sap_id) else ""
    return f"{gl_str}_{sap_str}"

# ---------------- DATE CONVERSION HELPER ----------------
def convert_iso_to_excel_format(iso_date):
    """Convert ISO date (YYYY-MM-DD) to Excel format (DD.MM.YYYY)"""
    dt = pd.to_datetime(iso_date, format="%Y-%m-%d")
    return dt.strftime("%d.%m.%Y")

# ---------------- LOAD EXCEL ----------------
df_excel = pd.read_excel(EXCEL_FILE, sheet_name=EXCEL_SHEET)

# Convert ISO reference date to Excel format for filtering
excel_reference_date = convert_iso_to_excel_format(REFERENCE_DATE)
df_excel_filtered = df_excel[df_excel["Reference_Date"] == excel_reference_date]

# Extract relevant columns
excel_accounts = clean_ids(df_excel_filtered["G/L_Account"])
excel_sap_ids = clean_ids(df_excel_filtered["SAP_ID_Counterparty"])
excel_cp_ids = clean_ids(df_excel_filtered["CP_ID"])

# ---------------- SNOWFLAKE CONNECTION ----------------
conn = snowflake.connector.connect(
    user='francisco.araujo@traderepublic.com',
    account='gm68377.eu-central-1',
    database='TEAMS_PRD',
    role='FINANCE',
    authenticator='externalbrowser'
)

# ---------------- QUERY SNOWFLAKE (GL Accounts) ----------------
query_gl = f"""
    SELECT DISTINCT GL_ACCOUNT
    FROM REGULATORY_REPORTING_SOURCE.SRC_SNAPSHOT__REGULATORY_REPORTING__REF_GL_ACCOUNT
    WHERE SNAPSHOT_DT = '{SNAPSHOT_DATE}'
"""
cur = conn.cursor()
cur.execute(query_gl)
snowflake_accounts = clean_ids(pd.Series([row[0] for row in cur.fetchall()]))
cur.close()

# ---------------- QUERY SNOWFLAKE (Counterparties) ----------------
query_cp = f"""
    SELECT DISTINCT SAP_ID, COUNTERPARTY_ID
    FROM REGULATORY_REPORTING_SOURCE.SRC_SNAPSHOT__REGULATORY_REPORTING__REF_COUNTERPARTY
    WHERE SNAPSHOT_DT = '{SNAPSHOT_DATE}'
"""
cur = conn.cursor()
cur.execute(query_cp)
rows = cur.fetchall()
cur.close()

snowflake_sap_ids = clean_ids(pd.Series([r[0] for r in rows if r[0] is not None]))
snowflake_cp_ids = clean_ids(pd.Series([r[1] for r in rows if r[1] is not None]))

# ---------------- VALIDATIONS ----------------
missing_accounts = [acc for acc in excel_accounts if acc not in snowflake_accounts]
all_present_accounts = len(missing_accounts) == 0

missing_sap = [sid for sid in excel_sap_ids if sid not in snowflake_sap_ids]
all_present_sap = len(missing_sap) == 0

missing_cp = [cid for cid in excel_cp_ids if cid not in snowflake_cp_ids]
all_present_cp = len(missing_cp) == 0

# ---------------- OUTPUT ----------------
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
header = [
    f"\nTrade Republic Bank GmbH {timestamp}",
    "Month End Closing Validations\n"
]

sf_rows = [
    ["Check 1", "G/L Account Consistency", excel_reference_date, SNAPSHOT_DATE, "✅ True" if all_present_accounts else "❌ False", len(missing_accounts)],
    ["Check 2", "SAP_ID Consistency", excel_reference_date, SNAPSHOT_DATE, "✅ True" if all_present_sap else "❌ False", len(missing_sap)],
    ["Check 3", "Counterparty_ID Consistency", excel_reference_date, SNAPSHOT_DATE, "✅ True" if all_present_cp else "❌ False", len(missing_cp)]
]

sf_table = tabulate(
    sf_rows,
    headers=["Check", "Description", "Reference_Date", "Snapshot_Date", "Result", "Missing_Count"],
    tablefmt="grid"
)

sf_details = []
if missing_accounts:
    sf_details.append("\nMissing G/L Accounts:")
    sf_details.extend([f"- {acc}" for acc in missing_accounts])
if missing_sap:
    sf_details.append("\nMissing SAP_IDs:")
    sf_details.extend([f"- {sid}" for sid in missing_sap])
if missing_cp:
    sf_details.append("\nMissing Counterparty_IDs:")
    sf_details.extend([f"- {cid}" for cid in missing_cp])

# ---------------- EXCEL-ONLY CHECKS ----------------
excel_only_header = ["\nExcel-Only Validations\n"]
excel_only_rows = []

# Check 1: Exposure Amount Consistency for EUR
df_eur = df_excel_filtered[df_excel_filtered["Original_CCY"] == "EUR"].copy()

mismatches = df_eur[df_eur["Exposure_Amt_Original_CCY"] != df_eur["Exposure_Amt_EUR"]].copy()
check1_result = "✅ True" if len(mismatches) == 0 else "❌ False"
excel_only_rows.append([
    "Check 1",
    "Exposure Amount Consistency for EUR",
    check1_result,
    len(mismatches)
])

# Check 2: Start_Date <= Reference_Date
# Parse Start_Date with European format (DD.MM.YYYY) and convert reference date to datetime
df_excel_filtered["Start_Date_Parsed"] = pd.to_datetime(df_excel_filtered["Start_Date"], format="%d.%m.%Y", errors='coerce')
ref_date_dt = pd.to_datetime(REFERENCE_DATE, format="%Y-%m-%d")

invalid_start_rows = df_excel_filtered[df_excel_filtered["Start_Date_Parsed"] > ref_date_dt].copy()
check2_result = "✅ True" if len(invalid_start_rows) == 0 else "❌ False"
excel_only_rows.append([
    "Check 2",
    "Start_Date must be <= Reference_Date",
    check2_result,
    len(invalid_start_rows)
])

# Check 3: End_Date > Reference_Date
# Parse End_Date with European format (DD.MM.YYYY)
df_excel_filtered["End_Date_Parsed"] = pd.to_datetime(df_excel_filtered["End_Date"], format="%d.%m.%Y", errors='coerce')

invalid_end_rows = df_excel_filtered[df_excel_filtered["End_Date_Parsed"] <= ref_date_dt].copy()
check3_result = "✅ True" if len(invalid_end_rows) == 0 else "❌ False"

excel_only_rows.append([
    "Check 3",
    "End_Date must be > Reference_Date",
    check3_result,
    len(invalid_end_rows)
])

# ---------------- Check 4: Maturity_Type and End_Date consistency ----------------
valid_maturity_types = ['Open-ended', 'Fixed end date', 'Overnight']
reporting_plus_one = ref_date_dt + pd.Timedelta(days=1)

# Filter rows that violate Maturity_Type or End_Date rule
invalid_maturity_rows = df_excel_filtered[
    (~df_excel_filtered["Maturity_Type"].isin(valid_maturity_types)) |
    (((df_excel_filtered["Maturity_Type"].isin(["Open-ended", "Overnight"])) &
      (df_excel_filtered["End_Date_Parsed"] != reporting_plus_one)))
].copy()

check4_result = "✅ True" if len(invalid_maturity_rows) == 0 else "❌ False"

excel_only_rows.append([
    "Check 4",
    "Maturity_Type must be valid; Open-ended/Overnight End_Date = day after Reference_Date",
    check4_result,
    len(invalid_maturity_rows)
])

# ---------------- Check 5: Encumbrance_Indicator validity ----------------
valid_encumbrance_values = ['unencumbered', 'encumbered']

invalid_encumbrance_rows = df_excel_filtered[
    ~df_excel_filtered["Encumbrance_Indicator"].isin(valid_encumbrance_values)
].copy()

check5_result = "✅ True" if len(invalid_encumbrance_rows) == 0 else "❌ False"

excel_only_rows.append([
    "Check 5",
    "Encumbrance_Indicator must be 'unencumbered' or 'encumbered'",
    check5_result,
    len(invalid_encumbrance_rows)
])

# ---------------- Check 6: Country_region must be populated if SAP_ID_Counterparty is empty ----------------
invalid_country_rows = df_excel_filtered[
    (df_excel_filtered["SAP_ID_Counterparty"].isna() | (df_excel_filtered["SAP_ID_Counterparty"] == "")) &
    (df_excel_filtered["Country_region"].isna() | (df_excel_filtered["Country_region"] == ""))
].copy()

check6_result = "✅ True" if len(invalid_country_rows) == 0 else "❌ False"

excel_only_rows.append([
    "Check 6",
    "Country_region must be populated if SAP_ID_Counterparty is empty",
    check6_result,
    len(invalid_country_rows)
])

excel_only_table = tabulate(
    excel_only_rows,
    headers=["Check", "Description", "Result", "Mismatch_Count"],
    tablefmt="grid"
)

excel_only_details = []

# Table for Exposure Amount mismatches
if not mismatches.empty:
    mismatched_table_rows = []
    for _, row in mismatches.iterrows():
        diff = row["Exposure_Amt_Original_CCY"] - row["Exposure_Amt_EUR"]
        mismatched_table_rows.append([
            format_id_pair(row["G/L_Account"], row["SAP_ID_Counterparty"]),
            row["Exposure_Amt_Original_CCY"],
            row["Exposure_Amt_EUR"],
            diff
        ])
    mismatched_table = tabulate(
        mismatched_table_rows,
        headers=["G/L_Account_SAP_ID", "Original_CCY", "EUR", "Diff"],
        tablefmt="grid"
    )
    excel_only_details.append("\nMismatched Records (Exposure Amounts):")
    excel_only_details.append(mismatched_table)

# Table for invalid Start_Date records
if not invalid_start_rows.empty:
    start_table_rows = []
    for _, row in invalid_start_rows.iterrows():
        start_table_rows.append([
            format_id_pair(row["G/L_Account"], row["SAP_ID_Counterparty"]),
            row["Start_Date_Parsed"].strftime("%d.%m.%Y"),
            ref_date_dt.strftime("%d.%m.%Y")
        ])
    invalid_start_table = tabulate(
        start_table_rows,
        headers=["G/L_Account_SAP_ID", "Start_Date", "Reference_Date"],
        tablefmt="grid"
    )
    excel_only_details.append("\nInvalid Start_Date Records:")
    excel_only_details.append(invalid_start_table)
    
# Table for invalid End_Date records
if not invalid_end_rows.empty:
    end_table_rows = []
    for _, row in invalid_end_rows.iterrows():
        end_table_rows.append([
            format_id_pair(row["G/L_Account"], row["SAP_ID_Counterparty"]),
            row["End_Date_Parsed"].strftime("%d.%m.%Y"),
            ref_date_dt.strftime("%d.%m.%Y")
        ])
    invalid_end_table = tabulate(
        end_table_rows,
        headers=["G/L_Account_SAP_ID", "End_Date", "Reference_Date"],
        tablefmt="grid"
    )
    excel_only_details.append("\nInvalid End_Date Records:")
    excel_only_details.append(invalid_end_table)
    
# Table for invalid Maturity_Type / End_Date records
if not invalid_maturity_rows.empty:
    maturity_table_rows = []
    for _, row in invalid_maturity_rows.iterrows():
        end_date_str = row["End_Date_Parsed"].strftime("%d.%m.%Y") if pd.notna(row["End_Date_Parsed"]) else ""
        maturity_table_rows.append([
            format_id_pair(row["G/L_Account"], row["SAP_ID_Counterparty"]),
            row["Maturity_Type"],
            end_date_str
        ])
    invalid_maturity_table = tabulate(
        maturity_table_rows,
        headers=["G/L_Account_SAP_ID", "Maturity_Type", "End_Date"],
        tablefmt="grid"
    )
    excel_only_details.append("\nInvalid Maturity_Type / End_Date Records:")
    excel_only_details.append(invalid_maturity_table)
    
# Table for invalid Encumbrance_Indicator records
if not invalid_encumbrance_rows.empty:
    encumbrance_table_rows = []
    for _, row in invalid_encumbrance_rows.iterrows():
        encumbrance_table_rows.append([
            format_id_pair(row["G/L_Account"], row["SAP_ID_Counterparty"]),
            row["Encumbrance_Indicator"]
        ])
    invalid_encumbrance_table = tabulate(
        encumbrance_table_rows,
        headers=["G/L_Account_SAP_ID", "Encumbrance_Indicator"],
        tablefmt="grid"
    )
    excel_only_details.append("\nInvalid Encumbrance_Indicator Records:")
    excel_only_details.append(invalid_encumbrance_table)
    
# Table for invalid Country_region records
if not invalid_country_rows.empty:
    country_table_rows = []
    for _, row in invalid_country_rows.iterrows():
        country_table_rows.append([
            format_id_pair(row["G/L_Account"], row["SAP_ID_Counterparty"]),
            row["Country_region"]
        ])
    invalid_country_table = tabulate(
        country_table_rows,
        headers=["G/L_Account_SAP_ID", "Country_region"],
        tablefmt="grid"
    )
    excel_only_details.append("\nInvalid Country_region Records:")
    excel_only_details.append(invalid_country_table)

# ---------------- FINAL OUTPUT ----------------
output_lines = header + [sf_table] + sf_details + excel_only_header + [excel_only_table] + excel_only_details

with open(OUTPUT_FILE, "w") as f:
    for line in output_lines:
        f.write(line + "\n")

for line in output_lines:
    print(line)

print(f"Validation complete. Results written to {OUTPUT_FILE}")
