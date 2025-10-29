import pandas as pd
import os
import snowflake.connector
from deep_translator import GoogleTranslator
import numpy as np 

# --- 1. CONFIGURATION AND PARAMETERS ---

# Execution Parameters
REPORT_DATE = '2025-07-31'
SNAPSHOT_DATE = '2025-08-24'

# File Paths
INPUT_EXCEL_FILE = '02.IT_Reporting/IT_A3/input_files/TotaleVoci[20250731A6].xlsx'
OUTPUT_EXCEL_FILE = '02.IT_Reporting/IT_A3/output_files/output_reconciliation_v5.xlsx'

# Column Mappings
JOIN_KEY_COL = 'A'
TRANSLATE_SRC_COL = 'B'
TRANSLATE_DEST_COL = 'DESCRIPTION'
SNOWFLAKE_VALUE_COL = 'VALUE TRB'

# New Variance Columns (G and H)
VAR_ABS_COL = 'VARIANCE (ABSOLUTE)'
VAR_PCT_COL = 'VARIANCE (PERCENTAGE)'

# Snowflake Connection Details
SNOWFLAKE_CONN_PARAMS = {
    'user': 'francisco.araujo@traderepublic.com',
    'account': 'gm68377.eu-central-1',
    'database': 'TEAMS_PRD',
    'role': 'FINANCE',
    'authenticator': 'externalbrowser',
}

# Snowflake Query (Uses f-string formatting for date parameters)
SNOWFLAKE_QUERY = f"""
WITH ValidationChecks AS (
    WITH params AS (
        SELECT
            '{REPORT_DATE}' AS report_dt,
            '{SNAPSHOT_DATE}' AS snapshot_dt
    )
    --Recon 41401.09
    SELECT '41401.09' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params)
    UNION ALL
    --Recon 41401.13
    SELECT '41401.13' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params)
    UNION ALL
    --Recon 41401.17
    SELECT '41401.17' AS validation_check,
    (
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params)
    )
    +
    (
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params)
    ) AS result
    UNION ALL
    --Recon 41410.03
    SELECT '41410.03' AS validation_check,
        SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params)
    UNION ALL
    --Recon 41410.09
    SELECT '41410.09' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND "REC-INIZIO-RESTO VALUE 5" = 06
    UNION ALL
    --Recon 41410.15
    SELECT '41410.15' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND "REC-INIZIO-RESTO VALUE 5" = 10
    UNION ALL
    --Recon 41419.10
    SELECT '41419.10' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 14", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params)
    UNION ALL
    --Recon 41419.22
    SELECT '41419.22' AS validation_check,
        COUNT(DISTINCT "REC-INIZIO-NDG-VALORE") AS result
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params)
)
SELECT *
FROM ValidationChecks;
"""

# --- 2. UTILITY FUNCTIONS ---

translator = GoogleTranslator(source='it', target='en')

def translate_cell(text):
    """Translates a single text cell, handling non-string/NaN values."""
    if not isinstance(text, str) or pd.isna(text) or not text.strip():
        return ''
    try:
        return translator.translate(text)
    except Exception:
        return text

def clean_key(series):
    """Standardizes join keys to string and removes common invisible characters."""
    return series.astype(str).str.strip().str.replace('\u00A0', '').str.replace('\u200B', '')

def apply_excel_formatting(df, writer, excel_translate_header, column_c_header, sheet_name='Sheet1'):
    """Applies formatting (text wrap, column width, number formats) to the output Excel file."""
    workbook  = writer.book
    worksheet = writer.sheets[sheet_name]
    
    # Define formats
    wrap_format = workbook.add_format({'text_wrap': True, 'align': 'top'})
    
    # Number format for currency/large numbers (explicitly using period for decimal)
    number_format = workbook.add_format({'num_format': '#,##0.00'}) 
    percentage_format = workbook.add_format({'num_format': '0.00%'}) 
    
    # Text format for codes (preserves the period/dot as a literal character)
    text_format = workbook.add_format({'num_format': '@'}) 
    
    # Identify the columns for specific formatting
    wrap_cols = [excel_translate_header, TRANSLATE_DEST_COL]
    number_cols = [column_c_header, SNOWFLAKE_VALUE_COL, VAR_ABS_COL]
    
    # Iterate over the columns and set width and format
    for i, col in enumerate(df.columns):
        # Calculate max length for sensible auto-fitting
        max_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
        
        if col in wrap_cols:
            # Description columns (B, E)
            width = min(max_len, 60) 
            worksheet.set_column(i, i, width, wrap_format) 
        elif col in number_cols:
            # Number/Currency columns (C, F, H)
            width = min(max_len, 25)
            worksheet.set_column(i, i, width, number_format)
        elif col == VAR_PCT_COL:
            # Percentage column (G)
            width = min(max_len, 20)
            worksheet.set_column(i, i, width, percentage_format)
        elif col == df.columns[0]: # Column A (VOCE.SV.)
            # Code column: Apply text format to ensure the period is preserved
            width = min(max_len, 20)
            worksheet.set_column(i, i, width, text_format)
        else:
            # All other columns (currently only D)
            width = min(max_len, 20)
            worksheet.set_column(i, i, width)
        
    # Apply wrap format to the header row (row 0)
    worksheet.set_row(0, None, wrap_format)


# --- 3. MAIN PROCESS ---

def run_reconciliation_script():
    """Executes the full reconciliation process."""
    conn = None
    print(f"Starting reconciliation script for {REPORT_DATE}...")

    try:
        # 3.1 Data Acquisition (Excel)
        if not os.path.exists(INPUT_EXCEL_FILE):
            raise FileNotFoundError(f"Input file not found: {INPUT_EXCEL_FILE}.")
            
        excel_data = pd.read_excel(INPUT_EXCEL_FILE, sheet_name=0, usecols='A:D')
        
        # Capture dynamic headers
        excel_join_header = excel_data.columns[0]
        excel_translate_header = excel_data.columns[1] 
        column_c_header = excel_data.columns[2] # Header of the "IMPORTO" column (C)
        
        # --- FIX: CONVERT JOIN KEY COLUMN TO STRING BEFORE ANY FURTHER PROCESSING ---
        # This prevents pandas from writing it as a number, which forces the period display.
        excel_data[excel_join_header] = excel_data[excel_join_header].astype(str)

        # 3.2 Translation (Column B -> Column E)
        print("Translating description column...")
        excel_data[TRANSLATE_DEST_COL] = excel_data[excel_translate_header].apply(translate_cell)
        
        # 3.3 Data Acquisition (Snowflake)
        print("Connecting to Snowflake and fetching validation data...")
        conn = snowflake.connector.connect(**SNOWFLAKE_CONN_PARAMS)
        snowflake_data = pd.read_sql(SNOWFLAKE_QUERY, conn)
        snowflake_data.columns = ['sf_join_key', SNOWFLAKE_VALUE_COL]

        # 3.4 Data Preparation and Merge
        print("Preparing data and merging based on codes...")
        
        # Create cleaned join columns for robust matching (Cleaning is still needed for safety)
        snowflake_data['sf_join_key_clean'] = clean_key(snowflake_data['sf_join_key'])
        excel_data['excel_join_key_clean'] = clean_key(excel_data[excel_join_header])

        # Perform left merge
        combined_data = excel_data.merge(
            snowflake_data,
            left_on='excel_join_key_clean',
            right_on='sf_join_key_clean',
            how='left'
        ).drop(columns=['sf_join_key_clean', 'excel_join_key_clean', 'sf_join_key'])

        # 3.5 Variance Calculation
        print("Calculating variance columns...")
        
        # Use pd.to_numeric(errors='coerce') for robust numeric conversion
        value_f = pd.to_numeric(combined_data[SNOWFLAKE_VALUE_COL], errors='coerce')
        value_c = pd.to_numeric(combined_data[column_c_header], errors='coerce')

        # Calculate Absolute Variance (F - C)
        combined_data[VAR_ABS_COL] = value_f - value_c
        
        # 1. Percentage Variance (G)
        combined_data[VAR_PCT_COL] = np.where(
            value_c != 0, 
            (combined_data[VAR_ABS_COL] / value_c), 
            np.nan
        )

        # --- Reorder Columns for final output ---
        base_cols = combined_data.columns.drop([VAR_PCT_COL, VAR_ABS_COL]).tolist()
        final_cols = base_cols + [VAR_PCT_COL, VAR_ABS_COL]
        combined_data = combined_data[final_cols]

        # 3.6 Output Generation
        output_dir = os.path.dirname(OUTPUT_EXCEL_FILE)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        print(f"Writing final output to {OUTPUT_EXCEL_FILE} and applying formatting...")
        
        writer = pd.ExcelWriter(OUTPUT_EXCEL_FILE, engine='xlsxwriter')
        sheet_name = 'Reconciliation'
        
        combined_data.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # Apply the formatting function
        apply_excel_formatting(combined_data, writer, excel_translate_header, column_c_header, sheet_name=sheet_name)
        
        writer.close()
        
        print("\n✅ Script completed successfully.")

    except Exception as e:
        print(f"\n❌ A critical error occurred: {e}")

    finally:
        if conn:
            conn.close()
            print("Snowflake connection closed.")

if __name__ == "__main__":
    run_reconciliation_script()
