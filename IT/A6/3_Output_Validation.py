import pandas as pd
import os
import snowflake.connector
from deep_translator import GoogleTranslator
import numpy as np 

# --- 1. CONFIGURATION AND PARAMETERS ---

# Execution Parameters
REPORT_DATE = '2025-07-31'
SNAPSHOT_DATE = '2025-08-24'
THRESHOLD_VALUE = 0.50 

# File Paths
INPUT_EXCEL_FILE = '02.IT_Reporting/IT_A6/input_files/TotaleVoci[20250731A6].xlsx'
INPUT_ERRORS_FILE = '02.IT_Reporting/IT_A6/input_files/036749_20250731_20251002-161912_AcquisizioneErrori.xlsx' 
OUTPUT_EXCEL_FILE = f'02.IT_Reporting/IT_A6/output_files/A6 - Recon - {REPORT_DATE} - v10.xlsx' 

# Column Mappings
JOIN_KEY_COL = 'A'
TRANSLATE_SRC_COL = 'B'
TRANSLATE_DEST_COL = 'DESCRIPTION'
SNOWFLAKE_VALUE_COL = 'VALUE TRB' 

# Filtered Columns (Source Data)
SNOWFLAKE_FILTERED_VALUE_COL = '1. NON-EXISTENT ISINs (00032E)'
SNOWFLAKE_BELOW_THRESHOLD_COL = '2. TRANSACTIONS < 0.50€' 

# New Variance Columns 
VAR_ABS_COL = 'VARIANCE'
VAR_PCT_COL = 'VARIANCE %'
VAR_ABS_FILTERED_COL = 'VARIANCE (FILTERED ABSOLUTE)'
VAR_PCT_FILTERED_COL = 'VARIANCE (FILTERED PERCENTAGE)'

# Error File Constants
ERROR_CODE_COL = 'COD. ERRORE'
ERROR_VALUE_COL = 'VALORE ATTRIBUTO'
ERROR_FTO_COL = 'FTO'
TARGET_ERROR_CODE = '00032E3'
TARGET_FTO_4140151 = '4140151' 
TARGET_FTO_4140153 = '4140153' 

# Snowflake Connection Details
SNOWFLAKE_CONN_PARAMS = {
    'user': 'francisco.araujo@traderepublic.com',
    'account': 'gm68377.eu-central-1',
    'database': 'TEAMS_PRD',
    'role': 'FINANCE',
    'authenticator': 'externalbrowser',
}

# Original Snowflake Query (for VALUE TRB - Column F)
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
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL 
    UNION ALL
    --Recon 41401.13
    SELECT '41401.13' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
    UNION ALL
    --Recon 41401.17
    SELECT '41401.17' AS validation_check,
    (
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
    )
    +
    (
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
    ) AS result
    UNION ALL
    --Recon 41410.03
    SELECT '41410.03' AS validation_check,
        SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
    UNION ALL
    --Recon 41410.09
    SELECT '41410.09' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND "REC-INIZIO-RESTO VALUE 5" = 06 AND OUTPUT IS NOT NULL
    UNION ALL
    --Recon 41410.15
    SELECT '41410.15' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND "REC-INIZIO-RESTO VALUE 5" = 10 AND OUTPUT IS NOT NULL
    UNION ALL
    --Recon 41419.10
    SELECT '41419.10' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 14", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
    UNION ALL
    --Recon 41419.22
    SELECT '41419.22' AS validation_check,
        COUNT(DISTINCT "REC-INIZIO-NDG-VALORE") AS result
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
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

def execute_snowflake_query(query, conn_params):
    """Handles Snowflake connection, query execution, and result return."""
    conn = None
    try:
        conn = snowflake.connector.connect(**conn_params)
        df = pd.read_sql(query, conn)
        return df
    finally:
        if conn:
            conn.close()

def get_excluded_isin_details(conn_params):
    """
    Reads the errors file to get the list of excluded ISINs, and then queries 
    Snowflake to get the detailed breakdown (ISIN, FTO, and Aggregated Value) for the
    'Excluded Instruments' sheet.
    
    Returns:
        tuple: (DataFrame for filtered Snowflake values, DataFrame of ISINs/FTO/Aggregated Value)
    """
    if not os.path.exists(INPUT_ERRORS_FILE):
        raise FileNotFoundError(f"Errors input file not found: {INPUT_ERRORS_FILE}.")
    
    print(f"Reading and filtering errors from {INPUT_ERRORS_FILE}...")
    
    # Read errors and get the unique list of ISINs (VALORE ATTRIBUTO)
    error_data = pd.read_excel(INPUT_ERRORS_FILE, 
                               sheet_name=0, 
                               usecols=[ERROR_CODE_COL, ERROR_VALUE_COL])
    
    error_data.columns = [ERROR_CODE_COL, ERROR_VALUE_COL]

    # Filter ONLY for the target error code and clean values
    filtered_errors = error_data[error_data[ERROR_CODE_COL] == TARGET_ERROR_CODE].copy()
    filtered_errors[ERROR_VALUE_COL] = filtered_errors[ERROR_VALUE_COL].astype(str).str.strip()
    
    # Get the complete unique list of VALORE ATTRIBUTO (ISINs)
    valore_attributo_list = filtered_errors[ERROR_VALUE_COL].unique().tolist()
    
    if not valore_attributo_list:
        print(f"Warning: No '{ERROR_VALUE_COL}' found for error '{TARGET_ERROR_CODE}'. Filtered column will be zero/NULL.")
        id_list_str = "''" 
    else:
        id_list_str = ", ".join([f"'{i}'" for i in valore_attributo_list])
        print(f"Found {len(valore_attributo_list)} unique excluded ISINs.")
    
    # --- 2. Build and execute the detailed query for the new sheet ---
    
    # MODIFICATION: The subqueries are placed within a CTE (DetailedBase), 
    # and a final SELECT aggregates the Amount by ISIN and FTO.
    DETAILED_ISIN_QUERY = f"""
    WITH params AS (
        SELECT
            '{REPORT_DATE}' AS report_dt,
            '{SNAPSHOT_DATE}' AS snapshot_dt
    ),
    DetailedBase AS (
        (
            -- FTO 4140151 (Recon 41401.09 / 41401.17 - Value 11)
            SELECT 
                TRIM("REC-INIZIO-RESTO VALUE 4") AS "ISINs",
                '4140151' AS "FTO",
                CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC) / 100 AS "Amount"
            FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
              AND report_dt = (SELECT report_dt FROM params) 
              AND OUTPUT IS NOT NULL 
              AND TRIM("REC-INIZIO-RESTO VALUE 4") IN ({id_list_str})
            
            UNION ALL
            
            -- FTO 4140151 (For Value 10 part of 41401.17)
            SELECT 
                TRIM("REC-INIZIO-RESTO VALUE 4") AS "ISINs",
                '4140151 (Value 10)' AS "FTO",
                CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC) / 100 AS "Amount"
            FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
              AND report_dt = (SELECT report_dt FROM params) 
              AND OUTPUT IS NOT NULL 
              AND TRIM("REC-INIZIO-RESTO VALUE 4") IN ({id_list_str})
        )
        UNION ALL
        (
            -- FTO 4140153 (Recon 41401.13 / 41401.17 - Value 11)
            SELECT 
                TRIM("REC-INIZIO-RESTO VALUE 4") AS "ISINs",
                '4140153' AS "FTO",
                CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC) / 100 AS "Amount"
            FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
              AND report_dt = (SELECT report_dt FROM params) 
              AND OUTPUT IS NOT NULL 
              AND TRIM("REC-INIZIO-RESTO VALUE 4") IN ({id_list_str})
              
            UNION ALL
            
            -- FTO 4140153 (For Value 10 part of 41401.17)
            SELECT 
                TRIM("REC-INIZIO-RESTO VALUE 4") AS "ISINs",
                '4140153 (Value 10)' AS "FTO",
                CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC) / 100 AS "Amount"
            FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
              AND report_dt = (SELECT report_dt FROM params) 
              AND OUTPUT IS NOT NULL 
              AND TRIM("REC-INIZIO-RESTO VALUE 4") IN ({id_list_str})
        )
        UNION ALL
        (
            -- FTO 4141002 (Recon 41410.03) - Note: Filter field is VALUE 1
            SELECT 
                TRIM("REC-INIZIO-RESTO VALUE 1") AS "ISINs",
                '4141002' AS "FTO",
                CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC) AS "Amount" -- No / 100 here
            FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
              AND report_dt = (SELECT report_dt FROM params) 
              AND OUTPUT IS NOT NULL
              AND TRIM("REC-INIZIO-RESTO VALUE 1") IN ({id_list_str})
        )
        UNION ALL
        (
            -- FTO 4141051 (Recon 41410.09 / 41410.15) - Note: Filter field is VALUE 3
            SELECT 
                TRIM("REC-INIZIO-RESTO VALUE 3") AS "ISINs",
                '4141051' AS "FTO",
                CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC) / 100 AS "Amount"
            FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
              AND report_dt = (SELECT report_dt FROM params) 
              AND OUTPUT IS NOT NULL
              AND TRIM("REC-INIZIO-RESTO VALUE 3") IN ({id_list_str})
        )
        UNION ALL
        (
            -- FTO 0162504 (Recon 41419.10 / 41419.22) - Note: Filter field is VALUE 6
            SELECT 
                TRIM("REC-INIZIO-RESTO VALUE 6") AS "ISINs",
                '0162504' AS "FTO",
                CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 14", 1, 15) AS NUMERIC) / 100 AS "Amount"
            FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
              AND report_dt = (SELECT report_dt FROM params) 
              AND OUTPUT IS NOT NULL
              AND TRIM("REC-INIZIO-RESTO VALUE 6") IN ({id_list_str})
        )
    )
    SELECT 
        "ISINs",
        "FTO",
        SUM("Amount") AS "Amount"
    FROM DetailedBase
    GROUP BY 1, 2
    ORDER BY 1, 2;
    """
    
    # Execute the detailed query
    print("Executing detailed Snowflake query for **aggregated** excluded ISINs breakdown (ISIN, FTO, Amount)...")
    detailed_isin_data = execute_snowflake_query(DETAILED_ISIN_QUERY, conn_params)
    
    # --- 3. Build and execute the summary query for the main sheet (this remains mostly the same) ---
    
    # Dynamic SQL for the main reconciliation sheet filter column:
    FILTERED_SNOWFLAKE_QUERY = f"""
    WITH params AS (
        SELECT
            '{REPORT_DATE}' AS report_dt,
            '{SNAPSHOT_DATE}' AS snapshot_dt
    ),
    BaseChecks AS (
        -- Filtered 41401.09 result
        SELECT '41401.09' AS validation_check,
            CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
          AND report_dt = (SELECT report_dt FROM params) 
          AND OUTPUT IS NOT NULL 
          AND TRIM("REC-INIZIO-RESTO VALUE 4") IN ({id_list_str}) 
        UNION ALL
        -- Filtered 41401.13 result
        SELECT '41401.13' AS validation_check,
            CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
          AND report_dt = (SELECT report_dt FROM params) 
          AND OUTPUT IS NOT NULL 
          AND TRIM("REC-INIZIO-RESTO VALUE 4") IN ({id_list_str}) 
        UNION ALL
        -- Filtered 41401.17 result (summing two filtered subqueries)
        SELECT '41401.17' AS validation_check,
        (
            SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC))
            FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
            AND TRIM("REC-INIZIO-RESTO VALUE 4") IN ({id_list_str})
        )
        +
        (
            SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC))
            FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
            AND TRIM("REC-INIZIO-RESTO VALUE 4") IN ({id_list_str})
        ) AS result
        UNION ALL
        -- Filtered 41410.03 result
        SELECT '41410.03' AS validation_check,
            SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
        AND TRIM("REC-INIZIO-RESTO VALUE 1") IN ({id_list_str})
        UNION ALL
        -- Filtered 41410.09 result
        SELECT '41410.09' AS validation_check,
            CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND "REC-INIZIO-RESTO VALUE 5" = 06 AND OUTPUT IS NOT NULL
        AND TRIM("REC-INIZIO-RESTO VALUE 3") IN ({id_list_str})
        UNION ALL
        -- Filtered 41410.15 result
        SELECT '41410.15' AS validation_check,
            CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND "REC-INIZIO-RESTO VALUE 5" = 10 AND OUTPUT IS NOT NULL
        AND TRIM("REC-INIZIO-RESTO VALUE 3") IN ({id_list_str})
        UNION ALL
        -- Filtered 41419.10 result
        SELECT '41419.10' AS validation_check,
            CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 14", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
        AND TRIM("REC-INIZIO-RESTO VALUE 6") IN ({id_list_str})
        UNION ALL
        -- Filtered 41419.22 result (Filter applied to restrict the count)
        SELECT '41419.22' AS validation_check,
            COUNT(DISTINCT "REC-INIZIO-NDG-VALORE") AS result
            FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
            WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
            AND TRIM("REC-INIZIO-RESTO VALUE 6") IN ({id_list_str})
    )
    SELECT validation_check, MAX(result)
    FROM BaseChecks
    GROUP BY validation_check;
    """
    
    # Execute the summary query
    filtered_snowflake_data = execute_snowflake_query(FILTERED_SNOWFLAKE_QUERY, conn_params)
    filtered_snowflake_data.columns = ['sf_join_key', SNOWFLAKE_FILTERED_VALUE_COL]

    return filtered_snowflake_data, detailed_isin_data 

def get_snowflake_below_threshold_values(conn_params):
    """
    Constructs a Snowflake query to aggregate values only where the individual 
    value is less than the THRESHOLD_VALUE (0.50), by using a WHERE clause filter 
    to match the expected aggregation behavior.
    """
    print(f"Executing Snowflake query to aggregate values below {THRESHOLD_VALUE} (using WHERE clause filter)...")

    # Dynamic SQL to filter for values < THRESHOLD_VALUE (0.50)
    BELOW_THRESHOLD_SNOWFLAKE_QUERY = f"""
    WITH params AS (
        SELECT
            '{REPORT_DATE}' AS report_dt,
            '{SNAPSHOT_DATE}' AS snapshot_dt,
            {THRESHOLD_VALUE} AS threshold
    )
    --Recon 41401.09
    SELECT '41401.09' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
      AND report_dt = (SELECT report_dt FROM params) 
      AND OUTPUT IS NOT NULL
      AND CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC) / 100 < (SELECT threshold FROM params)
    UNION ALL
    --Recon 41401.13
    SELECT '41401.13' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
      AND report_dt = (SELECT report_dt FROM params) 
      AND OUTPUT IS NOT NULL
      AND CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 11", 1, 15) AS NUMERIC) / 100 < (SELECT threshold FROM params)
    UNION ALL
    --Recon 41401.17 (Summing VALUE 10 for records where VALUE 10 is below threshold)
    SELECT '41401.17' AS validation_check,
    (
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
        AND CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC) / 100 < (SELECT threshold FROM params)
    )
    +
    (
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = (SELECT snapshot_dt FROM params) AND report_dt = (SELECT report_dt FROM params) AND OUTPUT IS NOT NULL
        AND CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC) / 100 < (SELECT threshold FROM params)
    ) AS result
    UNION ALL
    --Recon 41410.03 (Value 3 - no division by 100 in original)
    SELECT '41410.03' AS validation_check,
        SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
      AND report_dt = (SELECT report_dt FROM params) 
      AND OUTPUT IS NOT NULL
      AND CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC) < (SELECT threshold FROM params)
    UNION ALL
    --Recon 41410.09
    SELECT '41410.09' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
      AND report_dt = (SELECT report_dt FROM params) 
      AND "REC-INIZIO-RESTO VALUE 5" = 06 
      AND OUTPUT IS NOT NULL
      AND CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC) / 100 < (SELECT threshold FROM params)
    UNION ALL
    --Recon 41410.15
    SELECT '41410.15' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
      AND report_dt = (SELECT report_dt FROM params) 
      AND "REC-INIZIO-RESTO VALUE 5" = 10 
      AND OUTPUT IS NOT NULL
      AND CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC) / 100 < (SELECT threshold FROM params)
    UNION ALL
    --Recon 41419.10
    SELECT '41419.10' AS validation_check,
        CAST(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 14", 1, 15) AS NUMERIC)) / 100 AS NUMERIC(38, 2)) AS result
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = (SELECT snapshot_dt FROM params) 
      AND report_dt = (SELECT report_dt FROM params) 
      AND OUTPUT IS NOT NULL
      AND CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 14", 1, 15) AS NUMERIC) / 100 < (SELECT threshold FROM params)
    UNION ALL
    --Recon 41419.22 (Count, not a value, returning 0 as per previous logic)
    SELECT '41419.22' AS validation_check,
        0 AS result;
    """
    
    # Execute the new query
    below_threshold_snowflake_data = execute_snowflake_query(BELOW_THRESHOLD_SNOWFLAKE_QUERY, conn_params)
    below_threshold_snowflake_data.columns = ['sf_join_key', SNOWFLAKE_BELOW_THRESHOLD_COL]

    return below_threshold_snowflake_data


def apply_excel_formatting(df, writer, excel_translate_header, column_c_header, sheet_name='Sheet1'):
    """Applies formatting (text wrap, column width, number formats) to the output Excel file."""
    workbook  = writer.book
    worksheet = writer.sheets[sheet_name]
    
    # Define formats
    wrap_format = workbook.add_format({'text_wrap': True, 'align': 'top'})
    number_format = workbook.add_format({'num_format': '#,##0.00'}) 
    percentage_format = workbook.add_format({'num_format': '0.00%'}) 
    text_format = workbook.add_format({'num_format': '@'}) 
    
    # Identify the columns for specific formatting
    wrap_cols = [excel_translate_header, TRANSLATE_DEST_COL]
    number_cols = [column_c_header, SNOWFLAKE_VALUE_COL, SNOWFLAKE_FILTERED_VALUE_COL, SNOWFLAKE_BELOW_THRESHOLD_COL, VAR_ABS_COL] 
    percentage_cols = [VAR_PCT_COL]
    
    # Iterate over the columns and set width and format
    for i, col in enumerate(df.columns):
        # Increased max_len slightly for better visibility
        max_len = max(df[col].astype(str).str.len().max(), len(col)) + 4 
        
        if col in wrap_cols:
            width = min(max_len, 60) 
            worksheet.set_column(i, i, width, wrap_format) 
        elif col in number_cols:
            width = min(max_len, 25)
            worksheet.set_column(i, i, width, number_format)
        elif col in percentage_cols:
            width = min(max_len, 20)
            worksheet.set_column(i, i, width, percentage_format)
        elif col == df.columns[0]: # Column A (VOCE.SV.)
            width = min(max_len, 20)
            worksheet.set_column(i, i, width, text_format)
        else:
            width = min(max_len, 20)
            worksheet.set_column(i, i, width)
        
    # Apply wrap format to the header row (row 0)
    worksheet.set_row(0, None, wrap_format)
    
def write_excluded_instruments_sheet(isin_df, writer, sheet_name='Excluded Instruments'):
    """Creates a new sheet with the detailed DataFrame of excluded ISINs, FTO, and Amount."""
    workbook = writer.book
    
    # Write to Excel
    isin_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # Define formats
    number_format = workbook.add_format({'num_format': '#,##0.00'}) 
    text_format = workbook.add_format({'num_format': '@'}) 
    
    # Apply formatting
    worksheet = writer.sheets[sheet_name]
    
    # Set column widths
    worksheet.set_column('A:A', 25, text_format) # ISINs
    worksheet.set_column('B:B', 20, text_format) # FTO
    worksheet.set_column('C:C', 18, number_format) # Amount


# --- 3. MAIN PROCESS ---

def run_reconciliation_script():
    """Executes the full reconciliation process."""
    print(f"Starting reconciliation script for {REPORT_DATE}...")

    # Define the column name for the clean key locally
    clean_key_col_name = 'join_key_clean'
    # Initialize DataFrame to hold excluded ISIN details
    detailed_excluded_isins_df = pd.DataFrame() 

    try:
        # 3.1 Data Acquisition (Excel)
        if not os.path.exists(INPUT_EXCEL_FILE):
            raise FileNotFoundError(f"Input file not found: {INPUT_EXCEL_FILE}.")
            
        # FIX: Use dtype={0: str} to explicitly read the first column (A) as a string.
        excel_data = pd.read_excel(INPUT_EXCEL_FILE, sheet_name=0, usecols='A:D', dtype={0: str})
        
        # Capture dynamic headers
        excel_join_header = excel_data.columns[0]
        excel_translate_header = excel_data.columns[1] 
        column_c_header = excel_data.columns[2] # Header of the "IMPORTO" column (C)
        
        # 3.2 Translation (Column B -> Column E)
        print("Translating description column...")
        excel_data[TRANSLATE_DEST_COL] = excel_data[excel_translate_header].apply(translate_cell)
        
        # 3.3 Data Acquisition (Original Snowflake) - Column F
        print("Connecting to Snowflake and fetching original validation data...")
        snowflake_data_orig = execute_snowflake_query(SNOWFLAKE_QUERY, SNOWFLAKE_CONN_PARAMS)
        snowflake_data_orig.columns = ['sf_join_key', SNOWFLAKE_VALUE_COL]

        # 3.4 Data Acquisition (Below Threshold Snowflake) 
        print(f"Fetching Snowflake data for values below {THRESHOLD_VALUE}...")
        snowflake_data_threshold = get_snowflake_below_threshold_values(SNOWFLAKE_CONN_PARAMS)
        
        # 3.5 Data Acquisition (Filtered Snowflake) - MODIFIED to capture detailed ISIN data
        print("Processing errors file and fetching filtered/detailed Snowflake data...")
        snowflake_data_filtered, detailed_excluded_isins_df = get_excluded_isin_details(SNOWFLAKE_CONN_PARAMS)
        
        # 3.6 Data Preparation and Merge
        print("Preparing data and merging based on codes...")
        
        # 1. Prepare and Clean Keys
        excel_data[clean_key_col_name] = clean_key(excel_data[excel_join_header])
        
        snowflake_data_orig[clean_key_col_name] = clean_key(snowflake_data_orig['sf_join_key'])
        snowflake_data_threshold[clean_key_col_name] = clean_key(snowflake_data_threshold['sf_join_key'])
        snowflake_data_filtered[clean_key_col_name] = clean_key(snowflake_data_filtered['sf_join_key'])
        
        # Select only the clean key and the value columns
        sf_orig = snowflake_data_orig[[clean_key_col_name, SNOWFLAKE_VALUE_COL]]
        sf_thresh = snowflake_data_threshold[[clean_key_col_name, SNOWFLAKE_BELOW_THRESHOLD_COL]] 
        sf_filt = snowflake_data_filtered[[clean_key_col_name, SNOWFLAKE_FILTERED_VALUE_COL]]

        # 2. Merge all Snowflake data
        combined_data = excel_data.merge(sf_orig, on=clean_key_col_name, how='left')
        combined_data = combined_data.merge(sf_thresh, on=clean_key_col_name, how='left')
        combined_data = combined_data.merge(sf_filt, on=clean_key_col_name, how='left')
        
        # 3.7 Variance and Difference Calculation
        print("Calculating variance columns...")
        
        # Values from Column C (IMPORTO)
        value_c = pd.to_numeric(combined_data[column_c_header], errors='coerce')
        
        # Values from Column F (Original Snowflake)
        value_f = pd.to_numeric(combined_data[SNOWFLAKE_VALUE_COL], errors='coerce')
        
        # --- Original Variance (VAR_ABS_COL and VAR_PCT_COL) ---
        combined_data[VAR_ABS_COL] = value_f - value_c
        combined_data[VAR_PCT_COL] = np.where(
            value_c != 0, 
            (combined_data[VAR_ABS_COL] / value_c), 
            np.nan
        )
        
        # --- Filtered Variance (Calculated but will be deleted from final output) ---
        combined_data[VAR_ABS_FILTERED_COL] = (pd.to_numeric(combined_data[SNOWFLAKE_FILTERED_VALUE_COL], errors='coerce').fillna(0) + pd.to_numeric(combined_data[SNOWFLAKE_BELOW_THRESHOLD_COL], errors='coerce').fillna(0)) - value_c
        combined_data[VAR_PCT_FILTERED_COL] = np.where(
            value_c != 0, 
            (combined_data[VAR_ABS_FILTERED_COL] / value_c), 
            np.nan
        )

        # 5. Drop the temporary key column AND the two unwanted variance columns
        combined_data = combined_data.drop(columns=[
            clean_key_col_name,
            VAR_ABS_FILTERED_COL, 
            VAR_PCT_FILTERED_COL  
        ])

        # --- Reorder Columns for final output ---
        excel_info_cols = excel_data.columns.drop(clean_key_col_name).tolist()
        final_data_cols = [
            SNOWFLAKE_VALUE_COL, 
            VAR_ABS_COL, 
            VAR_PCT_COL,
            SNOWFLAKE_FILTERED_VALUE_COL, 
            SNOWFLAKE_BELOW_THRESHOLD_COL
        ]

        final_cols = excel_info_cols + final_data_cols
        combined_data = combined_data[final_cols]
        
        # 3.8 Output Generation
        output_dir = os.path.dirname(OUTPUT_EXCEL_FILE)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        print(f"Writing final output to {OUTPUT_EXCEL_FILE} and applying formatting...")
        
        writer = pd.ExcelWriter(OUTPUT_EXCEL_FILE, engine='xlsxwriter')
        
        # Sheet 1: Reconciliation Data
        sheet_name_recon = 'Reconciliation'
        combined_data.to_excel(writer, sheet_name=sheet_name_recon, index=False)
        apply_excel_formatting(combined_data, writer, excel_translate_header, column_c_header, sheet_name=sheet_name_recon)
        
        # Sheet 2: Excluded Instruments 
        sheet_name_excluded = 'Excluded Instruments'
        print(f"Writing detailed **aggregated** excluded ISINs/FTO/Value to sheet '{sheet_name_excluded}'...")
        write_excluded_instruments_sheet(detailed_excluded_isins_df, writer, sheet_name=sheet_name_excluded)
        
        writer.close()
        
        print("\n✅ Script completed successfully.")

    except Exception as e:
        print(f"\n❌ A critical error occurred: {e}")

if __name__ == "__main__":
    try:
        run_reconciliation_script()
    except ImportError as e:
        print(f"\n⚠️ Missing required library: {e}. Please ensure you have pandas, snowflake-connector-python, numpy, deep-translator, and xlsxwriter installed (pip install ...).")
