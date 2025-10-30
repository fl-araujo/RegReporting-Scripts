import snowflake.connector
from datetime import datetime
import csv
import os

# === PARAMETERS ===
SNAPSHOT_DT = '2025-07-04'
REPORT_DT = '2025-06-30'
OUTPUT_FILE = "italian_reporting/output_files/a6_validation_report.txt"
OUTPUT_PATH = "italian_reporting/output_files"  

# === CONNECT TO SNOWFLAKE ===
conn = snowflake.connector.connect(
    user='francisco.araujo@traderepublic.com',
    account='gm68377.eu-central-1',
    database='TEAMS_PRD',
    role='FINANCE',
    authenticator='externalbrowser',
)

cursor = conn.cursor()

# === CHECKS FOR FTO 0162504 ===
checks_0162504 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),

    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
        SELECT 
            CASE 
                WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
                THEN 'TRUE' ELSE 'FALSE' 
            END AS all_length_16
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Counterparties reported as consumer household", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 600 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_600
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Valid province codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 3" IN (
                    166,104,26,12,141,135,103,151,40,206,119,150,111,223,24,116,112,159,
                    48,167,38,149,169,44,155,109,162,114,222,102,168,205,130,28,157,132,
                    148,14,124,143,105,156,36,107,147,160,229,139,203,137,134,115,136,
                    161,165,16,129,204,34,101,173,174,121,46,127,113,30,133,154,126,140,
                    138,125,42,215,170,131,163,128,146,242,32,122,152,172,106,142,171,
                    110,179,158,153,144,10,164,18,120,22,123,108,20,224,100,117,428,118,
                    145,998
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_province_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),   
    ("8. Customers country code is equal to 86 (IT)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 4" = 86 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_600
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. NDG-VALORE = REC-INIZIO-RESTO VALUE 5", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 5" = "REC-INIZIO-NDG-VALORE" THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_ndg_matches
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Portfolio management type is equal to 0", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 7" = 0 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_portfolio_mgmt_zero
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Field 00120 is equal to 3", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 8" = 3 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_00120_equal_3
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. Field 00125 is equal to 3", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 9" = 0 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_00125_equal_0
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("13. REC-INIZIO-RESTO VALUE 11 in (0,1,2)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 11" IN (0,1,2) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_0_1_2
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("14. Nominal value = Fair value", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 12" = "REC-INIZIO-RESTO VALUE 14" THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS nominal_equals_fair_value
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("15. Nominal CCY = Fair CCY", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 13" = "REC-INIZIO-RESTO VALUE 15" THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS nominal_ccy_equals_fair_ccy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("16. Total fair value EUR", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 14", 1, 15) AS NUMERIC)) / 100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("17. Total fair value CCY", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 15", 1, 15) AS NUMERIC)) / 100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("18. Type of consultancy is equal to 7", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 16" = 7 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_type_of_consultancy_7
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("19. Option exercise date is equal to 0", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 17" = 0 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_option_exercise_date_zero
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("20. Depositary code is equal to 83", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 18" = 83 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_depositary_83
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("21. Unique key is null", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN TRIM("REC-INIZIO-RESTO VALUE 19") IS NULL OR TRIM("REC-INIZIO-RESTO VALUE 19") = '' THEN 1 
                ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_unique_key_blank
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("22. MIFID category is equal to 500", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 20" = 500 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_mifid_category_500
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("23. PMI CRR3 is equal to 0", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 21" = 0 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_pmi_crr3_zero
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("24. Early withdrawal % is equal to 0", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 22" = 0 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_early_withdrawal_zero
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("25. Creditor withdrawal % is equal to 0", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 23" = 0 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_creditor_withdrawal_zero
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("26. Customers are not SMEs", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 24" = 0 THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_customers_not_smes
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("27. Number of securities", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 25", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
]

# === CHECKS FOR FTO 4140151 ===
checks_4140151 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT \"REC-INIZIO-RAPP-VALORE\") AS sec_accounts
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
        SELECT CASE WHEN MIN(LENGTH(\"REC-INIZIO-NDG-VALORE\")) = 16 AND MAX(LENGTH(\"REC-INIZIO-NDG-VALORE\")) = 16 THEN 'TRUE' ELSE 'FALSE' END
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
        SELECT CASE WHEN MIN(LENGTH(\"REC-INIZIO-RAPP-VALORE\")) = 16 AND MAX(LENGTH(\"REC-INIZIO-RAPP-VALORE\")) = 16 THEN 'TRUE' ELSE 'FALSE' END
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. All ordinary customers (00028 = 0)", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS has_records
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 2" = 0;
    """),
    ("7. NDG = 00030", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 3" = "REC-INIZIO-NDG-VALORE"
    """),
   ("8. Portfolio management type is equal to 0", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS has_records
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 5" = 0
    """),
    ("9. Asset management type is equal to 0", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS has_records
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 7" = 0
    """),
    ("10. Quotation indicator is null", f"""
    SELECT CASE 
        WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 8" IS NULL OR TRIM("REC-INIZIO-RESTO VALUE 8") = '' THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE'
    END AS all_blank_or_null
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Trading markets breakdown", f"""
        SELECT TRIM(\"REC-INIZIO-RESTO VALUE 9\"), COUNT(*)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
        GROUP BY TRIM(\"REC-INIZIO-RESTO VALUE 9\")
        ORDER BY TRIM(\"REC-INIZIO-RESTO VALUE 9\")
    """),
    ("12. Number of orders", f"""
        SELECT SUM(CAST(SUBSTRING(\"REC-INIZIO-RESTO VALUE 10\", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("13. Total trade value", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING(\"REC-INIZIO-RESTO VALUE 11\", 1, 15) AS NUMERIC)) / 100, 2)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("14. Type of delegator is equal to 884", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 13" = 884
    """),
   ("15. Distribution channel is equal to 297", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 14" = 297
    """),
    ("16. Type of consultancy is equal to 7", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 15" = 7
    """),
    ("17. MIFID categorization is equal to 500", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 16" = 500
    """),
]

# === CHECKS FOR FTO 4140153 ===
checks_4140153 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT \"REC-INIZIO-RAPP-VALORE\")
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
        SELECT CASE WHEN MIN(LENGTH(\"REC-INIZIO-NDG-VALORE\")) = 16 AND MAX(LENGTH(\"REC-INIZIO-NDG-VALORE\")) = 16 THEN 'TRUE' ELSE 'FALSE' END
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
        SELECT CASE WHEN MIN(LENGTH(\"REC-INIZIO-RAPP-VALORE\")) = 16 AND MAX(LENGTH(\"REC-INIZIO-RAPP-VALORE\")) = 16 THEN 'TRUE' ELSE 'FALSE' END
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. All ordinary customers (00028 = 0)", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 2" = 0
    """),
    ("7. NDG is equal to 00030", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 3" = "REC-INIZIO-NDG-VALORE"
    """),
    ("8. Portfolio management type is equal to 0", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 5" = 0
    """),
    ("9. Asset management type is equal to 0", f"""
    SELECT CASE WHEN COUNT(*) > 0 THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    AND "REC-INIZIO-RESTO VALUE 7" = 0
    """),
    ("10. Quotation indicator is null", f"""
    SELECT CASE 
        WHEN COUNT(*) = 0 THEN FALSE
        WHEN COUNT(*) = (SELECT COUNT(*)
                        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
                        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}') 
        THEN 'TRUE'
        ELSE 'FALSE'
    END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE ( "REC-INIZIO-RESTO VALUE 8" IS NULL OR TRIM("REC-INIZIO-RESTO VALUE 8") = '')
      AND snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Trading markets breakdown", f"""
        SELECT TRIM(\"REC-INIZIO-RESTO VALUE 9\"), COUNT(*)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
        GROUP BY TRIM(\"REC-INIZIO-RESTO VALUE 9\")
        ORDER BY TRIM(\"REC-INIZIO-RESTO VALUE 9\")
    """),
    ("12. Number of orders", f"""
        SELECT SUM(CAST(SUBSTRING(\"REC-INIZIO-RESTO VALUE 10\", 1, 15) AS NUMERIC))
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("13. Value of trades", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING(\"REC-INIZIO-RESTO VALUE 11\", 1, 15) AS NUMERIC)) / 100, 2)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("14. Type of delegator is equal to 884", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 13" = 884 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("15. Distribution channel is equal to 297", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 14" = 297 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("16. Consultancy type is equal to 7", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 15" = 7 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("17. MIFID categorization is equal to 500", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 16" = 500 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
]

# === CHECKS FOR FTO 4141002 ===
checks_4141002 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT \"REC-INIZIO-RAPP-VALORE\")
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT CASE 
        WHEN COUNT(*) > 0 
            AND COUNT(*) = COUNT(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 END) 
        THEN 'TRUE' 
        ELSE 'FALSE' 
    END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RESTO VALUE 2 = Intermediated transactions (03)", f"""
    SELECT COUNT(*)
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
        AND LEFT(TRIM("REC-INIZIO-RESTO VALUE 2"), 2) = '03'
    """),
    ("5. Number of orders", f"""
        SELECT SUM(CAST(SUBSTRING(\"REC-INIZIO-RESTO VALUE 3\", 1, 15) AS NUMERIC)) AS total_sum
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Type of consultancy is equal to 7", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 4" = 7 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. MIFID categorization is equal to 500", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 5" = 500 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
]

# === CHECKS FOR FTO 4141051 ===
checks_4141051 = [
    ("1. Number of rows", f"""
        SELECT COUNT (*)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT \"REC-INIZIO-RAPP-VALORE\") AS cash_accounts
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. Only Ordinary Customers", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 0 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. NDG is equal to 00030", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = "REC-INIZIO-NDG-VALORE" THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. NDG is 16 chars", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Number of purchases", f"""
        SELECT COUNT (*)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
        AND \"REC-INIZIO-RESTO VALUE 5\" = 06
    """),
    ("7. Number of sales", f"""
        SELECT COUNT (*)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
        AND \"REC-INIZIO-RESTO VALUE 5\" = 10
    """),
    ("8. Trading markets 018, 120 or 226", f"""
        SELECT TRIM("REC-INIZIO-RESTO VALUE 6") AS trading_market, COUNT(*)
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
        GROUP BY TRIM("REC-INIZIO-RESTO VALUE 6")
        ORDER BY TRIM("REC-INIZIO-RESTO VALUE 6")
    """),
    ("9. Total trade value", f"""
    SELECT TRIM("REC-INIZIO-RESTO VALUE 6") AS group_value,
       TO_CHAR(
         SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) / 100,
         'FM9999999999999990.00'
       ) AS total_sum
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    GROUP BY TRIM("REC-INIZIO-RESTO VALUE 6")
    ORDER BY TRIM("REC-INIZIO-RESTO VALUE 6");
    """),
    ("10. Distribution channel is 297 for all records", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 8" = 297 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Consultancy type is 7 for all records", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 9" = 7 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. MIFID categorization is 500 for all records", f"""
    SELECT CASE
        WHEN COUNT(*) > 0
             AND COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 10" = 500 THEN 1 ELSE 0 END)
        THEN 'TRUE' ELSE 'FALSE' END AS indicator
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
]

# === Tables info for download (table name and output CSV filename) ===
tables_to_download = {
    "0162504": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504",
    "4140151": "teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151",
    "4140153": "teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153",
    "4141002": "teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002",
    "4141051": "teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051",
}

# === HELPER FUNCTION TO FORMAT A TABLE ===
def format_table(rows):
    col_widths = [max(len(str(row[i])) for row in rows) for i in range(2)]
    border = "+" + "+".join(["-" * (col_widths[i] + 2) for i in range(2)]) + "+"
    lines = [border]
    lines.append("| " + " | ".join(f"{rows[0][i]:<{col_widths[i]}}" for i in range(2)) + " |")
    lines.append(border)
    for row in rows[1:]:
        lines.append("| " + " | ".join(f"{row[i]:<{col_widths[i]}}" for i in range(2)) + " |")
    lines.append(border)
    return lines

# === RUN AND COLLECT CHECKS ===
def run_checks(label, checks):
    rows = [("Check", "Result")]
    validation_results = []  # Store TRUE/FALSE results for summary
    for check_label, query in checks:
        cursor.execute(query)
        result = cursor.fetchall()
        if len(result[0]) > 1:
            value = ", ".join(str(x) for x in result)
        else:
            value = str(result[0][0])
        rows.append((check_label, value))
        
        # Check if this is a TRUE/FALSE validation
        if value in ['TRUE', 'FALSE']:
            validation_results.append(value == 'TRUE')
    
    return (label, rows, validation_results)

# === DOWNLOAD FULL TABLE FILTERED BY SNAPSHOT_DT AND REPORT_DT ===
def download_table(table_name, output_csv_path):
    query = f"""
        SELECT *
        FROM {table_name}
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    with open(output_csv_path, "w", newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)

# === CREATE SUMMARY TABLE ===
def create_summary_table(results_list):
    summary_rows = [("Check", "Result")]
    for label, rows, validation_results in results_list:
        # Check if all TRUE/FALSE validations passed
        if validation_results:  # Only if there are TRUE/FALSE validations
            all_passed = all(validation_results)
            status = "✅" if all_passed else "❌"
        else:
            status = "N/A"  # No TRUE/FALSE validations found
        summary_rows.append((label, status))
    return summary_rows
        
# === Run all checks ===

results_0162504 = run_checks("FTO 0162504", checks_0162504)
results_4140151 = run_checks("FTO 4140151", checks_4140151)
results_4140153 = run_checks("FTO 4140153", checks_4140153)
results_4141002 = run_checks("FTO 4141002", checks_4141002)
results_4141051 = run_checks("FTO 4141051", checks_4141051)

# Create summary table
all_results = [results_0162504, results_4140151, results_4140153, results_4141002, results_4141051]
summary_table = create_summary_table(all_results)

# === SAVE TO FILE ===
with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
    f.write(f"Validation Report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    
    # Write summary table first
    f.write("VALIDATION SUMMARY\n")
    for line in format_table(summary_table):
        f.write(line + "\n")
    f.write("\n")
    
    # Write detailed results
    for section_title, rows, _ in all_results:
        f.write(f"{section_title}\n")
        for line in format_table(rows):
            f.write(line + "\n")
        f.write("\n")
        
# === Download full tables as CSVs ===
os.makedirs(OUTPUT_PATH, exist_ok=True)
for suffix, table in tables_to_download.items():
    csv_filename = os.path.join(OUTPUT_PATH, f"{table.split('.')[-1]}_{SNAPSHOT_DT}_{REPORT_DT}.csv")
    print(f"Downloading {table} to {csv_filename}...")
    download_table(table, csv_filename)

# === PRINT TO TERMINAL ===
print(f"\nTrade Republic Bank GmbH – {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Print summary table first
print(f"\nVALIDATION SUMMARY")
for line in format_table(summary_table):
    print(line)

# Print detailed results
for section_title, rows, _ in all_results:
    print(f"\n{section_title}")
    for line in format_table(rows):
        print(line)

print(f"\n✅ Validation report saved to: {OUTPUT_FILE}")
print(f"✅ Tables downloaded to: {os.path.abspath(OUTPUT_PATH)}")

# === CLEANUP ===
cursor.close()
conn.close()
