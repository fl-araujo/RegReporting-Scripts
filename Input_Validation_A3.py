import snowflake.connector
from datetime import datetime
import csv
import os

# === PARAMETERS ===
SNAPSHOT_DT = '2025-07-22'
REPORT_DT = '2025-06-30'
OUTPUT_FILE = "italian_reporting/output_files/a3_validation_report.txt"
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

# === CHECKS FOR FTO 5855001 ===
checks_5855001 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. Currency reported as EUR", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Counterparties reported as consumer household", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 3" = 600 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_600
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. REC-INIZIO-RESTO VALUE 4 (00015) = REC-INIZIO-RESTO VALUE 7 (00598)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 4" = "REC-INIZIO-RESTO VALUE 7" THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_matches
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 9", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 10", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Initiation mode reported as single initiation", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 11" = 41 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_41
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Non-SCA is equal to 777", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 13" = 777 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_non_sca_777
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. PISP is equal to 396", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 14" = 396 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_pisp_396
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("13. SCA is equal to 1", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 16" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_sca_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("14. Type of mobile transactions in (1,3)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 17" IN (1, 3) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_mobile_transaction_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("15. Online transfer reason equal to 2", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 18" = 2 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_online_reason_2
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("16. Valid province codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 4" IN (
                    00166,00104,00026,00012,00141,00135,00103,00151,00040,00206,00119,00150,00111,00223,
                    00024,00116,00112,00159,00048,00167,00038,00149,00169,00044,00155,00109,00162,00114,
                    00222,00102,00168,00205,00130,00028,00157,00132,00148,00014,00124,00143,00105,00156,
                    00036,00107,00147,00160,00229,00139,00203,00137,00134,00115,00136,00161,00165,00016,
                    00129,00204,00034,00101,00173,00174,00121,00046,00127,00113,00030,00133,00154,00126,
                    00140,00138,00125,00042,00215,00170,00131,00163,00128,00146,00242,00032,00122,00152,
                    00172,00106,00142,00171,00110,00179,00158,00153,00144,00010,00164,00018,00120,00022,
                    00123,00108,00020,00224,00100,00117,00428,00118,00145,00998
            ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_province_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("17. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 5" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("18. Mode of arrangement is equal to 4", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" = 4 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_field_mode_of_arrangement_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("19. Customers account balances in (66,67,89)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 8" IN (66,67,89) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_66_67_89
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("20. Scheme in (396,397,398,399)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 12" in (396,397,398,399) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_396_397_398_399
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5855001 ===
checks_5856501 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. Currency reported as EUR", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Counterparties reported as consumer household", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 3" = 600 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_600
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 4" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Valid province codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 5" IN (
                    00166,00104,00026,00012,00141,00135,00103,00151,00040,00206,00119,00150,00111,00223,
                    00024,00116,00112,00159,00048,00167,00038,00149,00169,00044,00155,00109,00162,00114,
                    00222,00102,00168,00205,00130,00028,00157,00132,00148,00014,00124,00143,00105,00156,
                    00036,00107,00147,00160,00229,00139,00203,00137,00134,00115,00136,00161,00165,00016,
                    00129,00204,00034,00101,00173,00174,00121,00046,00127,00113,00030,00133,00154,00126,
                    00140,00138,00125,00042,00215,00170,00131,00163,00128,00146,00242,00032,00122,00152,
                    00172,00106,00142,00171,00110,00179,00158,00153,00144,00010,00164,00018,00120,00022,
                    00123,00108,00020,00224,00100,00117,00428,00118,00145,00998
            ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_province_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Customers account balances in (66,67,89)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" IN (66,67,89) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_66_67_89
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 8", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. Initiation mode reported as single initiation", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 9" = 68 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_68
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5876002 ===
checks_5876002 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876002
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876002
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876002
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. Value of commissions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 1", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876002
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5878006 ===
checks_5878006 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5878006
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5878006
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5878006
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. Value of commissions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 1", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5878006
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5877504 ===
checks_5877504 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5877504
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5877504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5877504
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. Value of commissions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 1", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5877504
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5862591 ===
checks_5862591 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Counterparties reported as consumer household", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 600 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_600
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 3" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Place of operation in (4,5,7,8)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 4" IN (4,5,7,8) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_4_5_7_8
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Withdrawal/payment/deposit method in (1,2,9)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 5" IN (1,2,9) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_1_2_9
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Account type is equal to 1", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_account_type_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Valid province codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 7" IN (
                    00166,00104,00026,00012,00141,00135,00103,00151,00040,00206,00119,00150,00111,00223,
                    00024,00116,00112,00159,00048,00167,00038,00149,00169,00044,00155,00109,00162,00114,
                    00222,00102,00168,00205,00130,00028,00157,00132,00148,00014,00124,00143,00105,00156,
                    00036,00107,00147,00160,00229,00139,00203,00137,00134,00115,00136,00161,00165,00016,
                    00129,00204,00034,00101,00173,00174,00121,00046,00127,00113,00030,00133,00154,00126,
                    00140,00138,00125,00042,00215,00170,00131,00163,00128,00146,00242,00032,00122,00152,
                    00172,00106,00142,00171,00110,00179,00158,00153,00144,00010,00164,00018,00120,00022,
                    00123,00108,00020,00224,00100,00117,00428,00118,00145,00998
            ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_province_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 8", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("13. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 9", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("14. Initialization type is equal to 145", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 10" = 145 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_initialization_type_145
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("15. PCS is equal to 377", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 11" = 377 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_pcs_type_377
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("16. REC-INIZIO-RESTO VALUE 12 within valid country values", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 12" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,
                015,016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,
                038,039,040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,
                061,062,063,064,065,066,067,068,069,070,071,072,073,074,075,076,077,078,079,080,081,082,
                083,084,085,086,087,088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,
                109,110,112,113,114,115,116,117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,
                133,134,135,136,137,138,141,142,144,145,146,147,149,150,151,152,153,155,156,157,158,159,
                160,161,162,163,167,168,169,175,176,182,185,186,187,188,189,190,191,196,198,200,203,204,
                206,207,211,212,213,214,218,220,225,226,235,236,237,247,248,249,251,253,254,257,258,259,
                260,261,262,263,264,265,266,267,268,269,270,271,272,274,275,276,277,278,279,288,289,290,
                291,238,197
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5862593 ===
checks_5862593 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Counterparties reported as consumer household", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 600 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_600
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 3" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Initiation mode equal to remote or non-remote", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 4" IN (1,2) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_initiation_modes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Place of operation in (4,5,7,8)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 5" IN (4,5,7,8) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_4_5_7_8
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Withdrawal/payment/deposit method in (1,2,9)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" IN (1,2,9) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_1_2_9
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 8", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("13. Contactless Function (02119) reported in (Yes, No)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 9" IN (1,2) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_contactless_function_in_1_2
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("14. Merchant Category Code is not null", f"""
    SELECT CASE 
        WHEN COUNT(*) = 0 THEN 'FALSE'
        WHEN COUNT(*) = (SELECT COUNT(*)
                         FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
                         WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}') 
        THEN 'TRUE'
        ELSE 'FALSE'
    END AS indicator
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE "REC-INIZIO-RESTO VALUE 10" IS NOT NULL 
      AND TRIM("REC-INIZIO-RESTO VALUE 10") <> ''
      AND snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("15. Initialization type is reported as 145", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 11" = 145 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_initialization_type_145
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("16. PCS is equal to 377", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 12" = 377 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_pcs_type_377
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("17. Non-SCA operations show valid reason", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 13" IN (410,411,412,413,414,415,416,417,418,419,420,777
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_non_sca_operations_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}' AND "REC-INIZIO-RESTO VALUE 16" = 2
    """),
    ("18. REC-INIZIO-RESTO VALUE 14 within valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 14" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,
                015,016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,
                038,039,040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,
                061,062,063,064,065,066,067,068,069,070,071,072,073,074,075,076,077,078,079,080,081,082,
                083,084,085,086,087,088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,
                109,110,112,113,114,115,116,117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,
                133,134,135,136,137,138,141,142,144,145,146,147,149,150,151,152,153,155,156,157,158,159,
                160,161,162,163,167,168,169,175,176,182,185,186,187,188,189,190,191,196,198,200,203,204,
                206,207,211,212,213,214,218,220,225,226,235,236,237,247,248,249,251,253,254,257,258,259,
                260,261,262,263,264,265,266,267,268,269,270,271,272,274,275,276,277,278,279,288,289,290,
                291,238,197
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("19. Type of technology in (039,034,035,038)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 15" IN (039,034,035,038) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_technology_types_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("20. SCA in (0,1,2)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 16" IN (0,1,2) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_0_1_2
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("21. Mobile operation type in (0,1,2,3)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 17" IN (0,1,2,3) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_0_1_2_3
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5875315 ===
checks_5875315 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. REC-INIZIO-RESTO VALUE 2 within valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 2" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,
                015,016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,
                038,039,040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,
                061,062,063,064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,
                084,085,086,087,088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,
                112,113,114,115,116,117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,
                135,136,137,138,141,142,144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,
                162,163,167,168,169,175,176,182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,
                214,218,220,225,226,235,236,237,247,248,251,253,254,257,258,259,260,261,262,263,264,265,
                266,267,268,269,271,274,275,276,277,278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 4", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Initialization type is reported as 145", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 5" = 145 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_initialization_type_5
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 6" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5875320 ===
checks_5875320 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. REC-INIZIO-RESTO VALUE 2 within valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 2" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,
                015,016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,
                038,039,040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,
                061,062,063,064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,
                084,085,086,087,088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,
                112,113,114,115,116,117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,
                135,136,137,138,141,142,144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,
                162,163,167,168,169,175,176,182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,
                214,218,220,225,226,235,236,237,247,248,251,253,254,257,258,259,260,261,262,263,264,265,
                266,267,268,269,271,274,275,276,277,278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 4", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Initialization type is reported as 145", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 5" = 145 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_initialization_type_5
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 6" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5875911 ===
checks_5875911 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. REC-INIZIO-RESTO VALUE 2 within valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 2" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,
                015,016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,
                038,039,040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,
                061,062,063,064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,
                084,085,086,087,088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,
                112,113,114,115,116,117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,
                135,136,137,138,141,142,144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,
                162,163,167,168,169,175,176,182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,
                214,218,220,225,226,235,236,237,247,248,251,253,254,257,258,259,260,261,262,263,264,265,
                266,267,268,269,271,274,275,276,277,278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 4", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. REC-INIZIO-RESTO VALUE 6 within valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 6" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,
                015,016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,
                038,039,040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,
                061,062,063,064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,
                084,085,086,087,088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,
                112,113,114,115,116,117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,
                135,136,137,138,141,142,144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,
                162,163,167,168,169,175,176,182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,
                214,218,220,225,226,235,236,237,247,248,251,253,254,257,258,259,260,261,262,263,264,265,
                266,267,268,269,271,274,275,276,277,278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Type of technology in (039,034,035,038)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" IN (039,034,035,038) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_technology_types_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]   

# === CHECKS FOR FTO 5876311 ===
checks_5876311 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Currency reported as EUR", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_currency_eur
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Field within valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 3" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,
                015,016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,
                038,039,040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,
                061,062,063,064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,
                084,085,086,087,088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,
                112,113,114,115,116,117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,
                135,136,137,138,141,142,144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,
                162,163,167,168,169,175,176,182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,
                214,218,220,225,226,235,236,237,247,248,251,253,254,257,258,259,260,261,262,263,264,265,
                266,267,268,269,271,274,275,276,277,278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 4", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 5", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Fraud reasons (01006) in (0655,0657,0656,0658,0659)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" IN (0655,0657,0656,0658,0659) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_technology_types_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. PCS is equal to 377", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 7" = 377 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_pcs_type_377
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. Field REC-INIZIO-RESTO VALUE 8 within valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 8" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,
                015,016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,
                038,039,040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,
                061,062,063,064,065,066,067,068,069,070,071,072,073,074,075,076,077,078,079,080,081,082,
                083,084,085,086,087,088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,
                109,110,112,113,114,115,116,117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,
                133,134,135,136,137,138,141,142,144,145,146,147,149,150,151,152,153,155,156,157,158,159,
                160,161,162,163,167,168,169,175,176,182,185,186,187,188,189,190,191,196,198,200,203,204,
                206,207,211,212,213,214,218,220,225,226,235,236,237,247,248,249,251,253,254,257,258,259,
                260,261,262,263,264,265,266,267,268,269,270,271,272,274,275,276,277,278,279,288,289,290,
                291,238,197
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]  

# === CHECKS FOR FTO 5874751 ===
checks_5874751 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Currency reported as EUR", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_currency_eur
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Valid province codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 3" IN (00166,00104,00026,00012,00141,00135,00103,00151,00040,
                00206,00119,00150,00111,00223,00024,00116,00112,00159,00048,00167,00038,00149,00169,00044,
                00155,00109,00162,00114,00222,00102,00168,00205,00130,00028,00157,00132,00148,00014,00124,
                00143,00105,00156,00036,00107,00147,00160,00229,00139,00203,00137,00134,00115,00136,00161,
                00165,00016,00129,00204,00034,00101,00173,00174,00121,00046,00127,00113,00030,00133,00154,
                00126,00140,00138,00125,00042,00215,00170,00131,00163,00128,00146,00242,00032,00122,00152,
                00172,00106,00142,00171,00110,00179,00158,00153,00144,00010,00164,00018,00120,00022,00123,
                00108,00020,00224,00100,00117,00428,00118,00145,00998
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 4" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. REC-INIZIO-RESTO VALUE 5 = REC-INIZIO-RESTO VALUE 3", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 5" = "REC-INIZIO-RESTO VALUE 3" THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_matches
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 6", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. Initiation mode reported as single initiation", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 8" = 41 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_41
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("13. 005587  reported as '1' (online accounts)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 9" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_online_accounts_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]    

# === CHECKS FOR FTO 5874817 ===
checks_5874817 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Currency reported as EUR", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_currency_eur
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Valid province codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 3" IN (00166,00104,00026,00012,00141,00135,00103,00151,00040,
                00206,00119,00150,00111,00223,00024,00116,00112,00159,00048,00167,00038,00149,00169,00044,
                00155,00109,00162,00114,00222,00102,00168,00205,00130,00028,00157,00132,00148,00014,00124,
                00143,00105,00156,00036,00107,00147,00160,00229,00139,00203,00137,00134,00115,00136,00161,
                00165,00016,00129,00204,00034,00101,00173,00174,00121,00046,00127,00113,00030,00133,00154,
                00126,00140,00138,00125,00042,00215,00170,00131,00163,00128,00146,00242,00032,00122,00152,
                00172,00106,00142,00171,00110,00179,00158,00153,00144,00010,00164,00018,00120,00022,00123,
                00108,00020,00224,00100,00117,00428,00118,00145,00998
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 4" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. REC-INIZIO-RESTO VALUE 5 = REC-INIZIO-RESTO VALUE 3", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 5" = "REC-INIZIO-RESTO VALUE 3" THEN 1 ELSE 0 
            END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_matches
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 6", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),  
    ("12. 05587  reported as '1' (online accounts)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 8" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_online_accounts_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),   
    ]

# === CHECKS FOR FTO 5874921 ===
checks_5874921 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 2" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 4", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. REC-INIZIO-RESTO VALUE 5 in (650,651,652)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 5" IN (650,651,652) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_types_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. REC-INIZIO-RESTO VALUE 6 in (396,397,398,399)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" IN (396,397,398,399) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_types_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. REC-INIZIO-RESTO VALUE 7 equal to 396)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 7" = 396 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_types_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5876321 ===
checks_5876321 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Currency reported as EUR", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_currency_eur
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 3" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 4", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 5", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. REC-INIZIO-RESTO VALUE 6 in (0655,0657,0656,0658,0659)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" IN (0655,0657,0656,0658,0659) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_types_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. PCS is equal to 377", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 7" = 377 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_pcs_type_377
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. Non-SCA operations show valid reason", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 8" IN (410,411,412,413,414,415,416,417,418,419,420
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_non_sca_operations_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}' 
    """),
    ("13. Valid Device Location", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 9" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_device_location_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5876317 ===
checks_5876317 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Currency reported as EUR", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_currency_eur
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 3" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 4", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 5", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. REC-INIZIO-RESTO VALUE 6 in (0655,0657,0656,0658,0659)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" IN (0655,0657,0656,0658,0659) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_types_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. PCS is equal to 377", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 7" = 377 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_pcs_type_377
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. Valid Device Location", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 8" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_device_location_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 585550 ===
checks_5855501 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. Currency reported as EUR", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_currency_eur
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Counterparties reported as consumer household", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 3" = 600 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_counterparties_600
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Valid country codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 4" IN (002,003,004,005,006,007,008,009,010,011,012,013,014,015,
                016,017,018,019,020,021,022,023,024,025,026,027,028,029,031,032,033,034,035,036,037,038,039,
                040,041,042,043,044,045,046,047,048,049,050,051,052,053,054,055,056,057,058,059,061,062,063,
                064,065,066,067,068,069,070,071,072,074,075,076,077,078,079,080,081,082,083,084,085,086,087,
                088,089,090,091,092,093,094,095,097,098,101,102,103,104,105,106,107,109,112,113,114,115,116,
                117,118,119,120,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,141,142,
                144,145,146,147,149,150,151,152,153,155,156,157,158,159,160,161,162,163,167,168,169,175,176,
                182,185,186,187,188,189,190,191,198,200,203,204,206,207,213,214,218,220,225,226,235,236,237,
                247,248,251,253,254,257,258,259,260,261,262,263,264,265,266,267,268,269,271,274,275,276,277,
                278,288,289,290,291
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_country_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Valid province codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 5" IN (00166,00104,00026,00012,00141,00135,00103,00151,00040,
                00206,00119,00150,00111,00223,00024,00116,00112,00159,00048,00167,00038,00149,00169,00044,
                00155,00109,00162,00114,00222,00102,00168,00205,00130,00028,00157,00132,00148,00014,00124,
                00143,00105,00156,00036,00107,00147,00160,00229,00139,00203,00137,00134,00115,00136,00161,
                00165,00016,00129,00204,00034,00101,00173,00174,00121,00046,00127,00113,00030,00133,00154,
                00126,00140,00138,00125,00042,00215,00170,00131,00163,00128,00146,00242,00032,00122,00152,
                00172,00106,00142,00171,00110,00179,00158,00153,00144,00010,00164,00018,00120,00022,00123,
                00108,00020,00224,00100,00117,00428,00118,00145,00998
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Customers account balances in (66,67,89)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" IN (66,67,89) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_values_in_66_67_89
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 7", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 8", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5862001 ===
checks_5862001 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Valid province codes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE 
                WHEN "REC-INIZIO-RESTO VALUE 2" IN (00166,00104,00026,00012,00141,00135,00103,00151,00040,
                00206,00119,00150,00111,00223,00024,00116,00112,00159,00048,00167,00038,00149,00169,00044,
                00155,00109,00162,00114,00222,00102,00168,00205,00130,00028,00157,00132,00148,00014,00124,
                00143,00105,00156,00036,00107,00147,00160,00229,00139,00203,00137,00134,00115,00136,00161,
                00165,00016,00129,00204,00034,00101,00173,00174,00121,00046,00127,00113,00030,00133,00154,
                00126,00140,00138,00125,00042,00215,00170,00131,00163,00128,00146,00242,00032,00122,00152,
                00172,00106,00142,00171,00110,00179,00158,00153,00144,00010,00164,00018,00120,00022,00123,
                00108,00020,00224,00100,00117,00428,00118,00145,00998
                ) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE'
        END AS all_codes_valid
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Card type is equal to 1", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 3" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_card_type_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Number of cards", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 4", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Chip technology type is equal to 65", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 5" = 65 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_card_type_1
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Contactless Function set to Yes", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 6" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_contactless_yes
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("11. Digitalization in (302,303,304)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 7" IN (302,303,304) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_digitalization_in_302_303_304
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("12. PCS is equal to 377", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 8" = 377 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_pcs_type_377
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("13. Circuit type - national and international", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 9" = 2 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_circuit_type_national_international
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("14. Cards use type - ATMs and POS", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 10" = 2 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_card_use_type_atms_pos
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("15. Cards with IBAN", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 11" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_card_use_type_atms_pos
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5875110 ===
checks_5875110 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875110
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875110
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875110
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875110
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875110
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Responsible in (40,41,42)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" IN (40,41,42) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_responsible_in_40_41_42
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875110
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Amount of losses", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875110
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5875116 ===
checks_5875116 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875116
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875116
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875116
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875116
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875116
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Responsible in (40,41,42)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" IN (40,41,42) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_responsible_in_40_41_42
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875116
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Amount of losses", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 3", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875116
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === CHECKS FOR FTO 5874501 ===
checks_5874501 = [
    ("1. Number of rows", f"""
        SELECT COUNT(*) 
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("2. Number of distinct customers", f"""
        SELECT COUNT(DISTINCT "REC-INIZIO-RAPP-VALORE")
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("3. REC-INIZIO-NDG-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-NDG-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("4. REC-INIZIO-RAPP-VALORE length equal to 16", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN LENGTH("REC-INIZIO-RAPP-VALORE") = 16 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_length_16
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("5. Currency reported as EUR", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 1" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_currency_eur
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("6. Customers reside in Italy", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 2" = 1 THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
        END AS all_customers_italy
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("7. Debit and Credit Operations (1,2)", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 3" IN (1,2) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_debit_credit_operations
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("8. Transaction type - book entry or other", f"""
    SELECT 
        CASE 
            WHEN COUNT(*) = SUM(CASE WHEN "REC-INIZIO-RESTO VALUE 4" IN (1,0) THEN 1 ELSE 0 END)
            THEN 'TRUE' ELSE 'FALSE' 
            END AS all_transaction_type_book_entry_other
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
    WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("9. Number of transactions", f"""
        SELECT SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 5", 1, 15) AS NUMERIC)) AS total_count
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ("10. Value of transactions", f"""
        SELECT ROUND(SUM(CAST(SUBSTRING("REC-INIZIO-RESTO VALUE 6", 1, 15) AS NUMERIC))/100, 2) AS total_sum
        FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501
        WHERE snapshot_dt = '{SNAPSHOT_DT}' AND report_dt = '{REPORT_DT}'
    """),
    ]

# === Tables info for download (table name and output CSV filename) ===
tables_to_download = {
    "5855001": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855001",
    "5856501": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5856501",
    "5876002": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876002",
    "5878006": "teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5878006",
    "5877504": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5877504",
    "5862591": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862591",
    "5862593": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862593",
    "5875315": "teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875315",
    "5875320": "teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875320",
    "5875911": "teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875911",
    "5876311": "teams_prd.RMT_SENSITIVE_MART.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876311",
    "5874751": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874751",
    "5874817": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874817",
    "5874921": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874921",
    "5876321": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876321",
    "5876317": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5876317",
    "5855501": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5855501",
    "5862001": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5862001",
    "5875110": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875110",
    "5875116": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5875116",
    "5874501": "teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a3_fto_5874501",
}

# === HELPER FUNCTION TO FORMAT A TABLE ===
def format_table(rows):
    if not rows:
        return []
    
    # Calculate column widths properly
    num_cols = len(rows[0])
    col_widths = []
    for i in range(num_cols):
        col_widths.append(max(len(str(row[i])) for row in rows))
    
    # Create border - each column gets width + 2 spaces (1 on each side)
    border_parts = []
    for i in range(num_cols):
        border_parts.append("-" * (col_widths[i] + 2))
    border = "+" + "+".join(border_parts) + "+"
    
    lines = [border]
    
    # Header row
    header_parts = []
    for i in range(num_cols):
        header_parts.append(f" {str(rows[0][i]):<{col_widths[i]}} ")
    lines.append("|" + "|".join(header_parts) + "|")
    lines.append(border)
    
    # Data rows
    for row in rows[1:]:
        row_parts = []
        for i in range(num_cols):
            row_parts.append(f" {str(row[i]):<{col_widths[i]}} ")
        lines.append("|" + "|".join(row_parts) + "|")
    
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

results_5855001 = run_checks("FTO 5855001", checks_5855001)
results_5856501 = run_checks("FTO 5856501", checks_5856501)
results_5876002 = run_checks("FTO 5876002", checks_5876002)
results_5878006 = run_checks("FTO 5878006", checks_5878006)
results_5877504 = run_checks("FTO 5877504", checks_5877504)
results_5862591 = run_checks("FTO 5862591", checks_5862591)
results_5862593 = run_checks("FTO 5862593", checks_5862593)
results_5875315 = run_checks("FTO 5875315", checks_5875315)
results_5875320 = run_checks("FTO 5875320", checks_5875320)
results_5875911 = run_checks("FTO 5875911", checks_5875911)
results_5876311 = run_checks("FTO 5876311", checks_5876311)
results_5874751 = run_checks("FTO 5874751", checks_5874751)
results_5874817 = run_checks("FTO 5874817", checks_5874817)
results_5874921 = run_checks("FTO 5874921", checks_5874921)
results_5876321 = run_checks("FTO 5876321", checks_5876321)
results_5876317 = run_checks("FTO 5876317", checks_5876317)
results_5855501 = run_checks("FTO 5855501", checks_5855501)
results_5862001 = run_checks("FTO 5862001", checks_5862001)
results_5875110 = run_checks("FTO 5875110", checks_5875110)
results_5875116 = run_checks("FTO 5875116", checks_5875116)
results_5874501 = run_checks("FTO 5874501", checks_5874501)

# Create summary table
all_results = [results_5855001, results_5856501, results_5876002, results_5878006, results_5877504, results_5862591, results_5862593, results_5875315, results_5875320, results_5875911, results_5876311, results_5874751, results_5874817, results_5874921, results_5876321, results_5876317, results_5855501, results_5862001, results_5875110, results_5875116, results_5874501]
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