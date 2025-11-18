import pandas as pd
import snowflake.connector
import os
from typing import List, Set, Dict, Any

# ==============================================================================
# 1. Configuration Parameters (REQUIRED USER INPUT)
# ==============================================================================

# --- File Paths ---
INPUT_TXT_PATH = '02.IT_Reporting/IT_A6/input_files/ISIN_20251031.txt'      # Path to the input .txt file containing 'chiave' column
INPUT_XLSX_PATH = '02.IT_Reporting/IT_A6/input_files/ISIN request tracker [FEAT].xlsx' # Path to the input .xlsx file containing 'ISIN' column
OUTPUT_XLSX_PATH = '02.IT_Reporting/IT_A6/input_files/ISIN_Request_List.xlsx' # Path for the final output file

# --- Snowflake Parameters ---
REPORT_DT = '2025-10-31'
SNAPSHOT_DT = '2025-11-17'

SNOWFLAKE_CONN_PARAMS = {
    'user': 'francisco.araujo@traderepublic.com',
    'account': 'gm68377.eu-central-1',
    'database': 'TEAMS_PRD',
    'role': 'FINANCE',
    'authenticator': 'externalbrowser',
}

# --- Snowflake Queries (Parameterized) ---
# Note: Queries are designed to use the REPORT_DT and SNAPSHOT_DT variables
SNOWFLAKE_QUERIES = [
    f"""
    SELECT "REC-INIZIO-RESTO VALUE 3"
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141051
    WHERE report_dt = '{REPORT_DT}' AND snapshot_dt = '{SNAPSHOT_DT}'
    AND output IS NOT NULL
    """,
    f"""
    SELECT "REC-INIZIO-RESTO FIELD 6"
    FROM teams_prd.rmt_sensitive_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_0162504
    WHERE report_dt = '{REPORT_DT}' AND snapshot_dt = '{SNAPSHOT_DT}'
    AND output IS NOT NULL
    """,
    f"""
    SELECT "REC-INIZIO-RESTO VALUE 1"
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4141002
    WHERE report_dt = '{REPORT_DT}' AND snapshot_dt = '{SNAPSHOT_DT}'
    AND output IS NOT NULL
    """,
    f"""
    SELECT "REC-INIZIO-RESTO VALUE 4"
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140151
    WHERE report_dt = '{REPORT_DT}' AND snapshot_dt = '{SNAPSHOT_DT}'
    AND output IS NOT NULL
    """,
    f"""
    SELECT "REC-INIZIO-RESTO VALUE 4"
    FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__italy_a6_fto_4140153
    WHERE report_dt = '{REPORT_DT}' AND snapshot_dt = '{SNAPSHOT_DT}'
    AND output IS NOT NULL
    """
]

# ==============================================================================
# 2. Data Loading Functions
# ==============================================================================

def load_isins_from_txt(file_path: str) -> Set[str]:
    """Step 1: Processes the .txt file and extracts ISINs from the 'chiave' column."""
    print(f"-> Step 1: Loading ISINs from {file_path}...")
    
    # We assume the .txt file is structured (e.g., CSV/TSV) and can be read by pandas.
    # We use 'sep=None' and 'engine='python'' to let pandas infer the delimiter,
    # or you can explicitly set it, e.g., sep='\t' for TSV.
    try:
        df = pd.read_csv(file_path, sep=None, engine='python', encoding='utf-8')
        
        # Standardize column name for case-insensitivity
        df.columns = [col.lower() for col in df.columns]

        if 'chiave' not in df.columns:
            print(f"ERROR: Could not find 'chiave' column in {file_path}. Available columns: {list(df.columns)}")
            return set()
            
        # Clean and convert to set for fast lookup
        isins_set = set(df['chiave'].astype(str).str.strip().str.upper().tolist())
        print(f"   Successfully loaded {len(isins_set)} unique ISINs from TXT file.")
        return isins_set
        
    except FileNotFoundError:
        print(f"ERROR: Input file not found at {file_path}")
        return set()
    except Exception as e:
        print(f"ERROR: Failed to read TXT file: {e}")
        return set()

def fetch_isins_from_snowflake(conn_params: Dict[str, Any], queries: List[str]) -> Set[str]:
    """Step 2 & 3: Connects to Snowflake, runs queries, and aggregates results."""
    print(f"-> Step 2 & 3: Connecting to Snowflake and executing queries...")
    
    all_isins = set()
    conn = None
    try:
        # 2. Establish connection
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        print("   Snowflake connection established. (Browser authentication may be required)")

        # 3. Execute queries and aggregate results
        for i, query in enumerate(queries):
            print(f"   Running query {i+1}/{len(queries)}...")
            cursor.execute(query)
            # Fetch all results from the current query
            results = cursor.fetchall()
            
            # Since the queries select a single column, each row is a tuple (value,)
            # We extract the value and add it to the set.
            current_isins = {str(row[0]).strip().upper() for row in results if row[0] is not None}
            all_isins.update(current_isins)
            print(f"     Query {i+1} returned {len(current_isins)} rows. Total unique ISINs collected: {len(all_isins)}")

        print(f"   Finished fetching data. Total unique ISINs from Snowflake: {len(all_isins)}")
        return all_isins

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"ERROR: Snowflake query or connection error: {e}")
        return set()
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during Snowflake operation: {e}")
        return set()
    finally:
        if conn:
            conn.close()
            print("   Snowflake connection closed.")

def load_isins_from_xlsx(file_path: str) -> Set[str]:
    """Loads exclusion ISINs from the .xlsx file, column 'ISIN'."""
    print(f"-> Step 5: Loading exclusion ISINs from {file_path}...")
    try:
        df = pd.read_excel(file_path)
        
        # Standardize column name for case-insensitivity
        df.columns = [col.upper() for col in df.columns]

        if 'ISIN' not in df.columns:
            print(f"ERROR: Could not find 'ISIN' column in {file_path}. Available columns: {list(df.columns)}")
            return set()
            
        # Clean and convert to set for fast lookup
        isins_set = set(df['ISIN'].astype(str).str.strip().str.upper().tolist())
        print(f"   Successfully loaded {len(isins_set)} unique ISINs for exclusion.")
        return isins_set
        
    except FileNotFoundError:
        print(f"ERROR: Input XLSX file not found at {file_path}")
        return set()
    except Exception as e:
        print(f"ERROR: Failed to read XLSX file: {e}")
        return set()

# ==============================================================================
# 3. Main Logic
# ==============================================================================

def run_reconciliation():
    """Executes the complete data reconciliation workflow."""
    print("--- Starting ISIN Reconciliation Workflow ---")

    # 1. Load ISINs from .txt file (Input 1)
    isins_txt = load_isins_from_txt(INPUT_TXT_PATH)
    if not isins_txt:
        print("Workflow stopped due to error in loading TXT file ISINs.")
        return

    # 2. Load ISINs from Snowflake (Input 2)
    isins_snowflake = fetch_isins_from_snowflake(SNOWFLAKE_CONN_PARAMS, SNOWFLAKE_QUERIES)
    if not isins_snowflake:
        print("Workflow stopped due to error in fetching Snowflake ISINs.")
        return

    # 3. Step 4: Check which ISINs from Snowflake are NOT present in the .txt file
    print("-> Step 4: Comparing Snowflake ISINs vs TXT ISINs...")
    
    # Set difference: (Snowflake - TXT)
    isins_not_in_txt = isins_snowflake - isins_txt
    
    print(f"   ISINs in Snowflake ({len(isins_snowflake)}) but NOT in TXT ({len(isins_txt)}): {len(isins_not_in_txt)}")
    
    if not isins_not_in_txt:
        print("   No missing ISINs found after first filter. Output file will be empty.")
        # Export empty result and exit
        pd.DataFrame({'ISIN': []}).to_excel(OUTPUT_XLSX_PATH, index=False)
        return

    # 4. Step 5: Load ISINs from the exclusion .xlsx file (Input 3)
    isins_xlsx_exclusion = load_isins_from_xlsx(INPUT_XLSX_PATH)
    if not isins_xlsx_exclusion:
         print("   No exclusion ISINs loaded from XLSX file. Skipping exclusion step.")
         final_isins_set = isins_not_in_txt
    else:
        # Step 5: Exclude ISINs that show up in the exclusion .xlsx file
        print("-> Step 5: Applying exclusion filter using XLSX list...")
        
        # Set difference: (ISINs_not_in_txt - ISINs_xlsx_exclusion)
        final_isins_set = isins_not_in_txt - isins_xlsx_exclusion
        
        excluded_count = len(isins_not_in_txt) - len(final_isins_set)
        print(f"   {excluded_count} ISINs were excluded because they were found in the XLSX file.")
    
    print(f"   Final count of ISINs to be exported: {len(final_isins_set)}")
    
    # 5. Export the final subset to an XLSX file
    print("-> Step 6: Exporting final result to XLSX...")
    
    final_df = pd.DataFrame({'ISINs': sorted(list(final_isins_set))})
    
    try:
        final_df.to_excel(OUTPUT_XLSX_PATH, index=False)
        print(f"--- Workflow Complete! Final ISINs successfully exported to {OUTPUT_XLSX_PATH} ---")
    except Exception as e:
        print(f"ERROR: Failed to write output file: {e}")

# Entry point of the script
if __name__ == "__main__":
    # Create dummy input files if they don't exist, for testing purposes
    if not os.path.exists(INPUT_TXT_PATH):
        print(f"Creating dummy TXT file at {INPUT_TXT_PATH}...")
        dummy_txt_content = (
            "chiave,other_data\n"
            "US0378331005,A\n"
            "DE000BAY0017,B\n"
            "NL0000235190,C\n"
            "GB00B03MLX29,D"
        )
        with open(INPUT_TXT_PATH, 'w') as f:
            f.write(dummy_txt_content)

    if not os.path.exists(INPUT_XLSX_PATH):
        print(f"Creating dummy XLSX exclusion file at {INPUT_XLSX_PATH}...")
        dummy_xlsx_data = {
            'ISIN': ['US0378331005', 'GB00B4Z5R228', 'IE00BMT4M775'], # US ISIN is in dummy TXT, the others are not
            'Notes': ['Known exclusion', 'Known exclusion', 'Known exclusion']
        }
        pd.DataFrame(dummy_xlsx_data).to_excel(INPUT_XLSX_PATH, index=False)

    run_reconciliation()