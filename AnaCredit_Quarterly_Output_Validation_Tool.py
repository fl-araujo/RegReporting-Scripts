import xml.etree.ElementTree as ET
from datetime import datetime

# Namespace map for handling the XML namespaces
namespaces = {
    'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'data': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific',
    'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    'T2Q': 'http://www.bundesbank.de/statistik/anacredit/t2q/v2'
}

# Counter for INSTRMNT_IDs in the quarterly dataset
instrmnt_id_count = set()

# Function to process the quarterly XML report
def process_quarterly_report(file_path):
    global instrmnt_id_count
    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Find all datasets with type T2Q:BBK_ANCRDT_ACCNTNG_C
    for dataset in root.findall('message:DataSet', namespaces):
        dataset_type = dataset.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type')

        # Only process datasets with type T2Q:BBK_ANCRDT_ACCNTNG_C
        if dataset_type == 'T2Q:BBK_ANCRDT_ACCNTNG_C':
            for obs in dataset.findall('Obs', namespaces):
                instrmnt_id = obs.attrib.get('INSTRMNT_ID')
                if instrmnt_id:
                    instrmnt_id_count.add(instrmnt_id)  # Add INSTRMNT_ID to set for unique count

# Function to format results for display and write to file
def display_and_save_results(output_file):
    unique_instrmnt_id_count = len(instrmnt_id_count)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Display results
    print(f"\nTRADE REPUBLIC BANK GMBH")
    print(f"\nAnaCredit | Quarterly Report Validation Tool\n{current_time}")
    print(f"\nResults from Dataset T2Q:BBK_ANCRDT_ACCNTNG_C")
    print(f"INSTRMNT_ID Count = {unique_instrmnt_id_count}\n")

    # Write results to file
    with open(output_file, 'w') as f:
        f.write(f"\nTRADE REPUBLIC BANK GMBH")
        f.write(f"\n\nAnaCredit | Quarterly Report Output Validation Tool\n{current_time}")
        f.write(f"\n\nResults from Dataset T2Q:BBK_ANCRDT_ACCNTNG_C\n")
        f.write(f"INSTRMNT_ID Count = {unique_instrmnt_id_count}\n")

# --- Main Logic ---

# File paths for the quarterly XML reports
input_file_1 = 'input_files/ac2q_10012345_202503_5074_1e.xml'
input_file_2 = 'input_files/ac2q_99004014_202503_5077_1e.xml'
output_file_path = 'output_files/AnaCredit_Quarterly_Report_Output_Validation_Results.txt'

# Process both files
process_quarterly_report(input_file_1)
process_quarterly_report(input_file_2)

# Display and save results
display_and_save_results(output_file_path)
