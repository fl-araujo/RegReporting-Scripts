import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime

# Namespace map (required to handle the XML namespaces correctly)
namespaces = {
    'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'data': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific',
    'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    'T1M': 'http://www.bundesbank.de/statistik/anacredit/t1m/v2',
    'T2M': 'http://www.bundesbank.de/statistik/anacredit/t2m/v2',
    'RIAD': 'http://www.bundesbank.de/statistik/riad/v2'
}

# Initialize counters
cp_id_count_input1 = 0  # Counter for CP_IDs in Input File 1
cp_id_count_input3 = 0  # Counter for CP_IDs in Input File 3
instrument_counts_input2 = {
    'T1M:BBK_ANCRDT_INSTRMNT_C': set(),
    'T1M:BBK_ANCRDT_FNNCL_C': {
        'count': 0,          # Counter for total INSTRMNT_IDs
        'otstndng_nml_amt_sum': 0.0,  # Sum of OTSTNDNG_NMNL_AMNT
        'accrd_intrst_sum': 0.0       # Sum of ACCRD_INTRST
    },
    'T1M:BBK_ANCRDT_ENTTY_INSTRMNT_C': 0  # Counter for total INSTRMNT_IDs
}

# Function to process Input File 1 (count counterparties)
def process_input_file_1(file_path):
    global cp_id_count_input1
    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Find all DataSet elements
    datasets = root.findall('.//message:DataSet', namespaces)

    # Iterate over DataSet elements
    for dataset in datasets:
        # Find all Obs elements inside each DataSet
        obs_elements = dataset.findall('.//Obs', namespaces)

        for obs in obs_elements:
            # Check if the Obs element has a CP_ID attribute
            cp_id = obs.attrib.get('CP_ID')
            if cp_id:
                cp_id_count_input1 += 1

# Function to process Input File 2 (count INSTRMNT_IDs and sum amounts)
def process_input_file_2(file_path):
    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Loop over each dataset in the file
    for dataset in root.findall('message:DataSet', namespaces):
        # Safely get the xsi:type attribute; skip if not found
        dataset_type = dataset.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type')

        # If xsi:type is not found, skip this dataset
        if not dataset_type:
            print(f"Warning: Skipping dataset with no xsi:type in {file_path}")
            continue

        # Process dataset types
        if dataset_type == 'T1M:BBK_ANCRDT_INSTRMNT_C':
            for obs in dataset.findall('Obs', namespaces):
                instrmnt_id = obs.attrib.get('INSTRMNT_ID')
                if instrmnt_id:
                    # Add the INSTRMNT_ID to the set for uniqueness
                    instrument_counts_input2[dataset_type].add(instrmnt_id)

        elif dataset_type == 'T1M:BBK_ANCRDT_FNNCL_C':
            for obs in dataset.findall('Obs', namespaces):
                instrmnt_id = obs.attrib.get('INSTRMNT_ID')
                otstndng_nml_amt = obs.attrib.get('OTSTNDNG_NMNL_AMNT')
                accrd_intrst = obs.attrib.get('ACCRD_INTRST')

                if instrmnt_id:
                    # Increment the count for total INSTRMNT_IDs
                    instrument_counts_input2[dataset_type]['count'] += 1
                
                if otstndng_nml_amt:
                    try:
                        # Convert to float and sum the OTSTNDNG_NMNL_AMNT
                        instrument_counts_input2[dataset_type]['otstndng_nml_amt_sum'] += float(otstndng_nml_amt)
                    except ValueError:
                        print(f"Warning: Invalid OTSTNDNG_NMNL_AMNT value '{otstndng_nml_amt}' encountered.")

                if accrd_intrst:
                    try:
                        # Convert "NOT_APPL" to 0, otherwise convert to float and sum
                        accrd_intrst_value = 0.0 if accrd_intrst == "NOT_APPL" else float(accrd_intrst)
                        instrument_counts_input2[dataset_type]['accrd_intrst_sum'] += accrd_intrst_value
                    except ValueError:
                        print(f"Warning: Invalid ACCRD_INTRST value '{accrd_intrst}' encountered.")

        elif dataset_type == 'T1M:BBK_ANCRDT_ENTTY_INSTRMNT_C':
            for obs in dataset.findall('Obs', namespaces):
                instrmnt_id = obs.attrib.get('INSTRMNT_ID')
                if instrmnt_id:
                    # Increment the counter for total INSTRMNT_IDs
                    instrument_counts_input2[dataset_type] += 1

# Function to process Input File 3 (count CP_IDs)
def process_input_file_3(file_path):
    global cp_id_count_input3
    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Find all DataSet elements
    datasets = root.findall('.//message:DataSet', namespaces)

    # Iterate over DataSet elements
    for dataset in datasets:
        # Find all Obs elements inside each DataSet
        obs_elements = dataset.findall('.//Obs', namespaces)

        for obs in obs_elements:
            # Check if the Obs element has a CP_ID attribute
            cp_id = obs.attrib.get('CP_ID')
            if cp_id:
                cp_id_count_input3 += 1

# Function to format numbers in American currency format
def format_currency(amount):
    return "{:,.2f}".format(amount)

# Function to write results to a text file
def write_results_to_txt(output_file):
    with open(output_file, 'w') as f:
        f.write(f"\nTRADE REPUBLIC BANK GMBH")
        f.write(f"\n\nAnaCredit | Monthly Report Output Validation Tool\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Results for Input File 1
        f.write(f"\n\nResults from Dataset RIAD\n")
        f.write(f'CP_ID Count = {cp_id_count_input1}\n')

        # Results for Input File 2
        for dataset_type, count in instrument_counts_input2.items():
            f.write(f"\nResults from Dataset {dataset_type}\n")
            if dataset_type == 'T1M:BBK_ANCRDT_FNNCL_C':
                f.write(f"INSTRMNT_ID Count = {count['count']}\n")
                f.write(f"OTSTNDNG_NMNL_AMNT = {format_currency(count['otstndng_nml_amt_sum'])}\n")
                f.write(f"ACCRD_INTRST Sum = {format_currency(count['accrd_intrst_sum'])}\n")
            elif isinstance(count, set):
                f.write(f"INSTRMNT_ID Count = {len(count)}\n")
            else:
                f.write(f"INSTRMNT_ID Count = {count}\n")

        # Results for Input File 3
        f.write(f"\nResults from Dataset T2M:BBK_ANCRDT_ENTTY_DFLT_C\n")
        f.write(f'CP_ID Count = {cp_id_count_input3}\n')
        f.write(f"Obs: Less 1 counterparty due to TRB not having a default status\n")

# Function to display results from both Input Files
def display_results():
    print(f"\nTRADE REPUBLIC BANK GMBH")
    print(f"\nAnaCredit | Monthly Report Output Validation Tool\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Display results for counterparties (Input File 1)
    print(f"\nResults from Dataset RIAD")
    print(f'CP_ID Count = {cp_id_count_input1}')

    # Display results for INSTRMNT_ID counts and OTSTNDNG_NMNL_AMNT sums (Input File 2)
    for dataset_type, count in instrument_counts_input2.items():
        if dataset_type == 'T1M:BBK_ANCRDT_FNNCL_C':
            print(f"\nResults from Dataset {dataset_type}")
            print(f"INSTRMNT_ID Count = {count['count']}")
            print(f"OTSTNDNG_NMNL_AMNT = {format_currency(count['otstndng_nml_amt_sum'])}")
            print(f"ACCRD_INTRST = {format_currency(count['accrd_intrst_sum'])}")
        elif isinstance(count, set):
            print(f"\nResults from Dataset {dataset_type}")
            print(f"INSTRMNT_ID Count = {len(count)}")
        else:
            print(f"\nResults from Dataset {dataset_type}")
            print(f"INSTRMNT_ID Count = {count}")

    # Display results for CP_IDs (Input File 3)
    print(f"\nResults from Dataset T2M:BBK_ANCRDT_ENTTY_DFLT_C")
    print(f'CP_ID Count = {cp_id_count_input3}')
    print(f"Obs: Less 1 counterparty due to TRB not having a default status\n")

# --- Main Logic ---

# Process Input File 1 (RIAD)
input_file_1 = 'input_files/rdac_10012345_202411_5030.xml'  # Update with the actual file path
process_input_file_1(input_file_1)

# Process Input File 2 (AC1M)
input_file_2 = 'input_files/ac1m_10012345_202411_5031_1e.xml'  # Update with the actual file path
process_input_file_2(input_file_2)

# Process Input File 3 (AC2M)
input_file_3 = 'input_files/ac2m_10012345_202411_5032_1e.xml'  # Update with the actual file path
process_input_file_3(input_file_3)

# Write results to a text file
output_file_path = 'output_files/AnaCredit_Monthly_Report_Output_Validation_Results.txt'  # Set desired output file path
write_results_to_txt(output_file_path)

# Display the combined results from all files
display_results()
