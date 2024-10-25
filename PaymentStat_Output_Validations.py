import xml.etree.ElementTree as ET
import pandas as pd
from collections import defaultdict
from datetime import datetime

# Define the namespaces to handle the XML correctly
namespaces = {
    'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'PAY': 'http://www.bundesbank.de/statistik/pay/rs/v1',
    'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    'data': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific'
}

# Function to read countries or MCC codes from Excel
def read_codes_from_excel(file_path):
    df = pd.read_excel(file_path, skiprows=1, header=None)  # No header
    codes = df[0].astype(str).replace('nan', 'NA').tolist()  # Replace 'nan' with 'NA'
    return [code.strip() for code in codes if code.strip()]

# Read the countries in scope from the first Excel file
excel_file_path_countries = 'input_files/Country_List.xlsx'  # Adjust path to your Excel file
in_scope_areas = read_codes_from_excel(excel_file_path_countries)

# Read the MCC codes in scope from the second Excel file
excel_file_path_mcc = 'input_files/MCC_List.xlsx'  # Adjust path to your MCC codes Excel file
in_scope_mcc = read_codes_from_excel(excel_file_path_mcc)

# Normalize the in-scope lists
in_scope_areas = {area.strip().upper() for area in in_scope_areas}
in_scope_mcc = {mcc.strip().upper() for mcc in in_scope_mcc}

# Function to extract the required data
def extract_w0_data(file_path):
    non_mcc_sums = defaultdict(lambda: {'total_nmbr': 0, 'total_vl': 0.0})
    mcc_records = []
    processed_areas = set()
    processed_mcc_codes = set()

    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    for dataset in root.findall('.//message:DataSet', namespaces):
        obs_elements = dataset.findall('.//Obs', namespaces)

        for obs in obs_elements:
            pstn = obs.attrib.get('PSTN')
            area = obs.attrib.get('AREA')
            nmbr = obs.attrib.get('NMBR')
            vl = obs.attrib.get('VL')
            mcc = obs.attrib.get('MCC')

            if area:
                processed_areas.add(area.strip().upper())  # Normalize area

            if area == 'W0':
                try:
                    if pstn and '.mcc' in pstn:
                        mcc_records.append({
                            'pstn': pstn,
                            'nmbr': int(nmbr) if nmbr else 0,
                            'vl': float(vl) if vl else 0.0,
                            'mcc': mcc  # Store MCC code
                        })
                        if mcc:
                            processed_mcc_codes.add(mcc.strip().upper())
                    else:
                        non_mcc_sums[pstn]['total_nmbr'] += int(nmbr) if nmbr else 0
                        non_mcc_sums[pstn]['total_vl'] += float(vl) if vl else 0.0
                except ValueError:
                    print(f"Warning: Invalid values encountered for NMBR='{nmbr}' or VL='{vl}'.")

    return mcc_records, non_mcc_sums, processed_areas, processed_mcc_codes

# File path to the XML file
file_path = 'input_files/payq_DEA55FG_202406_10001.xml'
mcc_records, non_mcc_totals, processed_areas, processed_mcc_codes = extract_w0_data(file_path)

# Function to format output to both terminal and file
def output_to_terminal_and_file(message, file_obj):
    print(message)
    file_obj.write(message + "\n")

# Open the output file and write output to both terminal and file
with open("output_files/PaymentStat_Output_Validation_Results.txt", "w") as output_file:
    # Get current timestamp
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Header
    output_to_terminal_and_file("\nTRADE REPUBLIC BANK GMBH", output_file)
    output_to_terminal_and_file(f"\nPayment Statistics | Output Validation Tool\n{current_time}", output_file)

    # Identify areas not in scope
    not_in_scope_areas = processed_areas.difference(in_scope_areas)
    not_in_scope_mcc = processed_mcc_codes.difference(in_scope_mcc)

    # Print the countries not in scope from processed data
    if not_in_scope_areas:
        output_to_terminal_and_file("\nList of countries not in scope:", output_file)
        for area in not_in_scope_areas:
            output_to_terminal_and_file(f" - {area}", output_file)
    else:
        output_to_terminal_and_file("\nAll processed countries are in scope.", output_file)

    # Print the MCC codes not in scope from processed data
    if not_in_scope_mcc:
        output_to_terminal_and_file("\nList of MCC codes not in scope:", output_file)
        for mcc in not_in_scope_mcc:
            output_to_terminal_and_file(f" - {mcc}", output_file)
    else:
        output_to_terminal_and_file("\nAll processed MCC codes are in scope.", output_file)

    # Print totals for PSTN items
    output_to_terminal_and_file(f"\nLine Items Totals:",output_file)
    for pstn, totals in non_mcc_totals.items():
        output_to_terminal_and_file(
            f"\n{pstn} \nUnits: {totals['total_nmbr']:,} \nAmount: {totals['total_vl']:,.2f}\n", output_file)
