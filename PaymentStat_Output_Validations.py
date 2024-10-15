import xml.etree.ElementTree as ET
from collections import defaultdict

# Define the namespaces to handle the XML correctly
namespaces = {
    'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'PAY': 'http://www.bundesbank.de/statistik/pay/rs/v1',
    'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    'data': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific'
}

# Function to extract the required data
def extract_w0_data(file_path):
    # Dictionary to store summed totals for non-.mcc PSTNs
    non_mcc_sums = defaultdict(lambda: {'total_nmbr': 0, 'total_vl': 0.0})

    # List to store individual records for .mcc PSTNs
    mcc_records = []

    # Parse the XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Loop through the DataSet elements
    for dataset in root.findall('.//message:DataSet', namespaces):
        # Find all Obs elements
        obs_elements = dataset.findall('.//Obs', namespaces)

        # Loop through each Obs element
        for obs in obs_elements:
            pstn = obs.attrib.get('PSTN')
            area = obs.attrib.get('AREA')
            nmbr = obs.attrib.get('NMBR')
            vl = obs.attrib.get('VL')

            # Only process records where AREA is 'W0'
            if area == 'W0':
                try:
                    # If PSTN contains '.mcc', store individual records in the list
                    if pstn and '.mcc' in pstn:
                        mcc_records.append({
                            'pstn': pstn,
                            'nmbr': int(nmbr) if nmbr else 0,
                            'vl': float(vl) if vl else 0.0
                        })
                    # If PSTN doesn't contain '.mcc', accumulate the values
                    else:
                        non_mcc_sums[pstn]['total_nmbr'] += int(nmbr) if nmbr else 0
                        non_mcc_sums[pstn]['total_vl'] += float(vl) if vl else 0.0
                except ValueError:
                    print(f"Warning: Invalid values encountered for NMBR='{nmbr}' or VL='{vl}'.")

    return mcc_records, non_mcc_sums

# File path to the XML file
file_path = 'input_files/payq_DEA55FG_202406_10001.xml'  # Replace with your actual XML file path

# Call the function to extract the data
mcc_records, non_mcc_totals = extract_w0_data(file_path)

# Function to format numbers in American currency format
def format_currency(amount):
    return "{:,.2f}".format(amount)

# Print summed records for PSTN items
print("\nPAYMENT STATISTICS | OUTPUT VALIDATION TOOL:\n \nTOTALS: \n")
for pstn, totals in non_mcc_totals.items():
    print(f"{pstn} \nUnits: {format_currency(totals['total_nmbr'])} \nAmount: {format_currency(totals['total_vl'])}\n")

# Now write the output to a text file
output_file = 'output_files/PaymentStat_Output_Validation_Results.txt'

with open(output_file, 'w') as file:
    # Write header
    file.write("\nPAYMENT STATISTICS | OUTPUT VALIDATION TOOL:\n\nTOTALS:\n\n")
    
    # Write summed records for PSTN items
    for pstn, totals in non_mcc_totals.items():
        file.write(f"{pstn} \nUnits: {format_currency(totals['total_nmbr'])} \nAmount: {format_currency(totals['total_vl'])}\n\n")