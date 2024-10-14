import xml.etree.ElementTree as ET

# Path to your XML file
file_path = 'input_files/payak_flv_DEA55FG_202406_10001.xml'

# Parse the XML file
tree = ET.parse(file_path)
root = tree.getroot()

# Namespace map (required to handle the XML namespaces correctly)
ns = {
    'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'data': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific',
    'commons': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    'ACK': 'http://www.bundesbank.de/statistik/pay/ack/v1'
}

# Find all DataSet elements
datasets = root.findall('.//message:DataSet', ns)

# Use a set to store unique VLDTN_IDs
vldtn_ids = set()

# Iterate over DataSet elements
for dataset in datasets:
    # Find all Obs elements inside each DataSet
    obs_elements = dataset.findall('.//Obs', ns)

    for obs in obs_elements:
        # Check if the Obs element has a VLDTN_ID attribute
        vldtn_id = obs.attrib.get('VLDTN_ID')
        if vldtn_id:
            # Add VLDTN_ID to the set (automatically handles uniqueness)
            vldtn_ids.add(vldtn_id)

# Print all unique VLDTN_IDs found
print(f'NUMBER OF BREACHED VALIDATION RULES = {len(vldtn_ids)}')
for vldtn_id in vldtn_ids:
    print(f'VLDTN_ID = {vldtn_id}')
