import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import os
from tabulate import tabulate

now_str = datetime.now().strftime("%H%M%S")  # current time as HHMMSS

EXCEL_FILE_PATH = r"gromikv_reporting/input_files/GroMikV_Quarterly_Analysis_30062025.xlsx"
OUTPUT_XML_PATH = f"gromikv_reporting/output_files/MIO.A.5509464.20250630.{now_str}.xml"
OUTPUT_REPORT_PATH = "gromikv_reporting/output_files/validation_report.txt"

# Namespace constants
NS = "http://www.bundesbank.de/xmw/2019-03-31"
BBK_NS = "http://www.bundesbank.de/xmw/2010-12-31"
XSD_NS = "http://www.w3.org/2001/XMLSchema"
XSI_NS = "http://www.w3.org/2001/XMLSchema-instance"

# Register namespaces explicitly to ensure correct XML header output
ET.register_namespace('', NS)
ET.register_namespace('bbk', BBK_NS)
ET.register_namespace('xsd', XSD_NS)
ET.register_namespace('xsi', XSI_NS)


def pretty_root_element(xml_str: str) -> str:
    import re
    pattern = r'(<LIEFERUNG-MIO\s+)([^>]+)(>)'
    match = re.search(pattern, xml_str)
    if not match:
        return xml_str

    start_tag = match.group(1)
    attrs_str = match.group(2)
    end_tag = match.group(3)

    attr_pairs = re.findall(r'(\S+?="[^"]*")', attrs_str)
    indent = "               "
    formatted_attrs = ("\n" + indent).join(attr_pairs)

    new_start_tag = f"{start_tag}{formatted_attrs}{end_tag}"
    return xml_str.replace(match.group(0), new_start_tag)


def create_mio_xml(excel_file_path, output_file_path):
    ba_data = pd.read_excel(excel_file_path, sheet_name="Data_Analysis_Quarter")

    # Create root element with explicit xmlns:xsd and xmlns:xsi
    root = ET.Element(
        f"{{{NS}}}LIEFERUNG-MIO",
        {
            "xmlns:xsd": XSD_NS,
            "xmlns:xsi": XSI_NS,
            "dateireferenz": "0",
            "erstellzeit": datetime.now().strftime("%Y-%m-%dT%H:%M:%S+01:00"),
            "stufe": "Produktion",
            "version": "1.0"
        }
    )

    config = {
        'kreditgebernr': '55094643',
        'name': 'Trade Republic Bank GmbH',
        'strasse': 'Brunnenstrassee 19',
        'plz': '10119',
        'ort': 'Berlin',
        'land': 'DE',
        'kontakt': {
            'anrede': 'Brunnenstrassee 19',
            'vorname': 'Aleksandar',
            'zuname': 'Stojchevski',
            'abteilung': 'Regulatory Reporting',
            'telefon': '015751017771',
            'email': 'aleksandar.stojchevski@traderepublic.com',
            'extranet_id': 'exn2p16i'
        }
    }

    def add_company_info(parent):
        ET.SubElement(parent, "KREDITGEBERNR").text = config['kreditgebernr']
        ET.SubElement(parent, f"{{{BBK_NS}}}NAME").text = config['name']
        ET.SubElement(parent, f"{{{BBK_NS}}}STRASSE").text = config['strasse']
        ET.SubElement(parent, f"{{{BBK_NS}}}PLZ").text = config['plz']
        ET.SubElement(parent, f"{{{BBK_NS}}}ORT").text = config['ort']
        ET.SubElement(parent, f"{{{BBK_NS}}}LAND").text = config['land']

        kontakt = ET.SubElement(parent, f"{{{BBK_NS}}}KONTAKT")
        for key, value in config['kontakt'].items():
            tag = key.replace('_', '-').upper()
            ET.SubElement(kontakt, f"{{{BBK_NS}}}{tag}").text = value

    absender = ET.SubElement(root, f"{{{BBK_NS}}}ABSENDER")
    add_company_info(absender)

    ersteller = ET.SubElement(root, f"{{{BBK_NS}}}ERSTELLER")
    add_company_info(ersteller)

    meldung = ET.SubElement(root, "MELDUNG-MIO", {
        "erstellzeit": datetime.now().strftime("%Y-%m-%dT%H:%M:%S+01:00")
    })

    melder = ET.SubElement(meldung, f"{{{BBK_NS}}}MELDER")
    add_company_info(melder)

    total_sums = {key: 0 for key in ['POS100', 'POS108', 'POS110', 'POS111', 'POS112', 'POS113', 'POS114']}
    distinct_pos050 = set()
    
    # Track length validation issues
    pos040_length_issues = []
    pos050_length_issues = []
    pos051_length_issues = []

    for idx, row in ba_data.iterrows():
        ba = ET.SubElement(meldung, "BA")

        # Format POS010 as 'YYYY-MM'
        pos010_val = row.get('POS010', '2025-06')
        if pd.notna(pos010_val) and isinstance(pos010_val, (pd.Timestamp, datetime)):
            pos010_str = pos010_val.strftime('%Y-%m')
        else:
            pos010_str = str(pos010_val)
        ET.SubElement(ba, "POS010").text = pos010_str

        ET.SubElement(ba, "POS015").text = str(row.get('POS015', 'SA'))
        ET.SubElement(ba, "POS030").text = config['kreditgebernr']

        # POS040 with length validation and formatting
        if pd.notna(row.get('POS040')):
            pos040_raw = row.get('POS040')
            # Convert to int to remove decimal, then pad with leading zeros to 8 digits
            try:
                pos040_val = str(int(float(pos040_raw))).zfill(8)
            except (ValueError, TypeError):
                pos040_val = str(pos040_raw).zfill(8)
            
            if len(pos040_val) != 8:
                pos040_length_issues.append(f"Row {idx + 1}: '{pos040_val}' (length: {len(pos040_val)})")
            ET.SubElement(ba, "POS040").text = pos040_val

        for field in ['POS050', 'POS051', 'POS090', 'POS091']:
            raw_val = row.get(field, '')
            if field in ['POS090', 'POS091'] and pd.notna(raw_val):
                val = str(int(float(raw_val)))
            elif field == 'POS050' and pd.notna(raw_val):
                # Handle POS050 similar to POS040 - pad with leading zeros
                try:
                    val = str(int(float(raw_val))).zfill(8)
                except (ValueError, TypeError):
                    val = str(raw_val).zfill(8) if raw_val else ""
            elif field == 'POS051' and pd.notna(raw_val):
                # Handle POS051 - ensure exactly 20 characters
                val = str(raw_val)
                if len(val) > 20:
                    val = val[:20]  # Truncate to 20 characters
                elif len(val) < 20:
                    val = val.ljust(20)  # Pad with spaces to reach 20 characters
            else:
                val = str(raw_val) if pd.notna(raw_val) else ""
            
            # Skip POS051 if not populated (empty or NaN)
            if field == 'POS051' and (not val or val == 'nan' or pd.isna(raw_val)):
                continue
            
            # POS050 length validation (exactly 8 characters)
            if field == 'POS050' and val and len(val) != 8:
                pos050_length_issues.append(f"Row {idx + 1}: '{val}' (length: {len(val)})")
            
            # POS051 length validation (exactly 20 characters)
            if field == 'POS051' and val and len(val) != 20:
                pos051_length_issues.append(f"Row {idx + 1}: '{val}' (length: {len(val)})")
            
            ET.SubElement(ba, field).text = val
            if field == 'POS050':
                distinct_pos050.add(val)

        pos093_val = row.get('POS093', 0)
        ET.SubElement(ba, "POS093").text = f"{float(pos093_val):,.5f}".replace('.', ',') if pd.notna(pos093_val) else "0,00000"

        def safe_int(val):
            try:
                return round(float(val))
            except (ValueError, TypeError):
                return 0

        pos100_val = safe_int(row.get('POS100'))
        pos100 = ET.SubElement(ba, "POS100", wert=str(pos100_val))
        total_sums['POS100'] += pos100_val

        pos108_val = safe_int(row.get('POS108'))
        ET.SubElement(pos100, "POS108", wert=str(pos108_val))
        total_sums['POS108'] += pos108_val

        pos110_val = safe_int(row.get('POS110', pos100_val))
        pos110 = ET.SubElement(pos100, "POS110", wert=str(pos110_val))
        total_sums['POS110'] += pos110_val

        pos111_val = safe_int(row.get('POS111'))
        pos112_val = safe_int(row.get('POS112'))
        # Check if POS111 or POS112 columns exist and have non-null values in the input
        if pd.notna(row.get('POS111')) or pd.notna(row.get('POS112')):
            pos111 = ET.SubElement(pos110, "POS111", wert=str(pos111_val))
            ET.SubElement(pos111, "POS112", wert=str(pos112_val))
            total_sums['POS111'] += pos111_val
            total_sums['POS112'] += pos112_val

        pos113_val = safe_int(row.get('POS113'))
        pos114_val = safe_int(row.get('POS114'))
        # Check if POS113 or POS114 columns exist and have non-null values in the input
        if pd.notna(row.get('POS113')) or pd.notna(row.get('POS114')):
            pos113 = ET.SubElement(pos110, "POS113", wert=str(pos113_val))
            ET.SubElement(pos113, "POS114", wert=str(pos114_val))
            total_sums['POS113'] += pos113_val
            total_sums['POS114'] += pos114_val

    bas = ET.SubElement(meldung, "BAS")
    ET.SubElement(bas, "POS010").text = "2025-06"
    ET.SubElement(bas, "POS030").text = config['kreditgebernr']
    ET.SubElement(bas, "POS072").text = config['kontakt']['vorname']
    ET.SubElement(bas, "POS073").text = config['kontakt']['telefon']
    ET.SubElement(bas, "POS074").text = config['kontakt']['email']

    bas_pos100 = ET.SubElement(bas, "POS100", wert=str(total_sums['POS100']))
    ET.SubElement(bas_pos100, "POS108", wert=str(total_sums['POS108']))
    bas_pos110 = ET.SubElement(bas_pos100, "POS110", wert=str(total_sums['POS110']))

    # Check if POS111/POS112 have any non-null values in the data
    if any(pd.notna(row.get('POS111')) or pd.notna(row.get('POS112')) for _, row in ba_data.iterrows()):
        bas_pos111 = ET.SubElement(bas_pos110, "POS111", wert=str(total_sums['POS111']))
        ET.SubElement(bas_pos111, "POS112", wert=str(total_sums['POS112']))

    # Check if POS113/POS114 have any non-null values in the data
    if any(pd.notna(row.get('POS113')) or pd.notna(row.get('POS114')) for _, row in ba_data.iterrows()):
        bas_pos113 = ET.SubElement(bas_pos110, "POS113", wert=str(total_sums['POS113']))
        ET.SubElement(bas_pos113, "POS114", wert=str(total_sums['POS114']))

    ET.indent(root, space="   ")
    xml_bytes = ET.tostring(root, encoding='ISO-8859-15', xml_declaration=False)
    xml_str = pretty_root_element(xml_bytes.decode('ISO-8859-15'))

    os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
    with open(output_file_path, 'w', encoding='ISO-8859-15') as f:
        f.write('<?xml version="1.0" encoding="ISO-8859-15"?>\n')
        f.write(xml_str)

    return total_sums, distinct_pos050, pos040_length_issues, pos050_length_issues, pos051_length_issues


def extract_xml_data(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    namespaces = {
        'xmw': NS
    }

    sums = {key: 0 for key in ['POS100', 'POS108', 'POS110', 'POS111', 'POS112', 'POS113', 'POS114']}
    pos050_values = set()

    for ba in root.findall('.//xmw:BA', namespaces):
        pos050_elem = ba.find('xmw:POS050', namespaces)
        if pos050_elem is not None:
            pos050_values.add(pos050_elem.text)

        for pos in sums.keys():
            elem = ba.find(f".//xmw:{pos}", namespaces)
            if elem is not None:
                val_str = elem.get('wert') or elem.text
                if val_str:
                    val_str = val_str.replace('.', '').replace(',', '.')
                    try:
                        val = float(val_str)
                        sums[pos] += val
                    except ValueError:
                        pass

    return sums, pos050_values


def create_validation_report(excel_sums, xml_sums, excel_pos050, xml_pos050, pos040_issues, pos050_issues, pos051_issues, output_file):
    def check_mark(match: bool) -> str:
        return "✅" if match else "❌"

    headers_sums = ["Field", "Excel Sum", "XML Sum", "Match"]
    sum_rows = []
    for key in excel_sums.keys():
        match = excel_sums[key] == xml_sums.get(key, None)
        sum_rows.append([key, str(excel_sums[key]), str(xml_sums.get(key, "N/A")), check_mark(match)])

    pos050_missing = excel_pos050 - xml_pos050
    pos050_extra = xml_pos050 - excel_pos050

    header_name = "Trade Republic Bank GmbH"
    header_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"{header_name}\n")
        f.write(f"{header_time}\n\n")

        f.write("🔎 Sum Comparison\n\n")
        f.write(tabulate(sum_rows, headers=headers_sums, tablefmt="grid"))
        f.write("\n\n")

        f.write("📊 POS050 Values Comparison\n\n")
        pos050_table = [
            ["Excel POS050 count", str(len(excel_pos050))],
            ["XML POS050 count", str(len(xml_pos050))],
            ["Missing in XML", ", ".join(sorted(pos050_missing)) if pos050_missing else "None"],
            ["Extra in XML", ", ".join(sorted(pos050_extra)) if pos050_extra else "None"]
        ]
        f.write(tabulate(pos050_table, headers=["Description", "Value"], tablefmt="grid"))
        f.write("\n\n")

        # Add length validation section
        f.write("📏 Length Validation\n\n")
        
        # POS040 length validation
        pos040_status = "✅ All POS040 values have correct length (8 characters)" if not pos040_issues else f"❌ {len(pos040_issues)} POS040 values have incorrect length"
        f.write(f"POS040 Length Check: {pos040_status}\n")
        if pos040_issues:
            f.write("\nPOS040 Length Issues:\n")
            for issue in pos040_issues:
                f.write(f"  • {issue}\n")
        f.write("\n")

        # POS050 length validation
        pos050_status = "✅ All POS050 values have correct length (8 characters)" if not pos050_issues else f"❌ {len(pos050_issues)} POS050 values have incorrect length"
        f.write(f"POS050 Length Check: {pos050_status}\n")
        if pos050_issues:
            f.write("\nPOS050 Length Issues:\n")
            for issue in pos050_issues:
                f.write(f"  • {issue}\n")
        f.write("\n")

        # POS051 length validation
        pos051_status = "✅ All POS051 values have correct length (20 characters)" if not pos051_issues else f"❌ {len(pos051_issues)} POS051 values have incorrect length"
        f.write(f"POS051 Length Check: {pos051_status}\n")
        if pos051_issues:
            f.write("\nPOS051 Length Issues:\n")
            for issue in pos051_issues:
                f.write(f"  • {issue}\n")
        f.write("\n")


def main():
    excel_sums, excel_pos050, pos040_issues, pos050_issues, pos051_issues = create_mio_xml(EXCEL_FILE_PATH, OUTPUT_XML_PATH)
    xml_sums, xml_pos050 = extract_xml_data(OUTPUT_XML_PATH)
    create_validation_report(excel_sums, xml_sums, excel_pos050, xml_pos050, pos040_issues, pos050_issues, pos051_issues, OUTPUT_REPORT_PATH)
    
if __name__ == "__main__":
    main()
