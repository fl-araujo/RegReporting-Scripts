import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import logging
import os
import math

# Setup logging
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')

# Constants
T1M_INSTRMNT = 'T1M:BBK_ANCRDT_INSTRMNT_C'
T1M_FNNCL = 'T1M:BBK_ANCRDT_FNNCL_C'
T1M_ENTTY_INSTRMNT = 'T1M:BBK_ANCRDT_ENTTY_INSTRMNT_C'

namespaces = {
    'message': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message',
    'data': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/data/structurespecific',
    'common': 'http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common',
    'T1M': 'http://www.bundesbank.de/statistik/anacredit/t1m/v2',
    'T2M': 'http://www.bundesbank.de/statistik/anacredit/t2m/v2',
    'RIAD': 'http://www.bundesbank.de/statistik/riad/v2'
}

def format_currency(amount):
    return "{:,.2f}".format(amount)

def parse_xml(file_path):
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return None
    try:
        return ET.parse(file_path).getroot()
    except ET.ParseError as e:
        logging.error(f"Error parsing {file_path}: {e}")
        return None

def count_cp_ids(file_path):
    root = parse_xml(file_path)
    if root is None:
        return 0
    count = 0
    for dataset in root.findall('.//message:DataSet', namespaces):
        for obs in dataset.findall('.//Obs', namespaces):
            if obs.attrib.get('CP_ID'):
                count += 1
    return count

def process_ac1m_file(file_path, instrument_data):
    root = parse_xml(file_path)
    if root is None:
        return
    for dataset in root.findall('message:DataSet', namespaces):
        dataset_type = dataset.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type')
        if dataset_type == T1M_INSTRMNT:
            for obs in dataset.findall('Obs', namespaces):
                instrmnt_id = obs.attrib.get('INSTRMNT_ID')
                if instrmnt_id:
                    instrument_data[T1M_INSTRMNT].add(instrmnt_id)
        elif dataset_type == T1M_FNNCL:
            for obs in dataset.findall('Obs', namespaces):
                instrmnt_id = obs.attrib.get('INSTRMNT_ID')
                if instrmnt_id:
                    instrument_data[T1M_FNNCL]['count'] += 1

                amt = obs.attrib.get('OTSTNDNG_NMNL_AMNT')
                if amt:
                    try:
                        instrument_data[T1M_FNNCL]['otstndng_nml_amt_sum'] += float(amt)
                    except ValueError:
                        logging.warning(f"Invalid OTSTNDNG_NMNL_AMNT: {amt}")

                intrst = obs.attrib.get('ACCRD_INTRST')
                if intrst:
                    try:
                        value = 0.0 if intrst == "NOT_APPL" else float(intrst)
                        instrument_data[T1M_FNNCL]['accrd_intrst_sum'] += value
                    except ValueError:
                        logging.warning(f"Invalid ACCRD_INTRST: {intrst}")
        elif dataset_type == T1M_ENTTY_INSTRMNT:
            for obs in dataset.findall('Obs', namespaces):
                if obs.attrib.get('INSTRMNT_ID'):
                    instrument_data[T1M_ENTTY_INSTRMNT] += 1

def load_excel_data(filepath):
    df_entty = pd.read_excel(filepath, sheet_name='ax_ENTTY')
    df_instrmnt = pd.read_excel(filepath, sheet_name='ax_INSTRMNT')
    df_fnncl = pd.read_excel(filepath, sheet_name='ax_FNNCL')
    df_entty_instrmnt = pd.read_excel(filepath, sheet_name='ax_ENTTY_INSTRMNT')
    df_dflt = pd.read_excel(filepath, sheet_name='ax_ENTTY_DFLT')

    df_fnncl['OTSTNDNG_NMNL_AMNT'] = pd.to_numeric(df_fnncl.iloc[:, 11], errors='coerce')
    df_fnncl['ACCRD_INTRST'] = pd.to_numeric(df_fnncl.iloc[:, 12], errors='coerce')

    return {
        'riad_cp_count': len(df_entty),
        'instrmnt_count': len(df_instrmnt),
        'fnncl_count': len(df_fnncl),
        'fnncl_amt_sum': df_fnncl['OTSTNDNG_NMNL_AMNT'].sum(skipna=True),
        'fnncl_intrst_sum': df_fnncl['ACCRD_INTRST'].sum(skipna=True),
        'entty_instrmnt_count': len(df_entty_instrmnt),
        'entty_dflt_count': len(df_dflt)
    }

def is_approximately_equal(val1, val2, tolerance=0.01):
    return math.isclose(val1, val2, abs_tol=tolerance)

def compare_datasets_to_excel(submission_file, excel_file, output_file='output_validation.txt'):
    instrument_data = {
        T1M_INSTRMNT: set(),
        T1M_FNNCL: {'count': 0, 'otstndng_nml_amt_sum': 0.0, 'accrd_intrst_sum': 0.0},
        T1M_ENTTY_INSTRMNT: 0
    }

    cp_count_riad = count_cp_ids(submission_file['riad'])
    process_ac1m_file(submission_file['ac1m_1'], instrument_data)
    process_ac1m_file(submission_file['ac1m_2'], instrument_data)
    cp_count_t2m = count_cp_ids(submission_file['ac2m_1']) + count_cp_ids(submission_file['ac2m_2'])

    excel_data = load_excel_data(excel_file)

    rows = []
    rows.append(["Check", "Submission Files", "Input Files", "Match "])  # Space added after 'Match' header

    def add_row(check, sub, inp, match):
        rows.append([check, str(sub), str(inp), "✅ True" if match else "❌ False"])

    add_row("1. RIAD CP_ID Count", cp_count_riad, excel_data['riad_cp_count'], cp_count_riad == excel_data['riad_cp_count'])
    add_row("2. T1M_INSTRMNT ID Count", len(instrument_data[T1M_INSTRMNT]), excel_data['instrmnt_count'], len(instrument_data[T1M_INSTRMNT]) == excel_data['instrmnt_count'])
    add_row("3. T1M_FNNCL INSTRMNT_ID Count", instrument_data[T1M_FNNCL]['count'], excel_data['fnncl_count'], instrument_data[T1M_FNNCL]['count'] == excel_data['fnncl_count'])
    add_row("4. T1M_FNNCL OTSTNDNG_NMNL_AMNT", format_currency(instrument_data[T1M_FNNCL]['otstndng_nml_amt_sum']), format_currency(excel_data['fnncl_amt_sum']), is_approximately_equal(instrument_data[T1M_FNNCL]['otstndng_nml_amt_sum'], excel_data['fnncl_amt_sum']))
    add_row("5. T1M_FNNCL ACCRD_INTRST", format_currency(instrument_data[T1M_FNNCL]['accrd_intrst_sum']), format_currency(excel_data['fnncl_intrst_sum']), is_approximately_equal(instrument_data[T1M_FNNCL]['accrd_intrst_sum'], excel_data['fnncl_intrst_sum']))
    add_row("6. T1M_ENTTY_INSTRMNT Count", instrument_data[T1M_ENTTY_INSTRMNT], excel_data['entty_instrmnt_count'], instrument_data[T1M_ENTTY_INSTRMNT] == excel_data['entty_instrmnt_count'])
    add_row("7. T2M_ENTTY_DFLT CP_ID Count", cp_count_t2m, excel_data['entty_dflt_count'], cp_count_t2m == excel_data['entty_dflt_count'])

    col_widths = [max(len(str(row[i])) for row in rows) for i in range(4)]
    total_width = sum(col_widths) + len(col_widths) * 3 + 1
    border = "-" * total_width

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output = []
    output.append(f"\nTrade Republic Bank GmbH {timestamp}\n")
    output.append("AnaCredit Output File Validations\n")
    output.append(border)
    output.append(f"| {rows[0][0]:<{col_widths[0]}} | {rows[0][1]:<{col_widths[1]}} | {rows[0][2]:<{col_widths[2]}} | {rows[0][3]:<{col_widths[3]}} |")  # Fixed header with space
    output.append(border)
    for row in rows[1:]:
        output.append(f"| {row[0]:<{col_widths[0]}} | {row[1]:<{col_widths[1]}} | {row[2]:<{col_widths[2]}} | {row[3]:<{col_widths[3]}} |")
    output.append(border)

    output.append("\nExplanatory Notes:")
    output.append("Check 1. One counterparty, corresponding to the TRB IT Branch, appears twice in the input file but is reported once in RIAD.")
    output.append("Check 7. As both TRB and TRB Italy Branch do not have default statuses, the dataset reflects two fewer counterparties.")

    final_output = "\n".join(output)
    print(final_output)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(final_output)

if __name__ == "__main__":
    submission_files = {
        'riad': 'de/rdac_10012345_202503_5071.xml',
        'ac1m_1': 'input_files/ac1m_10012345_202503_5072_1e.xml',
        'ac1m_2': 'input_files/ac1m_99004014_202503_5075_1e.xml',
        'ac2m_1': 'input_files/ac2m_10012345_202503_5073_1e.xml',
        'ac2m_2': 'input_files/ac2m_99004014_202503_5076_1e.xml',
    }
    excel_file = 'input_files/AnaCredit_CalcsInput.xlsx'
    compare_datasets_to_excel(submission_files, excel_file)
