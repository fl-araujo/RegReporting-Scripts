import snowflake.connector
import pandas as pd

# Parameters
report_dt = '2025-07-31'
snapshot_dt = '2025-08-03'
report_granularity = 'month'

output_core = f"input_extractors/output_files/Input_Core_{report_dt}.xlsx"
output_de = f"input_extractors/output_files/Input_DE_{report_dt}.xlsx"
output_fin = f"input_extractors/output_files/Input_FIN_{report_dt}.xlsx"

# Helper function to get filtered dataframe based on start and end columns (inclusive)
def get_filtered_df(conn, query, start_col, end_col):
    df = pd.read_sql(query, conn)
    start_idx = df.columns.get_loc(start_col)
    end_idx = df.columns.get_loc(end_col)
    return df.iloc[:, start_idx:end_idx+1]

# Helper function to convert specified columns to datetime with format dd-mmm-yyyy
def convert_dates(df, date_columns):
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d-%b-%Y')
    return df

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='francisco.araujo@traderepublic.com',
    account='gm68377.eu-central-1',
    database='TEAMS_PRD',
    role='FINANCE',
    authenticator='externalbrowser'
)

try:
    # -------------- CORE INPUT ----------------

    # Client_static - null sheet (empty)
    client_static_data = {
        'Reporting Entity': [1001] * 59,
        'Report Reference': ['BISTA', 'DOMESTIC', 'D_R11', 'D_R12', 'D_R11B', 'D_R12B', 'D_R21', 'D_R22', 'D_FW', 'D_UR', 'DOMESTIC_D_FW_D_UR', 'VJKRE', 'BRANCHES', 'SUBSIDIARIES', 'A1', 'A2', 'A3', 'B1', 'B3', 'B4', 'B6', 'B7', 'BA', 'C1', 'C2', 'C3', 'C4', 'C5', 'D1', 'D2', 'E1', 'E1B', 'E2', 'E2B', 'E3', 'E3B', 'E4', 'E5', 'F1', 'F2', 'HV', 'H', 'I1', 'I2', 'L1', 'L2', 'M1', 'M2', 'O1', 'O2', 'P1', 'Q1', 'S1', 'V1', 'V2', 'V3', 'V4', 'VA', 'VB'],
        'Check digit for message number for sender': [3] * 59,
        'Firm Reference Number, Sender (ABSENDER - IDENTNR)': [100123457] * 59,
        'Entity Number, Creator (ERSTELLER - IDENTNR)': [100123457] * 59,
        'Reporting Firm Number, Reporter (MELDER - IDENTNR)': [100123457] * 59,
        'Firm Reference Number (Bank Code), Sender (ABSENDER: FIRMENNR or BLZ or RZNR)': [100123457] * 59,
        'Entity Number, Creator (ERSTELLER: FIRMENNR or BLZ or RZNR)': [100123457] * 59,
        'Number to identify the enterprise required to report, Reporter (MELDER: FIRMENNR or BLZ or RZNR)': [100123457] * 59,
        'Bank Identifier Code, BLZ (9 digits, incl, check digit), (MELDER  - IDENTNR), WP': [100123457] * 59,
        'Firm ID number for submission': [100123457] * 59,
        'Firm Name, Sender (ABSENDER - NAME), WP': ['Trade Republic Bank GmbH'] * 59,
        'Distinguishes between test and production data (stufe: Produktion,Test)': ['Produktion'] * 59,
        'Bereich (area) field for DatabaseRef page (bereich: Statistik)': ['Statistik'] * 59,
        'Counter for repeated submissions (dateireferenz: 0-99)': [''] * 59,
        'Result of a plausibility check run by reporter/sender (Nein, Fehler, Erfuellt)': [''] * 59,
        'Distinction between forms and assigned valuation  correction forms (normal or Bewkorr)': ['', '', 'Normal', 'Normal', 'Bewkorr', 'Bewkorr', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Normal', 'Bewkorr', 'Normal', 'Bewkorr', 'Normal', 'Bewkorr', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
        'Indicates whether data on the form have already been reported and are to be corrected (ja, nein)': [''] * 59,
        'Information on the unit of the form field entry (Anzahl, Prozent, Waehrung, Relation, Datum, JaNein)': [''] * 59,
        'ISO code of the currency in which field entries are given': ['EUR'] * 59,
        'Address of enterprise required to report, Reporter (MELDER: STRASSE or POSTFACH), WP': ['STRASSE'] * 59,
        'Contact Title, Sender (ABSENDER - ANREDE), WP': ['Herr'] * 59,
        'First name of contact person, Reporter (MELDER - VORNAME), WP': ['Aleksandar'] * 59,
        'Last name of contact person, Reporter (MELDER - ZUNAME), WP': ['Stojchevski'] * 59,
        'Contact Phone no, Reporter (MELDER - TELEFON), WP': ['015751017771'] * 59,
        'Fax number, Reporter (MELDER - FAX), WP': [''] * 59,
        'E-mail address, Reporter (MELDER - EMAIL), WP': ['aleksandar.stojchevski@traderepublic.com'] * 59,
        'User name for Bundesbank\'s extranet, Reporter (MELDER - EXTRANET-ID), WP': ['EXN2P16I'] * 59,
        'Comment (KOMMENTAR)': [''] * 59,
        'Comment, Reporter (MELDUNG - KOMMENTAR)': [''] * 59,
        'Comment, (MELDUNG - FORMULAR - KOMMENTAR)': [''] * 59,
        'Regional Office': [''] * 59,
        'Category of bank': [''] * 59,
        'Legal form of reporting party (RECHTSFORM)': [''] * 59,
        'Signature': [''] * 59,
        'Local Number': [''] * 59,
        'Check Name': [''] * 59,
        'Banking Group, Sender (ABSENDER - IDENTNR: BLZ, KAGNR, RZLZ), WP': [''] * 59,
        'LCB area': [''] * 59,
        'Financial institution': [''] * 59,
        'Registration number': [''] * 59,
        'Payment Service Providers (PSPs)': [''] * 59,
        'E-money institution': [''] * 59,
        'Quantity factor for numerical field entries': [''] * 59,
        'Transaction type': [''] * 59,
        'Buchwaehrung field for DatabaseRef page (ISO currency code (ISO 4217))': ['EUR'] * 59,
        'Typ field for DatabaseRef page (MELDUNG - typ: AUSFI: BSS_Elec_sub - Filiale or Gesamt , AUSTA: DOMESTIC - Inland,  BRANCHES - Filiale, SUBSIDIARIES - Tochter)': ['', 'Inland', 'Inland', 'Inland', 'Inland', 'Inland', 'Inland', 'Inland', 'Inland', 'Inland', 'Inland'] + [''] * 48,
        'Marker for "field content reported in Euro" (meldewaehrungEuro: ja, nein)': ['ja'] * 59,
        'Melder name for WP report': [''] * 59,
        'Type of reporting institution, Cross_Form_Validation': [''] * 59,
        'Meldungsref': [''] * 59
    }
    
    df_client_static = pd.DataFrame(client_static_data)

    # Position
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_position
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_position = get_filtered_df(conn, query_position, 'ID', 'LKUPISADVANCENOTLOAN')
    df_position = convert_dates(df_position, ['STARTDATE', 'MATURITYDATE', 'DUEDATE'])

    # Position Extended
    query_position_ext = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_position_extended
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_position_ext = get_filtered_df(conn, query_position_ext, 'ID', 'CARRYINGAMOUNT')

    # Counterparty
    query_counterparty = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_counterparty
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_counterparty = get_filtered_df(conn, query_counterparty, 'ID', 'LKUPISHOLDINGCOMPANY')

    # Other Assets Liabilities
    query_other_assets = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_other_assets_liabilities
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_other_assets = get_filtered_df(conn, query_other_assets, 'ID', 'INSTID')

    # Report Data - null with hardcoded columns
    df_report_data = pd.DataFrame(columns=[
        'entityId', 'reportName', 'sheetName', 'lineItem', 'subLineItem', 'colItem', 'amount', 'textValue'
    ])

    # Profit and Loss - null with hardcoded columns
    df_profit_loss = pd.DataFrame(columns=[
    'ORGUNITID',
    'CPTYID', 
    'AMOUNT', 
    'LKUPPERIODINDICATOR', 
    'LKUPCURRENCY', 
    'LKUPINCOMEEXPENSE', 
    'LKUPPLCLASSIFICATION', 
    'LKUPPRODTYPE', 
    'LKUPRESIDENCEOVERRIDE', 
    'LKUPSECTOROVERRIDE', 
    'LKUPINDUSTRYOVERRIDE', 
    'LKUPACCOUNTINGPORTFOLIOTYPE', 
    'LKUPASSETLIABILITYOFUNDERLYING', 
    'LKUPHEDGING', 
    'LKUPISSUBORDINATED', 
    'LKUPISINTERGROUP', 
    'LKUPISINTRAGROUP', 
    'LKUPISOWNISSUED', 
    'CUSTOM1', 
    'CUSTOM2', 
    'CUSTOM3', 
    'LKUPISQUOTED', 
    'LKUPCOLLATERALTYPE', 
    'LKUPISINVESTMENTPROPERTY', 
    'GLACCOUNT', 
    'LKUPISNONPERFORMING', 
    'NOTICEPERIOD', 
    'LKUPLOANDEPOSITTERM', 
    'LKUPIMPAIRMENTSTAGE', 
    'LKUPECONOMICHEDGE', 
    'LKUPISGENERALALLOWANCE', 
    'EXCLUSIONFLAG', 
    'LKUPISDERECOGNISE'
    ])

    # FX Rates - null sheet (empty)
    fx_rates_data = {
        'LKUPFROMCURRENCY': ['JPY', 'SEK', 'MXN', 'BRL', 'IDR', 'CZK', 'NZD', 'TRY', 'CHF', 'RON', 'MYR', 'BGN', 'NOK',	'ZAR', 'HUF', 'ISK', 'HKD', 'CAD', 'USD', 'INR', 'ILS', 'AUD', 'DKK', 'GBP', 'PLN', 'CNY', 'THB', 'PHP', 'SGD', 'KRW', 'TWD', 'RUB', 'COP', 'CLP', 'ARS', 'MAD', 'PEN', 'KZT', 'BMD', 'PGK', 'ANG', 'EUR'],
        'LKUPTOCURRENCY': ['EUR'] * 42,
        'LKUPTOCURRENCY': [1] * 42,
        'CUSTOM1': [''] * 42,
        'CUSTOM2': [''] * 42,
        'CUSTOM3': [''] * 42,
        'RATEAVGBOE': [''] * 42,
        'RATEAVGYTD': [1] * 42,
        'RATEAVGQTD': [''] * 42
    }
    
    df_fx_rates = pd.DataFrame(fx_rates_data)

    # Entity Hierarchy - hard coded data
    entity_hierarchy_data = {
        'ID': ['1001', '1002', '1098', '1101', '5', '1001133', '1001134', '10803007_1'],
        'NAME': ['Trade Republic Bank','Trade Republic Service','Trade Republic Hurdle Verwaltungs UG','Trade Republic Custody','Trade Republic Hurdle 2 UG & CO KG','YouCo B22-H131 Vorrats-AG','Youco B23-H395 Vorrats-GmbH','TR III GmbH'],
        'ENTITY_ID': [1001] * 8,
        'LKUP_TRADING_BOOK': ['yes', 'no', 'no', 'no', 'no', 'no', 'no', 'no'],
        'LKUP_RESIDENCE':['DEU', 'DEU', 'DEU', 'AUT', 'DEU', 'DEU', 'DEU', 'DEU'],
        'LKUP_LOCAL_CURRENCY': ['EUR'] * 8,
        'LKUP_RELATIONSHIP': ['na', 'associate', 'associate', 'associate', 'associate', 'associate', 'associate', 'associate'],
        'CUSTOM1': [''] * 8,
        'CUSTOM2': [''] * 8,
        'CUSTOM3': [''] * 8
    }
    
    df_entity_hierarchy = pd.DataFrame(entity_hierarchy_data)

    # Instrument
    query_instrument = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_instrument
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_instrument = get_filtered_df(conn, query_instrument, 'ID', 'LKUPCOUNTRYOFISSUE')
    df_instrument = convert_dates(df_instrument, ['STARTDATE'])

    # PL Movements - null with hardcoded columns
    df_pl_movements = pd.DataFrame(columns=[
    'ID', 
    'GLACCOUNT', 
    'MOVEMENTDATE', 
    'LKUPINCREASEDECREASE', 
    'LKUPMOVEMENTTYPE', 
    'AMOUNT'
    ])

    # Entity - hard coded data
    entity_data = {
        'ID': ['1001', '1001_solo', '1001_conso', '1098', '1101', '1002', '5', '1001133', '1001134', '10803007_1'],
        'NAME': ['Trade Republic Bank', 'Trade Republic Bank Solo', 'Trade Republic Bank Group','Trade Republic Hurdle Verwaltungs UG','Trade Republic Custody','Trade Republic Service','Trade Republic Hurdle 2 UG & CO KG','YouCo B22-H131 Vorrats-AG','Youco B23-H395 Vorrats-GmbH','Trade Republic Business III GmbH'],
        'LKUP_RESIDENCE':['DEU','DEU', 'DEU', 'DEU', 'AUT', 'DEU', 'DEU', 'DEU', 'DEU','DEU'],
        'LKUP_NATIONALITY':['DEU', 'DEU','DEU', 'DEU', 'AUT', 'DEU', 'DEU', 'DEU', 'DEU','DEU'],
        'LKUP_REP_CURRENCY': ['EUR'] * 10,
        'LKUP_LOCAL_CURRENCY': ['EUR'] * 10,
        'NUM_EMPLOYEES': [''] * 10,
        'NUM_EMPLOYEES_TOTAL': [''] * 10,
        'CUSTOM1': [''] * 10,
        'CUSTOM2': [''] * 10,
        'CUSTOM3': [''] * 10,
        'START_DATE_FINANCIAL_YEAR': ['01-Oct-2022'] * 10,
    }
    
    df_entity = pd.DataFrame(entity_data)

    # Facility - null with hardcoded columns
    df_facility = pd.DataFrame(columns=[
    'ID', 
    'ORGUNITID', 
    'TOTALFACILITYAMOUNT', 
    'LKUPCURRENCY'
    ])

    # OAL Movements - null with hardcoded columns
    df_oal_movements = pd.DataFrame(columns=[
    'ID', 
    'GLID', 
    'MOVEMENTDATE', 
    'LKUPINCREASEDECREASE', 
    'LKUPMOVEMENTTYPE', 
    'AMOUNT'
    ])

    # Position Movements
    query_position_movements = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_position_movements
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_position_movements = get_filtered_df(conn, query_position_movements, 'ID', 'EXCLUSIONFLAG_GB')
    df_position_movements = convert_dates(df_position_movements, ['MOVEMENTDATE'])

    # Write Core Excel file
    with pd.ExcelWriter(output_core, engine='xlsxwriter') as writer:
        # Write green tabs first to appear at front
        df_client_static.to_excel(writer, sheet_name='Client_static', index=False)
        df_fx_rates.to_excel(writer, sheet_name='FX Rates', index=False)
        df_entity_hierarchy.to_excel(writer, sheet_name='Entity Hierarchy', index=False)
        df_entity.to_excel(writer, sheet_name='Entity', index=False)

        # Write other sheets after green tabs
        df_position.to_excel(writer, sheet_name='Position', index=False)
        df_position_ext.to_excel(writer, sheet_name='Position Extended', index=False)
        df_counterparty.to_excel(writer, sheet_name='Counterparty', index=False)
        df_other_assets.to_excel(writer, sheet_name='Other Assets Liabilities', index=False)
        df_report_data.to_excel(writer, sheet_name='Report Data', index=False)
        df_profit_loss.to_excel(writer, sheet_name='Profit and Loss', index=False)
        df_instrument.to_excel(writer, sheet_name='Instrument', index=False)
        df_pl_movements.to_excel(writer, sheet_name='PL Movements', index=False)
        df_facility.to_excel(writer, sheet_name='Facility', index=False)
        df_oal_movements.to_excel(writer, sheet_name='OAL Movements', index=False)
        df_position_movements.to_excel(writer, sheet_name='Position Movements', index=False)

        workbook = writer.book

    # -------------- DE INPUT ----------------

    # Counterparty DE
    query_counterparty_de = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_counterparty_de
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_counterparty_de = get_filtered_df(conn, query_counterparty_de, 'ID', 'LKUPISSYSTEMICINVESTMENTFIRM')

    # Position DE
    query_position_de = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_position_de
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_position_de = get_filtered_df(conn, query_position_de, 'ID', 'LKUPISCONSUMPTIONPURPOSE')

    # Profit and Loss DE - null
    df_profit_loss_de = pd.DataFrame(columns=[
    'ID', 
    'LKUPISEXCEPTIONAL', 
    'EXCLUSIONFLAG' 
    ])

    # Entity DE - null
    df_entity_de = pd.DataFrame(columns=[
    'ID', 
    'LKUPISREPORTINGENTITY', 
    'LKUPISINDEPENDENTENTERPRISE',
    'LKUPISBRANCHPERMANENTESTAB',
    'LKUPISFIRSTSUBMISSION',
    'LKUPISNEWLYESTABLISHED',
    'LKUPISPURCHASEORSALE',
    'LKUPISOVERTHRESHOLD',
    'REGISTEREDADDRESS',
    'ECONOMICSECTOR',
    'LEGALFORM',
    'BALANCESHEETTOTAL',
    'BALANCESHEETTOTALAFFILIATE',
    'ANNUALTURNOVER',
    'ANNUALTURNOVERAFFILIATE',
    'NUMEMPLOYEESAFFILIATE',
    'BANKCODE',
    'BANKCODECHECKDIGIT'
    ])

    # Entity Hierarchy DE - null
    df_entity_hierarchy_de = pd.DataFrame(columns=[
    'ID',
    'SERIALNUMBER',
    'ADDRESS',
    'ENTITYISIN',
    'NAMEOFDIRECTCOMPANY',
    'DEPENDANYSTATUS',
    'INDUSTRYSECTOR',
    'BRANCHESTABLISHMENT',
    'VOTINGRIGHTSPERCENTAGE',
    'MARKETVALUE',
    'ANNUALTURNOVER',
    'NUMEMPLOYEES',
    'BALANCESHEETDATE',
    'LKUPCOUNTRYOFREGISTEREDTRUST',
    'LKUPFOREIGNENTITYSTATUS',
    'LKUPISNOLONGERREPORTED',
    'LKUPISNEWLYESTABLISHED',
    'LKUPISPURCHASEORSALE',
    'LKUPISOVERTHRESHOLD',
    'LKUPISUNDERTHRESHOLD',
    'LKUPISSALETORESIDENT',
    'LKUPISMERGERLIQUIDATION',
    'LKUPISSALETONONRESIDENT',
    'LKUPISDIRECTPARTICIPATION',
    'LKUPISINDIRECTPARTICIPATION',
    'LKUPISIMMEDIATEPARTICIPATION',
    'LKUPISINDEPENDENT',
    'LKUPISBRANCHESTABLISHMENT',
    'ITEM11AMOUNT',
    'ITEM12AMOUNT',
    'ITEM13AMOUNT',
    'ITEM54AMOUNT',
    'ITEM55AMOUNT',
    'ITEM49AMOUNT',
    'ITEM16AMOUNT',
    'ITEM50AMOUNT',
    'ITEM15AMOUNT',
    'ITEM17AMOUNT',
    'ITEM51AMOUNT',
    'ITEM20AMOUNT',
    'ITEM52AMOUNT',
    'ITEM19AMOUNT',
    'ITEM21AMOUNT',
    'ITEM22AMOUNT',
    'ITEM23AMOUNT',
    'ITEM24AMOUNT',
    'ITEM25AMOUNT',
    'ITEM29AMOUNT',
    'ITEM30AMOUNT',
    'ITEM53AMOUNT',
    'ITEM31AMOUNT',
    'ITEM32AMOUNT',
    'ITEM33AMOUNT',
    'ITEM35AMOUNT',
    'ITEM36AMOUNT',
    'ITEM37AMOUNT',
    'ITEM38AMOUNT',
    'ITEM39AMOUNT',
    'ITEM40AMOUNT',
    'INVESTMENTINPPE',
    'PERSONNELEXPENSES'
    ])

    # Report Data K4 Asset - null
    df_report_data_k4_asset = pd.DataFrame(columns=[
    'ID_NUMBER',
    'ENTITY',
    'LINE_ITEM',
    'SERIAL_NO',
    'AMOUNT'
    ])

    # Report Data K4 comp - null
    df_report_data_k4_comp = pd.DataFrame(columns=[
    'ID_NUMBER',
    'ENTITY',
    'SERIAL_NO',
    'REPORTED_OR_NOT',
    'FIRM_NAME',
    'FIRM_LOCATION',
    'INTST_A',
    'INTST_B',
    'INTST_C',
    'INTST_D',
    'INTST_E',
    'INTST_F',
    'INTST_G',
    'DIR_PART_INT_MAR_CAP',
    'ISIN',
    'DIR_PAR_INT',
    'INDIR_PAR_INT',
    'COUNTRY',
    'COUNTRY_TRUST',
    'NOTES',
    'SIGNATURE',
    'DES_RES',
    'MARKET_CAP_47',
    'SH_VOTING_RIGHTS',
    'AN_TURN_04',
    'NUMB_EMP_05'
    ])

    # Report Data K4 part - null
    df_report_data_k4_part = pd.DataFrame(columns=[
    'ENTITY',
    'FIRM_NM',
    'ADDRESS',
    'EC_SEC_OCC',
    'LEG_FORM_PR',
    'ENTERPR_INDEP',
    'BRANCH_ESTABL',
    'NEW_EST_ENTERP',
    'PURCH_MERGER',
    'OVERSH_TRESH'
    ])

    # Report Data SAKI - null
    df_report_data_saki = pd.DataFrame(columns=[
    'ID_NUMBER',
    'ENTITY',
    'REPORTNAME',
    'LINE_ITEM',
    'AMOUNT',
    'NUMB',
    'PERC'
    ])

    # Report Data DE - null
    df_report_data_de = pd.DataFrame(columns=[
    'ENTITY',
    'REPORTNAME',
    'SHEET_NAME',
    'LINE_ITEM',
    'SUB_LINE_ITEM',
    'COL_ITEM',
    'AMOUNT',
    'TEXTVALUE',
    'RESIDENCE',
    'CODE_905',
    'CODE_906',
    'CUSTOM1',
    'CUSTOM2',
    'CUSTOM3',
    'CUSTOM4',
    'CUSTOM5',
    'CUSTOM6'
    ])

    # Report Data Domestic - null
    df_report_data_domestic = pd.DataFrame(columns=[
    'ID',
    'ENTITYID',
    'REPORT_CODE',
    'LINE_ITEM',
    'LAND_NAME',
    'CURRENCY',
    'EUROPEAN_MON_UNION_TOT',
    'EUROPEAN_MON_UNION',
    'AMOUNT',
    'TOTAL_AMT',
    'TOTAL_GERMANY',
    'AMOUNT_EURO',
    'FX_RATE',
    'COL_CODE',
    'TOT_LAND_CODE_ALPHA2',
    'TOT_CUR_CODE',
    'TOT_LAND_CODE'
    ])

    # Report Data GVKI - null
    df_report_data_gvki = pd.DataFrame(columns=[
    'ID_NUMBER',
    'ENTITY',
    'REPORTNAME',
    'LINE_ITEM',
    'AMOUNT',
    'DATE_BIL'
    ])

    # Report Data K3 Asset - null
    df_report_data_k3_asset = pd.DataFrame(columns=[
    'ID_NUMBER',
    'ENTITY',
    'LINE_ITEM',
    'SERIAL_NO',
    'AMOUNT'
    ])

    # Report Data K3 comp - null
    df_report_data_k3_comp = pd.DataFrame(columns=[
    'ID_NUMBER',
    'ENTITY',
    'SERIAL_NO',
    'REPORTED_OR_NOT',
    'FIRM_NAME',
    'FIRM_LOCATION',
    'INTST_A',
    'INTST_B',
    'INTST_C',
    'INTST_D',
    'INTST_E',
    'INTST_F',
    'INTST_G',
    'DIR_PART_INT_MAR_CAP',
    'DEP_STAT',
    'EC_SEC',
    'ISIN',
    'DIR_PAR_INT',
    'COUNTRY',
    'IDIR_PAR_INT',
    'ENT_IND_LEG_STAT',
    'BR_PER_EST',
    'DES_NON_RES',
    'NOTES',
    'SIGNATURE',
    'MARKET_CAP_47',
    'SH_VOTING_RIGHTS',
    'AN_TURN_04',
    'NUMB_EMP_05',
    'CURRENCY_07'
    ])

    # Report Data K3 part - null
    df_report_data_k3_part = pd.DataFrame(columns=[
    'ID_NUMBER',
    'ENTITY',
    'FIRM_NM',
    'ADDRESS',
    'EC_SEC_OCC',
    'LEG_FORM_PR',
    'PART_REQ',
    'STAT_NM',
    'DATA_NAT',
    'DATA_INTER',
    'BAL_SHT_TOT_01',
    'AN_TURN_02',
    'NUMB_EMP_03',
    'BAL_SHT_TOT_04',
    'AN_TURN_05',
    'NUMB_EMP_06'
    ])

    # Report Data WP - null
    df_report_data_wp = pd.DataFrame(columns=[
    'ID_NUMBER',
    'ENTITY',
    'LINE_ITEM',
    'SHEET_NAME',
    'ISIN',
    'INT_SIN',
    'SEC_NAME',
    'SEC_CURR',
    'SEC_PRICE',
    'SEC_TYPE',
    'SEC_MATUR',
    'COUP_DATE',
    'ISS_GROUP',
    'ISS_COUNTRY',
    'SECTOR',
    'SECTOR_COUNTRY',
    'STOCK',
    'TR_BOOK_PF',
    'BOOK_VALUE',
    'BOOK_VALUE_TR_PF',
    'LZBEGINN',
    'LZENDE',
    'NORMAL_STOCK',
    'REP_SEC_LEND',
    'REP_SEC_BORR',
    'INT_RATE_TYPE',
    'NOMINAL_CURR_AV',
    'NOMINAL_UNIT_AV',
    'NOMINAL_CURR_NOT_AV',
    'NOMINAL_UNIT_NOT_AV',
    'NUM_DEPOS',
    'BOOK_VALUE_AMT',
    'BOOK_VALUE_TR_PF_AMT',
    'TR_BOOK_PF_AMT',
    'BOOK_VALUE_ST_AMT',
    'BK_VAL_TR_PF_AMT_CALC',
    'NORMAL_STOCK_AMT',
    'REP_SEC_LEND_AMT',
    'REP_SEC_BORR_AMT',
    'INT_RATE'
    ])

    # Report Data Z5 - null
    df_report_data_z5 = pd.DataFrame(columns=[
    'ID',
    'ENTITYID',
    'COUNTRY_NAME',
    'COUNTRY_CODE',
    'CURRENCY_NAME',
    'CURRENCY_CODE',
    'COL_ITEM',
    'AMOUNT'
    ])
    
    # Write DE Excel file
    with pd.ExcelWriter(output_de, engine='xlsxwriter') as writer:
        df_counterparty_de.to_excel(writer, sheet_name='Counterparty DE', index=False)
        df_position_de.to_excel(writer, sheet_name='Position DE', index=False)
        df_profit_loss_de.to_excel(writer, sheet_name='Profit and Loss DE', index=False)
        df_entity_de.to_excel(writer, sheet_name='Entity DE', index=False)
        df_entity_hierarchy_de.to_excel(writer, sheet_name='Entity Hierarchy DE', index=False)
        df_report_data_k4_asset.to_excel(writer, sheet_name='Report Data K4 Asset', index=False)
        df_report_data_k4_comp.to_excel(writer, sheet_name='Report Data K4 comp', index=False)
        df_report_data_k4_part.to_excel(writer, sheet_name='Report Data K4 part', index=False)
        df_report_data_saki.to_excel(writer, sheet_name='Report Data SAKI', index=False)
        df_report_data_de.to_excel(writer, sheet_name='Report Data DE', index=False)
        df_report_data_domestic.to_excel(writer, sheet_name='Report Data Domestic', index=False)
        df_report_data_gvki.to_excel(writer, sheet_name='Report Data GVKI', index=False)
        df_report_data_k3_asset.to_excel(writer, sheet_name='Report Data K3 Asset', index=False)
        df_report_data_k3_comp.to_excel(writer, sheet_name='Report Data K3 comp', index=False)
        df_report_data_k3_part.to_excel(writer, sheet_name='Report Data K3 part', index=False)
        df_report_data_wp.to_excel(writer, sheet_name='Report Data WP', index=False)
        df_report_data_z5.to_excel(writer, sheet_name='Report Data Z5', index=False)
    
   # -------------- FIN INPUT ----------------

    # Position FIN
    query_position_fin = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_position_fin
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_position_fin = get_filtered_df(conn, query_position_fin, 'ID', 'LKUPISNONPERFORMINGCALCEXCLUSION')
    
    # Position Extended FIN
    query_position_extended_fin = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_position_extended_fin
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_position_extended_fin = get_filtered_df(conn, query_position_extended_fin, 'ID', 'FAIRVALUEDUETOINTERESTRATERISK')
    
    # Counterparty FIN
    query_counterparty_fin = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_counterparty_fin
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_counterparty_fin = get_filtered_df(conn, query_counterparty_fin, 'ID', 'CONTAGIONGROUPID')
    
    # Other Assets Liabilities FIN
    query_other_assets_liabilities_fin = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_other_assets_liabilities_fin
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_other_assets_liabilities_fin = get_filtered_df(conn, query_other_assets_liabilities_fin, 'ID', 'LKUPISIRBINSTITUTIONCLASS')
    
    # Table Profit and Loss FIN
    query_profit_and_loss_fin = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_profit_and_loss_fin
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ID
    """
    df_profit_and_loss_fin = get_filtered_df(conn, query_profit_and_loss_fin, 'ID', 'LKUPISNONPERFORMINGCALCEXCLUSION')
    
    # ax_filing_indicators - null with hardcoded columns
    df_filing_indicators = pd.DataFrame(columns=[
    'AX_REPORT_NAME', 
    'AX_FILLING_REFERENCE', 
    'ENTITY_ID', 
    'AX_NON_PERFORM_THRESHOLD'
    ])
    
    # Collateral Possessed FIN - null with hardcoded columns
    df_collateral_possessed_fin = pd.DataFrame(columns=[
    'ID',
    'ORGUNITID',
    'INITIALRECOGNITIONDATE',
    'INITIALRECOGNITIONAMOUNT',
    'CARRYINGAMOUNT',
    'LOANGROSSCARRYINGAMOUNT',
    'LOANIMPAIRMENT',
    'LOANPROVISION',
    'LOANCREDITRISKAMOUNT',
    'CASHCOLLECTEDAMOUNT',
    'FINANCINGGRANTEDAMOUNT',
    'LKUPPOSSESSIONEVENT',
    'LKUPPOSSESSEDASSET',
    'LKUPISNONCURRHELDFORSALE',
    'LKUPCURRENCY',
    'INITIALRECOGNITIONOPGAMOUNT',
    'CARRYINGOPENINGAMOUNT',
    'LOANGROSSCARRYINGOPENINGAMOUNT',
    'LOANIMPAIRMENTOPENING',
    'LOANPROVISIONOPENING',
    'LOANCREDITRISKOPENINGAMOUNT'
    ])
    
    # Entity FIN - null with hardcoded columns
    entity_fin_data = {
        'ID': ['1001', '1001_solo', '1001_conso'],
        'LKUPISACCOUNTMOVEMENTONLY':['nGAAP'] * 3,
        'LKUPREPORTINGLEVELTYPE':['na'] * 3,
        'LKUPREPORTINGLEVELTYPE': ['individual', 'individual', 'consolidated'],
        'LKUPISSMALLNONCOMPLEX': ['yes'] * 3,
        'LKUPISTHRESHOLDEXCEEDEDF20': ['yes'] * 3,
    }
    
    df_entity_fin = pd.DataFrame(entity_fin_data)
    
    # Equity Change FIN - null with hardcoded columns
    df_equity_change_fin = pd.DataFrame(columns=[
    'ID',
    'AMOUNT',
    'LKUPEQUITYITEM',
    'LKUPEQUITYMOVEMENT',
    'LKUPINCREASEDECREASE',
    'ORGUNITID',
    'LKUPCURRENCY'
    ])
    
    # Benefit Plan FIN - null with hardcoded columns
    df_benefit_plan_fin = pd.DataFrame(columns=[
    'ID',
    'FAIRVALUEAMOUNT',
    'LKUPBENEFITPLANINSTRUMENT',
    'LKUPISOWNISSUED',
    'NETASSETAMOUNT',
    'PROVISIONOBLIGATIONAMOUNT',
    'ASSETCEILINGAMOUNT',
    'RIGHTTOREIMBURSEAMOUNT',
    'PRESENTVALUEOBLIGATIONAMOUNT',
    'ORGUNITID',
    'LKUPCURRENCY'
    ])
                                       
    # Benefit Plan Movements FIN - null with hardcoded columns
    df_benefit_plan_movements = pd.DataFrame(columns=[
    'ID',
    'LKUPBENEFITMOVEMENTTYPE',
    'LKUPINCREASEDECREASE',
    'AMOUNT',
    'ORGUNITID',
    'LKUPCURRENCY'
    ])
                                             
    # Report Data F40_01 - null with hardcoded columns
    df_report_data_f_40_01 = pd.DataFrame(columns=[
    'ENTITYID',
    'REPORT_ID',
    'INVESTEE',
    'INVESTEECODETYPE',
    'LINEITEM',
    'COLITEM',
    'AMOUNT',
    'TEXTVALUE',
    'DATEVALUE'
    ])
    
    report_data_f_40_01 = {
        'ENTITYID' : ['1001', '1098', '1101', '1002', '5', '1001133', '1001134'],
        'REPORT_ID' : ['F40_01'] * 7,
        'INVESTEE' : ['Trade Republic Bank','Trade Republic Hurdle Verwaltungs UG','Trade Republic Custody','Trade Republic Service','Trade Republic Hurdle 2 UG & CO KG','YouCo B22-H131 Vorrats-AG','Youco B23-H395 Vorrats-GmbH'],
        'INVESTEECODETYPE' : [''] * 7,
        'LINEITEM' : [''] * 7,
        'COLITEM' : [''] * 7,
        'AMOUNT' : ['', 3000, 35000, 125000, '', 50000, 25000],
        'TEXTVALUE' : [''] * 7,
        'DATEVALUE' : [''] * 7
    }
    
    df_report_data_f_40_01 = pd.DataFrame(report_data_f_40_01)
    
    # Report Data F40_02 - null with hardcoded columns
    df_report_data_f_40_02 = pd.DataFrame(columns=[
    'ENTITYID',
    'REPORT_ID',
    'INVESTEE',
    'INVESTEECODETYPE',
    'SECURITY',
    'HOLDINGCOMPANY',
    'HOLDINGCODETYPE',
    'LINEITEM',
    'COLITEM',
    'AMOUNT',
    'TEXTVALUE'
    ])
    
    # Report Data FIN - null with hardcoded columns
    df_report_data_fin = pd.DataFrame(columns=[
    'ENTITYID',
    'REPORT_ID',
    'LINEITEM',
    'COLITEM',
    'RESIDENCE',
    'AMOUNT',
    'NOMINAL_AMOUNT',
    'COLLATERAL_AMOUNT',
    'GUARANTEE_AMOUNT',
    'ACCUMULATED_IMPAIRMENT',
    'CREDIT_RISK_AMOUNT',
    'GROSS_CARRYING_AMOUNT',
    'WRITEOFFAMOUNT',
    'TEXTVALUE',
    'CNTINSTRUMENT'
    ])
                                      
    # Report Data C33 - null with hardcoded columns
    df_report_data_c33 = pd.DataFrame(columns=[
    'ENTITYID',
    'REPORT_ID',
    'LINEITEM',
    'COLITEM',
    'RESIDENCE',
    'EXPOSUREVALUE',
    'GROSSCARRYINGAMOUNT',
    'NOTINALAMOUNT',
    'RISKWEIGHTEDEXPOSUREAMOUNT',
    'NOMINALAMOUNT',
    'ACCUMULATEDNEGATIVECHANGES',
    'NONDERIVATIVECARRYINGAMOUNT',
    'CARRYINGAMOUNT',
    'ACCUMULATEDIMPAIRMENT',
    'TEXTVALUE'
    ])                                                                                                                                   
 
    # Write FIN Excel file
    with pd.ExcelWriter(output_fin, engine='xlsxwriter') as writer:
        df_entity_fin.to_excel(writer, sheet_name='Entity FIN', index=False)
        df_report_data_f_40_01.to_excel(writer, sheet_name='Report Data F40_01', index=False)
        df_position_fin.to_excel(writer, sheet_name='Position FIN', index=False)
        df_position_extended_fin.to_excel(writer, sheet_name='Position Extended FIN', index=False)
        df_counterparty_fin.to_excel(writer, sheet_name='Counterparty FIN', index=False)
        df_other_assets_liabilities_fin.to_excel(writer, sheet_name='Other Assets Liabilities FIN', index=False)
        df_profit_and_loss_fin.to_excel(writer, sheet_name='Profit and Loss FIN', index=False)
        df_filing_indicators.to_excel(writer, sheet_name='ax_filing_indicators', index=False)
        df_collateral_possessed_fin.to_excel(writer, sheet_name='Collateral Possessed FIN', index=False)
        df_equity_change_fin.to_excel(writer, sheet_name='Equity Change FIN', index=False)
        df_benefit_plan_fin.to_excel(writer, sheet_name='Benefit Plan FIN', index=False)
        df_benefit_plan_movements.to_excel(writer, sheet_name='Benefit Plan Movements FIN', index=False)
        df_report_data_f_40_02.to_excel(writer, sheet_name='Report Data F40_02', index=False)
        df_report_data_fin.to_excel(writer, sheet_name='Report Data FIN', index=False)
        df_report_data_c33.to_excel(writer, sheet_name='Report Data C33', index=False)
       
    print(f"Core input exported successfully to {output_core}")
    print(f"DE input exported successfully to {output_de}")
    print(f"FIN input exported successfully to {output_fin}")

finally:
    conn.close()
