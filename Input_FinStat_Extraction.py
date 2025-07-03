import snowflake.connector
import pandas as pd

# Parameters
report_dt = '2025-05-31'
snapshot_dt = '2025-07-02'
report_granularity = 'month'

output_core = f"output_files/finstat_input/Input_Core_{report_dt}.xlsx"
output_de = f"output_files/finstat_input/Input_DE_{report_dt}.xlsx"

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
    df_client_static = pd.DataFrame()

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
    df_fx_rates = pd.DataFrame()

    # Entity Hierarchy - null sheet (empty)
    df_entity_hierarchy = pd.DataFrame()

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

    # Entity - null sheet (empty)
    df_entity = pd.DataFrame()

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
        # Set green tab color for specified sheets
        for green_tab in ['Client_static', 'FX Rates', 'Entity Hierarchy', 'Entity']:
            worksheet = writer.sheets.get(green_tab)
            if worksheet:
                worksheet.set_tab_color('00FF00')

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

    print(f"Core input exported successfully to {output_core}")
    print(f"DE input exported successfully to {output_de}")

finally:
    conn.close()
