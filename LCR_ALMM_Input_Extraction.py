import snowflake.connector
import pandas as pd

# Parameters
report_dt = '2025-06-30'
snapshot_dt = '2025-07-27'
report_granularity = 'month'

output_liquidity = f"input_extractors/output_files/Liquidity_CalcsInput_{report_dt}.xlsx"

# Helper function to get filtered dataframe based on start and end columns (inclusive)
def get_filtered_df(conn, query, start_col, end_col):
    df = pd.read_sql(query, conn)
    start_idx = df.columns.get_loc(start_col)
    end_idx = df.columns.get_loc(end_col)
    return df.iloc[:, start_idx:end_idx+1]

# Helper function to convert specified columns to datetime with format YYYYMMDD
def convert_dates(df, date_columns):
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y%m%d')
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
    # -------------- LIQUIDITY CALCS INPUT ----------------

# CashflowCalc_Input
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__cashflow_lcr
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ORG_UNIT
    """
    df_cashflowcalc_input = get_filtered_df(conn, query_position, 'ORG_UNIT', 'CUSTOM_5')
    df_cashflowcalc_input = convert_dates(df_cashflowcalc_input, ['MATURITY_DATE', 'START_DATE'])
    
# PositionCalc_Input
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_positioncalc_input
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ORG_UNIT
    """
    df_positioncalc_input = get_filtered_df(conn, query_position, 'ORG_UNIT', 'CUSTOM_5')
    df_positioncalc_input = convert_dates(df_positioncalc_input, ['MATURITY_DATE', 'START_DATE'])

# CounterpartyCalc_Input
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_counterpartycalc_input
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY cparty_id
    """
    df_counterpartycalc_input = get_filtered_df(conn, query_position, 'CPARTY_ID', 'NATIONAL_CODE')
    
# ForeignExchangeRate
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_scd__regulatory_reporting__tbl_foreignexchangerate
    WHERE report_dt = '{report_dt}' 
    ORDER BY REPORTING_CCY_CODE
    """
    df_foreignexchangerate = get_filtered_df(conn, query_position, 'REPORTING_CCY_CODE', 'FX_RATE')
    
# ReportingEntity
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_reportingentity
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ENTITY
    """
    df_reportingentity = get_filtered_df(conn, query_position, 'ENTITY', 'NATIONAL_CODE')
    
# CapitalInstrument_Input
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_capitalinstrumentcalc_input
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ENTITY
    """
    df_capitalinstrument_input = get_filtered_df(conn, query_position, 'ENTITY', 'CUSTOM_3')
    df_capitalinstrument_input = convert_dates(df_capitalinstrument_input, ['MATURITY_DATE'])
    
# BenchmarkData_Input
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__benchmark_data_input
    ORDER BY ORG_UNIT
    """
    df_benchmarkdata_input = get_filtered_df(conn, query_position, 'ORG_UNIT', 'INTEREST_RATE_EFFECTIVE_DATE')
    
# ReportingParameters
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_reportingparameters
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ENTITY
    """
    df_reportingparameters = get_filtered_df(conn, query_position, 'ENTITY', 'CUSTOM_PARAMETER')
    
# JurisdictionCode
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_jurisdictioncode
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY JURISDICTION_CODE
    """
    df_jurisdictioncode = get_filtered_df(conn, query_position, 'JURISDICTION_CODE', 'LIQ_PERIOD_MONTHS')
    
# ReportingEntitySubsidiaries
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_reportingentitysubsidiaries
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY PARENT_ENTITY
    """
    df_reportingentitysubsidiaries = get_filtered_df(conn, query_position, 'PARENT_ENTITY', 'NATIONAL_CODE')
    
# BusinessLineHierarchy
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_businesslinegrouphierarchy
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY BUSINESS_LINE_GROUP
    """
    df_businesslinehierarchy = get_filtered_df(conn, query_position, 'BUSINESS_LINE_GROUP', 'BUSINESS_LINE')
    
# BusinessLineGroup
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_businesslinegroup
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY BUSINESS_LINE_GROUP
    """
    df_businesslinegroup = get_filtered_df(conn, query_position, 'BUSINESS_LINE_GROUP', 'BUSINESS_LINE_GROUP_DESC')
    
# ReportingEntityHierarchy
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_reportingentityhierarchy
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ENTITY
    """
    df_reportingentityhierarchy = get_filtered_df(conn, query_position, 'ENTITY', 'DOM_CCY_OF_ORG_UNIT')
    
# BehaviouralFlowCalc_Input
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_behaviouralflowcalc_input
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ORG_UNIT
    """
    df_behaviouralflowcalc_input = get_filtered_df(conn, query_position, 'ORG_UNIT', 'CARRYING_AMOUNT_ORIG')
    
# PostCRMRiskWeights_Input
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_postcrmriskweights_input
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ENTITY
    """
    df_postcrmriskweights_input = get_filtered_df(conn, query_position, 'ENTITY', 'CUSTOM_5')
    
# SignificantCurrency
    query_position = f"""
    SELECT * FROM teams_prd.regulatory_reporting_mart.mrt_snapshot__regulatory_reporting__tbl_significantcurrency
    WHERE report_dt = '{report_dt}' AND snapshot_dt = '{snapshot_dt}' AND report_granularity = '{report_granularity}'
    ORDER BY ENTITY
    """
    df_significantcurrency = get_filtered_df(conn, query_position, 'ENTITY', 'CCY_DESCRIPTION')

        # Write Liquidity Excel file
    with pd.ExcelWriter(output_liquidity, engine='xlsxwriter') as writer:
        # Create tabs
        df_counterpartycalc_input.to_excel(writer, sheet_name='CounterpartyCalc_Input', index=False)
        df_cashflowcalc_input.to_excel(writer, sheet_name='CashflowCalc_Input', index=False)
        df_positioncalc_input.to_excel(writer, sheet_name='PositionCalc_Input', index=False)
        df_foreignexchangerate.to_excel(writer, sheet_name='ForeignExchangeRate', index=False)
        df_reportingentity.to_excel(writer, sheet_name='ReportingEntity', index=False)
        df_capitalinstrument_input.to_excel(writer, sheet_name='CapitalInstrument_Input', index=False)
        df_benchmarkdata_input.to_excel(writer, sheet_name='BenchmarkData_Input', index=False)
        df_reportingparameters.to_excel(writer, sheet_name='ReportingParameters', index=False)
        df_jurisdictioncode.to_excel(writer, sheet_name='JurisdictionCode', index=False)
        df_reportingentitysubsidiaries.to_excel(writer, sheet_name='ReportingEntitySubsidiaries', index=False)
        df_businesslinehierarchy.to_excel(writer, sheet_name='BusinessLineHierarchy', index=False)
        df_businesslinegroup.to_excel(writer, sheet_name='BusinessLineGroup', index=False)
        df_reportingentityhierarchy.to_excel(writer, sheet_name='ReportingEntityHierarchy', index=False)
        df_behaviouralflowcalc_input.to_excel(writer, sheet_name='BehaviouralFlowCalc_Input', index=False)
        df_postcrmriskweights_input.to_excel(writer, sheet_name='PostCRMRiskWeights_Input', index=False)
        df_significantcurrency.to_excel(writer, sheet_name='SignificantCurrency', index=False)
        
    # Set green tab color for specified sheets
        for green_tab in ['CounterpartyCalc_Input']:
            worksheet = writer.sheets.get(green_tab)
            if worksheet:
                worksheet.set_tab_color('00FF00')
  
    print(f"Liquidity input exported successfully to {output_liquidity}")


finally:
    conn.close()
