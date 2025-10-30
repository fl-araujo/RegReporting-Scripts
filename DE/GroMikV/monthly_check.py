import snowflake.connector
from string import Template
import pandas as pd
import os
import numpy as np

# === PARAMETERS & PATHS ===
report_dt = '2025-04-30'
snapshot_dt = '2025-07-13'

sql_input_path_1 = 'gromikv_reporting/input_files/GroMikV_input.sql'
sql_input_path_2 = 'gromikv_reporting/input_files/dc-341.sql'
sql_input_path_3 = 'gromikv_reporting/input_files/party_groups.sql'

excel_output_path = f'gromikv_reporting/output_files/{report_dt} Monthly Analysis.xlsx'
output_folder = os.path.dirname(excel_output_path)
os.makedirs(output_folder, exist_ok=True)

# === CONNECT TO SNOWFLAKE ===
conn = snowflake.connector.connect(
    user='francisco.araujo@traderepublic.com',
    account='gm68377.eu-central-1',
    database='TEAMS_PRD',
    role='FINANCE',
    authenticator='externalbrowser',
)

# === EXECUTE SQL FILE 1 (Main Input) ===
with open(sql_input_path_1, 'r') as f:
    sql1 = Template(f.read()).substitute(report_dt=f"'{report_dt}'", snapshot_dt=f"'{snapshot_dt}'")
cursor = conn.cursor()
cursor.execute(sql1)
results1 = cursor.fetchall()
columns1 = [desc[0] for desc in cursor.description]
df = pd.DataFrame(results1, columns=columns1)
cursor.close()

for col in df.select_dtypes(include=['datetimetz']).columns:
    df[col] = df[col].dt.tz_localize(None)

most_freq_names = df.groupby('COUNTERPARTY_ID')['COUNTERPARTY_NAME'] \
                    .agg(lambda x: x.mode().iat[0] if not x.mode().empty else x.iloc[0])
df['COUNTERPARTY_NAME'] = df['COUNTERPARTY_ID'].map(most_freq_names)

replacement_id = 'F0HUI1NY1AZMJMD8LP67'
replacement_name = 'HSBC Continental Europe S.A., Germany'
ids_to_replace = ['JUNT405OW8OY5GN4DX16', 'DE05935']

df.loc[df['COUNTERPARTY_ID'].isin(ids_to_replace), 'COUNTERPARTY_ID'] = replacement_id
df.loc[df['COUNTERPARTY_ID'] == replacement_id, 'COUNTERPARTY_NAME'] = replacement_name

# === SQL FILE 2: dc-341.sql ===
with open(sql_input_path_2, 'r') as f:
    sql2 = Template(f.read()).substitute(report_dt=f"'{report_dt}'", snapshot_dt=f"'{snapshot_dt}'")
cursor = conn.cursor()
cursor.execute(sql2)
results2 = cursor.fetchall()
columns2 = [desc[0] for desc in cursor.description]
df_trading = pd.DataFrame(results2, columns=columns2)
cursor.close()

df_trading.rename(columns={
    'Counterparty ID': 'COUNTERPARTY_ID',
    'NAME': 'COUNTERPARTY_NAME',
    'Security Type': 'SECURITY_TYPE',
    'Total Market Value': 'TOTAL_MARKET_VALUE'
}, inplace=True)

pivot_df = df_trading.pivot_table(
    index=['COUNTERPARTY_ID', 'COUNTERPARTY_NAME'],
    columns='SECURITY_TYPE',
    values='TOTAL_MARKET_VALUE',
    aggfunc='sum',
    fill_value=0
).reset_index()

pivot_df.columns.name = None
pivot_df.rename(columns={
    'EQ': 'Trading_Book_EQ',
    'IR': 'Trading_Book_IR'
}, inplace=True)

# === FILTER OUT COUNTERPARTIES WITH NEGATIVE AGGREGATED VALUES ===
# Calculate total aggregated value for each counterparty
pivot_df['Total_Aggregated_Value'] = pivot_df.get('Trading_Book_EQ', 0) + pivot_df.get('Trading_Book_IR', 0)

# Filter out counterparties with negative aggregated values
pivot_df = pivot_df[pivot_df['Total_Aggregated_Value'] >= 0]

# Drop the temporary column
pivot_df.drop(columns=['Total_Aggregated_Value'], inplace=True)

# Update df_trading to only include counterparties with non-negative aggregated values
valid_counterparties = set(pivot_df['COUNTERPARTY_ID'])
df_trading = df_trading[df_trading['COUNTERPARTY_ID'].isin(valid_counterparties)]

# === AGGREGATION FOR ANALYSIS ===
filtered = df[df['EXPOSURE_AMT_EUR_ULTMT_PRNT'] >= 1_000_000]
agg_df = (
    filtered.groupby(['COUNTERPARTY_ID', 'COUNTERPARTY_NAME'], dropna=False)['EXPOSURE_AMT_EUR']
    .sum()
    .reset_index()
    .rename(columns={'EXPOSURE_AMT_EUR': 'SUM_EXPOSURE_AMT_EUR'})
    .sort_values(by='COUNTERPARTY_NAME')
)

agg_df = agg_df.merge(pivot_df[['COUNTERPARTY_ID', 'Trading_Book_EQ', 'Trading_Book_IR']], on='COUNTERPARTY_ID', how='left')
agg_df['Trading_Book_EQ'] = agg_df['Trading_Book_EQ'].fillna(0)
agg_df['Trading_Book_IR'] = agg_df['Trading_Book_IR'].fillna(0)

# === SQL FILE 3: party_groups.sql ===
with open(sql_input_path_3, 'r') as f:
    sql3 = Template(f.read()).substitute(snapshot_dt=f"'{snapshot_dt}'")
cursor = conn.cursor()
cursor.execute(sql3)
results3 = cursor.fetchall()
columns3 = [desc[0] for desc in cursor.description]
df_party_groups = pd.DataFrame(results3, columns=columns3)
cursor.close()

grouped_parties = df_party_groups.groupby('ID_ULTMT_PRNT')['ID_PRTY'].apply(set).to_dict()

included_ids = set(filtered['COUNTERPARTY_ID'])
name_lookup = df_trading.drop_duplicates('COUNTERPARTY_ID').set_index('COUNTERPARTY_ID')['COUNTERPARTY_NAME'].to_dict()
eq_lookup = df_trading[df_trading['SECURITY_TYPE'] == 'EQ'].groupby('COUNTERPARTY_ID')['TOTAL_MARKET_VALUE'].sum().to_dict()
ir_lookup = df_trading[df_trading['SECURITY_TYPE'] == 'IR'].groupby('COUNTERPARTY_ID')['TOTAL_MARKET_VALUE'].sum().to_dict()

new_rows = []
existing_ids = set(agg_df['COUNTERPARTY_ID'])

for ultimate_parent, associated_ids in grouped_parties.items():
    if not associated_ids & included_ids:
        continue

    if ultimate_parent not in existing_ids:
        eq_val = eq_lookup.get(ultimate_parent, 0)
        ir_val = ir_lookup.get(ultimate_parent, 0)
        # Only add if the aggregated value is non-negative
        if (eq_val + ir_val) >= 0 and (eq_val > 0 or ir_val > 0):
            name = name_lookup.get(ultimate_parent, f"Ultimate Parent {ultimate_parent}")
            new_rows.append({
                'COUNTERPARTY_ID': ultimate_parent,
                'COUNTERPARTY_NAME': name,
                'SUM_EXPOSURE_AMT_EUR': eq_val + ir_val,
                'Trading_Book_EQ': eq_val,
                'Trading_Book_IR': ir_val
            })
            existing_ids.add(ultimate_parent)

    new_ids = associated_ids - existing_ids
    for assoc_id in sorted(new_ids):
        eq_val = eq_lookup.get(assoc_id, 0)
        ir_val = ir_lookup.get(assoc_id, 0)
        # Only add if the aggregated value is non-negative and not zero
        if (eq_val + ir_val) >= 0 and (eq_val > 0 or ir_val > 0):
            name = name_lookup.get(assoc_id, f"Grouped under {ultimate_parent}")
            new_rows.append({
                'COUNTERPARTY_ID': assoc_id,
                'COUNTERPARTY_NAME': name,
                'SUM_EXPOSURE_AMT_EUR': eq_val + ir_val,
                'Trading_Book_EQ': eq_val,
                'Trading_Book_IR': ir_val
            })
            existing_ids.add(assoc_id)

if new_rows:
    agg_df = pd.concat([agg_df, pd.DataFrame(new_rows)], ignore_index=True)

agg_df_dict = agg_df.set_index('COUNTERPARTY_ID').to_dict('index')
ordered_rows = []
already_seen = set()

for ultimate_parent, party_ids in grouped_parties.items():
    group_block = []

    if ultimate_parent in agg_df_dict and ultimate_parent not in already_seen:
        row = agg_df_dict[ultimate_parent].copy()
        row['COUNTERPARTY_ID'] = ultimate_parent
        group_block.append(row)
        already_seen.add(ultimate_parent)

    for party_id in sorted(party_ids):
        if party_id in agg_df_dict and party_id not in already_seen:
            row = agg_df_dict[party_id].copy()
            row['COUNTERPARTY_ID'] = party_id
            group_block.append(row)
            already_seen.add(party_id)

    if group_block:
        ordered_rows.extend(group_block)

for cp_id, row in agg_df_dict.items():
    if cp_id not in already_seen:
        row_copy = row.copy()
        row_copy['COUNTERPARTY_ID'] = cp_id
        ordered_rows.append(row_copy)

final_df = pd.DataFrame(ordered_rows)

cols = final_df.columns.tolist()
if 'COUNTERPARTY_ID' in cols and 'COUNTERPARTY_NAME' in cols:
    cols.remove('COUNTERPARTY_ID')
    cols.remove('COUNTERPARTY_NAME')
final_df = final_df[['COUNTERPARTY_ID', 'COUNTERPARTY_NAME'] + cols]

final_df.rename(columns={
    'SUM_EXPOSURE_AMT_EUR': '100 - Gesamtposition Millionenkredite',
    'Trading_Book_IR': '111 - darunter (Bezug Position 110) Schuldverschreibungen und andere',
}, inplace=True)

final_df['112 - darunter (Bezug Position 111) Handelsbuch'] = final_df['111 - darunter (Bezug Position 110) Schuldverschreibungen und andere']
final_df['113 - darunter Aktien, Beteiligungen, Anteile an Unternehmen (only stocks and not ETFs)'] = final_df['Trading_Book_EQ']
final_df['114 - darunter Handelsbuch'] = final_df['Trading_Book_EQ']
final_df.drop(columns=['Trading_Book_EQ'], inplace=True)

value_cols = [
    '100 - Gesamtposition Millionenkredite',
    '111 - darunter (Bezug Position 110) Schuldverschreibungen und andere',
    '112 - darunter (Bezug Position 111) Handelsbuch',
    '113 - darunter Aktien, Beteiligungen, Anteile an Unternehmen (only stocks and not ETFs)',
    '114 - darunter Handelsbuch'
]

final_df_raw = final_df.copy()

for col in value_cols:
    original = final_df[col]
    divided = original / 1000
    rounded = divided.round().astype('Int64')
    masked = rounded.mask(original == 0, pd.NA)
    final_df[col] = masked.astype(object).where(masked.notna(), "")
    final_df[col] = final_df[col].replace({np.nan: ""})

summary_data = {
    'COUNTERPARTY_ID': 'TOTAL',
    'COUNTERPARTY_NAME': '',
}
for col in value_cols:
    numeric_col = pd.to_numeric(final_df[col], errors='coerce')
    summary_data[col] = int(numeric_col.sum(skipna=True))

final_df = pd.concat([final_df, pd.DataFrame([summary_data])], ignore_index=True)

with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
    final_df.to_excel(writer, sheet_name='Data_Analysis', index=False)
    pd.DataFrame().to_excel(writer, sheet_name='input >>', index=False)
    df.to_excel(writer, sheet_name='Input_data', index=False)
    df_trading.to_excel(writer, sheet_name='dc-341', index=False)
    df_party_groups.to_excel(writer, sheet_name='party_groups', index=False)

print(f"Excel file '{excel_output_path}' created successfully.")

conn.close()
