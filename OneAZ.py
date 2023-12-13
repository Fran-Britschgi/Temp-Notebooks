import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas

# SQL queries
sql = "SELECT * FROM core_member_actuals"
sql2 = "SELECT * FROM ev_assumptions_pivot"

# Execute queries and fetch data
cur.execute(sql)
df = cur.fetch_pandas_all()

cur.execute(sql2)
assumptions_production = cur.fetch_pandas_all()

# Data manipulation
growthratedf = pd.DataFrame([
    assumptions_production.Year, 
    assumptions_production.Channel, 
    assumptions_production.GrowthRate, 
    assumptions_production.MemberClosureRate
]).transpose()

growthratedf['PriorYear'] = growthratedf['Year'] - 1

df = df[df['YEAR'].astype(int) < datetime.now().year]
df = df.rename(columns={
    'BRANCHNBR': 'BranchNbr', 
    'BRANCH': 'Branch', 
    'CHANNEL': 'Channel', 
    'YEAR': 'Year', 
    'NEW_CORE_MEMBERS': 'NewCoreMembers', 
    'TOTAL_CORE_MEMBERS': 'TotalCoreMembers'
})

# Function definitions
def get_next_year_member_projection(df_prev_yr_members, growthratedf):
    # Merge data
    df_new = pd.merge(
        left=growthratedf, 
        right=df_prev_yr_members, 
        left_on=['Channel', 'PriorYear'], 
        right_on=['Channel', 'Year'], 
        how='inner'
    )

    if len(df_new) == 0:
        return df_new

    # Calculate new and total core members
    df_new['NewCoreMembers'] = df_new['NewCoreMembers'] * (1 + df_new['GrowthRate'])
    df_new['NewCoreMembers'] = df_new['NewCoreMembers'].apply(np.ceil)
    df_new = df_new.rename(columns={'Year_x': 'Year'})
    df_new['TotalCoreMembers'] = (df_new['TotalCoreMembers'] * (1 - df_new['MemberClosureRate'])) + df_new['NewCoreMembers']
    df_new['TotalCoreMembers'] = df_new['TotalCoreMembers'].apply(np.ceil)
    df_new = df_new.astype({'NewCoreMembers': 'int', 'TotalCoreMembers': 'int'})

    df_new = df_new[['Branch', 'Channel', 'Year', 'NewCoreMembers', 'TotalCoreMembers']]
    return df_new

def project_members(df, growthratedf):
    years_to_project = growthratedf['Year'].unique()
    print('\nProjections requested for the years ', years_to_project, '\n')

    for x in years_to_project:
        prev = x - 1
        df_prev_yr = df[df['Year'] == prev]
        next_yr = get_next_year_member_projection(df_prev_yr, growthratedf)
        df = pd.concat([df, pd.DataFrame(next_yr)]).reset_index(drop=True)
    return df

# Member projection
members = project_members(df, growthratedf)
members = pd.melt(members, id_vars=['Branch', 'Channel', 'Year'], value_vars=['NewCoreMembers', 'TotalCoreMembers'], var_name='MemberCategory', value_name='Members', ignore_index=False)
members['MemberCategory'] = members['MemberCategory'].replace(['NewCoreMembers', 'TotalCoreMembers'], ['New', 'Existing'])

members.to_csv('out/notebook/MemberProjection.csv')
out_members = members.rename(columns={'Branch': 'BRANCH', 'Channel': 'CHANNEL', 'Year': 'YEAR', 'MemberCategory': 'MEMBER_CATEGORY', 'Members': 'MEMBERS'})

# Snowflake table creation and data upload
sql = "CREATE OR REPLACE TABLE MODELING.PRODUCTION_MODEL.TMP_OUT_CORE_MEMBER(Branch varchar(100), Channel varchar(100), Year varchar(100), MEMBER_CATEGORY varchar(100), Members numeric)"
cur.execute(sql)
table_name = "TMP_OUT_CORE_MEMBER"
success, nchunks, nrows, _ = write_pandas(ctx, out_members, table_name)
