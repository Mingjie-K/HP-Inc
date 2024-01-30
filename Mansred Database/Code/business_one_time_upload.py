# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 13:53:48 2024

@author: kohm
"""

from datetime import datetime
# import importlib
import pyodbc
import pandas as pd
import sqlalchemy as db
import os
import credentials
# importlib.reload(credentials)

from urllib.parse import quote_plus
user = os.getenv('USERPROFILE')
pd.set_option('display.max.columns', None)

business_path = os.path.join(user, 
                             'OneDrive - HP Inc\Projects\Mansred Database\S4')
feather_path = os.path.join(user, 
                             'OneDrive - HP Inc\Projects\Mansred Database\Data',
                             'Feather')
# Set today for updated date
today = datetime.today()
today_weekday = today.weekday()
today_str = today.strftime('%Y-%m-%d %H:%M:%S')

def str_strip(df):
    df_obj = df.select_dtypes(['object'])
    df[df_obj.columns] = df.apply(lambda x: x.str.strip())
    return df
# %% Connection to Database
try:
    # =============================================================================
    # Microsoft SQL Server Connection (MMOPS)
    # =============================================================================
    con_str = credentials.connection()
    sql_conn = pyodbc.connect(con_str)
    quoted = quote_plus(con_str)
    new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(quoted)
    engine = db.create_engine(new_con, fast_executemany=True)
    print('Connected to Microsoft SQL Server: MMOPS')
except:
    engine = None
    
try:
    # =============================================================================
    # HP Microsoft SQL Server Connection (MMOPS)
    # =============================================================================
    hp_con_str = credentials.connection_hp()
    hp_sql_conn = pyodbc.connect(hp_con_str)
    hp_quoted = quote_plus(hp_con_str)
    hp_new_con = 'mssql+pyodbc:///?odbc_connect={}'.format(hp_quoted)
    hp_engine = db.create_engine(hp_new_con, fast_executemany=True)
    print('Connected to HP Microsoft SQL Server: MMOPS')

except:
    hp_engine = None
    
# %% Function for pumping to both database


def db_insertdata(df, table, hp_engine, own_engine, col_dtype, if_exist):
    try:
        df.to_sql(table,
                  hp_engine,
                  if_exists=if_exist,
                  chunksize=None,
                  index=False,
                  dtype=col_dtype)
    except:
        pass

    try:
        df.to_sql(table,
                  own_engine,
                  if_exists=if_exist,
                  chunksize=None,
                  index=False,
                  dtype=col_dtype)
    except:
        pass

def db_appenddata(df, table, engine, col_dtype):
    try:
        df.to_sql(table,
                  engine,
                  if_exists='append',
                  chunksize=None,
                  index=False,
                  dtype=col_dtype)
    except:
        pass

# %% ONE TIME UPLOAD BUSINESS
os.chdir(business_path)
vendor_df = pd.read_excel('Consolidation.xlsx',
                          keep_default_na=False, 
                          sheet_name='vendor',
                          dtype={'TPO_PO_Vendor_Code':str})
b_family_df = pd.read_excel('Consolidation.xlsx', 
                          sheet_name='business_family',
                          usecols='A:D')
fxnwhl_pc_df = pd.read_excel('Consolidation.xlsx', 
                          sheet_name='FXN WH Laser Profit Center',
                          usecols='A:C')
canon_site_df = pd.read_excel('Consolidation.xlsx', 
                          sheet_name='canon_site',
                          usecols='A:B')
e_family_df = pd.read_excel('Consolidation.xlsx', 
                          sheet_name='exe_family',
                          usecols='A:C')

vendor_df = str_strip(vendor_df)
b_family_df = str_strip(b_family_df)
fxnwhl_pc_df = str_strip(fxnwhl_pc_df)
canon_site_df = str_strip(canon_site_df)
e_family_df = str_strip(e_family_df)

os.chdir(feather_path)
vendor_df.to_feather('vendor.feather.zstd', compression = 'zstd')
b_family_df.to_feather('business_family.feather.zstd', compression = 'zstd')
fxnwhl_pc_df.to_feather('fxnwhl_pc.feather.zstd', compression = 'zstd')
canon_site_df.to_feather('canon_site.feather.zstd', compression = 'zstd')
e_family_df.to_feather('exe_family.feather.zstd', compression = 'zstd')

db_insertdata(vendor_df, 'vendor', hp_engine, engine,
              {
                  'TPO_PO_Vendor_Code': db.types.VARCHAR(),
                  'S4_MPA_NAME': db.types.VARCHAR(),
                  'ISAAC_MPA_NAME': db.types.VARCHAR(),
                  'MPA_NAME': db.types.VARCHAR(),
                  'BU': db.types.VARCHAR(),
                  'TPO_Plant': db.types.VARCHAR(),
                  'TPO_Shipping_Point' : db.types.VARCHAR(),
                  'Active': db.types.VARCHAR()
              }, 'replace')

db_insertdata(b_family_df, 'business_family', hp_engine, engine,
              {
                  'MPA_NAME': db.types.VARCHAR(),
                  'PLTFRM_NM': db.types.VARCHAR(),
                  'FAMILY': db.types.VARCHAR(),
                  'PART_NR': db.types.VARCHAR(),
              }, 'replace')

db_insertdata(fxnwhl_pc_df, 'fxnwhl_pc', hp_engine, engine,
              {
                  'TPO_Profit_Center': db.types.VARCHAR(),
                  'CATEGORY': db.types.VARCHAR(),
                  'SKU PL OVERWRITE': db.types.VARCHAR(),
              }, 'replace')

db_insertdata(canon_site_df, 'canon_site', hp_engine, engine,
              {
                  'TPO_LA_Reference': db.types.VARCHAR(),
                  'SITE': db.types.VARCHAR(),
              }, 'replace')

db_insertdata(e_family_df, 'exe_family', hp_engine, engine,
              {
                  'BU': db.types.VARCHAR(),
                  'PLTFRM_NM': db.types.VARCHAR(),
                  'FAMILY': db.types.VARCHAR(),
              }, 'replace')
# PLANT DATA
plant_df = pd.read_excel('S4 Plants.xlsx', keep_default_na=False)
region = pd.read_excel('Consolidation.xlsx', 
                       sheet_name='plant', keep_default_na=False)
plant_df['1st Reminder/Exped.'] = plant_df['1st Reminder/Exped.'].astype(str)
plant_df['2nd Reminder/Exped.'] = plant_df['2nd Reminder/Exped.'].astype(str)
plant_df['3rd Reminder/Exped.'] = plant_df['3rd Reminder/Exped.'].astype(str)
plant_df['PO tolerance'] = plant_df['PO tolerance'].astype(str)
plant_df['Cost Obj.Controlling'] = plant_df['Cost Obj.Controlling'].astype(str)
plant_df['Mixed Costing'] = plant_df['Mixed Costing'].astype(str)
plant_df['Actual Costing'] = plant_df['Actual Costing'].astype(str)
plant_df = plant_df.merge(region, how='left', on='Plant')
str_cols = plant_df.select_dtypes(['object'])
plant_df[str_cols.columns] = str_cols.apply(lambda x: x.str.strip())

db_insertdata(plant_df, 'plant', hp_engine, engine,
              {'Plant':db.types.VARCHAR(),
               'Name 1':db.types.VARCHAR(),
               'Valuation area':db.types.VARCHAR(),
               'Customer Number of Plant':db.types.VARCHAR(),
               'Supplier Number of Plant':db.types.VARCHAR(),
               'Factory Calendar':db.types.VARCHAR(),
               'Name 2':db.types.VARCHAR(),
               'Street and House Number':db.types.VARCHAR(),
               'PO Box':db.types.VARCHAR(),
               'Postal Code':db.types.VARCHAR(),
               'City':db.types.VARCHAR(),
               'Purch. organization':db.types.VARCHAR(),
               'Sales Organization':db.types.VARCHAR(),
               'Reqmts planning':db.types.VARCHAR(),
               'Country/Region Key':db.types.VARCHAR(),
               'Region':db.types.VARCHAR(),
               'Address':db.types.VARCHAR(),
               'Planning Plant':db.types.VARCHAR(),
               'Tax Jurisdiction':db.types.VARCHAR(),
               'Distrib.channel':db.types.VARCHAR(),
               'Int.co billing div.':db.types.VARCHAR(),
               'Language Key':db.types.VARCHAR(),
               'Variance Key':db.types.VARCHAR(),
               'Tax Indicator: Plant':db.types.VARCHAR(),
               '1st Reminder/Exped.':db.types.VARCHAR(),
               '2nd Reminder/Exped.':db.types.VARCHAR(),
               '3rd Reminder/Exped.':db.types.VARCHAR(),
               'Text 1st dunning':db.types.VARCHAR(),
               'Text 2nd dunning':db.types.VARCHAR(),
               'Text 3rd dunning':db.types.VARCHAR(),
               'PO tolerance':db.types.VARCHAR(),
               'Business Place':db.types.VARCHAR(),
               'Naming Structure':db.types.VARCHAR(),
               'Cost Obj.Controlling':db.types.VARCHAR(),
               'Mixed Costing':db.types.VARCHAR(),
               'Actual Costing':db.types.VARCHAR(),
               'Shipping Point/Receiving Pt':db.types.VARCHAR(),
               'Actual Activities Updated':db.types.VARCHAR(),
               'Batch Management Not Active by Default':db.types.VARCHAR(),
               'Region_Ship':db.types.VARCHAR(),
               'Sub_Region':db.types.VARCHAR(),
               'Source':db.types.VARCHAR(),
               'Plant Location':db.types.VARCHAR(),
               'Legacy Plant Code':db.types.VARCHAR(),
               'Interplant':db.types.VARCHAR()}, 'replace')
os.chdir(feather_path)
plant_df.to_feather('plant.feather.zstd', compression = 'zstd')
# table_update = {'TABLE': ['plant'], 
#                 'UPDATED': [today_str]}
# table_update = pd.DataFrame(table_update)
# db_appenddata(table_update, 'table_update', engine, {
#                   'TABLE': db.types.VARCHAR(),
#                   'UPDATED': db.types.VARCHAR(),
#               })
# table_update = {'TABLE': ['vendor', 'business_family', 'fxnwhl_pc', 'canon_site',
#                           'exe_family'], 
#                 'UPDATED': [today_str, today_str, today_str,today_str,today_str]}
# table_update = pd.DataFrame(table_update)
# db_insertdata(table_update, 'table_update', hp_engine, engine,
#               {
#                   'TABLE': db.types.VARCHAR(),
#                   'UPDATED': db.types.VARCHAR(),
#               }, 'replace')