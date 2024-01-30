# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 14:44:17 2024

@author: kohm
"""

from datetime import datetime
# import importlib
import pyodbc
import pandas as pd
import sqlalchemy as db
import os
import credentials
import numpy as np
# importlib.reload(credentials)
from urllib.parse import quote_plus
user = os.getenv('USERPROFILE')
pd.set_option('display.max.columns', None)

# Set today for updated date
today = datetime.today()
today_weekday = today.weekday()
today_str = today.strftime('%Y-%m-%d %H:%M:%S')

current_week = today.isocalendar()[1]
current_week = today.strftime('%Y-W' + f'{current_week:02}')

# Set Data path
data_path = os.path.join(user, 'OneDrive - HP Inc\Projects',
                         'Mansred Database\Data')
feather_path = os.path.join(user, 
                             'OneDrive - HP Inc\Projects\Mansred Database\Data',
                             'Feather')
# Set ISAAC path
por_master_path = os.path.join(user, 'OneDrive - HP Inc\Projects',
                               'Mansred Database\ISAAC')
file_name = 'ISAAC - MANSRED.xlsx'

por_file = os.path.join(por_master_path, file_name)

# Set one time upload file for S4 and Business
business_path = os.path.join(user, 
                             'OneDrive - HP Inc\Projects\Mansred Database\S4')

# %% Connecting to database

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
    
engines = [hp_engine, engine]

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
# =============================================================================
# LASER POR BUILD PLAN WEEKLY
# =============================================================================
def read_all_build(path):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\BUILD',
                        path)
    os.chdir(path)
    build_df = pd.DataFrame()
    for file in os.listdir():
        build = pd.read_csv(file, parse_dates=['CAL_WK_DT'])
        build_df = pd.concat([build_df, build],ignore_index=True)
    return build_df

fxncz_build_df = read_all_build('LASER WEEKLY BUILD\FXN CZ')
jcuu_build_df = read_all_build('LASER WEEKLY BUILD\JABIL CUU')
jwhl_build_df = read_all_build('LASER WEEKLY BUILD\JWH LASER')
fxnwhacc_build_df = read_all_build('LASER WEEKLY BUILD\FXN WH LASER\ACC')
fxnwhhw_build_df = read_all_build('LASER WEEKLY BUILD\FXN WH LASER\HW')
fxnwhton_build_df = read_all_build('LASER WEEKLY BUILD\FXN WH LASER\TONER')

laser_por_build_plan = pd.concat([fxncz_build_df,jcuu_build_df,
                            jwhl_build_df, fxnwhacc_build_df,
                            fxnwhhw_build_df, fxnwhton_build_df], 
                            ignore_index=True)

os.chdir(feather_path)

laser_por_build_plan.to_feather('laser_por_build_plan.feather.zstd', 
                          compression = 'zstd')

db_insertdata(laser_por_build_plan, 'laser_por_build_plan', 
              hp_engine, engine,
              {
                  'CYCLE_WK_NM': db.types.VARCHAR(),
                  'LOC_TO_CD': db.types.VARCHAR(),
                  'LOC_FROM_NM': db.types.VARCHAR(),
                  'PLTFRM_NM': db.types.VARCHAR(),
                  'BUS_UNIT_NM': db.types.VARCHAR(),
                  'PART_NR': db.types.VARCHAR(),
                  'CAL_WK_DT': db.types.Date,
                  'QTY': db.types.INT()
              }, 'replace')


# =============================================================================
# INKJET POR BUILD PLAN WEEKLY
# =============================================================================

fxncq_build_df = read_all_build('INKJET WEEKLY BUILD\FXN CQ')
fxnwhi_build_df = read_all_build('INKJET WEEKLY BUILD\FXN WH INK')
jwhi_build_df = read_all_build('INKJET WEEKLY BUILD\JWH INK')
nkgth_build_df = read_all_build(r'INKJET WEEKLY BUILD\NKG TH')
nkgyy_build_df = read_all_build(r'INKJET WEEKLY BUILD\NKG YY')

ink_por_build_plan = pd.concat([fxncq_build_df,fxnwhi_build_df,
                            jwhi_build_df, nkgth_build_df,
                            nkgyy_build_df], 
                            ignore_index=True)

# REPLACE OLD NAMING OF NKG
ink_por_build_plan.loc[
    ink_por_build_plan['LOC_FROM_NM'] == 'NKG Thailand',
    'LOC_FROM_NM'] = 'CALCOMP TH'

ink_por_build_plan.loc[
    ink_por_build_plan['LOC_FROM_NM'] == 'NKG Yue Yang',
    'LOC_FROM_NM'] = 'CALCOMP YY'

os.chdir(feather_path)
ink_por_build_plan.to_feather('ink_por_build_plan.feather.zstd', 
                          compression = 'zstd')

db_insertdata(ink_por_build_plan, 'ink_por_build_plan', 
              hp_engine, engine,
              {
                  'CYCLE_WK_NM': db.types.VARCHAR(),
                  'LOC_TO_CD': db.types.VARCHAR(),
                  'LOC_FROM_NM': db.types.VARCHAR(),
                  'PLTFRM_NM': db.types.VARCHAR(),
                  'BUS_UNIT_NM': db.types.VARCHAR(),
                  'PART_NR': db.types.VARCHAR(),
                  'CAL_WK_DT': db.types.Date,
                  'QTY': db.types.INT()
              }, 'replace')

# %% READ LAST TIME CLEANUP DATA FROM DATABASE
def sql_get_all(table,eng):
    query = \
    """
    SELECT * FROM mmops.dbo.{}
    """.format(table)
    df = pd.read_sql(query, eng)
    
    df = df.replace('nan',np.nan)
    df = df.replace('None',np.nan)
    
    
    df = df.drop(columns=['UPDATED'])
    df.to_feather(table + '.feather.zstd', 
                  compression = 'zstd')
    return df

canon_eng_jp_skus = sql_get_all('canon_eng_jp_skus', engine)

db_insertdata(canon_eng_jp_skus, 'canon_eng_jp_skus', 
              hp_engine, engine,
              {
                  'CYCLE_WK_NM': db.types.VARCHAR(),
                  'LOC_TO_CD': db.types.VARCHAR(),
                  'LOC_FROM_NM': db.types.VARCHAR(),
                  'PLTFRM_NM': db.types.VARCHAR(),
                  'BUS_UNIT_NM': db.types.VARCHAR(),
                  'PART_NR': db.types.VARCHAR(),
                  'CAL_WK_DT': db.types.Date,
                  'QTY': db.types.INT()
              }, 'replace')
table_update = {'TABLE': ['canon_eng_jp_skus'], 
                'UPDATED': ['2023-12-04 13:34:05']}
table_update = pd.DataFrame(table_update)
for engine in engines:
    db_appenddata(
        table_update, 'table_update', engine,
        {'TABLE': db.types.VARCHAR(),
          'UPDATED': db.types.VARCHAR()})



# table_update = {'TABLE': ['ink_por_build_plan', 'laser_por_build_plan'], 
#                 'UPDATED': [today_str,today_str]}
# table_update = pd.DataFrame(table_update)
# for engine in engines:
#     db_appenddata(
#         table_update, 'table_update', engine,
#         {'ink_por_build_plan': db.types.VARCHAR(),
#          'laser_por_build_plan': db.types.VARCHAR()})
    
