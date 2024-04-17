# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 14:44:17 2024

@author: kohm
"""

from datetime import datetime
# import importlib
import pandas as pd
import sqlalchemy as sa

from sqlalchemy import text
import os
user = os.getenv('USERPROFILE')
code_path = os.path.join(user, 
                         'OneDrive - HP Inc/Projects/Database/Code')
data_path = os.path.join(user, 'OneDrive - HP Inc/Projects/Database/Data/Feather')
os.chdir(code_path)
import credentials
pd.set_option('display.max.columns', None)

# Set today for updated date
today = datetime.today()
# Replace today if necessary
# today = datetime(2024, 1, 4)
previous_day = today - pd.DateOffset(days=1)
previous_day = previous_day.replace(hour=0, minute=0, second=0, microsecond=0)
today_weekday = today.weekday()
today_str = today.strftime('%Y-%m-%d %H:%M:%S')

# %% Connection to Database
engine = credentials.connection()

def db_insertdata(df, table, engine, col_dtype, if_exist):
    try:
        df.to_sql(table,
                  engine,
                  if_exists=if_exist,
                  chunksize=None,
                  index=False,
                  dtype=col_dtype)
        print('Insertion of data to Database table {} SUCCEEDED!'.format(table))
    except:
        print('Insertion of data to Database table {} FAILED!'.format(table))

os.chdir(data_path)
# =============================================================================
# LASER POR BUILD PLAN WEEKLY & SKUS
# =============================================================================
laser_por_build_plan = pd.read_feather('laser_por_build_plan.feather.zstd')
laser_skus = pd.read_feather('laser_skus.feather.zstd')
# =============================================================================
# INKJET POR BUILD PLAN WEEKLY & SKUS
# =============================================================================
ink_por_build_plan = pd.read_feather('ink_por_build_plan.feather.zstd')
ink_skus = pd.read_feather('ink_skus.feather.zstd')
# =============================================================================
# CANON FFGI POR BUILD PLAN WEEKLY & SKUS
# =============================================================================
canon_ffgi_por_build_plan = pd.read_feather('canon_ffgi_por_build_plan.feather.zstd')
canon_ffgi_por_build_plan = canon_ffgi_por_build_plan.drop(columns=['FAMILY_NM'])
canon_ffgi_skus = pd.read_feather('canon_ffgi_skus.feather.zstd')
# =============================================================================
# CANON ENGINE POR BUILD PLAN WEEKLY & SKUS
# =============================================================================
canon_eng_por_build_plan = pd.read_feather('canon_eng_por_build_plan.feather.zstd')
canon_eng_skus = pd.read_feather('canon_eng_skus.feather.zstd')

# db_insertdata(canon_eng_skus, 'canon_eng_skus', engine,
#               {
#                 'PLTFRM_NM': sa.types.VARCHAR(length=50),
#                 'BUS_UNIT_NM': sa.types.VARCHAR(length=30),
#                 'PART_NR': sa.types.VARCHAR(length=20),
#                 'PROD_LINE_CD': sa.types.VARCHAR(length=5),
#                 'PROD_TYPE_CD': sa.types.VARCHAR(length=15),
#                 'BUS_GRP_NM': sa.types.VARCHAR(length=15),
#                 'CAT_NM': sa.types.VARCHAR(length=40),
#                 'CAT_SUB': sa.types.VARCHAR(length=40),
#                 'PROD_GRP': sa.types.VARCHAR(length=40),
#                 'BUS_UNIT_L3': sa.types.VARCHAR(length=30),
#                 'FACTORY': sa.types.VARCHAR(length=50)
#               },'replace')

# table_update = {'TABLE': ['canon_eng_por_build_plan', 'canon_eng_skus', 'canon_ffgi_por_build_plan', 'canon_ffgi_skus','fac_report'] ,
#                 'UPDATED': [today_str,today_str,today_str,today_str,today_str]}
# table_update = pd.DataFrame(table_update)
# db_insertdata(table_update, 'table_update', engine, 
# {
#     'PLTFRM_NM': sa.types.VARCHAR(length=50),
#     'BUS_UNIT_NM': sa.types.VARCHAR(length=25)
# }, 'append')
    
