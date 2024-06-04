# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 10:21:07 2024

@author: kohm
"""

import credentials
from datetime import datetime
import pandas as pd
import sqlalchemy as sa
import os
user = os.getenv('USERPROFILE')
code_path = os.path.join(user,
                         'OneDrive - HP Inc/Projects/Database/Code')
os.chdir(code_path)
import credentials
pd.set_option('display.max.columns', None)
user = os.getenv('USERPROFILE')
# Set data path
feather_path = os.path.join(user,
                            'OneDrive - HP Inc\Projects\Database\Data',
                            'Feather')

aws_engine = credentials.aws_connection()
# %% Connection to Database
engine = credentials.connection()
i_engine = credentials.isaac_connection()
# Set today for updated date
today = datetime.today()
today_weekday = today.weekday()
today_str = today.strftime('%Y-%m-%d %H:%M:%S')

# %% Data Types
por_dtypes = {'CYCLE_WK_NM': sa.types.VARCHAR(length=10),
              'LOC_TO_CD': sa.types.VARCHAR(length=8),
              'LOC_FROM_NM': sa.types.VARCHAR(length=30),
              'FAMILY_NM': sa.types.VARCHAR(length=30),
              'PLTFRM_NM': sa.types.VARCHAR(length=50),
              'BUS_UNIT_NM': sa.types.VARCHAR(length=30),
              'PART_NR': sa.types.VARCHAR(length=20),
              'CAL_WK_DT': sa.types.Date,
              'QTY': sa.types.FLOAT()}

skus_dtypes = {'LOC_FROM_NM': sa.types.VARCHAR(length=30),
               'PLTFRM_NM': sa.types.VARCHAR(length=50),
               'BUS_UNIT_NM': sa.types.VARCHAR(length=30),
               'PART_NR': sa.types.VARCHAR(length=20),
               'PROD_LINE_CD': sa.types.VARCHAR(length=5),
               'PROD_TYPE_CD': sa.types.VARCHAR(length=15),
               'BUS_GRP_NM': sa.types.VARCHAR(length=15),
               'CAT_NM': sa.types.VARCHAR(length=40),
               'CAT_SUB': sa.types.VARCHAR(length=40),
               'PROD_GRP': sa.types.VARCHAR(length=40),
               'BUS_UNIT_L3': sa.types.VARCHAR(length=30)}

# =============================================================================
# INKJET WEEKLY POR BUILD PLAN
# =============================================================================
os.chdir(feather_path)
ink_por_build_plan = pd.read_feather('ink_por_build_plan.feather.zstd')

ink_por_build_plan.to_sql('ink_por_build_plan',
                          aws_engine,
                          if_exists='replace',
                          chunksize=None,
                          index=False,
                          dtype=por_dtypes)

# =============================================================================
# LASER WEEKLY POR BUILD PLAN
# =============================================================================
laser_por_build_plan = pd.read_feather('laser_por_build_plan.feather.zstd')

laser_por_build_plan.to_sql('laser_por_build_plan',
                            aws_engine,
                            if_exists='replace',
                            chunksize=None,
                            index=False,
                            dtype=por_dtypes)


# =============================================================================
# INK SKUS
# =============================================================================
ink_skus = pd.read_feather('ink_skus.feather.zstd')

ink_skus.to_sql('ink_skus',
                aws_engine,
                if_exists='replace',
                chunksize=None,
                index=False,
                dtype=skus_dtypes)

# =============================================================================
# LASER SKUS
# =============================================================================
laser_skus = pd.read_feather('laser_skus.feather.zstd')

laser_skus.to_sql('laser_skus',
                  aws_engine,
                  if_exists='replace',
                  chunksize=None,
                  index=False,
                  dtype=skus_dtypes)

# =============================================================================
# TABLE UPDATE
# =============================================================================
d = {'TABLE': ['business_family', 'exe_family',
               'fxnwhl_pc', 'ink_por_build_plan',
               'laser_por_build_plan', 'plant', 'vendor'],
     'UPDATED': [today_str, today_str, today_str, today_str,
                 today_str, today_str, today_str]}
table_update = pd.DataFrame(data=d)
table_update.to_sql('table_update',
                    aws_engine,
                    if_exists='replace',
                    chunksize=None,
                    index=False,
                    dtype={'TABLE': sa.types.VARCHAR(length=50),
                           'UPDATED': sa.types.VARCHAR(length=25)})
