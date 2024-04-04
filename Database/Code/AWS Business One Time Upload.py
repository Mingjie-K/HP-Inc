# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 10:21:07 2024

@author: kohm
"""

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
# =============================================================================
# VENDOR
# =============================================================================
os.chdir(feather_path)
vendor = pd.read_feather('vendor.feather.zstd')

vendor.to_sql('vendor',
          aws_engine,
          if_exists='replace',
          chunksize=None,
          index=False,
          dtype={'TPO_PO_Vendor_Code': sa.types.VARCHAR(length=10),
          'S4_MPA_NAME': sa.types.VARCHAR(length=50),
          'ISAAC_MPA_NAME': sa.types.VARCHAR(length=50),
          'MPA_NAME': sa.types.VARCHAR(length=50),
          'BU': sa.types.VARCHAR(length=20),
          'TPO_Plant': sa.types.VARCHAR(length=20),
          'TPO_Shipping_Point' : sa.types.VARCHAR(length=20),
          'Active': sa.types.VARCHAR(length=1)})

# =============================================================================
# PLANT
# =============================================================================
plant = pd.read_feather('plant.feather.zstd')
plant.to_sql('plant',
          aws_engine,
          if_exists='replace',
          chunksize=None,
          index=False,
          dtype={'Plant': sa.types.VARCHAR(length=8),
          'Name 1': sa.types.VARCHAR(length=50),
          'Valuation area': sa.types.VARCHAR(length=8),
          'Customer Number of Plant': sa.types.VARCHAR(length=20),
          'Supplier Number of Plant': sa.types.VARCHAR(length=20),
          'Factory Calendar': sa.types.VARCHAR(length=5),
          'Name 2' : sa.types.VARCHAR(length=50),
          'Street and House Number': sa.types.VARCHAR(length=50),
          'PO Box': sa.types.VARCHAR(length=15),
          'Postal Code': sa.types.VARCHAR(length=20),
          'City': sa.types.VARCHAR(length=40),
          'Purch. organization': sa.types.VARCHAR(length=10),
          'Sales Organization': sa.types.VARCHAR(length=10),
          'Reqmts planning': sa.types.VARCHAR(length=5),
          'Country/Region Key': sa.types.VARCHAR(length=5),
          'Region': sa.types.VARCHAR(length=5),
          'Address': sa.types.VARCHAR(length=20),
          'Planning Plant': sa.types.VARCHAR(length=10),
          'Tax Jurisdiction': sa.types.VARCHAR(length=20),
          'Distrib.channel': sa.types.VARCHAR(length=5),
          'Int.co billing div.': sa.types.VARCHAR(length=5),
          'Language Key': sa.types.VARCHAR(length=5),
          'Variance Key': sa.types.VARCHAR(length=10),
          'Tax Indicator: Plant': sa.types.VARCHAR(length=5),
          '1st Reminder/Exped.': sa.types.VARCHAR(length=5),
          '2nd Reminder/Exped.': sa.types.VARCHAR(length=5),
          '3rd Reminder/Exped.': sa.types.VARCHAR(length=5),
          'Text 1st dunning': sa.types.VARCHAR(length=25),
          'Text 2nd dunning': sa.types.VARCHAR(length=25),
          'Text 3rd dunning': sa.types.VARCHAR(length=25),
          'PO tolerance': sa.types.VARCHAR(length=10),
          'Business Place': sa.types.VARCHAR(length=10),
          'Naming Structure': sa.types.VARCHAR(length=10),
          'Cost Obj.Controlling': sa.types.VARCHAR(length=10),
          'Mixed Costing': sa.types.VARCHAR(length=10),
          'Actual Costing': sa.types.VARCHAR(length=10),
          'Shipping Point/Receiving Pt': sa.types.VARCHAR(length=10),
          'Actual Activities Updated': sa.types.VARCHAR(length=5),
          'Batch Management Not Active by Default': sa.types.VARCHAR(length=5),
          'Region_Ship': sa.types.VARCHAR(length=10),
          'Sub_Region': sa.types.VARCHAR(length=10),
          'Source': sa.types.VARCHAR(length=15),
          'Plant Location': sa.types.VARCHAR(length=100),
          'Legacy Plant Code': sa.types.VARCHAR(length=20),
          'Interplant': sa.types.VARCHAR(length=15)
          })

# =============================================================================
# BUSINESS FAMILY
# =============================================================================
business_family = pd.read_feather('business_family.feather.zstd')
business_family.to_sql('business_family',
          aws_engine,
          if_exists='replace',
          chunksize=None,
          index=False,
          dtype={'MPA_NAME': sa.types.VARCHAR(length=30),
          'PLTFRM_NM': sa.types.VARCHAR(length=50),
          'FAMILY': sa.types.VARCHAR(length=50),
          'PART_NR': sa.types.VARCHAR(length=25)
          })

# =============================================================================
# EXECUTIVE FAMILY
# =============================================================================
exe_family = pd.read_feather('exe_family.feather.zstd')
exe_family.to_sql('exe_family',
                  aws_engine,
                  if_exists='replace',
                  chunksize=None,
                  index=False,
                  dtype={'BU': sa.types.VARCHAR(length=20),
                         'PLTFRM_NM': sa.types.VARCHAR(length=50),
                         'FAMILY': sa.types.VARCHAR(length=50)})

# =============================================================================
# LASER CATEGORIZATION
# =============================================================================
fxnwhl_pc = pd.read_feather('fxnwhl_pc.feather.zstd')
fxnwhl_pc.to_sql('fxnwhl_pc',
                  aws_engine,
                  if_exists='replace',
                  chunksize=None,
                  index=False,
                  dtype={'TPO_Profit_Center': sa.types.VARCHAR(length=10),
                         'CATEGORY': sa.types.VARCHAR(length=25),
                         'SKU PL OVERWRITE': sa.types.VARCHAR(length=150)})













