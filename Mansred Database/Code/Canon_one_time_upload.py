# -*- coding: utf-8 -*-
"""
Created on Fri Jan 26 14:41:21 2024

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

# Set today for updated date
today = datetime.today()
today_weekday = today.weekday()
today_str = today.strftime('%Y-%m-%d %H:%M:%S')

current_week = today.isocalendar()[1]
current_week = today.strftime('%Y-W' + f'{current_week:02}')

# Set Canon Etd Path
canon_etd_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput',
                              'Data\CANON\ETD')
feather_path = os.path.join(user,
                            'OneDrive - HP Inc\Projects\Mansred Database\Data',
                            'Feather')
canon_etd_data_type = {
    'Trade_PO': str, 'TRADE_PO_LINE': str, 'CDO_IC_PO': str, 'DC_IC_PO': str,
    'DC_IC_PO_LINE': str, 'TPO_PO_Vendor_Code': str, 'TPO_Material': str,
    'TPO_Qty': float, 'TPO_Plant': str, 'TPO_Shipping_Point': str,
    'TPO_PO_Type': str, 'TPO_AB_Line': str, 'TPO_AB_Qty': float,
    'TPO_LA_Inbound_Delivery_No': str, 'TPO_LA_Qty': float,
    'TPO_LA_Reference': str, 'Trade_PO_Open_Quantity': float,
    'DC_IC_PO_Qty': float, 'DC_IC_PO_Plant': str,
    'CDO_IC_PO_Plant': str, 'DC_IC_PO_Requisitioner': str,
    'CDO_IC_PO_Requisitioner': str, 'Requisitioner': str,
    'TPOLATABLE_DELIVERY_NR': str, 'TPOLATABLE_BILL_LADING': str,
    'TPOLATABLE_MNS_OF_TRANS_ID': str}

canon_dates = ['TPO_Created_On',
               'TPO_Requested_Delivery_Date',
               'TPO_AB_Conf_Delivery_Date',
               'TPO_LA_Conf_Delivery_Date',
               'DC_IC_PO_Req_Delivery_Date',
               'CDO_IC_PO_Req_Delivery_Date',
               'TPOLATABLE_IBP_ETA_DC',
               'TPOLATABLE_IBP_ETD_FACTORY',
               'TPOLATABLE_IBD_PORT_ARRIVAL_DT']

canon_etd_dtype = {'Trade_PO': db.types.VARCHAR(),
                   'TRADE_PO_LINE': db.types.VARCHAR(),
                   'CDO_IC_PO': db.types.VARCHAR(),
                   'DC_IC_PO': db.types.VARCHAR(),
                   'DC_IC_PO_LINE': db.types.VARCHAR(),
                   'TPO_PO_Vendor_Code': db.types.VARCHAR(),
                   'TPO_Created_On': db.types.Date,
                   'TPO_Material': db.types.VARCHAR(),
                   'TPO_Qty': db.types.FLOAT(),
                   'TPO_PO_Type': db.types.VARCHAR(),
                   'TPO_AB_Line': db.types.VARCHAR(),
                   'TPO_AB_Conf_Delivery_Date': db.types.Date,
                   'TPO_AB_Qty': db.types.FLOAT(),
                   'TPO_LA_Conf_Delivery_Date': db.types.Date,
                   'TPO_LA_Inbound_Delivery_No': db.types.VARCHAR(),
                   'TPO_LA_Qty': db.types.FLOAT(),
                   'TPO_LA_Reference': db.types.VARCHAR(),
                   'Trade_PO_Open_Quantity': db.types.FLOAT(),
                   'DC_IC_PO_Req_Delivery_Date': db.types.Date,
                   'DC_IC_PO_Qty': db.types.FLOAT(),
                   'DC_IC_PO_Plant': db.types.VARCHAR(),
                   'CDO_IC_PO_Req_Delivery_Date': db.types.Date,
                   'CDO_IC_PO_Plant': db.types.VARCHAR(),
                   'DC_IC_PO_Requisitioner': db.types.VARCHAR(),
                   'CDO_IC_PO_Requisitioner': db.types.VARCHAR(),
                   'Requisitioner': db.types.VARCHAR(),
                   'TPOLATABLE_DELIVERY_NR': db.types.VARCHAR(),
                   'TPOLATABLE_IBP_ETA_DC': db.types.Date,
                   'TPOLATABLE_IBP_ETD_FACTORY': db.types.Date,
                   'TPOLATABLE_IBD_PORT_ARRIVAL_DT': db.types.Date,
                   'TPOLATABLE_BILL_LADING': db.types.VARCHAR(),
                   'TPOLATABLE_MNS_OF_TRANS_ID': db.types.VARCHAR()
                   }

os.chdir(canon_etd_path)
canon_etd_df = pd.read_csv(
    'Canon Shipment_Report.csv',
    dtype=canon_etd_data_type,
    parse_dates=canon_dates,
    date_format='%d/%m/%Y')

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

# %% Output
os.chdir(feather_path)

canon_etd_df.to_feather('canon_etd.feather.zstd', compression='zstd')

db_insertdata(canon_etd_df, 'canon_etd', hp_engine, engine,
              canon_etd_dtype, 'replace')

update_df = pd.DataFrame({'TABLE': ['canon_etd'], 'UPDATED': [today_str]})

for engine in engines:
    db_appenddata(update_df, 'table_update',
                  engine, {'TABLE': db.types.VARCHAR(),
                           'UPDATED': db.types.VARCHAR()})
