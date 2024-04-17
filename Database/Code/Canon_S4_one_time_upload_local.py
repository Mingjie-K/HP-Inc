from datetime import datetime
# import importlib
import pandas as pd
import sqlalchemy as sa

from sqlalchemy import text
import os
user = os.getenv('USERPROFILE')
code_path = os.path.join(user, 
                         'OneDrive - HP Inc/Projects/Database/Code')
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

# Set Canon Etd Path
canon_etd_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput',
                              'Data\CANON\ETD')
feather_path = os.path.join(user,
                            'OneDrive - HP Inc\Projects\Database\Data',
                            'Feather')

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

# %% CSV Data

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

canon_etd_dtype = {'Trade_PO': sa.types.VARCHAR(),
                   'TRADE_PO_LINE': sa.types.VARCHAR(),
                   'CDO_IC_PO': sa.types.VARCHAR(),
                   'DC_IC_PO': sa.types.VARCHAR(),
                   'DC_IC_PO_LINE': sa.types.VARCHAR(),
                   'TPO_PO_Vendor_Code': sa.types.VARCHAR(),
                   'TPO_Created_On': sa.types.Date,
                   'TPO_Material': sa.types.VARCHAR(),
                   'TPO_Qty': sa.types.FLOAT(),
                   'TPO_PO_Type': sa.types.VARCHAR(),
                   'TPO_AB_Line': sa.types.VARCHAR(),
                   'TPO_AB_Conf_Delivery_Date': sa.types.Date,
                   'TPO_AB_Qty': sa.types.FLOAT(),
                   'TPO_LA_Conf_Delivery_Date': sa.types.Date,
                   'TPO_LA_Inbound_Delivery_No': sa.types.VARCHAR(),
                   'TPO_LA_Qty': sa.types.FLOAT(),
                   'TPO_LA_Reference': sa.types.VARCHAR(),
                   'Trade_PO_Open_Quantity': sa.types.FLOAT(),
                   'DC_IC_PO_Req_Delivery_Date': sa.types.Date,
                   'DC_IC_PO_Qty': sa.types.FLOAT(),
                   'DC_IC_PO_Plant': sa.types.VARCHAR(),
                   'CDO_IC_PO_Req_Delivery_Date': sa.types.Date,
                   'CDO_IC_PO_Plant': sa.types.VARCHAR(),
                   'DC_IC_PO_Requisitioner': sa.types.VARCHAR(),
                   'CDO_IC_PO_Requisitioner': sa.types.VARCHAR(),
                   'Requisitioner': sa.types.VARCHAR(),
                   'TPOLATABLE_DELIVERY_NR': sa.types.VARCHAR(),
                   'TPOLATABLE_IBP_ETA_DC': sa.types.Date,
                   'TPOLATABLE_IBP_ETD_FACTORY': sa.types.Date,
                   'TPOLATABLE_IBD_PORT_ARRIVAL_DT': sa.types.Date,
                   'TPOLATABLE_BILL_LADING': sa.types.VARCHAR(),
                   'TPOLATABLE_MNS_OF_TRANS_ID': sa.types.VARCHAR()
                   }

os.chdir(canon_etd_path)
canon_etd_df = pd.read_csv(
    'Canon Shipment_Report.csv',
    dtype=canon_etd_data_type,
    parse_dates=canon_dates,
    date_format='%d/%m/%Y')

os.chdir(feather_path)

canon_etd_df.to_feather('canon_etd.feather.zstd', compression='zstd')

db_insertdata(canon_etd_df, 'canon_etd', engine, canon_etd_dtype, 'replace')