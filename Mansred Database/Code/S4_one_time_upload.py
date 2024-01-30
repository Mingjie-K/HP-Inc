# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 15:19:03 2024

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

# Set Data path
data_path = os.path.join(user, 'OneDrive - HP Inc\Projects',
                         'Mansred Database\Data')

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


fac_path = os.path.join(user, 
                        'OneDrive - HP Inc\Projects\Mansred Database\S4',
                        'Factory Report April 2021 to 4 Jan 2024')

fac_data_type = {'TPO_PO_Vendor_Code': str, 'TPO_PO_Vendor_Name': str,
                  'Trade_PO': str, 'Trade_PO_Item': str,
                  'TPO_Deletion Indicator': str, 'TPO_Material': str,
                  'TPO_Material_Text': str, 'TPO_Qty': float, 
                  'TPO_Total_Net_Price': float, 'TPO_Price_Unit': str, 
                  'TPO_Plant': str, 'TPO_Storage_Loc': str,
                  'TPO_MRP_Controller': str, 'TPO_Shipping_Point':str,
                  'TPO_Profit_Center':str, 'TPO_Ship_mode_Name':str,
                  'TPO_PO_Type': str, 'TPO_DIO_Flag':str, 
                  'TPO_Incoterm': str, 'TPO_Purchase_Org': str, 
                  'TPO_Purch_Group': str, 'Purchasing Group Description': str,
                  'TPO_Vendor_Material_Num': str, 'TPO_AB_Line': str,
                  'TPO_AB_Qty': float, 'TPO_LA_Line': str,                 
                  'TPO_LA_Inbound_Delivery_No': str, 'TPO_LA_Qty': float, 
                  'TPO_LA_Reference': str, 'TPO_GR_No':str,
                  'TPO_GR_Qty': float, 'TPO_GR_Indicator':str,
                  'TPO_Net_Price': float, 
                  'Trade_PO_Total_Net_Price_Value': float,
                  'TPO_Currency': str, 'TPO_Delivery_Priority': str,
                  'TPO_IR_Indicator': str, 'TPO_IR_No': str, 
                  'Trade_PO_Open_Quantity': float,
                  'Trade_PO_Open_Qty_Value': float, 'Trade_PO_Status': str,
                  'TPO_Act_Asgmt_Category': str, 'IC_SO_To_Factory':str, 
                  'IC_SO_To_Factory_Item': str,
                  'IC_SO_To_Factory_Sold_To': str,
                  'IC_SO_To_Factory_Sold_To_Name': str,
                  'IC_SO_To_Factory_Ship_To_Name': str, 
                  'IC_SO_To_Factory_Route_Code': str,
                  'IC_SO_To_Factory_Route_Transit_Time_Days': str,
                  'ICSO_TO_Factory_OBDelivery_Num': str,
                  'ICSO_TO_Factory_OBDelivery_Item': str,
                  'IC_SO_Segment_Code': str,
                  'DC_IC_PO': str,
                  'DC_IC_PO_ITEM': str, 'DC_IC_PO_Qty':str,
                  'DC_IC_PO_Plant': str, 'DC_IC_PO_Requisitioner': str,
                  'CDO_IC_SO_TO_DC':str, 'CDO_IC_SO_TO_DC_Item': str,
                  'CDO_IC_SO_TO_DC_Ship_mode_Name': str, 
                  'CDO_IC_PO': str, 'CDO_IC_PO_ITEM': str, 
                  'CDO_IC_PO_Plant': str, 'CDO_IC_PO_Requisitioner':str}

dates = ['TPO_Created_On', 'TPO_Requested_Delivery_Date',
          'TPO_PR_Creation_Date', 'TPO_AB_Conf_Delivery_Date',
          'TPO_LA_Conf_Delivery_Date', 'TPO_GR_Date','TPO_IR_Posting_Date',
          'DC_IC_PO_Req_Delivery_Date','CDO_IC_PO_Req_Delivery_Date']

fac_df_dtype = {'TPO_PO_Vendor_Code': db.types.VARCHAR(), 
                'TPO_PO_Vendor_Name': db.types.VARCHAR(),
                'Trade_PO': db.types.VARCHAR(), 
                'Trade_PO_Item': db.types.VARCHAR(),
                'TPO_Deletion Indicator': db.types.VARCHAR(), 
                'TPO_Material': db.types.VARCHAR(),
                'TPO_Material_Text': db.types.VARCHAR(), 
                'TPO_Qty': db.types.INT(), 
                'TPO_Total_Net_Price': db.types.FLOAT(), 
                'TPO_Price_Unit': db.types.VARCHAR(), 
                'TPO_Plant': db.types.VARCHAR(), 
                'TPO_Storage_Loc': db.types.VARCHAR(),
                'TPO_MRP_Controller': db.types.VARCHAR(), 
                'TPO_Shipping_Point':db.types.VARCHAR(),
                'TPO_Profit_Center':db.types.VARCHAR(), 
                'TPO_Ship_mode_Name':db.types.VARCHAR(),
                'TPO_PO_Type': db.types.VARCHAR(), 
                'TPO_DIO_Flag':db.types.VARCHAR(), 
                'TPO_Incoterm': db.types.VARCHAR(), 
                'TPO_Purchase_Org': db.types.VARCHAR(), 
                'TPO_Purch_Group': db.types.VARCHAR(), 
                'Purchasing Group Description': db.types.VARCHAR(),
                'TPO_Vendor_Material_Num': db.types.VARCHAR(), 
                'TPO_AB_Line': db.types.VARCHAR(),
                'TPO_AB_Qty': db.types.FLOAT(), 
                'TPO_LA_Line': db.types.VARCHAR(),                 
                'TPO_LA_Inbound_Delivery_No': db.types.VARCHAR(), 
                'TPO_LA_Qty': db.types.FLOAT(), 
                'TPO_LA_Reference': db.types.VARCHAR(), 
                'TPO_GR_No':db.types.VARCHAR(),
                'TPO_GR_Qty': db.types.FLOAT(), 
                'TPO_GR_Indicator':db.types.VARCHAR(),
                'TPO_Net_Price': db.types.FLOAT(), 
                'Trade_PO_Total_Net_Price_Value': db.types.FLOAT(),
                'TPO_Currency': db.types.VARCHAR(), 
                'TPO_Delivery_Priority': db.types.VARCHAR(),
                'TPO_IR_Indicator': db.types.VARCHAR(), 
                'TPO_IR_No': db.types.VARCHAR(), 
                'Trade_PO_Open_Quantity': db.types.FLOAT(),
                'Trade_PO_Open_Qty_Value': db.types.FLOAT(), 
                'Trade_PO_Status': db.types.VARCHAR(),
                'TPO_Act_Asgmt_Category': db.types.VARCHAR(), 
                'IC_SO_To_Factory':db.types.VARCHAR(), 
                'IC_SO_To_Factory_Item': db.types.VARCHAR(),
                'IC_SO_To_Factory_Sold_To': db.types.VARCHAR(),
                'IC_SO_To_Factory_Sold_To_Name': db.types.VARCHAR(),
                'IC_SO_To_Factory_Ship_To_Name': db.types.VARCHAR(), 
                'IC_SO_To_Factory_Route_Code': db.types.VARCHAR(),
                'IC_SO_To_Factory_Route_Transit_Time_Days': db.types.VARCHAR(),
                'ICSO_TO_Factory_OBDelivery_Num': db.types.VARCHAR(),
                'ICSO_TO_Factory_OBDelivery_Item': db.types.VARCHAR(),
                'IC_SO_Segment_Code': db.types.VARCHAR(),
                'DC_IC_PO': db.types.VARCHAR(),
                'DC_IC_PO_ITEM': db.types.VARCHAR(), 
                'DC_IC_PO_Qty':db.types.VARCHAR(),
                'DC_IC_PO_Plant': db.types.VARCHAR(), 
                'DC_IC_PO_Requisitioner': db.types.VARCHAR(),
                'CDO_IC_SO_TO_DC':db.types.VARCHAR(), 
                'CDO_IC_SO_TO_DC_Item': db.types.VARCHAR(),
                'CDO_IC_SO_TO_DC_Ship_mode_Name': db.types.VARCHAR(), 
                'CDO_IC_PO': db.types.VARCHAR(), 
                'CDO_IC_PO_ITEM': db.types.VARCHAR(), 
                'CDO_IC_PO_Plant': db.types.VARCHAR(), 
                'CDO_IC_PO_Requisitioner':db.types.VARCHAR(),
                'TPO_Created_On':db.types.Date, 
                'TPO_Requested_Delivery_Date':db.types.Date,
                'TPO_PR_Creation_Date':db.types.Date, 
                'TPO_AB_Conf_Delivery_Date':db.types.Date,
                'TPO_LA_Conf_Delivery_Date':db.types.Date, 
                'TPO_GR_Date':db.types.Date,
                'TPO_IR_Posting_Date':db.types.Date,
                'DC_IC_PO_Req_Delivery_Date':db.types.Date,
                'CDO_IC_PO_Req_Delivery_Date':db.types.Date
              }


fac_df = pd.DataFrame()
os.chdir(fac_path)
for file in os.listdir():
    fac_data = pd.read_csv(file, dtype=fac_data_type,parse_dates=dates,
                           date_format='%m/%d/%y',
                            skiprows=3,thousands=',')
    fac_df = pd.concat([fac_df, fac_data], ignore_index=True)

feather_path = os.path.join(user, 
                             'OneDrive - HP Inc\Projects\Mansred Database\Data',
                             'Feather')
os.chdir(feather_path)

fac_df.to_feather('factory_report.feather.zstd', compression = 'zstd')

db_insertdata(fac_df, 'fac_report', hp_engine, engine,
              {'TPO_PO_Vendor_Code': db.types.VARCHAR(), 
                'TPO_PO_Vendor_Name': db.types.VARCHAR(),
                'Trade_PO': db.types.VARCHAR(), 
                'Trade_PO_Item': db.types.VARCHAR(),
                'TPO_Deletion Indicator': db.types.VARCHAR(), 
                'TPO_Material': db.types.VARCHAR(),
                'TPO_Material_Text': db.types.VARCHAR(), 
                'TPO_Qty': db.types.INT(), 
                'TPO_Total_Net_Price': db.types.FLOAT(), 
                'TPO_Price_Unit': db.types.VARCHAR(), 
                'TPO_Plant': db.types.VARCHAR(), 
                'TPO_Storage_Loc': db.types.VARCHAR(),
                'TPO_MRP_Controller': db.types.VARCHAR(), 
                'TPO_Shipping_Point':db.types.VARCHAR(),
                'TPO_Profit_Center':db.types.VARCHAR(), 
                'TPO_Ship_mode_Name':db.types.VARCHAR(),
                'TPO_PO_Type': db.types.VARCHAR(), 
                'TPO_DIO_Flag':db.types.VARCHAR(), 
                'TPO_Incoterm': db.types.VARCHAR(), 
                'TPO_Purchase_Org': db.types.VARCHAR(), 
                'TPO_Purch_Group': db.types.VARCHAR(), 
                'Purchasing Group Description': db.types.VARCHAR(),
                'TPO_Vendor_Material_Num': db.types.VARCHAR(), 
                'TPO_AB_Line': db.types.VARCHAR(),
                'TPO_AB_Qty': db.types.FLOAT(), 
                'TPO_LA_Line': db.types.VARCHAR(),                 
                'TPO_LA_Inbound_Delivery_No': db.types.VARCHAR(), 
                'TPO_LA_Qty': db.types.FLOAT(), 
                'TPO_LA_Reference': db.types.VARCHAR(), 
                'TPO_GR_No':db.types.VARCHAR(),
                'TPO_GR_Qty': db.types.FLOAT(), 
                'TPO_GR_Indicator':db.types.VARCHAR(),
                'TPO_Net_Price': db.types.FLOAT(), 
                'Trade_PO_Total_Net_Price_Value': db.types.FLOAT(),
                'TPO_Currency': db.types.VARCHAR(), 
                'TPO_Delivery_Priority': db.types.VARCHAR(),
                'TPO_IR_Indicator': db.types.VARCHAR(), 
                'TPO_IR_No': db.types.VARCHAR(), 
                'Trade_PO_Open_Quantity': db.types.FLOAT(),
                'Trade_PO_Open_Qty_Value': db.types.FLOAT(), 
                'Trade_PO_Status': db.types.VARCHAR(),
                'TPO_Act_Asgmt_Category': db.types.VARCHAR(), 
                'IC_SO_To_Factory':db.types.VARCHAR(), 
                'IC_SO_To_Factory_Item': db.types.VARCHAR(),
                'IC_SO_To_Factory_Sold_To': db.types.VARCHAR(),
                'IC_SO_To_Factory_Sold_To_Name': db.types.VARCHAR(),
                'IC_SO_To_Factory_Ship_To_Name': db.types.VARCHAR(), 
                'IC_SO_To_Factory_Route_Code': db.types.VARCHAR(),
                'IC_SO_To_Factory_Route_Transit_Time_Days': db.types.VARCHAR(),
                'ICSO_TO_Factory_OBDelivery_Num': db.types.VARCHAR(),
                'ICSO_TO_Factory_OBDelivery_Item': db.types.VARCHAR(),
                'IC_SO_Segment_Code': db.types.VARCHAR(),
                'DC_IC_PO': db.types.VARCHAR(),
                'DC_IC_PO_ITEM': db.types.VARCHAR(), 
                'DC_IC_PO_Qty':db.types.VARCHAR(),
                'DC_IC_PO_Plant': db.types.VARCHAR(), 
                'DC_IC_PO_Requisitioner': db.types.VARCHAR(),
                'CDO_IC_SO_TO_DC':db.types.VARCHAR(), 
                'CDO_IC_SO_TO_DC_Item': db.types.VARCHAR(),
                'CDO_IC_SO_TO_DC_Ship_mode_Name': db.types.VARCHAR(), 
                'CDO_IC_PO': db.types.VARCHAR(), 
                'CDO_IC_PO_ITEM': db.types.VARCHAR(), 
                'CDO_IC_PO_Plant': db.types.VARCHAR(), 
                'CDO_IC_PO_Requisitioner':db.types.VARCHAR(),
                'TPO_Created_On':db.types.Date, 
                'TPO_Requested_Delivery_Date':db.types.Date,
                'TPO_PR_Creation_Date':db.types.Date, 
                'TPO_AB_Conf_Delivery_Date':db.types.Date,
                'TPO_LA_Conf_Delivery_Date':db.types.Date, 
                'TPO_GR_Date':db.types.Date,
                'TPO_IR_Posting_Date':db.types.Date,
                'DC_IC_PO_Req_Delivery_Date':db.types.Date,
                'CDO_IC_PO_Req_Delivery_Date':db.types.Date
              }, 'replace')