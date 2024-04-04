# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 10:21:07 2024

@author: kohm
"""

import pandas as pd
import sqlalchemy as sa
import os
from datetime import datetime

pd.set_option('display.max.columns', None)
user = os.getenv('USERPROFILE')
code_path = os.path.join(user, 
                         'OneDrive - HP Inc/Projects/Database/Code')
os.chdir(code_path)
import credentials

pd.set_option('display.max.columns', None)
# Set data path
feather_path = os.path.join(user,
                            'OneDrive - HP Inc\Projects\Database\Data',
                            'Feather')

aws_engine = credentials.aws_connection()

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
        pass


# Set today for updated date
today = datetime.today()
today_weekday = today.weekday()
today_str = today.strftime('%Y-%m-%d %H:%M:%S')

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

fac_df_dtype = {'TPO_PO_Vendor_Code': sa.types.VARCHAR(length=15), 
                'TPO_PO_Vendor_Name': sa.types.VARCHAR(length=50),
                'Trade_PO': sa.types.VARCHAR(length=15), 
                'Trade_PO_Item': sa.types.VARCHAR(length=10),
                'TPO_Deletion Indicator': sa.types.VARCHAR(length=2), 
                'TPO_Material': sa.types.VARCHAR(length=15),
                'TPO_Material_Text': sa.types.VARCHAR(length=50), 
                'TPO_Qty': sa.types.FLOAT(), 
                'TPO_Total_Net_Price': sa.types.FLOAT(), 
                'TPO_Price_Unit': sa.types.VARCHAR(length=5), 
                'TPO_Plant': sa.types.VARCHAR(length=10), 
                'TPO_Storage_Loc': sa.types.VARCHAR(length=10),
                'TPO_MRP_Controller': sa.types.VARCHAR(length=6), 
                'TPO_Shipping_Point':sa.types.VARCHAR(length=6),
                'TPO_Profit_Center':sa.types.VARCHAR(length=6), 
                'TPO_Ship_mode_Name':sa.types.VARCHAR(length=40),
                'TPO_PO_Type': sa.types.VARCHAR(length=10), 
                'TPO_DIO_Flag':sa.types.VARCHAR(length=6), 
                'TPO_Incoterm': sa.types.VARCHAR(length=6), 
                'TPO_Purchase_Org': sa.types.VARCHAR(length=6), 
                'TPO_Purch_Group': sa.types.VARCHAR(length=6), 
                'Purchasing Group Description': sa.types.VARCHAR(length=30),
                'TPO_Vendor_Material_Num': sa.types.VARCHAR(length=30), 
                'TPO_AB_Line': sa.types.VARCHAR(length=8),
                'TPO_AB_Qty': sa.types.FLOAT(), 
                'TPO_LA_Line': sa.types.VARCHAR(length=8),                 
                'TPO_LA_Inbound_Delivery_No': sa.types.VARCHAR(length=20), 
                'TPO_LA_Qty': sa.types.FLOAT(), 
                'TPO_LA_Reference': sa.types.VARCHAR(length=50), 
                'TPO_GR_No':sa.types.VARCHAR(length=20),
                'TPO_GR_Qty': sa.types.FLOAT(), 
                'TPO_GR_Indicator':sa.types.VARCHAR(length=2),
                'TPO_Net_Price': sa.types.FLOAT(), 
                'Trade_PO_Total_Net_Price_Value': sa.types.FLOAT(),
                'TPO_Currency': sa.types.VARCHAR(length=6), 
                'TPO_Delivery_Priority': sa.types.VARCHAR(length=4),
                'TPO_IR_Indicator': sa.types.VARCHAR(length=2), 
                'TPO_IR_No': sa.types.VARCHAR(length=20), 
                'Trade_PO_Open_Quantity': sa.types.FLOAT(),
                'Trade_PO_Open_Qty_Value': sa.types.FLOAT(), 
                'Trade_PO_Status': sa.types.VARCHAR(length=15),
                'TPO_Act_Asgmt_Category': sa.types.VARCHAR(length=2), 
                'IC_SO_To_Factory':sa.types.VARCHAR(length=20), 
                'IC_SO_To_Factory_Item': sa.types.VARCHAR(length=15),
                'IC_SO_To_Factory_Sold_To': sa.types.VARCHAR(length=20),
                'IC_SO_To_Factory_Sold_To_Name': sa.types.VARCHAR(length=50),
                'IC_SO_To_Factory_Ship_To': sa.types.VARCHAR(length=20),
                'IC_SO_To_Factory_Ship_To_Name': sa.types.VARCHAR(length=50), 
                'IC_SO_To_Factory_Route_Code': sa.types.VARCHAR(length=15),
                'IC_SO_To_Factory_Route_Transit_Time_Days': sa.types.VARCHAR(length=6),
                'ICSO_TO_Factory_OBDelivery_Num': sa.types.VARCHAR(length=20),
                'ICSO_TO_Factory_OBDelivery_Item': sa.types.VARCHAR(length=15),
                'IC_SO_Segment_Code': sa.types.VARCHAR(length=15),
                'DC_IC_PO': sa.types.VARCHAR(length=20),
                'DC_IC_PO_ITEM': sa.types.VARCHAR(length=10), 
                'DC_IC_PO_Qty':sa.types.FLOAT(),
                'DC_IC_PO_Plant': sa.types.VARCHAR(length=8), 
                'DC_IC_PO_Requisitioner': sa.types.VARCHAR(length=15),
                'CDO_IC_SO_TO_DC':sa.types.VARCHAR(length=20), 
                'CDO_IC_SO_TO_DC_Item': sa.types.VARCHAR(length=15),
                'CDO_IC_SO_TO_DC_Ship_mode_Name': sa.types.VARCHAR(length=40), 
                'CDO_IC_PO': sa.types.VARCHAR(length=20), 
                'CDO_IC_PO_ITEM': sa.types.VARCHAR(length=10), 
                'CDO_IC_PO_Plant': sa.types.VARCHAR(length=8), 
                'CDO_IC_PO_Requisitioner':sa.types.VARCHAR(length=25),
                'TPO_Created_On':sa.types.Date, 
                'TPO_Requested_Delivery_Date':sa.types.Date,
                'TPO_PR_Creation_Date':sa.types.Date, 
                'TPO_AB_Conf_Delivery_Date':sa.types.Date,
                'TPO_LA_Conf_Delivery_Date':sa.types.Date, 
                'TPO_GR_Date':sa.types.Date,
                'TPO_IR_Posting_Date':sa.types.Date,
                'DC_IC_PO_Req_Delivery_Date':sa.types.Date,
                'CDO_IC_PO_Req_Delivery_Date':sa.types.Date
              }


os.chdir(feather_path)
fac_df = pd.read_feather('factory_report.feather.zstd')

db_insertdata(fac_df, 'fac_report', aws_engine, fac_df_dtype, 'replace')

# =============================================================================
# ONE TIME APPEND
# =============================================================================
# d = {'TABLE': ['fac_report'], 
#      'UPDATED': [today_str]}
# table_update = pd.DataFrame(data=d)
# table_update.to_sql('table_update',
#                   aws_engine,
#                   if_exists='append',
#                   chunksize=None,
#                   index=False,
#                   dtype={'TABLE': sa.types.VARCHAR(length=50),
#                          'UPDATED': sa.types.VARCHAR(length=25)})
