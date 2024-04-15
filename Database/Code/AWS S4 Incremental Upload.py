# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 10:21:07 2024

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
os.chdir(code_path)
import credentials  # nopep8
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


# %% S4
# =============================================================================
# Factory Report
# =============================================================================
# Specify Data types
fac_data_type = {'TPO_PO_Vendor_Code': str, 'TPO_PO_Vendor_Name': str,
                 'Trade_PO': str, 'Trade_PO_Item': str,
                 'TPO_Deletion Indicator': str, 'TPO_Material': str,
                 'TPO_Material_Text': str, 'TPO_Qty': float,
                 'TPO_Total_Net_Price': float, 'TPO_Price_Unit': str,
                 'TPO_Plant': str, 'TPO_Storage_Loc': str,
                 'TPO_MRP_Controller': str, 'TPO_Shipping_Point': str,
                 'TPO_Profit_Center': str, 'TPO_Ship_mode_Name': str,
                 'TPO_PO_Type': str, 'TPO_DIO_Flag': str,
                 'TPO_Incoterm': str, 'TPO_Purchase_Org': str,
                 'TPO_Purch_Group': str, 'Purchasing Group Description': str,
                 'TPO_Vendor_Material_Num': str, 'TPO_AB_Line': str,
                 'TPO_AB_Qty': float, 'TPO_LA_Line': str,
                 'TPO_LA_Inbound_Delivery_No': str, 'TPO_LA_Qty': float,
                 'TPO_LA_Reference': str, 'TPO_GR_No': str,
                 'TPO_GR_Qty': float, 'TPO_GR_Indicator': str,
                 'TPO_Net_Price': float,
                 'Trade_PO_Total_Net_Price_Value': float,
                 'TPO_Currency': str, 'TPO_Delivery_Priority': str,
                 'TPO_IR_Indicator': str, 'TPO_IR_No': str,
                 'Trade_PO_Open_Quantity': float,
                 'Trade_PO_Open_Qty_Value': float, 'Trade_PO_Status': str,
                 'TPO_Act_Asgmt_Category': str, 'IC_SO_To_Factory': str,
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
                 'DC_IC_PO_ITEM': str, 'DC_IC_PO_Qty': str,
                 'DC_IC_PO_Plant': str, 'DC_IC_PO_Requisitioner': str,
                 'CDO_IC_SO_TO_DC': str, 'CDO_IC_SO_TO_DC_Item': str,
                 'CDO_IC_SO_TO_DC_Ship_mode_Name': str,
                 'CDO_IC_PO': str, 'CDO_IC_PO_ITEM': str,
                 'CDO_IC_PO_Plant': str, 'CDO_IC_PO_Requisitioner': str}

dates = ['TPO_Created_On', 'TPO_Requested_Delivery_Date',
         'TPO_PR_Creation_Date', 'TPO_AB_Conf_Delivery_Date',
         'TPO_LA_Conf_Delivery_Date', 'TPO_GR_Date', 'TPO_IR_Posting_Date',
         'DC_IC_PO_Req_Delivery_Date', 'CDO_IC_PO_Req_Delivery_Date']

fac_df_dtype = {'TPO_PO_Vendor_Code': sa.types.VARCHAR(),
                'TPO_PO_Vendor_Name': sa.types.VARCHAR(),
                'Trade_PO': sa.types.VARCHAR(),
                'Trade_PO_Item': sa.types.VARCHAR(),
                'TPO_Deletion Indicator': sa.types.VARCHAR(),
                'TPO_Material': sa.types.VARCHAR(),
                'TPO_Material_Text': sa.types.VARCHAR(),
                'TPO_Qty': sa.types.FLOAT(),
                'TPO_Total_Net_Price': sa.types.FLOAT(),
                'TPO_Price_Unit': sa.types.VARCHAR(),
                'TPO_Plant': sa.types.VARCHAR(),
                'TPO_Storage_Loc': sa.types.VARCHAR(),
                'TPO_MRP_Controller': sa.types.VARCHAR(),
                'TPO_Shipping_Point': sa.types.VARCHAR(),
                'TPO_Profit_Center': sa.types.VARCHAR(),
                'TPO_Ship_mode_Name': sa.types.VARCHAR(),
                'TPO_PO_Type': sa.types.VARCHAR(),
                'TPO_DIO_Flag': sa.types.VARCHAR(),
                'TPO_Incoterm': sa.types.VARCHAR(),
                'TPO_Purchase_Org': sa.types.VARCHAR(),
                'TPO_Purch_Group': sa.types.VARCHAR(),
                'Purchasing Group Description': sa.types.VARCHAR(),
                'TPO_Vendor_Material_Num': sa.types.VARCHAR(),
                'TPO_AB_Line': sa.types.VARCHAR(),
                'TPO_AB_Qty': sa.types.FLOAT(),
                'TPO_LA_Line': sa.types.VARCHAR(),
                'TPO_LA_Inbound_Delivery_No': sa.types.VARCHAR(),
                'TPO_LA_Qty': sa.types.FLOAT(),
                'TPO_LA_Reference': sa.types.VARCHAR(),
                'TPO_GR_No': sa.types.VARCHAR(),
                'TPO_GR_Qty': sa.types.FLOAT(),
                'TPO_GR_Indicator': sa.types.VARCHAR(),
                'TPO_Net_Price': sa.types.FLOAT(),
                'Trade_PO_Total_Net_Price_Value': sa.types.FLOAT(),
                'TPO_Currency': sa.types.VARCHAR(),
                'TPO_Delivery_Priority': sa.types.VARCHAR(),
                'TPO_IR_Indicator': sa.types.VARCHAR(),
                'TPO_IR_No': sa.types.VARCHAR(),
                'Trade_PO_Open_Quantity': sa.types.FLOAT(),
                'Trade_PO_Open_Qty_Value': sa.types.FLOAT(),
                'Trade_PO_Status': sa.types.VARCHAR(),
                'TPO_Act_Asgmt_Category': sa.types.VARCHAR(),
                'IC_SO_To_Factory': sa.types.VARCHAR(),
                'IC_SO_To_Factory_Item': sa.types.VARCHAR(),
                'IC_SO_To_Factory_Sold_To': sa.types.VARCHAR(),
                'IC_SO_To_Factory_Sold_To_Name': sa.types.VARCHAR(),
                'IC_SO_To_Factory_Ship_To_Name': sa.types.VARCHAR(),
                'IC_SO_To_Factory_Route_Code': sa.types.VARCHAR(),
                'IC_SO_To_Factory_Route_Transit_Time_Days': sa.types.VARCHAR(),
                'ICSO_TO_Factory_OBDelivery_Num': sa.types.VARCHAR(),
                'ICSO_TO_Factory_OBDelivery_Item': sa.types.VARCHAR(),
                'IC_SO_Segment_Code': sa.types.VARCHAR(),
                'DC_IC_PO': sa.types.VARCHAR(),
                'DC_IC_PO_ITEM': sa.types.VARCHAR(),
                'DC_IC_PO_Qty': sa.types.FLOAT(),
                'DC_IC_PO_Plant': sa.types.VARCHAR(),
                'DC_IC_PO_Requisitioner': sa.types.VARCHAR(),
                'CDO_IC_SO_TO_DC': sa.types.VARCHAR(),
                'CDO_IC_SO_TO_DC_Item': sa.types.VARCHAR(),
                'CDO_IC_SO_TO_DC_Ship_mode_Name': sa.types.VARCHAR(),
                'CDO_IC_PO': sa.types.VARCHAR(),
                'CDO_IC_PO_ITEM': sa.types.VARCHAR(),
                'CDO_IC_PO_Plant': sa.types.VARCHAR(),
                'CDO_IC_PO_Requisitioner': sa.types.VARCHAR(),
                'TPO_Created_On': sa.types.Date,
                'TPO_Requested_Delivery_Date': sa.types.Date,
                'TPO_PR_Creation_Date': sa.types.Date,
                'TPO_AB_Conf_Delivery_Date': sa.types.Date,
                'TPO_LA_Conf_Delivery_Date': sa.types.Date,
                'TPO_GR_Date': sa.types.Date,
                'TPO_IR_Posting_Date': sa.types.Date,
                'DC_IC_PO_Req_Delivery_Date': sa.types.Date,
                'CDO_IC_PO_Req_Delivery_Date': sa.types.Date
                }


def read_csv_s4(mpa_str):
    # READING CURRENT FACTORY PURCHASE ORDER REPORT
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                        mpa_str, 'CSV')
    os.chdir(path)
    df = pd.read_csv('Factory Purchase Order Report.csv',
                     dtype=fac_data_type, parse_dates=dates,
                     date_format='%m/%d/%y',
                     skiprows=3, thousands=',')
    return df


inkjet_list = ['NKG TH', 'NKG YY', 'FXN WH INK']

ink_dataset = pd.DataFrame()
for mpa in inkjet_list:
    ink_get = read_csv_s4(mpa)
    ink_dataset = pd.concat([ink_dataset, ink_get], ignore_index=True)

laser_list = ['JABIL CUU', 'JWH LASER', 'FXN CZ']
laser_dataset = pd.DataFrame()
for mpa in laser_list:
    laser_get = read_csv_s4(mpa)
    laser_dataset = pd.concat([laser_dataset, laser_get],
                              ignore_index=True)

canon_list = ['CANON\EUROPA', r'CANON\USA SG']
canon_dataset = pd.DataFrame()
for mpa in canon_list:
    canon_get = read_csv_s4(mpa)
    canon_dataset = pd.concat([canon_dataset, canon_get],
                              ignore_index=True)

fac_dataset = pd.concat([ink_dataset, laser_dataset, canon_dataset],
                        ignore_index=True)

seven_months_back = previous_day - pd.DateOffset(months=7)
seven_months_back_str = seven_months_back.strftime('%Y-%m-%d')

active_vendor_query = \
    """
SELECT DISTINCT TPO_PO_Vendor_Code FROM FactoryOps.vendor v
WHERE v.Active = 'Y'
"""

vendor_df = pd.read_sql(active_vendor_query, aws_engine)

try:
    for vendor in vendor_df['TPO_PO_Vendor_Code']:
        drop_query = """DELETE FROM FactoryOps.fac_report
        WHERE TPO_Created_On >= '{}' AND TPO_PO_Vendor_Code = '{}'
        """.format(seven_months_back_str, vendor)
        get_fac = fac_dataset.loc[
            (fac_dataset['TPO_Created_On'] >= seven_months_back) &
            (fac_dataset['TPO_PO_Vendor_Code'] == vendor)].copy()
        with aws_engine.connect() as conn:
            conn.execute(text(drop_query))
            conn.execute(text("COMMIT"))
        # engine.execute(drop_query)
        db_insertdata(get_fac, 'fac_report', aws_engine,
                      fac_df_dtype, 'append')
        print(vendor + ' Factory Report updated in database')
except:
    print('Insertion of data to fac_report table FAILED!')


update_query = \
    """UPDATE FactoryOps.table_update
SET UPDATED = '{}'
WHERE table_update.`TABLE` = 'fac_report'""".format(today_str)

try:
    with aws_engine.connect() as conn:
        conn.execute(text(update_query))
        conn.execute(text("COMMIT"))
        print('Table update for fac_report SUCCEEDED!')
except:
    print('Table update for fac_report FAILED!')
