from datetime import datetime

# import pyodbc
# import importlib
import glob
import pandas as pd
import sqlalchemy as sa
import os
import warnings
from sqlalchemy import text
# from urllib.parse import quote_plus
user = os.getenv('USERPROFILE')

pd.set_option('display.max.columns', None)

code_path = os.path.join(user,
                         'OneDrive - HP Inc/Projects/Database/Code')
os.chdir(code_path)
import credentials  # nopep8
pd.set_option('display.max.columns', None)

engine = credentials.connection()
aws_engine = credentials.aws_connection()

# Set today for updated date
today = datetime.today()
today_weekday = today.weekday()
today_str = today.strftime('%Y-%m-%d %H:%M:%S')

canon_etd_his_path = os.path.join(
    user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data\CANON\ETD')

canon_pbi_path = os.path.join(
    user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data\CANON\PBI DOWNLOAD')


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


def read_pbi_etd(path):
    os.chdir(path)
    excel_files = glob.glob(path + '/*' + '.xlsx')
    latest_excel = max(excel_files, key=os.path.getctime)
    # print(latest_csv)
    # REPLACE TO CROSSTAB FORMAT
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        etd_df = pd.read_excel(latest_excel,
                               dtype={'TPO_PO_VENDOR_CODE': str,
                                      'TRADE_PO': str,
                                      'TRADE_PO_LINE': str,
                                      'CDO_IC_PO': str,
                                      'DC_IC_PO': str,
                                      'DC_IC_PO_LINE': str,
                                      'TPO_MATERIAL': str,
                                      'TPO_QTY': float,
                                      'TPO_PLANT': str,
                                      'TPO_SHIPPING_POINT': str,
                                      'TPO_PO_TYPE': str,
                                      'TPO_AB_LINE': str,
                                      'TPO_AB_QTY': float,
                                      'TPO_LA_INBOUND_DELIVERY_NO': str,
                                      'TPO_LA_QTY': float,
                                      'TPO_LA_REFERENCE': str,
                                      'TRADE_PO_OPEN_QUANTITY': float,
                                      'DC_IC_PO_QTY': float,
                                      'DC_IC_PO_PLANT': str,
                                      'CDO_IC_PO_PLANT': str,
                                      'DC_IC_PO_REQUISITIONER': str,
                                      'CDO_IC_PO_REQUISITIONER': str,
                                      'REQUISITIONER': str,
                                      'TPOLATABLE_DELIVERY_NR': str,
                                      'TPOLATABLE_BILL_LADING': str,
                                      'TPOLATABLE_MNS_OF_TRANS_ID': str},
                               parse_dates=['TPO_CREATED_ON',
                                            'TPO_REQUESTED_DELIVERY_DATE',
                                            'TPO_AB_CONF_DELIVERY_DATE',
                                            'TPO_LA_CONF_DELIVERY_DATE',
                                            'DC_IC_PO_REQ_DELIVERY_DATE',
                                            'CDO_IC_PO_REQ_DELIVERY_DATE',
                                            'TPOLATABLE_IBP_ETD_FACTORY',
                                            'TPOLATABLE_IBP_ETA_DC',
                                            'TPOLATABLE_IBD_PORT_ARRIVAL_DT'],
                               thousands=',')
    # DROP UNNECESSARY COLUMNS
    etd_df = etd_df.loc[:, ~etd_df.columns.str.contains('^Unnamed')].copy()

    etd_df[['TPOLATABLE_IBP_ETA_DC', 'TPOLATABLE_IBP_ETD_FACTORY',
            'TPOLATABLE_IBD_PORT_ARRIVAL_DT']] = \
        etd_df[['TPOLATABLE_IBP_ETA_DC', 'TPOLATABLE_IBP_ETD_FACTORY',
                'TPOLATABLE_IBD_PORT_ARRIVAL_DT']].apply(pd.to_datetime, errors='coerce')
    return etd_df


def read_old_etd(path):
    os.chdir(path)
    csv_files = glob.glob(path + '/*' + '.csv')
    latest_csv = max(csv_files, key=os.path.getctime)
    # print(latest_csv)
    # REPLACE TO CROSSTAB FORMAT
    etd_df = pd.read_csv(latest_csv,
                         dtype={'TPO_PO_Vendor_Code': str,
                                'Trade_PO': str,
                                'TRADE_PO_LINE': str,
                                'CDO_IC_PO': str,
                                'DC_IC_PO': str,
                                'DC_IC_PO_LINE': str,
                                'TPO_Material': str,
                                'TPO_Qty': float,
                                'TPO_Plant': str,
                                'TPO_Shipping_Point': str,
                                'TPO_PO_Type': str,
                                'TPO_AB_Line': str,
                                'TPO_AB_Qty': float,
                                'TPO_LA_Inbound_Delivery_No': str,
                                'TPO_LA_Qty': float,
                                'TPO_LA_Reference': str,
                                'Trade_PO_Open_Quantity': float,
                                'DC_IC_PO_Qty': float,
                                'DC_IC_PO_Plant': str,
                                'CDO_IC_PO_Plant': str,
                                'DC_IC_PO_Requisitioner': str,
                                'CDO_IC_PO_Requisitioner': str,
                                'Requisitioner': str,
                                'TPOLATABLE_DELIVERY_NR': str,
                                'TPOLATABLE_BILL_LADING': str,
                                'TPOLATABLE_MNS_OF_TRANS_ID': str},
                         parse_dates=['TPO_Created_On',
                                      'TPO_Requested_Delivery_Date',
                                      'TPO_AB_Conf_Delivery_Date',
                                      'TPO_LA_Conf_Delivery_Date',
                                      'DC_IC_PO_Req_Delivery_Date',
                                      'CDO_IC_PO_Req_Delivery_Date',
                                      'TPOLATABLE_IBP_ETD_FACTORY',
                                      'TPOLATABLE_IBP_ETA_DC',
                                      'TPOLATABLE_IBD_PORT_ARRIVAL_DT'])
    # DROP UNNECESSARY COLUMNS
    etd_df = etd_df.loc[:, ~etd_df.columns.str.contains('^Unnamed')].copy()

    return etd_df


canon_etd_hist = read_old_etd(canon_etd_his_path)
canon_etd_pbi = read_pbi_etd(canon_pbi_path)

# OVER WRITE IF NECCESSARY
min_date = canon_etd_pbi['TPO_CREATED_ON'].min()
canon_etd_hist = canon_etd_hist.loc[
    canon_etd_hist['TPO_Created_On'] < min_date].copy()

# REPLACE COLUMN NAMES
canon_etd_pbi.columns = canon_etd_hist.columns
# COMBINE DATA AND OUTPUT
canon_etd_df = pd.concat([canon_etd_hist, canon_etd_pbi], ignore_index=True)
canon_etd_df = canon_etd_df.loc[(canon_etd_df['Trade_PO'] != 'No filters applied') & (
    canon_etd_df['Trade_PO'].notnull())].copy()
os.chdir(canon_etd_his_path)
canon_etd_df.to_csv('Canon Shipment_Report.csv', index=False)

canon_dates = ['TPO_Created_On',
               'TPO_Requested_Delivery_Date',
               'TPO_AB_Conf_Delivery_Date',
               'TPO_LA_Conf_Delivery_Date',
               'DC_IC_PO_Req_Delivery_Date',
               'CDO_IC_PO_Req_Delivery_Date',
               'TPOLATABLE_IBP_ETA_DC',
               'TPOLATABLE_IBP_ETD_FACTORY',
               'TPOLATABLE_IBD_PORT_ARRIVAL_DT']

canon_etd_dtype = {'Trade_PO': sa.types.VARCHAR(15),
                   'TRADE_PO_LINE': sa.types.VARCHAR(10),
                   'CDO_IC_PO': sa.types.VARCHAR(20),
                   'DC_IC_PO': sa.types.VARCHAR(20),
                   'DC_IC_PO_LINE': sa.types.VARCHAR(10),
                   'TPO_PO_Vendor_Code': sa.types.VARCHAR(15),
                   'TPO_Created_On': sa.types.Date,
                   'TPO_Material': sa.types.VARCHAR(15),
                   'TPO_Qty': sa.types.FLOAT(),
                   'TPO_PO_Type': sa.types.VARCHAR(10),
                   'TPO_AB_Line': sa.types.VARCHAR(8),
                   'TPO_AB_Conf_Delivery_Date': sa.types.Date,
                   'TPO_AB_Qty': sa.types.FLOAT(),
                   'TPO_LA_Conf_Delivery_Date': sa.types.Date,
                   'TPO_LA_Inbound_Delivery_No': sa.types.VARCHAR(20),
                   'TPO_LA_Qty': sa.types.FLOAT(),
                   'TPO_LA_Reference': sa.types.VARCHAR(50),
                   'Trade_PO_Open_Quantity': sa.types.FLOAT(),
                   'DC_IC_PO_Req_Delivery_Date': sa.types.Date,
                   'DC_IC_PO_Qty': sa.types.FLOAT(),
                   'DC_IC_PO_Plant': sa.types.VARCHAR(8),
                   'CDO_IC_PO_Req_Delivery_Date': sa.types.Date,
                   'CDO_IC_PO_Plant': sa.types.VARCHAR(8),
                   'DC_IC_PO_Requisitioner': sa.types.VARCHAR(15),
                   'CDO_IC_PO_Requisitioner': sa.types.VARCHAR(25),
                   'Requisitioner': sa.types.VARCHAR(20),
                   'TPOLATABLE_DELIVERY_NR': sa.types.VARCHAR(20),
                   'TPOLATABLE_IBP_ETA_DC': sa.types.Date,
                   'TPOLATABLE_IBP_ETD_FACTORY': sa.types.Date,
                   'TPOLATABLE_IBD_PORT_ARRIVAL_DT': sa.types.Date,
                   'TPOLATABLE_BILL_LADING': sa.types.VARCHAR(50),
                   'TPOLATABLE_MNS_OF_TRANS_ID': sa.types.VARCHAR(40)
                   }

# DATABASE
previous_day = today - pd.DateOffset(days=1)
previous_day = previous_day.replace(hour=0, minute=0, second=0, microsecond=0)
seven_months_back = previous_day - pd.DateOffset(months=7)
seven_months_back_str = seven_months_back.strftime('%Y-%m-%d')

try:
    drop_query = """DELETE FROM FactoryOps.canon_etd
    WHERE TPO_Created_On >= '{}'
    """.format(seven_months_back_str)
    get_etd = canon_etd_df.loc[
        (canon_etd_df['TPO_Created_On'] >= seven_months_back)].copy()
    with aws_engine.connect() as conn:
        conn.execute(text(drop_query))
        conn.execute(text("COMMIT"))
    # engine.execute(drop_query)
    db_insertdata(get_etd, 'canon_etd', aws_engine,
                  canon_etd_dtype, 'append')
    print('Canon ETD updated in database')
except:
    print('Insertion of data to canon_etd table FAILED!')

update_query = \
    """UPDATE FactoryOps.table_update
SET UPDATED = '{}'
WHERE table_update.`TABLE` = 'canon_etd'""".format(today_str)

try:
    with aws_engine.connect() as conn:
        conn.execute(text(update_query))
        conn.execute(text("COMMIT"))
        print('Table update for canon_etd SUCCEEDED!')
except:
    print('Table update for canon_etd FAILED!')