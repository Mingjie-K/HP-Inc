# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 10:21:49 2022

@author: kohm
"""

# %% Import Libraries
from datetime import datetime
# import importlib
import pandas as pd
import numpy as np
import sqlalchemy as sa
from dateutil.relativedelta import relativedelta

from sqlalchemy import text
import os
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

seven_months_back = previous_day - pd.DateOffset(months=7)
seven_months_back_str = seven_months_back.strftime('%Y-%m-%d')

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

def incremental_update(query, df, tbl, col_dtype, engine, if_exist, date_update):
    try:
        update_query = """UPDATE FactoryOps.table_update
        SET UPDATED = '{}'
        WHERE table_update.`TABLE` = '{}'""".format(date_update, tbl)
        with engine.connect() as conn:
            conn.execute(text(query))
            conn.execute(text("COMMIT"))
            db_insertdata(df, tbl, engine, col_dtype, if_exist)
            conn.execute(text(update_query))
            conn.execute(text("COMMIT"))
    except:
        print('Table update for {} FAILED!'.format(tbl))
# %% Functions

def convert_monday(df, new_col, col):
    df[new_col] = \
        df[col].dt.to_period('W').dt.start_time
    return df


def convert_month_start(df, new_col, col):
    df[new_col] = df[col].to_numpy().astype('datetime64[M]')
    return df


def convert_iso(df, new_col, col):
    df[new_col] = df[col].dt.strftime('%y %b W%V')
    return df

def str_clean(df):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.upper()
    df_str_cols = df.select_dtypes(['object'])
    df[df_str_cols.columns] = df_str_cols.apply(lambda x: x.str.strip())
    df[df_str_cols.columns] = df_str_cols.apply(lambda x: x.str.upper())
    return df

def shipment(cum_df):
    ship_df = cum_df.groupby(
        ['BU', 'MPA_NAME', 'TPO_Created_On', 'Trade_PO', 'TPO_LA_Conf_Delivery_Date',
         'TPO_Material', 'TPO_LA_Reference', 'TPO_DIO_Flag',
         'DC_IC_PO_Plant', 'REGION', 'SUB_REGION', 'BUS_UNIT_NM',
         'PLTFRM_NM', 'PROD_LINE_CD', 'PROD_TYPE_CD', 'BUS_GRP_NM', 'CAT_NM',
         'CAT_SUB', 'PROD_GRP', 'BUS_UNIT_L3',
         'FAMILY', 'CATEGORY'])['TPO_LA_Qty'].sum().reset_index()

    ship_df = convert_monday(
        ship_df, 'TPO_LA_CONF_DELIVERY_DATE_POR', 'TPO_LA_Conf_Delivery_Date')

    ship_df = convert_month_start(
        ship_df, 'TPO_LA_CONF_DELIVERY_MONTH_POR', 'TPO_LA_CONF_DELIVERY_DATE_POR')

    ship_df = convert_iso(
        ship_df, 'ISO_POR', 'TPO_LA_CONF_DELIVERY_DATE_POR')

    return ship_df

def read_process(tbl, date_query, engine):
    fac_query = """SELECT * FROM FactoryOps.{} 
    WHERE TPO_Created_On >= '{}'""".format(tbl, date_query)

    fac_df = pd.read_sql(
        fac_query, engine,
        parse_dates=['TPO_Created_On',
                     'TPO_Requested_Delivery_Date',
                     'TPO_LA_Conf_Delivery_Date'])

    return fac_df



canonfg_cum_df = read_process('canon_clean', seven_months_back_str, aws_engine)
canonfg_cum_df = canonfg_cum_df.loc[canonfg_cum_df['CATEGORY'] == 'CANON FFGI'].copy()
canonfg_ship_df = shipment(canonfg_cum_df)

ink_laser_cum_df = read_process('ink_laser_clean', seven_months_back_str, aws_engine)
ink_laser_ship_df = shipment(ink_laser_cum_df)

ship_df = pd.concat([canonfg_ship_df, ink_laser_ship_df],
                    ignore_index=True)

ship_df = str_clean(ship_df)

ship_incre_q = """DELETE FROM FactoryOps.sail_ship WHERE TPO_CREATED_ON >= '{}'""".format(
    seven_months_back_str)

incremental_update(ship_incre_q, ship_df, 'sail_ship',
                   {'MPA_NAME': sa.types.VARCHAR(50),
                    'TPO_CREATED_ON': sa.types.Date,
                    'TRADE_PO': sa.types.VARCHAR(15),
                    'TPO_LA_CONF_DELIVERY_DATE': sa.types.Date,
                    'TPO_MATERIAL': sa.types.VARCHAR(15),
                    'TPO_LA_REFERENCE': sa.types.VARCHAR(50),
                    'TPO_DIO_FLAG': sa.types.VARCHAR(6),
                    'DC_IC_PO_PLANT': sa.types.VARCHAR(8),
                    'REGION': sa.types.VARCHAR(10),
                    'SUB_REGION': sa.types.VARCHAR(10),
                    'BUS_UNIT_NM': sa.types.VARCHAR(30),
                    'PLTFRM_NM': sa.types.VARCHAR(50),
                    'PROD_TYPE_CD': sa.types.VARCHAR(15),
                    'PROD_LINE_CD': sa.types.VARCHAR(5),
                    'BUS_GRP_NM': sa.types.VARCHAR(15),
                    'CAT_NM': sa.types.VARCHAR(40),
                    'CAT_SUB': sa.types.VARCHAR(40),
                    'PROD_GRP': sa.types.VARCHAR(40),
                    'BUS_UNIT_L3': sa.types.VARCHAR(30),
                    'TPO_LA_QTY': sa.types.FLOAT(),
                    'TPO_LA_CONF_DELIVERY_DATE_POR': sa.types.Date,
                    'TPO_LA_CONF_DELIVERY_MONTH_POR': sa.types.Date,
                    'BU': sa.types.VARCHAR(20),
                    'FAMILY': sa.types.VARCHAR(50),
                    'CATEGORY': sa.types.VARCHAR(25),
                    'ISO_POR': sa.types.VARCHAR(20)}, aws_engine, 'append', today_str)
