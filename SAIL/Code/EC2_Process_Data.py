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


def str_fill(df):
    df_str_cols = df.select_dtypes(['object'])
    df[df_str_cols.columns] = df_str_cols.apply(lambda x: x.fillna(''))
    return df


def str_clean(df):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.upper()
    df_str_cols = df.select_dtypes(['object'])
    df[df_str_cols.columns] = df_str_cols.apply(lambda x: x.str.strip())
    df[df_str_cols.columns] = df_str_cols.apply(lambda x: x.str.upper())
    return df


def s4_data_cleaning(df):
    df[['TPO_Ship_mode_Name', 'IC_SO_To_Factory_Sold_To_Name',
        'IC_SO_To_Factory_Ship_To_Name', 'DC_IC_PO_Plant',
        'TPO_DIO_Flag', 'REGION', 'SUB_REGION']] = \
        df[['TPO_Ship_mode_Name', 'IC_SO_To_Factory_Sold_To_Name',
            'IC_SO_To_Factory_Ship_To_Name', 'DC_IC_PO_Plant',
            'TPO_DIO_Flag', 'REGION', 'SUB_REGION']].fillna('')

    df[['TPO_LA_Qty', 'TPO_GR_Qty']] = df[[
        'TPO_LA_Qty', 'TPO_GR_Qty']].fillna(0)

    df = df.drop_duplicates(keep='last')

    return df


def duplicated_tpo_clean(df):
    df.loc[df.duplicated(
        ['TPO_PO_Vendor_Code',
         'Trade_PO', 'Trade_PO_Item',
         'TPO_Requested_Delivery_Date',
         'TPO_Material',
         'TPO_Qty',
         'TPO_Total_Net_Price',
         'TPO_Price_Unit',
         'TPO_Plant', 'TPO_Storage_Loc',
         'TPO_Profit_Center',
         'TPO_Ship_mode_Name',
         'TPO_Net_Price',
         'Trade_PO_Total_Net_Price_Value',
         'TPO_Currency',
         'Trade_PO_Open_Quantity',
         'Trade_PO_Open_Qty_Value',
         'Trade_PO_Status',
         'IC_SO_To_Factory_Ship_To_Name']),
        ['TPO_Qty', 'TPO_Total_Net_Price', 'TPO_Net_Price',
            'Trade_PO_Total_Net_Price_Value',
         'Trade_PO_Open_Quantity',
         'Trade_PO_Open_Qty_Value']] = np.nan
    return df


def category(df):
    df.loc[df['PROD_TYPE_CD'] == 'FFGI', 'CATEGORY'] = 'FFGI'
    df.loc[df['PROD_TYPE_CD'] == 'ACCSRY', 'CATEGORY'] = 'ACCESSORIES'
    df.loc[df['PROD_TYPE_CD'] == 'FGI', 'CATEGORY'] = 'FFGI'
    df.loc[df['PROD_TYPE_CD'] == 'PRE', 'CATEGORY'] = 'ENGINE'
    df.loc[df['PROD_TYPE_CD'] == 'LLC', 'CATEGORY'] = 'SUPPLIES'
    df.loc[df['PROD_TYPE_CD'] == '', 'CATEGORY'] = ''
    df.loc[df['PROD_TYPE_CD'] == 'SRFR', 'CATEGORY'] = 'SERVICE REPLACEMENT'
    return df


def read_factory(bu, vendor, skus, date_query, engine):
    fac_query = """SELECT '{}' AS 'BU', fac.TPO_PO_Vendor_Code, 
    fac.Trade_PO, fac.Trade_PO_Item, 
    fac.TPO_Created_On, fac.TPO_Material,fac.TPO_Qty, 
    fac.TPO_Total_Net_Price, 
    fac.TPO_Price_Unit, fac.TPO_Requested_Delivery_Date,
    fac.TPO_PR_Creation_Date, fac.TPO_Plant, fac.TPO_Storage_Loc, 
    fac.TPO_Shipping_Point,fac.TPO_Profit_Center, 
    fac.TPO_Ship_mode_Name, fac.TPO_PO_Type, fac.TPO_DIO_Flag,
    fac.TPO_LA_Conf_Delivery_Date, fac.TPO_LA_Inbound_Delivery_No,
    fac.TPO_LA_Qty, fac.TPO_LA_Reference, 
    fac.TPO_GR_Qty, fac.TPO_GR_Indicator, 
    fac.TPO_Net_Price, fac.Trade_PO_Total_Net_Price_Value, 
    fac.TPO_Currency, fac.Trade_PO_Open_Quantity, 
    fac.Trade_PO_Open_Qty_Value, fac.Trade_PO_Status, 
    fac.IC_SO_To_Factory_Sold_To_Name, 
    fac.IC_SO_To_Factory_Ship_To_Name, 
    fac.DC_IC_PO_Plant, fac.CDO_IC_PO_Plant, 
    p.Region_Ship AS REGION, 
    p.Sub_Region AS SUB_REGION, p.Interplant
    FROM
    FactoryOps.fac_report AS fac
    LEFT JOIN FactoryOps.plant AS p
    ON fac.DC_IC_PO_Plant = p.Plant
    WHERE (`TPO_Deletion Indicator` != 'L' 
    OR `TPO_Deletion Indicator` IS NULL)
    AND fac.TPO_PO_Vendor_Code IN {} AND fac.TPO_Material IN {}
    AND fac.TPO_Created_On >= '{}'
    """.format(bu, vendor, skus, date_query)

    fac_df = pd.read_sql(
        fac_query, engine,
        parse_dates=['TPO_Created_On',
                     'TPO_Requested_Delivery_Date',
                     'TPO_LA_Conf_Delivery_Date',
                     'TPO_PR_Creation_Date'])

    return fac_df


def group_cum(df, vendor, skus, canon_bool):
    df = df.groupby(
        ['BU',
         'TPO_PO_Vendor_Code',
         'Trade_PO',
         'Trade_PO_Item',
         'TPO_Created_On',
         'TPO_Requested_Delivery_Date',
         'TPO_LA_Conf_Delivery_Date',
         'TPO_Material',
         'TPO_Qty', 'TPO_LA_Reference',
         'TPO_Total_Net_Price', 'TPO_Price_Unit',
         'TPO_Plant', 'TPO_Storage_Loc',
         'TPO_Profit_Center', 'TPO_Ship_mode_Name',
         'TPO_DIO_Flag',
         'TPO_Net_Price',
         'Trade_PO_Total_Net_Price_Value',
         'TPO_Currency',
         'Trade_PO_Open_Quantity',
         'Trade_PO_Open_Qty_Value',
         'Trade_PO_Status',
         'TPO_LA_Inbound_Delivery_No',
         'IC_SO_To_Factory_Ship_To_Name',
         'DC_IC_PO_Plant', 'REGION', 'SUB_REGION'],
        dropna=False)[['TPO_LA_Qty', 'TPO_GR_Qty']].sum().reset_index()

    df = df.merge(vendor, how='left', on='TPO_PO_Vendor_Code')
    if not canon_bool:
        df = df.merge(skus, how='left', on=[
            'TPO_PO_Vendor_Code', 'TPO_Material'])

    return df


def canon_cum_clean(cum_df, etd):
    # NEED GROUP ETD DATASET FIRST
    etd = etd[['TPO_PO_Vendor_Code', 'Trade_PO', 'TPO_Material',
               'TPO_LA_Inbound_Delivery_No',
               'TPOLATABLE_IBP_ETD_FACTORY']]
    # GET THE MIN ETD FACTORY
    etd = etd.groupby(['TPO_PO_Vendor_Code', 'Trade_PO', 'TPO_Material',
                       'TPO_LA_Inbound_Delivery_No'])['TPOLATABLE_IBP_ETD_FACTORY'].min().reset_index()

    cum_df = cum_df.groupby(
        ['BU', 'MPA_NAME', 'TPO_PO_Vendor_Code', 'Trade_PO', 'TPO_Created_On',
         'TPO_Requested_Delivery_Date', 'TPO_LA_Conf_Delivery_Date', 'TPO_Material',
         'TPO_LA_Reference', 'TPO_Plant', 'TPO_Profit_Center', 'TPO_DIO_Flag',
         'TPO_LA_Inbound_Delivery_No', 'DC_IC_PO_Plant', 'REGION', 'SUB_REGION'],
        dropna=False)[['TPO_Qty', 'TPO_LA_Qty']].sum().reset_index()

    canon_merged = cum_df.merge(etd, how='left',
                                on=['TPO_PO_Vendor_Code',
                                    'Trade_PO',
                                    'TPO_Material',
                                    'TPO_LA_Inbound_Delivery_No'])
    # Replace TPO_LA_Conf_Delivery_Date with SAP DATE
    canon_merged['TPO_LA_Conf_Delivery_Date'] = \
        canon_merged['TPOLATABLE_IBP_ETD_FACTORY']

    # FFILL MISSING DATE COLUMN
    canon_merged['TPO_LA_Conf_Delivery_Date'] = \
        canon_merged.groupby(
            ['TPO_Created_On', 'TPO_Plant', 'Trade_PO', 'TPO_Material',
                'REGION', 'SUB_REGION', 'DC_IC_PO_Plant'])['TPO_LA_Conf_Delivery_Date'].ffill()
    canon_merged['TPO_LA_Conf_Delivery_Date'] = \
        canon_merged.groupby(
            ['TPO_Created_On', 'TPO_Plant', 'Trade_PO', 'TPO_Material',
                'REGION', 'SUB_REGION', 'DC_IC_PO_Plant'])['TPO_LA_Conf_Delivery_Date'].bfill()

    canon_merged = canon_merged.drop(columns=['TPOLATABLE_IBP_ETD_FACTORY'])
    return canon_merged


# %% QUERIES
skus_query = """SELECT TPO_PO_Vendor_Code, PLTFRM_NM, BUS_UNIT_NM, PART_NR AS TPO_Material,
PROD_LINE_CD ,PROD_TYPE_CD ,BUS_GRP_NM ,CAT_NM ,CAT_SUB ,PROD_GRP ,BUS_UNIT_L3 
FROM FactoryOps.{} 
LEFT JOIN FactoryOps.vendor 
ON ISAAC_MPA_NAME = LOC_FROM_NM 
WHERE vendor.BU = '{}' AND Active = 'Y'
"""

ink_skus_query = skus_query.format('ink_skus', 'Inkjet')
laser_skus_query = skus_query.format('laser_skus', 'Laser')

ink_vendors_query = """SELECT DISTINCT TPO_PO_Vendor_Code, MPA_NAME
FROM FactoryOps.vendor WHERE BU = 'Inkjet' AND Active = 'Y'"""

laser_vendors_query = """SELECT DISTINCT TPO_PO_Vendor_Code, MPA_NAME
FROM FactoryOps.vendor WHERE BU = 'Laser' AND MPA_NAME != 'Laser Canon' AND Active = 'Y'"""

laser_ex_query = """
SELECT DISTINCT Plant FROM FactoryOps.plant p 
WHERE Interplant = 'FXNWH L'
"""

bus_family_query = """SELECT MPA_NAME, PLTFRM_NM, FAMILY, PART_NR AS TPO_Material
FROM FactoryOps.business_family"""

fxnwhl_pc_query = """SELECT * FROM FactoryOps.fxnwhl_pc"""

canon_skus_query = """SELECT PLTFRM_NM, BUS_UNIT_NM, PART_NR AS TPO_Material ,
PROD_LINE_CD, PROD_TYPE_CD, BUS_GRP_NM, CAT_NM, CAT_SUB, PROD_GRP, FACTORY, BUS_UNIT_L3
FROM FactoryOps.{}"""

canoneng_region_query = """SELECT Plant AS TPO_Plant, Region_Ship AS REGION
FROM FactoryOps.plant WHERE Plant IN {}
"""

canon_fg_query = canon_skus_query.format('canon_ffgi_skus')
canon_eng_query = canon_skus_query.format('canon_eng_skus')

canon_vendors_query = """SELECT DISTINCT TPO_PO_Vendor_Code, MPA_NAME
FROM FactoryOps.vendor WHERE BU = 'Laser' AND MPA_NAME = 'Laser Canon' AND Active = 'Y'"""

canon_etd_query = """SELECT * FROM FactoryOps.canon_etd"""

canon_site_query = """SELECT * FROM FactoryOps.canon_site"""
# %% DATASET
bus_fam = pd.read_sql(bus_family_query, aws_engine)
# INKJET
ink_skus = pd.read_sql(ink_skus_query, aws_engine)
ink_skus = ink_skus.fillna('')
ink_vendors = pd.read_sql(ink_vendors_query, aws_engine)

ink_fac_df = read_factory('INKJET', tuple(ink_vendors['TPO_PO_Vendor_Code']),
                          tuple(tuple(ink_skus['TPO_Material'].unique())), seven_months_back_str, aws_engine)

ink_fac_df = s4_data_cleaning(ink_fac_df)

ink_cum_df = group_cum(ink_fac_df, ink_vendors, ink_skus, False)

ink_cum_df = duplicated_tpo_clean(ink_cum_df)

ink_cum_df = ink_cum_df.merge(
    bus_fam[['MPA_NAME', 'PLTFRM_NM', 'FAMILY']], how='left', on=[
        'MPA_NAME', 'PLTFRM_NM'])

ink_cum_df = category(ink_cum_df)

ink_cum_df = str_fill(ink_cum_df)

# LASER
laser_skus = pd.read_sql(laser_skus_query, aws_engine)
laser_skus = laser_skus.fillna('')
laser_vendors = pd.read_sql(laser_vendors_query, aws_engine)

laser_fac_df = read_factory('LASER', tuple(laser_vendors['TPO_PO_Vendor_Code']),
                            tuple(tuple(laser_skus['TPO_Material'].unique())), seven_months_back_str, aws_engine)

laser_fac_df = s4_data_cleaning(laser_fac_df)

laser_cum_df = group_cum(laser_fac_df, laser_vendors, laser_skus, False)

laser_cum_df = duplicated_tpo_clean(laser_cum_df)

# Remove interplant and replace skus for fxnwhl
laser_ex_df = pd.read_sql(laser_ex_query, aws_engine)
laser_cum_df = laser_cum_df.loc[
    ~((laser_cum_df['MPA_NAME'] == ('Laser Foxconn Weihai')) & (
        laser_cum_df['DC_IC_PO_Plant'].isin(laser_ex_df['Plant'])))].copy()

# Separate out FXNWHL to map family and category
fxnwhl_cum = laser_cum_df.loc[
    (laser_cum_df['MPA_NAME'] == 'Laser Foxconn Weihai')].copy()

pcc_cum = laser_cum_df.loc[
    (laser_cum_df['MPA_NAME'] != 'Laser Foxconn Weihai')].copy()

fxnwhl_cum = fxnwhl_cum.merge(
    bus_fam[['MPA_NAME', 'FAMILY', 'TPO_Material']],
    how='left', on=['MPA_NAME', 'TPO_Material'])

pcc_cum = pcc_cum.merge(
    bus_fam[['MPA_NAME', 'PLTFRM_NM', 'FAMILY']],
    how='left', on=['MPA_NAME', 'PLTFRM_NM'])

fxnwhl_pc = pd.read_sql(fxnwhl_pc_query, aws_engine)
fxnwhl_ovr = fxnwhl_pc.loc[fxnwhl_pc['SKU PL OVERWRITE'].notnull()].copy()
fxnwhl_ovr['SKU PL OVERWRITE'] = fxnwhl_ovr['SKU PL OVERWRITE'].str.split(',')
fxnwhl_ovr = fxnwhl_ovr.explode(['SKU PL OVERWRITE'])

fxnwhl_cum = fxnwhl_cum.merge(
    fxnwhl_pc[['TPO_Profit_Center', 'CATEGORY']],
    how='left', on='TPO_Profit_Center')

for cat, sku in zip(fxnwhl_ovr['CATEGORY'],
                    fxnwhl_ovr['SKU PL OVERWRITE']):
    fxnwhl_cum.loc[
        fxnwhl_cum['TPO_Material'] == sku, 'CATEGORY'] = cat

pcc_cum = category(pcc_cum)

laser_cum_df = pd.concat([pcc_cum, fxnwhl_cum], ignore_index=True)

laser_cum_df = str_fill(laser_cum_df)

# Laser Canon
canonfg_skus = pd.read_sql(canon_fg_query, aws_engine)
canonfg_skus = canonfg_skus.fillna('')

canoneng_skus = pd.read_sql(canon_eng_query, aws_engine)
canoneng_skus = canoneng_skus.fillna('')
canon_vendors = pd.read_sql(canon_vendors_query, aws_engine)

canonfg_fac_df = read_factory('LASER', tuple(canon_vendors['TPO_PO_Vendor_Code']),
                              tuple(tuple(canonfg_skus['TPO_Material'].unique())), seven_months_back_str, aws_engine)

canoneng_fac_df = read_factory('LASER', tuple(canon_vendors['TPO_PO_Vendor_Code']),
                               tuple(tuple(canoneng_skus['TPO_Material'].unique())), seven_months_back_str, aws_engine)

canonfg_fac_df = s4_data_cleaning(canonfg_fac_df)

canoneng_fac_df = s4_data_cleaning(canoneng_fac_df)

canonfg_cum_df = group_cum(canonfg_fac_df, canon_vendors, canonfg_skus, True)

canoneng_cum_df = group_cum(
    canoneng_fac_df, canon_vendors, canoneng_skus, True)

canonfg_cum_df = duplicated_tpo_clean(canonfg_cum_df)

canoneng_cum_df = duplicated_tpo_clean(canoneng_cum_df)

etd_df = pd.read_sql(canon_etd_query, aws_engine,
                     parse_dates=['TPO_Created_On',
                                  'TPO_Requested_Delivery_Date',
                                  'TPO_AB_Conf_Delivery_Date',
                                  'TPO_LA_Conf_Delivery_Date',
                                  'DC_IC_PO_Req_Delivery_Date',
                                  'CDO_IC_PO_Req_Delivery_Date',
                                  'TPOLATABLE_IBP_ETD_FACTORY',
                                  'TPOLATABLE_IBP_ETA_DC',
                                  'TPOLATABLE_IBD_PORT_ARRIVAL_DT'])

canonfg_cum_df = canon_cum_clean(canonfg_cum_df, etd_df)
canoneng_cum_df = canon_cum_clean(canoneng_cum_df, etd_df)

canon_site_df = pd.read_sql(canon_site_query, aws_engine)
canon_site_dict = canon_site_df.set_index('TPO_LA_Reference').to_dict()
canon_site_dict = canon_site_dict['SITE']
canonfg_cum_df['TPO_REF'] = canonfg_cum_df['TPO_LA_Reference'].str[:2]
canonfg_cum_df['MPA_NAME'] = canonfg_cum_df['TPO_REF'].map(canon_site_dict)

canoneng_cum_df['TPO_REF'] = canoneng_cum_df['TPO_LA_Reference'].str[:2]
canoneng_cum_df['MPA_NAME'] = canoneng_cum_df['TPO_REF'].map(canon_site_dict)

canonfg_cum_df = canonfg_cum_df.drop(columns=['TPO_REF'])
canonfg_cum_df = canonfg_cum_df.merge(
    canonfg_skus, how='left', left_on=['MPA_NAME', 'TPO_Material'],
    right_on=['FACTORY', 'TPO_Material'])

canoneng_cum_df = canoneng_cum_df.drop(columns=['TPO_REF'])
canoneng_cum_df = canoneng_cum_df.merge(
    canoneng_skus, how='left', left_on=['MPA_NAME', 'TPO_Material'],
    right_on=['FACTORY', 'TPO_Material'])

canonfg_cum_df['FAMILY'] = canonfg_cum_df['PLTFRM_NM']
canonfg_cum_df['CATEGORY'] = 'CANON FFGI'

canoneng_cum_df['FAMILY'] = canoneng_cum_df['PLTFRM_NM']
canoneng_cum_df['CATEGORY'] = 'CANON ENGINE'

canonfg_cum_df = str_fill(canonfg_cum_df)
canonfg_cum_df = canonfg_cum_df.loc[canonfg_cum_df['MPA_NAME'] != ''].copy()

canoneng_cum_df = str_fill(canoneng_cum_df)
canoneng_cum_df = canoneng_cum_df.loc[canoneng_cum_df['MPA_NAME'] != ''].copy()

canoneng_map = pd.read_sql(canoneng_region_query.format(
    tuple(canoneng_cum_df['TPO_Plant'].unique())), aws_engine)

canoneng_cum_df = canoneng_cum_df.drop(columns=['REGION'])
canoneng_cum_df = canoneng_cum_df.merge(
    canoneng_map, how='left', on='TPO_Plant')


inklaser_process = pd.concat([ink_cum_df, laser_cum_df], ignore_index=True)

inklaser_p_incre_q = """DELETE FROM FactoryOps.ink_laser_clean WHERE TPO_Created_On >= '{}'""".format(
    seven_months_back_str)

incremental_update(inklaser_p_incre_q, inklaser_process, 'ink_laser_clean',
              {'BU': sa.types.VARCHAR(length=20), 
                'TPO_PO_Vendor_Code': sa.types.VARCHAR(length=15),
                'Trade_PO': sa.types.VARCHAR(length=15), 
                'Trade_PO_Item': sa.types.VARCHAR(length=10),
                'TPO_Created_On': sa.types.Date,
                'TPO_Requested_Delivery_Date': sa.types.Date, 
                'TPO_LA_Conf_Delivery_Date': sa.types.Date, 
                'TPO_Material': sa.types.VARCHAR(length=15), 
                'TPO_Qty': sa.types.FLOAT(), 
                'TPO_LA_Reference': sa.types.VARCHAR(length=50), 
                'TPO_Total_Net_Price': sa.types.FLOAT(),
                'TPO_Price_Unit': sa.types.VARCHAR(length=5), 
                'TPO_Plant':sa.types.VARCHAR(length=10),
                'TPO_Storage_Loc':sa.types.VARCHAR(length=10), 
                'TPO_Profit_Center':sa.types.VARCHAR(length=6),
                'TPO_Ship_mode_Name': sa.types.VARCHAR(length=40), 
                'TPO_DIO_Flag':sa.types.VARCHAR(length=6), 
                'TPO_Net_Price': sa.types.FLOAT(), 
                'Trade_PO_Total_Net_Price_Value': sa.types.FLOAT(), 
                'TPO_Currency': sa.types.VARCHAR(length=6), 
                'Trade_PO_Open_Quantity': sa.types.FLOAT(),
                'Trade_PO_Open_Qty_Value': sa.types.FLOAT(), 
                'Trade_PO_Status': sa.types.VARCHAR(length=15),
                'TPO_LA_Inbound_Delivery_No': sa.types.VARCHAR(length=20), 
                'IC_SO_To_Factory_Ship_To_Name': sa.types.VARCHAR(length=50),                 
                'DC_IC_PO_Plant': sa.types.VARCHAR(length=8),
                'REGION': sa.types.VARCHAR(length=10),
                'SUB_REGION': sa.types.VARCHAR(length=10),
                'TPO_LA_Qty': sa.types.FLOAT(), 
                'TPO_GR_Qty': sa.types.FLOAT(),
                'MPA_NAME':sa.types.VARCHAR(length=50),
                'PLTFRM_NM': sa.types.VARCHAR(length=50), 
                'BUS_UNIT_NM':sa.types.VARCHAR(length=30),
                'PROD_LINE_CD': sa.types.VARCHAR(length=5), 
                'PROD_TYPE_CD': sa.types.VARCHAR(length=15),
                'BUS_GRP_NM': sa.types.VARCHAR(length=15),
                'CAT_NM': sa.types.VARCHAR(length=40),
                'CAT_SUB': sa.types.VARCHAR(length=40), 
                'PROD_GRP': sa.types.VARCHAR(length=40), 
                'BUS_UNIT_L3': sa.types.VARCHAR(length=30), 
                'FAMILY': sa.types.VARCHAR(length=50),
                'CATEGORY': sa.types.VARCHAR(length=25)}, aws_engine, 'append', today_str)

canon_process = pd.concat([canonfg_cum_df, canoneng_cum_df], ignore_index=True)

canon_p_incre_q = """DELETE FROM FactoryOps.canon_clean WHERE TPO_Created_On >= '{}'""".format(
    seven_months_back_str)
incremental_update(canon_p_incre_q, canon_process, 'canon_clean', 
              {'BU': sa.types.VARCHAR(length=20), 
                'MPA_NAME':sa.types.VARCHAR(length=50),
                'TPO_PO_Vendor_Code': sa.types.VARCHAR(length=15),
                'Trade_PO': sa.types.VARCHAR(length=15), 
                'TPO_Created_On': sa.types.Date,
                'TPO_Requested_Delivery_Date': sa.types.Date, 
                'TPO_LA_Conf_Delivery_Date': sa.types.Date, 
                'TPO_Material': sa.types.VARCHAR(length=15), 
                'TPO_LA_Reference': sa.types.VARCHAR(length=50), 
                'TPO_Plant':sa.types.VARCHAR(length=10),
                'TPO_Profit_Center':sa.types.VARCHAR(length=6),
                'TPO_DIO_Flag':sa.types.VARCHAR(length=6), 
                'TPO_LA_Inbound_Delivery_No': sa.types.VARCHAR(length=20), 
                'DC_IC_PO_Plant': sa.types.VARCHAR(length=8),
                'REGION': sa.types.VARCHAR(length=10),
                'SUB_REGION': sa.types.VARCHAR(length=10),
                'TPO_Qty': sa.types.FLOAT(), 
                'TPO_LA_Qty': sa.types.FLOAT(), 
                'PLTFRM_NM': sa.types.VARCHAR(length=50), 
                'BUS_UNIT_NM':sa.types.VARCHAR(length=30),
                'PROD_LINE_CD': sa.types.VARCHAR(length=5), 
                'PROD_TYPE_CD': sa.types.VARCHAR(length=15),
                'BUS_GRP_NM': sa.types.VARCHAR(length=15),
                'CAT_NM': sa.types.VARCHAR(length=40),
                'CAT_SUB': sa.types.VARCHAR(length=40), 
                'PROD_GRP': sa.types.VARCHAR(length=40), 
                'FACTORY': sa.types.VARCHAR(length=50),
                'BUS_UNIT_L3': sa.types.VARCHAR(length=30), 
                'FAMILY': sa.types.VARCHAR(length=50),
                'CATEGORY': sa.types.VARCHAR(length=25)
}, aws_engine, 'append', today_str)
