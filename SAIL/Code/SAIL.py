# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 10:21:49 2022

@author: kohm
"""

# %% Import Libraries
import pandas as pd
import os
import glob
from datetime import datetime, date
import numpy as np
from dateutil.relativedelta import relativedelta, MO
pd.set_option('display.max_columns', None)
user = os.getenv('USERPROFILE')

start_run = datetime.now()

# %% Define all Paths

cols_to_use = ['TPO_PO_Vendor_Code', 'TPO_PO_Vendor_Name', 'Trade_PO',
               'Trade_PO_Item', 'TPO_Deletion Indicator', 'TPO_Created_On',
               'TPO_Material', 'TPO_Qty', 'TPO_Total_Net_Price',
               'TPO_Price_Unit', 'TPO_Requested_Delivery_Date',
               'TPO_PR_Creation_Date', 'TPO_Plant', 'TPO_Storage_Loc',
               'TPO_Shipping_Point', 'TPO_Profit_Center',
               'TPO_Ship_mode_Name', 'Purchasing Group Description',
               'TPO_Vendor_Material_Num', 'TPO_LA_Conf_Delivery_Date',
               'TPO_LA_Inbound_Delivery_No', 'TPO_LA_Qty', 'TPO_LA_Reference',
               'TPO_GR_Qty', 'TPO_Net_Price', 'Trade_PO_Total_Net_Price_Value',
               'TPO_Currency', 'Trade_PO_Open_Quantity',
               'Trade_PO_Open_Qty_Value',
               'Trade_PO_Status', 'IC_SO_To_Factory_Sold_To_Name',
               'IC_SO_To_Factory_Ship_To_Name', 'DC_IC_PO_Plant']

data_type = {'TPO_PO_Vendor_Code': str, 'TPO_PO_Vendor_Name': str,
             'Trade_PO': str, 'Trade_PO_Item': str,
             'TPO_Deletion Indicator': str, 'TPO_Material': str,
             'TPO_Qty': float, 'TPO_Total_Net_Price': float,
             'TPO_Price_Unit': str, 'TPO_Plant': str, 'TPO_Storage_Loc': str,
             'TPO_Shipping_Point': str, 'TPO_Profit_Center': str,
             'TPO_Ship_mode_Name': str, 'Purchasing Group Description': str,
             'TPO_Vendor_Material_Num': str, 'TPO_LA_Inbound_Delivery_No': str,
             'TPO_LA_Qty': float, 'TPO_LA_Reference': str, 'TPO_GR_Qty': float,
             'TPO_Net_Price': float, 'Trade_PO_Total_Net_Price_Value': float,
             'TPO_Currency': str, 'Trade_PO_Open_Quantity': float,
             'Trade_PO_Open_Qty_Value': float, 'Trade_PO_Status': str,
             'IC_SO_To_Factory_Sold_To_Name': str,
             'IC_SO_To_Factory_Ship_To_Name': str, 'DC_IC_PO_Plant': str,
             'REGION': str, 'Sub_Region': str}

dates = ['TPO_Created_On', 'TPO_Requested_Delivery_Date',
         'TPO_PR_Creation_Date', 'TPO_LA_Conf_Delivery_Date']

past_dates = ['TPO_Created_On', 'TPO_Requested_Delivery_Date',
              'TPO_LA_Conf_Delivery_Date']

past_dates_1 = ['TPO_Requested_Delivery_Date', 'TPO_LA_Conf_Delivery_Date']


def read_csv_s4(mpa_str):

    path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                        mpa_str, 'CSV')
    os.chdir(path)
    df = pd.read_csv('Factory Purchase Order Report.csv',
                     usecols=cols_to_use,
                     dtype=data_type,
                     skiprows=3,
                     parse_dates=dates,
                     thousands=',')
    return df


def read_past_csv(TYPE, mpa_str, date_in):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                        'PAST PROCESSED DATA', TYPE, mpa_str)
    os.chdir(path)
    past_csv_files = glob.glob(path + '/*' + '.csv')
    past_files = [os.path.basename(x) for x in past_csv_files]
    past_df = pd.DataFrame()
    for file in past_files:
        df = pd.read_csv(file,
                         dtype=data_type,
                         parse_dates=date_in)
        past_df = pd.concat([past_df,df],ignore_index=True)
    past_df['DC_IC_PO_Plant'] = past_df['DC_IC_PO_Plant'].fillna('')
    past_df['REGION'] = past_df['REGION'].fillna('')
    past_df['Sub_Region'] = past_df['Sub_Region'].fillna('')
    return past_df


def read_por(mpa_str):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\POR',
                        mpa_str)
    os.chdir(path)
    por_csv_files = glob.glob(path + '/*' + '.csv')
    por_files = [os.path.basename(x) for x in por_csv_files]
    por_df = pd.DataFrame()
    for file in por_files:
        df = pd.read_csv(file)
        por_df = pd.concat([por_df,df], ignore_index=True)
    return por_df

# =============================================================================
# ETD DATA
# =============================================================================
canon_etd_path = os.path.join(
    user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data\CANON\ETD')

# %% Business User Filtering Excel File

filter_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput',
                           'Data Filtering')

os.chdir(filter_path)

mpa_map = pd.read_excel('Data Filtering.xlsx', sheet_name='MPA',
                        dtype={'MPA VENDOR CODE': str,
                               'MPA': str})
# GET ONLY INKJET MPA
ink_mpa_map = mpa_map.loc[mpa_map['BU'] == 'Inkjet'].copy()

# GET ONLY LASER MPA
laser_mpa_map = mpa_map.loc[mpa_map['BU'] == 'Laser'].copy()

# REGION MAPPING
region_map = pd.read_excel('Data Filtering.xlsx', sheet_name='Region',
                           dtype={'DC_IC_PO_Plant': str,
                                  'Region': str})
# SUB-REGION MAPPING
sub_region_map = pd.read_excel('Data Filtering.xlsx', sheet_name='Sub Region',
                               dtype={'DC_IC_PO_Plant': str,
                                      'Sub_Region': str},
                               keep_default_na=False)

# FAMILY MAPPING
family_map = pd.read_excel('Data Filtering.xlsx', sheet_name='Family',
                           dtype={'MPA': str,
                                  'PLTFRM_NM': str,
                                  'FAMILY': str})
# HPPS PROFIT CENTER
hpps_pc = pd.read_excel('Data Filtering.xlsx', sheet_name='HPPS Profit Center',
                        dtype={'Profit Center': str,
                               'CATEGORY': str})
# HPPS FAMILY MAPPING
hpps_family = pd.read_excel('Data Filtering.xlsx', sheet_name='HPPS Family',
                            dtype={'MPA': str,
                                   'PLTFRM_NM': str,
                                   'PART_NR': str,
                                   'FAMILY': str})
# HPPS SHIPPING POINT
hpps_ship_point = pd.read_excel('Data Filtering.xlsx',
                                sheet_name='Shipping Point',
                                dtype={'MPA': str,
                                       'Shipping Point': str})

ship_point = hpps_ship_point['Shipping Point'].unique()

# HPPS EXCLUDE INTERPLANT
hpps_ex_plant = pd.read_excel('Data Filtering.xlsx',
                              sheet_name='HPPS Exclude Interplant')
hpps_ex_plant = hpps_ex_plant['Excluded DC_IC_PO_Plant'].unique()


# FXN WH LASER FAMILY MAPPING (HPPS MOVE TO FXN WH LASER)
fxnwhl_family = pd.read_excel('Data Filtering.xlsx',
                              sheet_name='FXN WH Laser Family',
                              dtype={'MPA': str,
                                     'PLTFRM_NM': str,
                                     'PART_NR': str,
                                     'FAMILY': str})

# FXN WH LASER PROFIT CENTER (HPPS MOVE TO FXN WH LASER)
fxnwhl_pc = pd.read_excel('Data Filtering.xlsx',
                          sheet_name='FXN WH Laser Profit Center',
                          dtype={'MPA': str,
                                 'PLTFRM_NM': str,
                                 'PART_NR': str,
                                 'FAMILY': str})

# FXN WH LASER PL OVERWRITE (CHANGE IN PL BY PLANNING)
fxnwhl_pl = pd.read_excel('Data Filtering.xlsx',
                          sheet_name='PL Change',
                          dtype={'MPA': str,
                                 'SKU': str,
                                 'PL OVERWRITE': str})

# FXN WH LASER EXCLUDE INTERPLANT
fxnwhl_ex_plant = pd.read_excel('Data Filtering.xlsx',
                                sheet_name='FXN WH Laser Exclude Interplant')
fxnwhl_ex_plant = fxnwhl_ex_plant['Excluded DC_IC_PO_Plant'].unique()

# CANON SITE MAPPING
canon_site = pd.read_excel('Data Filtering.xlsx', sheet_name='Canon Site',
                           dtype={'TPO_LA_REFERENCE': str,
                                  'SITE': str})
# ONLY EXCLUDE FLEX SKUS
ex_skus = pd.read_excel('Data Filtering.xlsx', sheet_name='Exempted SKUs',
                        dtype={'MPA': str,
                               'Exempted SKUs': str})

# REMOVE SPACES AT THE BACK
family_map['PLTFRM_NM'] = family_map['PLTFRM_NM'].str.rstrip()
family_map['FAMILY'] = family_map['FAMILY'].str.rstrip()
hpps_family['PLTFRM_NM'] = hpps_family['PLTFRM_NM'].str.rstrip()
hpps_family['PART_NR'] = hpps_family['PART_NR'].str.rstrip()
hpps_family['FAMILY'] = hpps_family['FAMILY'].str.rstrip()
fxnwhl_family['PLTFRM_NM'] = fxnwhl_family['PLTFRM_NM'].str.rstrip()
fxnwhl_family['PART_NR'] = fxnwhl_family['PART_NR'].str.rstrip()
fxnwhl_family['FAMILY'] = fxnwhl_family['FAMILY'].str.rstrip()
canon_site['TPO_LA_REFERENCE'] = canon_site['TPO_LA_REFERENCE'].str.rstrip()


canon_site_dict = canon_site.set_index('TPO_LA_REFERENCE').to_dict()

today = datetime.today()

# %% MAP MPA & REGION / REPLACE MPA NAMING


def map_mpa(dataset, mpa_df):
    dataset = dataset.merge(mpa_df, how='left',
                            left_on='TPO_PO_Vendor_Code',
                            right_on='MPA VENDOR CODE')
    dataset = dataset.drop(columns=['TPO_PO_Vendor_Name',
                                    'MPA VENDOR CODE'])
    dataset = dataset.rename(columns={'MPA': 'TPO_PO_Vendor_Name'})
    return dataset


def map_region(dataset, region_df, sub_region_df):
    dataset = dataset.merge(region_df, how='left', on='DC_IC_PO_Plant')
    dataset = dataset.merge(sub_region_df, how='left', on='DC_IC_PO_Plant')
    dataset['REGION'] = dataset['REGION'].fillna('')
    dataset['Sub_Region'] = dataset['Sub_Region'].fillna('')
    return dataset


def replace_mpa_name(por, ori_por, filter_name):
    por.loc[por['LOC_FROM_NM'] == ori_por, 'LOC_FROM_NM'] = \
        filter_name


# %% INKJET DATA
inkjet_list = ['NKG TH', 'NKG YY', 'FLEX PTP', 'FLEX ZH', 'FXN CQ', 'JWH INK',
               'FXN WH INK']

ink_dataset = pd.DataFrame()
for mpa in inkjet_list:
    ink_get = read_csv_s4(mpa)
    ink_dataset = pd.concat([ink_dataset,ink_get], ignore_index=True)

ink_por = pd.DataFrame()
for mpa in inkjet_list:
    ink_get = read_por(mpa)
    ink_por = pd.concat([ink_por,ink_get], ignore_index=True)

replace_mpa_name(ink_por, 'JABIL WEIHAI', 'INK Jabil Weihai')
replace_mpa_name(ink_por, 'NKG Thailand', 'INK NKG Thailand')
replace_mpa_name(ink_por, 'NKG Yue Yang', 'INK NKG Yue Yang')
replace_mpa_name(ink_por, 'Flex PTP Malaysia', 'INK Flex PTP Malaysia')
replace_mpa_name(ink_por, 'Flex Zhuhai', 'INK Flex Zhuhai')
replace_mpa_name(ink_por, 'Foxconn ChongQing', 'INK Foxconn ChongQing')
replace_mpa_name(ink_por, 'FOXCONN WEIHAI', 'INK Foxconn Weihai')
ink_dataset = map_mpa(ink_dataset, ink_mpa_map)
# EXCLUDE FLEX ZH
ink_dataset = pd.merge(ink_dataset, ex_skus, how='outer',
                       left_on=['TPO_PO_Vendor_Name', 'TPO_Material'],
                       right_on=['MPA', 'Exempted SKUs'], indicator=True)
ink_dataset = ink_dataset.loc[ink_dataset['_merge'] == 'left_only'].copy()
ink_dataset = ink_dataset.drop(columns=['MPA', 'Exempted SKUs', '_merge'])

# %% LASER DATA
laser_list = ['JABIL CUU', 'JWH LASER', 'FXN CZ']
laser_dataset = pd.DataFrame()
for mpa in laser_list:
    laser_get = read_csv_s4(mpa)
    laser_dataset = pd.concat([laser_dataset, laser_get], ignore_index=True)

laser_por = pd.DataFrame()
for mpa in laser_list:
    laser_get = read_por(mpa)
    laser_por = pd.concat([laser_por,laser_get], ignore_index=True)

replace_mpa_name(laser_por, 'HP FOXCONN PARDUBICE MFG(SG31)',
                 'Laser Foxconn Czech')
replace_mpa_name(laser_por, 'JABIL CHIHUAHUA', 'Laser Jabil Chihuahua')
replace_mpa_name(laser_por, 'JABIL WEIHAI', 'Laser Jabil Weihai')

laser_dataset = map_mpa(laser_dataset, laser_mpa_map)

# HPPS
hpps_dataset = read_csv_s4('HPPS')
hpps_por_df = read_por('HPPS')
hpps_por_df['LOC_FROM_NM'] = 'Laser HPPS'

hpps_dataset = map_mpa(hpps_dataset, laser_mpa_map)

# FXN WH LASER
fxnwhl_dataset = read_csv_s4('FXN WH LASER')
fxnwhl_por_df = read_por('FXN WH LASER')
fxnwhl_por_df['LOC_FROM_NM'] = 'Laser Foxconn Weihai'

fxnwhl_dataset = map_mpa(fxnwhl_dataset, laser_mpa_map)

# CANON
canon_list = ['CANON\EUROPA', r'CANON\USA SG']
canon_dataset = pd.DataFrame()
for mpa in canon_list:
    canon_get = read_csv_s4(mpa)
    canon_dataset = pd.concat([canon_dataset,canon_get],ignore_index=True)

canon_ffgi_por_df = read_por('CANON\CANON FFGI')
canon_ffgi_por_df['LOC_FROM_NM'] = 'Laser Canon'
canon_ffgi_por_df['CATEGORY'] = 'CANON FFGI'

canon_dataset = map_mpa(canon_dataset, laser_mpa_map)

# %% OUTPUT AS BACKUP (TO CHANGE DATE)
# laser_dataset = laser_dataset.loc[laser_dataset['TPO_Created_On']
#                                   < '2022-03-01'].copy()
# ink_dataset = ink_dataset.loc[ink_dataset['TPO_Created_On']
#                               < '2022-03-01'].copy()
# hpps_dataset = hpps_dataset.loc[hpps_dataset['TPO_Created_On']
#                                 < '2022-03-01'].copy()
# canon_dataset = canon_dataset.loc[canon_dataset['TPO_Created_On']
#                                   < '2022-03-01'].copy()

# %% Data Cleaning


def data_cleaning(dataset, hpps_bool, canon_bool, ship_bool):
    # =========================================================================
    # Remove rows where TPO is deleted
    # =========================================================================
    dataset = dataset.loc[dataset['TPO_Deletion Indicator'] != 'L'].copy()

# =============================================================================
# Remove rows of missing materials (Exceptional Charges Materials)
# =============================================================================
    dataset = dataset.loc[dataset['TPO_Material'].notnull()].copy()

# =============================================================================
# Fill in empty strings with ''
# =============================================================================
    dataset[['TPO_Ship_mode_Name', 'IC_SO_To_Factory_Sold_To_Name',
             'IC_SO_To_Factory_Ship_To_Name', 'DC_IC_PO_Plant']] = \
        dataset[['TPO_Ship_mode_Name', 'IC_SO_To_Factory_Sold_To_Name',
                 'IC_SO_To_Factory_Ship_To_Name', 'DC_IC_PO_Plant']].fillna('')

# =============================================================================
# Fill in missing_TPO_LA_QTY and TPO_GR_QTY with 0
# =============================================================================
    dataset[['TPO_LA_Qty', 'TPO_GR_Qty']] = dataset[['TPO_LA_Qty',
                                                    'TPO_GR_Qty']].fillna(0)

# =============================================================================
# Drop everything and keep last for all columns duplicated
# =============================================================================
    dataset = dataset.drop_duplicates(keep='last')

    # For HPPS Dataset
    if hpps_bool:
        # =====================================================================
        # HPPS Drop Duplicates to remove OEM Duplicated data
        # =====================================================================
        dataset = dataset.drop_duplicates(
            subset=['TPO_PO_Vendor_Code',
                    'TPO_PO_Vendor_Name',
                    'Trade_PO',
                    'Trade_PO_Item',
                    'TPO_Deletion Indicator',
                    'TPO_Material',
                    'TPO_Qty',
                    'TPO_Total_Net_Price',
                    'TPO_Price_Unit',
                    'TPO_Requested_Delivery_Date',
                    'TPO_PR_Creation_Date',
                    'TPO_Plant',
                    'TPO_Storage_Loc',
                    'TPO_Profit_Center',
                    'TPO_Ship_mode_Name',
                    'TPO_LA_Conf_Delivery_Date',
                    'TPO_LA_Inbound_Delivery_No',
                    'TPO_LA_Qty',
                    'TPO_LA_Reference',
                    'TPO_GR_Qty',
                    'TPO_Net_Price',
                    'Trade_PO_Total_Net_Price_Value',
                    'TPO_Currency',
                    'Trade_PO_Open_Quantity',
                    'Trade_PO_Open_Qty_Value',
                    'Trade_PO_Status',
                    'DC_IC_PO_Plant'], keep='last')

# =============================================================================
# HPPS Include relevant Shipping Point
# =============================================================================
    if hpps_bool and ship_bool:
        dataset = dataset.loc[dataset['TPO_Shipping_Point'].isin(
            ship_point)].copy()
# =============================================================================
# FOR CUMULATIVE
# =============================================================================
    dataset_cum = dataset.copy()
    if hpps_bool:
        dataset_cum = dataset_cum.groupby(
            ['TPO_PO_Vendor_Name',
             'Trade_PO',
             'Trade_PO_Item',
             'TPO_Requested_Delivery_Date',
             'TPO_LA_Conf_Delivery_Date',
             'TPO_Material',
             'MPA Site',
             'BU',
             'TPO_Qty',
             'TPO_Total_Net_Price',
             'TPO_Price_Unit',
             'TPO_Plant',
             'TPO_Storage_Loc',
             'TPO_Profit_Center',
             'TPO_Ship_mode_Name',
             'TPO_Net_Price',
             'Trade_PO_Total_Net_Price_Value',
             'TPO_Currency',
             'Trade_PO_Open_Quantity',
             'Trade_PO_Open_Qty_Value',
             'Trade_PO_Status',
             'IC_SO_To_Factory_Ship_To_Name',
             'DC_IC_PO_Plant'],
            dropna=False)[['TPO_LA_Qty', 'TPO_GR_Qty']].sum().reset_index()
        # Copy
        dataset_cum.loc[dataset_cum.duplicated(
            ['TPO_PO_Vendor_Name',
             'Trade_PO',
             'Trade_PO_Item',
             'TPO_Requested_Delivery_Date',
             'TPO_Material',
             'TPO_Qty',
             'TPO_Total_Net_Price',
             'TPO_Price_Unit',
             'TPO_Plant',
             'TPO_Storage_Loc',
             'TPO_Profit_Center',
             'TPO_Ship_mode_Name',
             'TPO_Net_Price',
             'Trade_PO_Total_Net_Price_Value',
             'TPO_Currency',
             'Trade_PO_Open_Quantity',
             'Trade_PO_Open_Qty_Value',
             'Trade_PO_Status',
             'IC_SO_To_Factory_Ship_To_Name'],
            keep=False),
            'TPO_Qty'] = dataset_cum['TPO_LA_Qty']

        dataset = dataset.groupby(
            ['TPO_PO_Vendor_Name',
             'Trade_PO',
             'Trade_PO_Item',
             'TPO_Created_On', 'TPO_LA_Reference',
             'TPO_Requested_Delivery_Date',
             'TPO_LA_Conf_Delivery_Date',
             'TPO_Material', 'MPA Site', 'BU',
             'TPO_Qty',
             'TPO_Total_Net_Price', 'TPO_Price_Unit',
             'TPO_Plant', 'TPO_Storage_Loc',
             'TPO_Profit_Center',
             'TPO_Ship_mode_Name',
             'TPO_Shipping_Point',
             'TPO_Net_Price',
             'Trade_PO_Total_Net_Price_Value',
             'TPO_Currency',
             'Purchasing Group Description',
             'TPO_Vendor_Material_Num',
             'IC_SO_To_Factory_Sold_To_Name',
             'Trade_PO_Open_Quantity',
             'Trade_PO_Open_Qty_Value',
             'Trade_PO_Status',
             'IC_SO_To_Factory_Ship_To_Name',
             'DC_IC_PO_Plant'])[['TPO_LA_Qty', 'TPO_GR_Qty']].sum().reset_index()
        # Copy
        dataset.loc[dataset.duplicated(
            ['TPO_PO_Vendor_Name',
             'Trade_PO',
             'Trade_PO_Item',
             'TPO_Requested_Delivery_Date',
             'TPO_Material',
             'MPA Site', 'BU',
             'TPO_Qty',
             'TPO_Total_Net_Price', 'TPO_Price_Unit',
             'TPO_Plant', 'TPO_Storage_Loc',
             'TPO_Profit_Center',
             'TPO_Ship_mode_Name',
             'TPO_Net_Price',
             'Trade_PO_Total_Net_Price_Value',
             'TPO_Currency',
             'Trade_PO_Open_Quantity',
             'Trade_PO_Open_Qty_Value',
             'Trade_PO_Status',
             'IC_SO_To_Factory_Ship_To_Name'],
            keep=False),
            ['TPO_Qty']] = \
            dataset['TPO_LA_Qty']
    elif canon_bool:
        dataset_cum = dataset_cum.groupby(
            ['TPO_PO_Vendor_Code',
             'TPO_PO_Vendor_Name',
             'Trade_PO', 'Trade_PO_Item',
             'TPO_Requested_Delivery_Date',
             'TPO_LA_Conf_Delivery_Date',
             'TPO_LA_Reference',
             'TPO_Material', 'MPA Site', 'BU',
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
             'TPO_LA_Inbound_Delivery_No',
             'IC_SO_To_Factory_Ship_To_Name',
             'DC_IC_PO_Plant'],
            dropna=False)[['TPO_LA_Qty', 'TPO_GR_Qty']].sum().reset_index()

    # # # PUT 0 for duplicated
        dataset_cum.loc[dataset_cum.duplicated(
            ['TPO_PO_Vendor_Code',
             'TPO_PO_Vendor_Name',
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

        dataset = dataset.groupby(
            ['TPO_PO_Vendor_Code',
             'TPO_PO_Vendor_Name',
             'Trade_PO',
             'Trade_PO_Item',
             'TPO_Requested_Delivery_Date',
             'MPA Site', 'BU',
             'TPO_LA_Conf_Delivery_Date',
             'TPO_Material',
             'TPO_Qty', 'TPO_LA_Reference',
             'TPO_Total_Net_Price', 'TPO_Price_Unit',
             'TPO_Plant', 'TPO_Storage_Loc',
             'TPO_Profit_Center', 'TPO_Ship_mode_Name',
             'TPO_Net_Price',
             'Trade_PO_Total_Net_Price_Value',
             'TPO_Currency',
             'Trade_PO_Open_Quantity',
             'Trade_PO_Open_Qty_Value',
             'Trade_PO_Status',
             'TPO_LA_Inbound_Delivery_No',
             'IC_SO_To_Factory_Ship_To_Name',
             'DC_IC_PO_Plant'])[['TPO_LA_Qty', 'TPO_GR_Qty']].sum().reset_index()

    # Copy
        dataset.loc[dataset.duplicated(
            ['TPO_PO_Vendor_Code',
             'TPO_PO_Vendor_Name', 'Trade_PO',
             'Trade_PO_Item',
             'TPO_Requested_Delivery_Date',
             'TPO_Material',
             'TPO_Qty',
             'TPO_LA_Reference',
             'TPO_Total_Net_Price', 'TPO_Price_Unit',
             'TPO_Plant', 'TPO_Storage_Loc',
             'TPO_Profit_Center', 'TPO_Ship_mode_Name',
             'TPO_Net_Price',
             'Trade_PO_Total_Net_Price_Value',
             'TPO_Currency',
             'Trade_PO_Open_Quantity',
             'Trade_PO_Open_Qty_Value',
             'Trade_PO_Status',
             'IC_SO_To_Factory_Ship_To_Name'],
            keep=False),
            ['TPO_Qty']] = dataset['TPO_LA_Qty']
    else:
        dataset_cum = dataset_cum.groupby(
            ['TPO_PO_Vendor_Name',
             'Trade_PO',
             'Trade_PO_Item',
             'TPO_Requested_Delivery_Date',
             'TPO_LA_Conf_Delivery_Date',
             'TPO_Material',
             'MPA Site',
             'BU',
             'TPO_Qty',
             'TPO_Total_Net_Price',
             'TPO_Price_Unit',
             'TPO_Plant',
             'TPO_Storage_Loc',
             'TPO_Profit_Center',
             'TPO_Ship_mode_Name',
             'TPO_Net_Price',
             'Trade_PO_Total_Net_Price_Value',
             'TPO_Currency',
             'Trade_PO_Open_Quantity',
             'Trade_PO_Open_Qty_Value',
             'Trade_PO_Status',
             'IC_SO_To_Factory_Ship_To_Name',
             'DC_IC_PO_Plant'],
            dropna=False)[['TPO_LA_Qty', 'TPO_GR_Qty']].sum().reset_index()
        # PUT 0 for duplicated
        dataset_cum.loc[dataset_cum.duplicated(
            ['TPO_PO_Vendor_Name',
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

# =============================================================================
# Group data together and sum for valid PO but many rows PO 5203091596
# =============================================================================
        dataset = dataset.groupby(
            ['TPO_PO_Vendor_Name',
             'Trade_PO',
             'Trade_PO_Item',
             'TPO_Requested_Delivery_Date',
             'MPA Site', 'BU', 'TPO_LA_Conf_Delivery_Date',
             'TPO_Material', 'TPO_Qty', 'TPO_LA_Reference',
             'TPO_Total_Net_Price', 'TPO_Price_Unit',
             'TPO_Plant', 'TPO_Storage_Loc',
             'TPO_Profit_Center', 'TPO_Ship_mode_Name',
             'TPO_Net_Price',
             'Trade_PO_Total_Net_Price_Value',
             'TPO_Currency', 'Trade_PO_Open_Quantity',
             'Trade_PO_Open_Qty_Value',
             'Trade_PO_Status',
             'IC_SO_To_Factory_Ship_To_Name',
             'DC_IC_PO_Plant'])[['TPO_LA_Qty', 'TPO_GR_Qty']].sum().reset_index()

# # PUT 0 for duplicated
# dataset.loc[dataset.duplicated(['TPO_PO_Vendor_Name','Trade_PO','Trade_PO_Item',
#                                 'TPO_Requested_Delivery_Date','TPO_Material',
#                                 'TPO_Qty',
#                                 'TPO_Total_Net_Price','TPO_Price_Unit',
#                                 'TPO_Plant','TPO_Storage_Loc',
#                                 'TPO_Profit_Center','TPO_Ship_mode_Name',
#                                 'TPO_Net_Price',
#                                 'Trade_PO_Total_Net_Price_Value','TPO_Currency',
#                                 'Trade_PO_Open_Quantity','Trade_PO_Open_Qty_Value',
#                                 'Trade_PO_Status','IC_SO_To_Factory_Sold_To_Name',
#                                 'IC_SO_To_Factory_Ship_To_Name']),
# ['TPO_Qty','TPO_Total_Net_Price','TPO_Net_Price','Trade_PO_Total_Net_Price_Value',
#   'Trade_PO_Open_Quantity','Trade_PO_Open_Qty_Value']] = np.nan
# Copy
        dataset.loc[dataset.duplicated(
            ['TPO_PO_Vendor_Name', 'Trade_PO',
             'Trade_PO_Item',
             'TPO_Requested_Delivery_Date',
             'TPO_Material',
             'TPO_Qty',
             'TPO_LA_Reference',
             'TPO_Total_Net_Price', 'TPO_Price_Unit',
             'TPO_Plant', 'TPO_Storage_Loc',
             'TPO_Profit_Center', 'TPO_Ship_mode_Name',
             'TPO_Net_Price',
             'Trade_PO_Total_Net_Price_Value',
             'TPO_Currency',
             'Trade_PO_Open_Quantity',
             'Trade_PO_Open_Qty_Value',
             'Trade_PO_Status',
             'IC_SO_To_Factory_Ship_To_Name'],
            keep=False),
            ['TPO_Qty']] = dataset['TPO_LA_Qty']

    dataset['TPO_Qty'] = dataset['TPO_Qty'].fillna(0)

    return dataset, dataset_cum


laser_df, laser_df_cum = data_cleaning(laser_dataset, False, False, False)
ink_df, ink_df_cum = data_cleaning(ink_dataset, False, False, False)
hpps_df, hpps_df_cum = data_cleaning(hpps_dataset, True, False, True)
fxnwhl_df, fxnwhl_df_cum = data_cleaning(fxnwhl_dataset, True, False, False)
canon_df, canon_df_cum = data_cleaning(canon_dataset, False, True, False)

# MAP REGION
laser_df = map_region(laser_df, region_map, sub_region_map)
laser_df_cum = map_region(laser_df_cum, region_map, sub_region_map)
ink_df = map_region(ink_df, region_map, sub_region_map)
ink_df_cum = map_region(ink_df_cum, region_map, sub_region_map)
hpps_df = map_region(hpps_df, region_map, sub_region_map)
hpps_df_cum = map_region(hpps_df_cum, region_map, sub_region_map)
fxnwhl_df = map_region(fxnwhl_df, region_map, sub_region_map)
fxnwhl_df_cum = map_region(fxnwhl_df_cum, region_map, sub_region_map)
canon_df = map_region(canon_df, region_map, sub_region_map)
canon_df_cum = map_region(canon_df_cum, region_map, sub_region_map)

# Set HPPS SEC OEM Orders to APJ
hpps_df.loc[hpps_df['DC_IC_PO_Plant'] == '', 'REGION'] = 'APJ'
hpps_df_cum.loc[hpps_df_cum['DC_IC_PO_Plant'] == '', 'REGION'] = 'APJ'

# %% OUTPUT AS CSV


# def csv_out(df, TYPE, folder, name):
#     path = os.path.join(
#         user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data\PAST PROCESSED DATA', TYPE, folder)
#     os.chdir(path)
#     df.to_csv(name+'.csv', index=False)


# csv_out(ink_df, 'Shipment', 'Inkjet', 'End Feb Ink')
# csv_out(laser_df, 'Shipment', 'Laser', 'End Feb Laser')
# csv_out(hpps_df, 'Shipment', 'HPPS', 'End Feb HPPS')
# csv_out(canon_df, 'Shipment', 'Canon', 'End Feb Canon')

# csv_out(ink_df_cum, 'Cumulative', 'Inkjet', 'End Feb Ink')
# csv_out(laser_df_cum, 'Cumulative', 'Laser', 'End Feb Laser')
# csv_out(hpps_df_cum, 'Cumulative', 'HPPS', 'End Feb HPPS')
# csv_out(canon_df_cum, 'Cumulative', 'Canon', 'End Feb Canon')
# %% READ PAST OUTPUT DATA
hpps_past_df = read_past_csv('Shipment', 'HPPS', past_dates)
canon_past_df = read_past_csv('Shipment', 'Canon', past_dates_1)
ink_past_df = read_past_csv('Shipment', 'Inkjet', past_dates_1)
laser_past_df = read_past_csv('Shipment', 'Laser', past_dates_1)

hpps_past_df_cum = read_past_csv('Cumulative', 'HPPS', past_dates_1)
canon_past_df_cum = read_past_csv('Cumulative', 'Canon', past_dates_1)
ink_past_df_cum = read_past_csv('Cumulative', 'Inkjet', past_dates_1)
laser_past_df_cum = read_past_csv('Cumulative', 'Laser', past_dates_1)

# %% COMBINE DATA
laser_df = pd.concat([laser_past_df, laser_df], ignore_index=True)
ink_df = pd.concat([ink_df, ink_past_df], ignore_index=True)
hpps_df = pd.concat([hpps_past_df, hpps_df], ignore_index=True)
canon_df = pd.concat([canon_past_df, canon_df], ignore_index=True)

laser_df_cum = pd.concat([laser_past_df_cum, laser_df_cum], ignore_index=True)
ink_df_cum = pd.concat([ink_past_df_cum, ink_df_cum], ignore_index=True)
hpps_df_cum = pd.concat([hpps_past_df_cum, hpps_df_cum], ignore_index=True)
canon_df_cum = pd.concat([canon_past_df_cum, canon_df_cum], ignore_index=True)

# Merge Laser with HPPS and FXNWHL
hpps_df_cum = hpps_df_cum.merge(hpps_pc, how='left', on='TPO_Profit_Center')
fxnwhl_df_cum = fxnwhl_df_cum.merge(
    fxnwhl_pc, how='left', on='TPO_Profit_Center')
# EXCLUDE HPPS/FXNWHL INTERPLANT ITEMS
hpps_df_cum = hpps_df_cum.loc[~hpps_df_cum['DC_IC_PO_Plant'].isin(
    hpps_ex_plant)].copy()
fxnwhl_df_cum = fxnwhl_df_cum.loc[~fxnwhl_df_cum['DC_IC_PO_Plant'].isin(
    fxnwhl_ex_plant)].copy()
# OVERWRITE FXNWHL PL CHANGE
fxnwhl_df = fxnwhl_df.merge(fxnwhl_pl,how='left',
                left_on=['TPO_PO_Vendor_Name','TPO_Material'],
                right_on=['MPA','SKU'])
fxnwhl_df.loc[fxnwhl_df['SKU'].notnull(), 
              'TPO_Profit_Center'] = fxnwhl_df['PL OVERWRITE']

fxnwhl_df_cum = fxnwhl_df_cum.merge(fxnwhl_pl,how='left',
                left_on=['TPO_PO_Vendor_Name','TPO_Material'],
                right_on=['MPA','SKU'])

fxnwhl_df_cum.loc[fxnwhl_df_cum['SKU'].notnull(), 
              'TPO_Profit_Center'] = fxnwhl_df_cum['PL OVERWRITE']

# %% SHIPMENT DATA


def shipment_data(dataset, por_df, hpps_bool, pc, family, ex_plant):
    if hpps_bool:
        df_grouped = dataset.groupby(
            ['TPO_Requested_Delivery_Date',
             'TPO_LA_Reference', 'MPA Site', 'BU',
             'TPO_LA_Conf_Delivery_Date',
             'TPO_PO_Vendor_Name', 'Trade_PO',
             'TPO_Material', 'DC_IC_PO_Plant',
             'REGION', 'Sub_Region',
             'TPO_Profit_Center'])[['TPO_Qty', 'TPO_LA_Qty']].sum().reset_index()
        df_grouped = df_grouped.merge(
            pc, how='left', on='TPO_Profit_Center')
        df_grouped = df_grouped.drop(columns=['TPO_Profit_Center'])
    else:
        df_grouped = dataset.groupby(
            ['TPO_Requested_Delivery_Date',
             'TPO_LA_Reference', 'MPA Site', 'BU',
             'TPO_LA_Conf_Delivery_Date',
             'TPO_PO_Vendor_Name', 'Trade_PO',
             'TPO_Material', 'DC_IC_PO_Plant',
             'REGION',
             'Sub_Region'])[['TPO_Qty', 'TPO_LA_Qty']].sum().reset_index()

    df_grouped = df_grouped.rename(columns={'TPO_PO_Vendor_Name': 'LOC_FROM_NM',
                                            'TPO_Material': 'PART_NR'})

    por_df = por_df.drop_duplicates(
        subset=['LOC_FROM_NM', 'PART_NR'], keep='last')

    por_items = por_df.groupby(['LOC_FROM_NM',
                                'PLTFRM_NM', 'BUS_UNIT_NM',
                                'PART_NR'], dropna=False)['PLTFRM_NM']. \
        nunique()

    por_items = por_items.reset_index(name='nunique')
    # print(por_items['nunique'].unique())
    por_items = por_items.drop(columns=['nunique'])
    # print('Duplicated MPA and SKU : ',
    #       por_items[['LOC_FROM_NM', 'PART_NR']].duplicated().any())

# =============================================================================
# Merge POR Items with TPO
# =============================================================================
    TPO_Final = df_grouped.merge(por_items, how='left', on=['LOC_FROM_NM',
                                                            'PART_NR'])

    TPO_Final['PLTFRM_NM'] = TPO_Final['PLTFRM_NM'].fillna('')
    TPO_Final['REGION'] = TPO_Final['REGION'].fillna('')
    TPO_Final['Sub_Region'] = TPO_Final['Sub_Region'].fillna('')
    if hpps_bool:
        TPO_Final = TPO_Final.merge(family, how='left',
                                    left_on=['LOC_FROM_NM',
                                             'PLTFRM_NM', 'PART_NR'],
                                    right_on=['MPA', 'PLTFRM_NM', 'PART_NR'])
        # EXCLUDE HPPS INTERPLANT ITEMS
        TPO_Final = TPO_Final.loc[~TPO_Final['DC_IC_PO_Plant'].isin(
            ex_plant)].copy()
    else:
        TPO_Final = TPO_Final.merge(family_map, how='left',
                                    left_on=['LOC_FROM_NM', 'PLTFRM_NM'],
                                    right_on=['MPA', 'PLTFRM_NM'])
    return TPO_Final, por_items


TPO_Ink_Final, Ink_POR_grouped = shipment_data(
    ink_df, ink_por, False, None, None, None)
TPO_Laser_Final, Laser_POR_grouped = shipment_data(
    laser_df, laser_por, False, None, None, None)
TPO_HPPS_Final, HPPS_POR_grouped = shipment_data(
    hpps_df, hpps_por_df, True, hpps_pc, hpps_family, hpps_ex_plant)
TPO_FXNWHL_Final, FXNWHL_POR_grouped = shipment_data(
    fxnwhl_df, fxnwhl_por_df, True, fxnwhl_pc, fxnwhl_family, fxnwhl_ex_plant)


def convert_date(grouped_df, por_items, canon_cum_bool):

    grouped_df['TPO_LA_Conf_Delivery_Date_POR'] = \
        grouped_df['TPO_LA_Conf_Delivery_Date'].dt.to_period('W').dt.start_time

    grouped_df['TPO_LA_Conf_Delivery_Month_POR'] = \
        grouped_df['TPO_LA_Conf_Delivery_Date_POR'].dt.strftime('%Y-%m-01')
    grouped_df['TPO_LA_Conf_Delivery_Month_POR'] = \
        grouped_df['TPO_LA_Conf_Delivery_Month_POR'].apply(pd.to_datetime)
    grouped_df['MONTH_NUM_POR'] = \
        grouped_df['TPO_LA_Conf_Delivery_Month_POR'].dt.month
    grouped_df['ISO_POR'] = \
        grouped_df['TPO_LA_Conf_Delivery_Date_POR'].dt.strftime('%y %b W%V')

    # Start from August
    if canon_cum_bool:
        grouped_df = grouped_df.loc[
            (grouped_df['TPO_LA_Conf_Delivery_Month_POR'] >= '2021-08-01') |
            (grouped_df['TPO_LA_Conf_Delivery_Month_POR'].isnull())].copy()
    else:
        grouped_df = grouped_df.loc[
            grouped_df['TPO_LA_Conf_Delivery_Month_POR'] >= '2021-08-01'].copy()
    # Only include POR SKUs
    TPO_Clean = pd.DataFrame()
    for mpa in grouped_df['LOC_FROM_NM'].unique():
        find_df = grouped_df.loc[grouped_df['LOC_FROM_NM'] == mpa].copy()
        por_skus = por_items.loc[por_items['LOC_FROM_NM']
                                 == mpa]['PART_NR'].unique()
        find_df = find_df.loc[find_df['PART_NR'].isin(por_skus)]
        TPO_Clean = pd.concat([TPO_Clean,find_df],ignore_index=True)

    # Rename columns
    if canon_cum_bool:
        TPO_Clean.columns = TPO_Clean.columns
    else:
        TPO_Clean.columns = TPO_Clean.columns.str.upper()
        TPO_Clean = TPO_Clean.rename(columns={'LOC_FROM_NM': 'MPa',
                                              'PART_NR': 'SKU',
                                              'PLTFRM_NM': 'PLATFORM'})
    # Drop Irrelevant Columns
    if 'MPA' in TPO_Clean.columns:
        TPO_Clean = TPO_Clean.drop(columns=['MPA'])

    return TPO_Clean


TPO_Ink_Final = convert_date(TPO_Ink_Final, Ink_POR_grouped, False)

# REMOVE FLEX PTP AND FLEX ZH FROM 2022 JULY
TPO_Ink_Final = TPO_Ink_Final.loc[
    ~((TPO_Ink_Final['MPa'].isin(['INK Flex PTP Malaysia',
                                  'INK Flex Zhuhai'])) &
      (TPO_Ink_Final['TPO_LA_CONF_DELIVERY_MONTH_POR'] >= '2022-07-01'))].copy()

TPO_Laser_Final = convert_date(TPO_Laser_Final, Laser_POR_grouped, False)
TPO_HPPS_Final = convert_date(TPO_HPPS_Final, HPPS_POR_grouped, False)
TPO_FXNWHL_Final = convert_date(TPO_FXNWHL_Final, FXNWHL_POR_grouped, False)
# =============================================================================
# CANON SHIPMENT DATA
# =============================================================================


def canon_shipment_data(dataset, cum_bool, etd_path, por_df):
    if cum_bool:
        df_grouped = dataset.groupby(
            ['TPO_Requested_Delivery_Date',
             'TPO_LA_Reference',
             'MPA Site', 'BU',
             'TPO_LA_Inbound_Delivery_No',
             'TPO_LA_Conf_Delivery_Date',
             'TPO_PO_Vendor_Code', 'TPO_PO_Vendor_Name',
             'Trade_PO', 'REGION', 'Sub_Region',
             'TPO_Material', 'DC_IC_PO_Plant'],
            dropna=False)[['TPO_Qty', 'TPO_LA_Qty']].sum().reset_index()
    else:
        df_grouped = dataset.groupby(
            ['TPO_Requested_Delivery_Date',
             'TPO_LA_Reference',
             'MPA Site', 'BU',
             'TPO_LA_Inbound_Delivery_No',
             'TPO_LA_Conf_Delivery_Date',
             'TPO_PO_Vendor_Code', 'TPO_PO_Vendor_Name',
             'Trade_PO', 'REGION', 'Sub_Region',
             'TPO_Material', 'DC_IC_PO_Plant'])[['TPO_Qty', 'TPO_LA_Qty']].sum().reset_index()

    os.chdir(etd_path)
    csv_files = glob.glob(etd_path + '/*' + '.csv')
    latest_csv = max(csv_files, key=os.path.getctime)
    # print(latest_csv)
    # REPLACE TO CROSSTAB FORMAT
    etd_df = pd.read_csv(latest_csv, encoding='UTF-16 LE',
                         sep='\t',
                         dtype={'TPO_PO_Vendor_Code': str,
                                'Trade_PO': str,
                                'TPO_Material': str,
                                'TPO_LA_Inbound_Delivery_No': str},
                         parse_dates=['TPOLATABLE_IBP_ETD_FACTORY'])

    etd_df['TPOLATABLE_IBP_ETD_FACTORY'] = pd.to_datetime(
        etd_df['TPOLATABLE_IBP_ETD_FACTORY'],
        errors='coerce')

    # # NEED GROUP ETD DATASET FIRST
    etd_df = etd_df[['TPO_PO_Vendor_Code', 'Trade_PO', 'TPO_Material',
                     'TPO_LA_Inbound_Delivery_No',
                     'TPO_LA_Conf_Delivery_Date', 'TPO_Qty', 'TPO_LA_Qty',
                     'TPOLATABLE_IBP_ETD_FACTORY']]
    etd_df = etd_df.groupby(
        ['TPO_PO_Vendor_Code', 'Trade_PO', 'TPO_Material',
         'TPO_LA_Inbound_Delivery_No',
         'TPO_LA_Conf_Delivery_Date', 'TPO_Qty',
         'TPOLATABLE_IBP_ETD_FACTORY'])['TPO_LA_Qty'].sum().reset_index()

    canon_merged = df_grouped.merge(etd_df, how='left',
                                    on=['TPO_PO_Vendor_Code',
                                        'Trade_PO',
                                        'TPO_Material',
                                        'TPO_LA_Inbound_Delivery_No'])

    # Replace TPO_LA_Conf_Delivery_Date with SAP DATE
    canon_merged['TPO_LA_Conf_Delivery_Date'] = \
        canon_merged['TPOLATABLE_IBP_ETD_FACTORY']
    canon_merged = canon_merged[['TPO_Requested_Delivery_Date', 'TPO_LA_Reference',
                                 'MPA Site', 'BU', 'REGION', 'Sub_Region',
                                 'TPO_LA_Conf_Delivery_Date', 'TPO_PO_Vendor_Code',
                                 'TPO_PO_Vendor_Name', 'Trade_PO', 'TPO_Material',
                                 'DC_IC_PO_Plant', 'TPO_Qty_x', 'TPO_LA_Qty_x']]

    # # # Copy
    canon_merged.loc[
        canon_merged.duplicated(['TPO_PO_Vendor_Name',
                                 'TPO_Requested_Delivery_Date', 'TPO_PO_Vendor_Code',
                                 'Trade_PO', 'TPO_Material',
                                 'DC_IC_PO_Plant'], keep=False),
        ['TPO_Qty_x']] = \
        canon_merged['TPO_LA_Qty_x']
    # Group data again (No Group Vendor Code)
    if cum_bool:
        canon_final = canon_merged.groupby(
            ['TPO_Requested_Delivery_Date',
             'TPO_LA_Reference',
             'TPO_LA_Conf_Delivery_Date', 'MPA Site', 'BU',
             'TPO_PO_Vendor_Name', 'Trade_PO', 'TPO_Material',
             'REGION', 'Sub_Region',
             'DC_IC_PO_Plant',
             'TPO_Qty_x'], dropna=False)['TPO_LA_Qty_x'].sum().reset_index()
    else:
        canon_final = canon_merged.groupby(
            ['TPO_Requested_Delivery_Date',
             'TPO_LA_Reference',
             'TPO_LA_Conf_Delivery_Date', 'MPA Site', 'BU',
             'TPO_PO_Vendor_Name', 'Trade_PO', 'TPO_Material',
             'REGION', 'Sub_Region',
             'DC_IC_PO_Plant', 'TPO_Qty_x'])['TPO_LA_Qty_x'].sum().reset_index()

    canon_final = canon_final.rename(columns={'TPO_PO_Vendor_Name': 'LOC_FROM_NM',
                                              'TPO_Material': 'PART_NR',
                                              'TPO_Qty_x': 'TPO_Qty',
                                              'TPO_LA_Qty_x': 'TPO_LA_Qty'})

    por_df = por_df.drop_duplicates(
        subset=['LOC_FROM_NM', 'PART_NR'], keep='last')

    por_items = por_df.groupby(['LOC_FROM_NM',
                                'FAMILY_NM',
                                'PLTFRM_NM',
                                'BUS_UNIT_NM',
                                'PART_NR',
                                'CATEGORY'], dropna=False)['PLTFRM_NM']. \
        nunique()

    por_items = por_items.reset_index(name='nunique')
    # print(por_items['nunique'].unique())
    por_items = por_items.drop(columns=['nunique'])
    # print('Duplicated MPA and SKU : ',
    #       por_items[['LOC_FROM_NM', 'PART_NR']].duplicated().any())

    # CANON FAMILY NAMING TO FOLLOW ISAAC (REPLACE NAN WITH PLATFORM)
    por_items.loc[por_items['FAMILY_NM'].isnull(), 'FAMILY_NM'] = \
        por_items['PLTFRM_NM']

    TPO_Final = canon_final.merge(por_items, how='left', on=['LOC_FROM_NM',
                                                             'PART_NR'])

    TPO_Final['PLTFRM_NM'] = TPO_Final['PLTFRM_NM'].fillna('')
    TPO_Final['REGION'] = TPO_Final['REGION'].fillna('')

    return TPO_Final, por_items


TPO_Canon_Final, Canon_POR_grouped = \
    canon_shipment_data(canon_df, False, canon_etd_path, canon_ffgi_por_df)
TPO_Canon_Final = convert_date(TPO_Canon_Final, Canon_POR_grouped, False)

TPO_Cum_Canon, Canon_POR_grouped = \
    canon_shipment_data(canon_df_cum, True, canon_etd_path, canon_ffgi_por_df)
TPO_Cum_Canon = convert_date(TPO_Cum_Canon, Canon_POR_grouped, True)

TPO_Canon_Final = TPO_Canon_Final.rename(columns={'FAMILY_NM': 'FAMILY'})
TPO_Cum_Canon = TPO_Cum_Canon.rename(columns={'FAMILY_NM': 'FAMILY'})

# =============================================================================
# MAP TO CANON SITES
# =============================================================================
TPO_Canon_Final['TPO_REF'] = TPO_Canon_Final['TPO_LA_REFERENCE'].str[:2]
canon_site_dict = canon_site_dict['SITE']
TPO_Canon_Final['MPa'] = TPO_Canon_Final['TPO_REF'].map(canon_site_dict)
TPO_Canon_Final = TPO_Canon_Final.drop(columns=['TPO_REF'])

TPO_Cum_Canon['TPO_REF'] = TPO_Cum_Canon['TPO_LA_Reference'].str[:2]
TPO_Cum_Canon['TPO_PO_Vendor_Name'] = TPO_Cum_Canon['TPO_REF'].map(
    canon_site_dict)
TPO_Cum_Canon = TPO_Cum_Canon.drop(columns=['TPO_REF'])


# %% CONVERT DATE FUNCTION
def convert_date_week(grouped_df, col):
    grouped_df[col] = \
        grouped_df[col].dt.to_period('W').dt.start_time
    return grouped_df


def convert_date_month(grouped_df, col_name, col):
    grouped_df[col_name] = \
        grouped_df[col].dt.strftime('%Y-%m-01')
    grouped_df[col_name] = \
        grouped_df[col_name].apply(pd.to_datetime)
    return grouped_df
# %% MONTH SHIPMENT VS TPO QTY


def por_family(cum_df, por_grouped, family, hpps_bool):
    # Get only POR SKUs
    cum_month = cum_df.merge(por_grouped,
                             left_on=['TPO_PO_Vendor_Name', 'TPO_Material'],
                             right_on=['LOC_FROM_NM', 'PART_NR'], how='left')

    cum_month = cum_month.loc[cum_month['PART_NR'].notnull()].copy()

# Add Family
    if hpps_bool:
        cum_month = cum_month.merge(family, how='left',
                                    left_on=['LOC_FROM_NM',
                                             'PLTFRM_NM', 'PART_NR'],
                                    right_on=['MPA', 'PLTFRM_NM', 'PART_NR'])
    else:
        cum_month = cum_month.merge(family, how='left',
                                    left_on=[
                                        'TPO_PO_Vendor_Name', 'PLTFRM_NM'],
                                    right_on=['MPA', 'PLTFRM_NM'])
    return cum_month


# =============================================================================
# FILTER POR SKUs AND ADD FAMILY
# =============================================================================
ink_added = por_family(ink_df_cum, Ink_POR_grouped, family_map, False)
laser_added = por_family(laser_df_cum, Laser_POR_grouped, family_map, False)
hpps_added = por_family(hpps_df_cum, HPPS_POR_grouped, hpps_family, True)
fxnwhl_added = por_family(
    fxnwhl_df_cum, FXNWHL_POR_grouped, fxnwhl_family, True)

# MAKE A COPY FOR EXECUTIVE REPORT (INKJET)
# ADDING LA AND NA & CHINA
ink_added_exe = ink_added.copy()
ink_added_exe.loc[ink_added_exe['Sub_Region'] == 'NA', 'REGION'] = 'NA'
ink_added_exe.loc[ink_added_exe['Sub_Region'] == 'LA', 'REGION'] = 'LA'
ink_added_exe.loc[ink_added_exe['TPO_Material'].str.contains('#AB2'), 'REGION'] = 'CHINA'


def po_ship(cum_month, hpps_canon_bool, ink_exe_bool):
    # START AUGUST 2021
    cum_month = cum_month.loc[cum_month['TPO_Requested_Delivery_Date']
                              >= '2021-08-02'].copy()
    if hpps_canon_bool:
        po_df = cum_month.groupby(['TPO_PO_Vendor_Name',
                                   'Trade_PO',
                                   'TPO_Requested_Delivery_Date',
                                   'BU', 'PLTFRM_NM',
                                   'BUS_UNIT_NM', 'FAMILY',
                                   'REGION', 'CATEGORY'],
                                  dropna=False)['TPO_Qty'].sum().reset_index()

        ship_df = cum_month.groupby(['TPO_PO_Vendor_Name',
                                     'Trade_PO',
                                     'TPO_LA_Conf_Delivery_Date',
                                     'BU', 'PLTFRM_NM',
                                     'BUS_UNIT_NM', 'FAMILY',
                                     'REGION', 'CATEGORY'],
                                    dropna=False)['TPO_LA_Qty'].sum().reset_index()
        ship_df = ship_df.loc[ship_df
                              ['TPO_LA_Conf_Delivery_Date'].notnull()].copy(
        )
    elif ink_exe_bool:
        po_df = cum_month.groupby(['TPO_PO_Vendor_Name',
                                   'Trade_PO',
                                   'TPO_Requested_Delivery_Date',
                                   'BU', 'PLTFRM_NM',
                                   'BUS_UNIT_NM', 'FAMILY',
                                   'REGION','PART_NR'],
                                  dropna=False)['TPO_Qty'].sum().reset_index()

        ship_df = cum_month.groupby(['TPO_PO_Vendor_Name',
                                     'Trade_PO',
                                     'TPO_LA_Conf_Delivery_Date',
                                     'BU', 'PLTFRM_NM',
                                     'BUS_UNIT_NM', 'FAMILY',
                                     'REGION','PART_NR'],
                                    dropna=False)['TPO_LA_Qty'].sum().reset_index()
        ship_df = ship_df.loc[ship_df
                              ['TPO_LA_Conf_Delivery_Date'].notnull()].copy(
        ) 

    else:
        po_df = cum_month.groupby(['TPO_PO_Vendor_Name',
                                   'Trade_PO',
                                   'TPO_Requested_Delivery_Date',
                                   'BU', 'PLTFRM_NM',
                                   'BUS_UNIT_NM', 'FAMILY',
                                   'REGION'],
                                  dropna=False)['TPO_Qty'].sum().reset_index()

        ship_df = cum_month.groupby(['TPO_PO_Vendor_Name',
                                     'Trade_PO',
                                     'TPO_LA_Conf_Delivery_Date',
                                     'BU', 'PLTFRM_NM',
                                     'BUS_UNIT_NM', 'FAMILY',
                                     'REGION'],
                                    dropna=False)['TPO_LA_Qty'].sum().reset_index()
        ship_df = ship_df.loc[ship_df
                              ['TPO_LA_Conf_Delivery_Date'].notnull()].copy(
        )

    return po_df, ship_df

# =============================================================================
# GROUP INTO SHIPMENT AND PO
# =============================================================================
ink_po_df, ink_ship_df = po_ship(ink_added, False, False)
# INKJET EXECUTIVE
ink_po_exe_df, ink_ship_exe_df = po_ship(ink_added_exe, False, True)
laser_po_df, laser_ship_df = po_ship(laser_added, False, False)
hpps_po_df, hpps_ship_df = po_ship(hpps_added, True, False)
fxnwhl_po_df, fxnwhl_ship_df = po_ship(fxnwhl_added, True, False)
canon_po_df, canon_ship_df = po_ship(TPO_Cum_Canon, True, False)


po_list = [ink_po_df, ink_po_exe_df, laser_po_df, hpps_po_df, fxnwhl_po_df, canon_po_df]
ship_list = [ink_ship_df, ink_ship_exe_df, laser_ship_df, hpps_ship_df, fxnwhl_ship_df,
             canon_ship_df]

# =============================================================================
# CONVERT DATE TO MONDAY AND CHANGE TO MONTH
# =============================================================================
for po_data in po_list:
    po_data = convert_date_week(po_data, 'TPO_Requested_Delivery_Date')
    po_data = convert_date_month(po_data, 'TPO_Requested_Delivery_Month_POR',
                                 'TPO_Requested_Delivery_Date')

for ship_data in ship_list:
    ship_data = convert_date_week(ship_data, 'TPO_LA_Conf_Delivery_Date')
    ship_data = convert_date_month(ship_data, 'TPO_LA_Conf_Delivery_Month_POR',
                                   'TPO_LA_Conf_Delivery_Date')


# GROUP BY MONTH AND WEEK
def combine_po_ship(po_df, ship_df, hpps_bool, ink_exe_bool):
    # current_mon_date = today - relativedelta(days=today.weekday())
    # current_mon_date = current_mon_date.replace(
    #     hour=0, minute=0, second=0, microsecond=0)
    if hpps_bool:
        po_df = po_df.groupby(['TPO_PO_Vendor_Name',
                               'Trade_PO',
                               'TPO_Requested_Delivery_Date',
                               'TPO_Requested_Delivery_Month_POR',
                               'BU', 'PLTFRM_NM',
                               'BUS_UNIT_NM',
                               'FAMILY', 'REGION',
                               'CATEGORY'],
                              dropna=False)['TPO_Qty'].sum().reset_index()
        ship_df = ship_df.groupby(['TPO_PO_Vendor_Name',
                                   'Trade_PO',
                                   'TPO_LA_Conf_Delivery_Date',
                                   'TPO_LA_Conf_Delivery_Month_POR',
                                   'BU', 'PLTFRM_NM',
                                   'BUS_UNIT_NM',
                                   'FAMILY', 'REGION',
                                   'CATEGORY'],
                                  dropna=False)['TPO_LA_Qty'].sum().reset_index()
        po_ship_df = po_df.merge(ship_df, how='outer',
                                 left_on=['TPO_PO_Vendor_Name',
                                          'Trade_PO',
                                          'TPO_Requested_Delivery_Date',
                                          'TPO_Requested_Delivery_Month_POR',
                                          'BU', 'PLTFRM_NM',
                                          'BUS_UNIT_NM',
                                          'FAMILY', 'REGION', 'CATEGORY'],
                                 right_on=['TPO_PO_Vendor_Name',
                                           'Trade_PO',
                                           'TPO_LA_Conf_Delivery_Date',
                                           'TPO_LA_Conf_Delivery_Month_POR',
                                           'BU', 'PLTFRM_NM',
                                           'BUS_UNIT_NM',
                                           'FAMILY', 'REGION', 'CATEGORY'])
    elif ink_exe_bool:
        po_df = po_df.groupby(['TPO_PO_Vendor_Name',
                               'Trade_PO',
                               'TPO_Requested_Delivery_Date',
                               'TPO_Requested_Delivery_Month_POR',
                               'BU', 'PLTFRM_NM',
                               'BUS_UNIT_NM', 'FAMILY',
                               'REGION','PART_NR'],
                              dropna=False)['TPO_Qty'].sum().reset_index()

        ship_df = ship_df.groupby(['TPO_PO_Vendor_Name',
                                   'Trade_PO',
                                   'TPO_LA_Conf_Delivery_Date',
                                   'TPO_LA_Conf_Delivery_Month_POR',
                                   'BU', 'PLTFRM_NM',
                                   'BUS_UNIT_NM', 'FAMILY',
                                   'REGION','PART_NR'],
                                  dropna=False)['TPO_LA_Qty'].sum().reset_index()

        po_ship_df = po_df.merge(ship_df, how='outer',
                                 left_on=['TPO_PO_Vendor_Name',
                                          'Trade_PO',
                                          'TPO_Requested_Delivery_Date',
                                          'TPO_Requested_Delivery_Month_POR',
                                          'BU', 'PLTFRM_NM',
                                          'BUS_UNIT_NM',
                                          'FAMILY',
                                          'REGION','PART_NR'],
                                 right_on=['TPO_PO_Vendor_Name',
                                           'Trade_PO',
                                           'TPO_LA_Conf_Delivery_Date',
                                           'TPO_LA_Conf_Delivery_Month_POR',
                                           'BU',
                                           'PLTFRM_NM',
                                           'BUS_UNIT_NM',
                                           'FAMILY',
                                           'REGION','PART_NR'])        

    else:
        po_df = po_df.groupby(['TPO_PO_Vendor_Name',
                               'Trade_PO',
                               'TPO_Requested_Delivery_Date',
                               'TPO_Requested_Delivery_Month_POR',
                               'BU', 'PLTFRM_NM',
                               'BUS_UNIT_NM', 'FAMILY',
                               'REGION'],
                              dropna=False)['TPO_Qty'].sum().reset_index()

        ship_df = ship_df.groupby(['TPO_PO_Vendor_Name',
                                   'Trade_PO',
                                   'TPO_LA_Conf_Delivery_Date',
                                   'TPO_LA_Conf_Delivery_Month_POR',
                                   'BU', 'PLTFRM_NM',
                                   'BUS_UNIT_NM', 'FAMILY',
                                   'REGION'],
                                  dropna=False)['TPO_LA_Qty'].sum().reset_index()

        po_ship_df = po_df.merge(ship_df, how='outer',
                                 left_on=['TPO_PO_Vendor_Name',
                                          'Trade_PO',
                                          'TPO_Requested_Delivery_Date',
                                          'TPO_Requested_Delivery_Month_POR',
                                          'BU', 'PLTFRM_NM',
                                          'BUS_UNIT_NM',
                                          'FAMILY',
                                          'REGION'],
                                 right_on=['TPO_PO_Vendor_Name',
                                           'Trade_PO',
                                           'TPO_LA_Conf_Delivery_Date',
                                           'TPO_LA_Conf_Delivery_Month_POR',
                                           'BU',
                                           'PLTFRM_NM',
                                           'BUS_UNIT_NM',
                                           'FAMILY',
                                           'REGION'])

    po_ship_df['TPO_LA_Qty'] = po_ship_df['TPO_LA_Qty'].fillna(0)

    po_ship_df.loc[(po_ship_df['TPO_Requested_Delivery_Month_POR'].isnull()),
                   'TPO_Requested_Delivery_Month_POR'] = \
        po_ship_df['TPO_LA_Conf_Delivery_Month_POR']
    po_ship_df.loc[(po_ship_df['TPO_Requested_Delivery_Date'].isnull()),
                   'TPO_Requested_Delivery_Date'] = \
        po_ship_df['TPO_LA_Conf_Delivery_Date']
    po_ship_df['TPO_Qty'] = po_ship_df['TPO_Qty'].fillna(0)

    # REMOVE FLEX PTP AND FLEX ZH FOR INKJET AND HPPS FROM 2022 JULY
    po_ship_df = po_ship_df.loc[
        ~((po_ship_df['TPO_PO_Vendor_Name'].isin(['INK Flex PTP Malaysia',
                                                  'INK Flex Zhuhai', 'Laser HPPS'])) &
          (po_ship_df['TPO_Requested_Delivery_Month_POR'] >= '2022-07-01'))].copy()
    # Start from August and get less than today
    last_day_month = today.replace(
        day=1) + relativedelta(months=1) - relativedelta(days=1)
    po_ship_df = po_ship_df.loc[
        (po_ship_df['TPO_Requested_Delivery_Month_POR'] >= '2021-08-01') &
        (po_ship_df['TPO_Requested_Delivery_Date'] <= last_day_month)].copy()
    po_ship_df = po_ship_df.rename(columns={'TPO_PO_Vendor_Name': 'MPA'})
    if ink_exe_bool:
        po_ship_df = po_ship_df.rename(columns={'PART_NR':'SKU'})
    po_ship_df.columns = po_ship_df.columns.str.upper()
    # Try for dynamic
    po_ship_df['TPO_REQUESTED_DELIVERY_MONTH_STR'] =  \
        po_ship_df['TPO_REQUESTED_DELIVERY_MONTH_POR'].dt.strftime('%Y_%m')
    if ink_exe_bool:
        po_ship_df['QUARTER'] = \
            po_ship_df['TPO_REQUESTED_DELIVERY_MONTH_POR'].dt.to_period('Q-OCT') 
        po_ship_df['TYPE'] = 'TPO_SHIP'
    else:
        po_ship_df['QUARTER'] = \
            po_ship_df['TPO_REQUESTED_DELIVERY_MONTH_POR'].dt.to_period('Q-AUG')

    po_ship_df['QUARTER_YEAR'] = po_ship_df['QUARTER'].astype(str)
    po_ship_df['QUARTER_YEAR'] = po_ship_df['QUARTER_YEAR'].str[0:4]
    po_ship_df['QUARTER_NAME'] = po_ship_df['QUARTER'].astype(str)
    po_ship_df['QUARTER_NAME'] = 'F' + po_ship_df['QUARTER_NAME'].str[-2:] + \
        """'""" + po_ship_df['QUARTER_NAME'].str[2:4]
    po_ship_df['FY_NAME'] = 'ALL - FY' + po_ship_df['QUARTER_YEAR']
    # COMBINE TO SLICE
    if hpps_bool:
        cols = ['TPO_REQUESTED_DELIVERY_MONTH_STR', 'MPA',
                'REGION', 'PLTFRM_NM', 'FAMILY', 'CATEGORY']

        po_ship_df['COMBINED'] = po_ship_df[cols] \
            .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
    elif ink_exe_bool:
        cols = ['MPA', 'REGION', 'PLTFRM_NM', 'FAMILY','SKU','TYPE']

        po_ship_df['COMBINED'] = po_ship_df[cols] \
            .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
    else:
        cols = ['TPO_REQUESTED_DELIVERY_MONTH_STR', 'MPA',
                'REGION', 'PLTFRM_NM', 'FAMILY']

        po_ship_df['COMBINED'] = po_ship_df[cols] \
            .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)

    return po_ship_df


ink_month = combine_po_ship(ink_po_df, ink_ship_df, False, False)
# INKJET EXECUTIVE
ink_month_exe = combine_po_ship(ink_po_exe_df, ink_ship_exe_df, False, True)
laser_month = combine_po_ship(
    laser_po_df, laser_ship_df, False, False)
hpps_month = combine_po_ship(
    hpps_po_df, hpps_ship_df, True, False)
fxnwhl_month = combine_po_ship(
    fxnwhl_po_df, fxnwhl_ship_df, True, False)
canon_month = combine_po_ship(
    canon_po_df, canon_ship_df, True, False)
# Remove null MPA for CANON as no delivery
canon_month = canon_month.loc[~canon_month['MPA'].isnull()].copy()

laser_month = pd.concat(
    [laser_month, hpps_month, fxnwhl_month, canon_month], ignore_index=True)

# RECREATE COMBINED COLUMNS FOR LASER
cols = ['TPO_REQUESTED_DELIVERY_MONTH_STR', 'MPA',
        'REGION', 'PLTFRM_NM', 'FAMILY', 'CATEGORY']
laser_month['COMBINED'] = laser_month[cols] \
    .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)

# Try creating tables for relationship


def create_relationship(df,ink_exe_bool):
    unique_month = pd.DataFrame({'MONTH':
                                 df['TPO_REQUESTED_DELIVERY_MONTH_POR'].unique()
                                 }).sort_values(by='MONTH')
    # TO SLICE MPA PERFORMANCE
    month_slice = df[['TPO_REQUESTED_DELIVERY_MONTH_POR',
                      'TPO_REQUESTED_DELIVERY_DATE']].drop_duplicates(). \
        sort_values(by='TPO_REQUESTED_DELIVERY_DATE')
    # TO SLICE FY AND QUARTER
    month_q_slice = df[['TPO_REQUESTED_DELIVERY_MONTH_POR',
                        'QUARTER_NAME']].drop_duplicates(). \
        sort_values(by='TPO_REQUESTED_DELIVERY_MONTH_POR')

    month_fy_slice = df[['TPO_REQUESTED_DELIVERY_MONTH_POR',
                        'FY_NAME']].drop_duplicates(). \
        sort_values(by='TPO_REQUESTED_DELIVERY_MONTH_POR')

    # TO SLICE OTHER SLICERS
    # month_slice_df = df[['TPO_REQUESTED_DELIVERY_MONTH_POR',
    #                      'COMBINED']].drop_duplicates()
    mpa_slice_df = df[['MPA', 'COMBINED']].drop_duplicates()
    region_slice_df = df[['REGION', 'COMBINED']].drop_duplicates()
    plt_slice_df = df[['PLTFRM_NM', 'COMBINED']].drop_duplicates()
    fam_slice_df = df[['FAMILY', 'COMBINED']].drop_duplicates()
    if ink_exe_bool:
        sku_slice_df = df[['SKU', 'COMBINED']].drop_duplicates()
        return unique_month, month_slice, month_q_slice, month_fy_slice, \
            mpa_slice_df, region_slice_df, \
            plt_slice_df, fam_slice_df, sku_slice_df
    else:
        return unique_month, month_slice, month_q_slice, month_fy_slice, \
            mpa_slice_df, region_slice_df, \
            plt_slice_df, fam_slice_df


unique_month, month_slice, q_slice, fy_slice, \
    ink_mpa_slice_df, ink_region_slice_df, \
    ink_plt_slice_df, ink_fam_slice_df = create_relationship(
        ink_month,False)
# INKJET EXECUTIVE
unique_month_exe, month_slice_exe, q_slice_exe, fy_slice_exe, \
    ink_mpa_slice_exe_df, ink_region_slice_exe_df, \
    ink_plt_slice_exe_df, ink_fam_slice_exe_df, ink_sku_slice_exe_df = create_relationship(
        ink_month_exe,True)
# CREATE NAME FOR CUM CHART
fy_slice['FY_CUM_NAME'] = 'Cum to Date (' + fy_slice['FY_NAME'].str[6:8] + \
    fy_slice['FY_NAME'].str[-2:] + ')'

laser_unique_month, laser_month_slice, laser_q_slice, laser_fy_slice, \
    laser_mpa_slice_df, laser_region_slice_df, \
    laser_plt_slice_df, laser_fam_slice_df = create_relationship(
        laser_month,False)

# ONLY FOR LASER CATEGORY
laser_cat_slice_df = laser_month[['CATEGORY', 'COMBINED']].drop_duplicates()


def get_common_unique(df, col_name):
    df_unique = pd.DataFrame(
        {col_name: ink_month[col_name].unique()})
    return df_unique


por_month_unique = get_common_unique(
    ink_month, 'TPO_REQUESTED_DELIVERY_MONTH_POR')
quarter_unique = get_common_unique(ink_month, 'QUARTER_NAME')
fy_unique = get_common_unique(ink_month, 'FY_NAME')

# INKJET EXECUTIVE
por_month_unique_exe = get_common_unique(
    ink_month_exe, 'TPO_REQUESTED_DELIVERY_MONTH_POR')
quarter_unique_exe = get_common_unique(ink_month_exe, 'QUARTER_NAME')
fy_unique_exe = get_common_unique(ink_month_exe, 'FY_NAME')


# %% FULL SHIPMENT DATA

TPO_Final = pd.concat([TPO_Ink_Final, TPO_Laser_Final, TPO_HPPS_Final, TPO_FXNWHL_Final,
                       TPO_Canon_Final], ignore_index=True)

# %% SHIPMENT WITH POR


def read_build(mpa_str, ink_bool):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\BUILD',
                        mpa_str)
    os.chdir(path)
    # READ LATEST CSV FOR INK
    if ink_bool:
        csv_files = glob.glob(path + '/*' + '.csv')
        latest_csv = max(csv_files, key=os.path.getctime)
        filename = latest_csv.split('\\')[-1]
        build_df = pd.read_csv(filename,
                               parse_dates=['CAL_WK_DT'])
    else:
        week_num = 1  
        last_mon_date = today - relativedelta(days=today.weekday(), weeks=1)
        filename = last_mon_date.strftime('%Y-W%V') + '.csv'
        csv_files = glob.glob(path + '/*' + '.csv')
        csv_files = [os.path.basename(x) for x in csv_files]
        while(filename not in csv_files):
            week_num = week_num + 1
            last_mon_date = today - relativedelta(days=today.weekday(), weeks=week_num)
            filename = last_mon_date.strftime('%Y-W%V') + '.csv'
        build_df = pd.read_csv(filename,
                               parse_dates=['CAL_WK_DT'])
    return build_df


inkjet_b_list = ['NKG TH', 'NKG YY', 'FXN CQ', 'JWH INK', 'FXN WH INK']

ink_build_df = pd.DataFrame()
for mpa in inkjet_b_list:
    ink_b_get = read_build(mpa, True)
    ink_build_df = pd.concat([ink_build_df,ink_b_get], ignore_index=True)

laser_b_list = ['JABIL CUU', 'JWH LASER', 'FXN CZ']

laser_build_df = pd.DataFrame()
for mpa in laser_b_list:
    laser_b_get = read_build(mpa, False)
    laser_build_df = pd.concat([laser_build_df,laser_b_get], ignore_index=True)
# FXN WH LASER (HW)
fxnwhl_build_df = read_build('FXN WH LASER', False)

# CANON
canon_jp_build = read_build('CANON\CANON FFGI\JP', False)
canon_ph_build = read_build('CANON\CANON FFGI\PH', False)
canon_vn_build = read_build('CANON\CANON FFGI\VN', False)
canon_zs_build = read_build('CANON\CANON FFGI\ZS', False)


# REPLACE INK BUILD PLAN NAME
replace_mpa_name(ink_build_df, 'NKG Thailand', 'INK NKG Thailand')
replace_mpa_name(ink_build_df, 'NKG Yue Yang', 'INK NKG Yue Yang')
replace_mpa_name(ink_build_df, 'Foxconn ChongQing', 'INK Foxconn ChongQing')
replace_mpa_name(ink_build_df, 'JABIL WEIHAI', 'INK Jabil Weihai')
replace_mpa_name(ink_build_df, 'FOXCONN WEIHAI', 'INK Foxconn Weihai')
# REPLACE LASER BUILD PLAN NAME
replace_mpa_name(laser_build_df, 'JABIL CHIHUAHUA', 'Laser Jabil Chihuahua')
replace_mpa_name(laser_build_df, 'JABIL WEIHAI', 'Laser Jabil Weihai')
replace_mpa_name(
    laser_build_df, 'HP FOXCONN PARDUBICE MFG(SG31)', 'Laser Foxconn Czech')
replace_mpa_name(
    fxnwhl_build_df, 'FOXCONN WEIHAI', 'Laser Foxconn Weihai')

# REPLACE CANON BUILD PLAN NAME
canon_jp_build['LOC_FROM_NM'] = 'Canon JP'
canon_ph_build['LOC_FROM_NM'] = 'Canon PH'
canon_vn_build['LOC_FROM_NM'] = 'Canon VN'
canon_zs_build['LOC_FROM_NM'] = 'Canon CN, Zhongshan'
canon_build_df = pd.concat([canon_jp_build, canon_ph_build,
                            canon_vn_build, canon_zs_build], ignore_index=True)


def combine_ship_build(df, ship_df, hpps_bool, hpps_hw_str, canon_bool,ink_exe_bool):
    current_mon_date = today - relativedelta(days=today.weekday())
    min_por_date = df['CAL_WK_DT'].min()
    if current_mon_date > min_por_date:
        get_date = current_mon_date
        get_date = get_date.replace(hour=0, minute=0, second=0, microsecond=0)
        df = df.loc[df['CAL_WK_DT'] >= get_date].\
            copy()
    else:
        get_date = min_por_date
        df = df.loc[df['CAL_WK_DT'] >= get_date].\
            copy()

    if hpps_bool:
        # GET ONLY HW
        WR_Final = ship_df.loc[(ship_df['TPO_LA_CONF_DELIVERY_DATE_POR']
                               < get_date) & (ship_df['CATEGORY'] == hpps_hw_str)].copy()
    else:
        WR_Final = ship_df.loc[ship_df['TPO_LA_CONF_DELIVERY_DATE_POR']
                               < get_date].copy()

    # REMOVE MPA For Flex
    WR_Final = WR_Final.loc[(WR_Final['MPa'] != 'INK Flex PTP Malaysia') &
                            (WR_Final['MPa'] != 'INK Flex Zhuhai')].copy()

    WR_Final = WR_Final.groupby(['BU', 'BUS_UNIT_NM', 'MPA SITE', 'MPa',
                                 'SKU', 'REGION', 'PLATFORM', 'FAMILY',
                                 'TPO_LA_CONF_DELIVERY_DATE_POR',
                                 'DC_IC_PO_PLANT', 'SUB_REGION',
                                 'TPO_LA_CONF_DELIVERY_MONTH_POR',
                                 'MONTH_NUM_POR', 'ISO_POR',
                                 ])['TPO_LA_QTY'].sum().reset_index()

    WR_Final['TPO_LA_CONF_DELIVERY_MONTH_POR'] = \
        WR_Final['TPO_LA_CONF_DELIVERY_MONTH_POR'].apply(pd.to_datetime)
    if ink_exe_bool:
        month_range = pd.date_range(start='10/10/2021', periods=480, freq='M')
        month_df = pd.DataFrame({'Month': month_range})
        month_df['Month'] = month_df['Month'].dt.strftime('%Y-%m-01')
        month_df['Month'] = month_df['Month'].apply(pd.to_datetime)
        month_df['Quarter'] = month_df['Month'].dt.to_period('Q-OCT')
        month_df['Quarter'] = month_df['Quarter'].astype(str)
        month_df['Year'] = month_df['Quarter'].str[0:4]
        # Get this monday month
        current_month = current_mon_date.replace(day=1,hour=0, minute=0, second=0, microsecond=0)
        current_fy = month_df.loc[month_df['Month'] == current_month]['Year'].unique().item()
        WR_Final['Quarter'] = WR_Final['TPO_LA_CONF_DELIVERY_MONTH_POR'].dt.to_period('Q-OCT')
        WR_Final['Quarter'] = WR_Final['Quarter'].astype(str)
        WR_Final['Year'] = WR_Final['Quarter'].str[0:4]
        WR_Final = WR_Final.loc[WR_Final['Year'] == current_fy].copy()
        WR_Final = WR_Final.drop(columns=['Quarter','Year'])
        WR_Final['SHIP/POR'] = 'POR'
    else:
    # GET ONLY CURRENT MONTH AND PAST 3 MONTHS OF SHIPMENT DATA
        three_months_back = today.replace(day=1, hour=0, minute=0,
                                          second=0, microsecond=0) - relativedelta(months=3)
        WR_Final = WR_Final.loc[WR_Final['TPO_LA_CONF_DELIVERY_MONTH_POR']
                                >= three_months_back].copy()
        WR_Final['SHIP/POR'] = 'SHIP'

    WR_Final['POR_YEAR'] = WR_Final['TPO_LA_CONF_DELIVERY_MONTH_POR'].dt.year
    WR_Final = WR_Final.rename(columns={'TPO_LA_QTY': 'QTY'})
    

    df = df.groupby(['LOC_TO_CD', 'LOC_FROM_NM', 'PLTFRM_NM',
                     'BUS_UNIT_NM', 'PART_NR', 'CAL_WK_DT'])['QTY'].sum().reset_index()

    df['ISO_POR'] = \
        df['CAL_WK_DT'].dt.strftime('%y %b W%V')

    df = df.rename(columns={'LOC_TO_CD': 'DC_IC_PO_Plant',
                            'LOC_FROM_NM': 'MPa',
                            'PLTFRM_NM': 'PLATFORM',
                            'PART_NR': 'SKU',
                            'CAL_WK_DT': 'TPO_LA_CONF_DELIVERY_DATE_POR'
                            })

    # MAP BUILD PLAN REGION
    df = map_region(df, region_map, sub_region_map)
    
    if ink_exe_bool:
        # ADDING LA AND NA & CHINA
        df.loc[df['Sub_Region'] == 'NA', 'REGION'] = 'NA'
        df.loc[df['Sub_Region'] == 'LA', 'REGION'] = 'LA'
        df.loc[df['SKU'].str.contains('#AB2'), 'REGION'] = 'CHINA'

    # MAP BU, MPA SITE
    df = \
        df.merge(mpa_map, how='left', left_on='MPa', right_on='MPA')

    df = df.drop(columns=['MPA', 'MPA VENDOR CODE'])

    # ADD FAMILY MAPPING
    if hpps_bool:
        df = df.merge(hpps_family, how='left',
                      left_on=['MPa',
                               'PLATFORM', 'SKU'],
                      right_on=['MPA', 'PLTFRM_NM', 'PART_NR'])
        df = df.drop(columns=['MPA', 'PLTFRM_NM', 'PART_NR'])
    else:
        df = df.merge(family_map, how='left',
                      left_on=['MPa', 'PLATFORM'],
                      right_on=['MPA', 'PLTFRM_NM'])
        df = df.drop(columns=['MPA', 'PLTFRM_NM'])

    df['TPO_LA_CONF_DELIVERY_MONTH_POR'] = \
        df['TPO_LA_CONF_DELIVERY_DATE_POR'].dt.strftime('%Y-%m-01')
    df['TPO_LA_CONF_DELIVERY_MONTH_POR'] = \
        df['TPO_LA_CONF_DELIVERY_MONTH_POR'].apply(pd.to_datetime)
    if ink_exe_bool:
        # Get current Fiscal Year data 
        df['Quarter'] = df['TPO_LA_CONF_DELIVERY_MONTH_POR'].dt.to_period('Q-OCT')
        df['Quarter'] = df['Quarter'].astype(str)
        df['Year'] = df['Quarter'].str[0:4]
        df = df.loc[df['Year'] == current_fy].copy()
        df = df.drop(columns=['Quarter','Year'])
    else: 
        # Get 1 Year of future data
        one_year_fw = today.replace(day=1, hour=0, minute=0,
                                    second=0, microsecond=0) + relativedelta(months=12)
        df = df.loc[df['TPO_LA_CONF_DELIVERY_MONTH_POR'] < one_year_fw].copy()

    df['POR_YEAR'] = df['TPO_LA_CONF_DELIVERY_MONTH_POR'].dt.year
    df['MONTH_NUM_POR'] = \
        df['TPO_LA_CONF_DELIVERY_MONTH_POR'].dt.month

    df['SHIP/POR'] = 'POR'
    df.columns = df.columns.str.upper()
    df = df.rename(columns={'MPA Site': 'MPA SITE',
                            'MPA': 'MPa'})

    WR_Final = pd.concat([WR_Final, df], ignore_index=True)

    WR_Final['SHIP/POR_REGION'] = WR_Final['SHIP/POR'] + '-' + \
        WR_Final['REGION']

    if canon_bool:
        # CANON FAMILY NAMING TO FOLLOW ISAAC (REPLACE NAN WITH PLATFORM)
        WR_Final.loc[WR_Final['FAMILY'].isnull(
        ), 'FAMILY'] = WR_Final['PLATFORM']
        WR_Final['BU'] = WR_Final['BU'].fillna('Laser')
        WR_Final['MPA SITE'] = WR_Final['MPA SITE'].fillna('CANON')
    return WR_Final


def mpa_finished(ship_df, hpps_bool, hpps_hw_str):
    if hpps_bool:
        ship_df = ship_df.loc[ship_df['CATEGORY'] == hpps_hw_str].copy()
    ship_df = ship_df.groupby(['BU', 'BUS_UNIT_NM', 'MPA SITE', 'MPa',
                               'SKU', 'REGION', 'PLATFORM', 'FAMILY',
                               'TPO_LA_CONF_DELIVERY_DATE_POR',
                               'DC_IC_PO_PLANT', 'SUB_REGION',
                               'TPO_LA_CONF_DELIVERY_MONTH_POR',
                               'MONTH_NUM_POR', 'ISO_POR',
                               ])['TPO_LA_QTY'].sum().reset_index()

    ship_df['TPO_LA_CONF_DELIVERY_MONTH_POR'] = \
        ship_df['TPO_LA_CONF_DELIVERY_MONTH_POR'].apply(pd.to_datetime)

    ship_df['POR_YEAR'] = ship_df['TPO_LA_CONF_DELIVERY_MONTH_POR'].dt.year
    ship_df = ship_df.rename(columns={'TPO_LA_QTY': 'QTY'})
    ship_df['SHIP/POR'] = 'SHIP'
    ship_df['SHIP/POR_REGION'] = ship_df['SHIP/POR'] + '-' + \
        ship_df['REGION']
    return ship_df


WR_INK = combine_ship_build(ink_build_df, TPO_Ink_Final, False, None, False, False)
# INKJET EXECUTIVE
WR_INK_EXE = combine_ship_build(ink_build_df, TPO_Ink_Final, False, None, False, True)
WR_LASER = combine_ship_build(
    laser_build_df, TPO_Laser_Final, False, None, False, False)
# WR_HPPS = mpa_finished(TPO_HPPS_Final, True, 'HPPS HW')
WR_FXNWHL = combine_ship_build(fxnwhl_build_df, TPO_FXNWHL_Final, True,
                               'Laser FXNWH HW', False, False)
WR_CANON = combine_ship_build(
    canon_build_df, TPO_Canon_Final, False, None, True, False)
# REMOVE WR_HPPS
WR_FINAL = pd.concat([WR_INK, WR_LASER, WR_FXNWHL,
                      WR_CANON], ignore_index=True)

# =============================================================================
# FOR INKJET EXECUTIVE
# =============================================================================
WR_INK_EXE.columns = WR_INK_EXE.columns.str.upper()
WR_INK_EXE = WR_INK_EXE.rename(columns={'TPO_LA_CONF_DELIVERY_DATE_POR':'TPO_REQUESTED_DELIVERY_MONTH_POR',
                                'PLATFORM':'PLTFRM_NM'})
# Try for dynamic
WR_INK_EXE['QUARTER'] = \
    WR_INK_EXE['TPO_REQUESTED_DELIVERY_MONTH_POR'].dt.to_period('Q-OCT')


WR_INK_EXE['QUARTER_YEAR'] = WR_INK_EXE['QUARTER'].astype(str)
WR_INK_EXE['QUARTER_YEAR'] = WR_INK_EXE['QUARTER_YEAR'].str[0:4]

WR_INK_EXE['QUARTER_NAME'] = WR_INK_EXE['QUARTER'].astype(str)
WR_INK_EXE['QUARTER_NAME'] = 'F' + WR_INK_EXE['QUARTER_NAME'].str[-2:] + \
    """'""" + WR_INK_EXE['QUARTER_NAME'].str[2:4]
WR_INK_EXE['FY_NAME'] = 'ALL - FY' + WR_INK_EXE['QUARTER_YEAR']
WR_INK_EXE['TYPE'] = 'POR'
cols = ['MPA', 'REGION', 'PLTFRM_NM', 'FAMILY','SKU','TYPE']
WR_INK_EXE['COMBINED'] = WR_INK_EXE[cols] \
    .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
    
wr_mpa_slice_df = WR_INK_EXE[['MPA', 'COMBINED']].drop_duplicates()
wr_region_slice_df = WR_INK_EXE[['REGION', 'COMBINED']].drop_duplicates()
wr_plt_slice_df = WR_INK_EXE[['PLTFRM_NM', 'COMBINED']].drop_duplicates()
wr_fam_slice_df = WR_INK_EXE[['FAMILY', 'COMBINED']].drop_duplicates()
wr_sku_slice_df = WR_INK_EXE[['SKU', 'COMBINED']].drop_duplicates()

ink_mpa_slice_exe_df = pd.concat([ink_mpa_slice_exe_df,wr_mpa_slice_df])
ink_region_slice_exe_df = pd.concat([ink_region_slice_exe_df,wr_region_slice_df])
ink_plt_slice_exe_df = pd.concat([ink_plt_slice_exe_df,wr_plt_slice_df])
ink_fam_slice_exe_df = pd.concat([ink_fam_slice_exe_df,wr_fam_slice_df])
ink_sku_slice_exe_df = pd.concat([ink_sku_slice_exe_df,wr_sku_slice_df])

ink_cycle = ink_build_df[['CYCLE_WK_NM', 'LOC_FROM_NM']].drop_duplicates()
ink_cycle['BU'] = 'Inkjet'
laser_cycle = laser_build_df[['CYCLE_WK_NM', 'LOC_FROM_NM']].drop_duplicates()
laser_cycle['BU'] = 'Laser'
fxnwhl_cycle = fxnwhl_build_df[[
    'CYCLE_WK_NM', 'LOC_FROM_NM']].drop_duplicates()
fxnwhl_cycle['BU'] = 'Laser'
canon_cycle = canon_build_df[['CYCLE_WK_NM', 'LOC_FROM_NM']].drop_duplicates()
canon_cycle['BU'] = 'Laser'
cycle_ref_df = pd.concat([ink_cycle, laser_cycle, fxnwhl_cycle, canon_cycle],
                         ignore_index=True)
cycle_ref_df['REF_POR_NAME'] = 'Reference to ' + \
    cycle_ref_df['CYCLE_WK_NM'] + ' POR'

cycle_ref_df = cycle_ref_df.rename(columns={'LOC_FROM_NM': 'MPa'})
WR_bu_df = cycle_ref_df[['BU']].drop_duplicates()

# %% SHIPMENT VS POR

path_dir = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                           'PARQUET\Shipment')
os.chdir(path_dir)
past_ink_ship = pd.read_parquet('April to July 2021.parquet')

def read_past_build(mpa_str):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\BUILD',
                        mpa_str)
    os.chdir(path)
    current_mon_date = today - relativedelta(days=today.weekday())
    current_mon_date = current_mon_date.replace(
        hour=0, minute=0, second=0, microsecond=0)
    csv_files = glob.glob(path + '/*' + '.csv')
    past_build_df = pd.DataFrame()
    for file in csv_files:
        past_build = pd.read_csv(file, parse_dates=['CAL_WK_DT'])
        past_build_df = pd.concat([past_build, past_build_df])
    past_build_df = past_build_df.loc[past_build_df['CAL_WK_DT']
                                      <= current_mon_date]
    past_build_df = past_build_df.rename(
        columns={'LOC_TO_CD': 'DC_IC_PO_Plant'})
    past_build_df = map_region(past_build_df, region_map, sub_region_map)
    past_build_df.columns = past_build_df.columns.str.upper()
    return past_build_df


ink_past_build_df = pd.DataFrame()
for mpa in inkjet_b_list:
    ink_b_get = read_past_build(mpa)
    ink_past_build_df = pd.concat([ink_past_build_df,ink_b_get],ignore_index=True)

# REPLACE INK BUILD PLAN NAME
replace_mpa_name(ink_past_build_df, 'NKG Thailand', 'INK NKG Thailand')
replace_mpa_name(ink_past_build_df, 'NKG Yue Yang', 'INK NKG Yue Yang')
replace_mpa_name(ink_past_build_df, 'Foxconn ChongQing',
                 'INK Foxconn ChongQing')
replace_mpa_name(ink_past_build_df, 'JABIL WEIHAI', 'INK Jabil Weihai')
replace_mpa_name(ink_past_build_df, 'FOXCONN WEIHAI', 'INK Foxconn Weihai')
# first_por_date = ink_past_build_df['CAL_WK_DT'].min()

# GET START DATE OF POR
por_start_df = ink_past_build_df.groupby(
    ['CYCLE_WK_NM'])['CAL_WK_DT'].min().reset_index(name='POR_START')


# ink_ship_por_df = TPO_Ink_Final.loc[
#     TPO_Ink_Final['TPO_LA_CONF_DELIVERY_DATE_POR'] >= first_por_date].copy()

ink_ship_group = TPO_Ink_Final.groupby([
    'MPa', 'TPO_LA_CONF_DELIVERY_DATE_POR', 'BUS_UNIT_NM', 'PLATFORM',
    'REGION', 'SKU'], dropna=False)['TPO_LA_QTY'].sum().reset_index()

# Remove Flex ZH and Flex PTP
ink_ship_group = ink_ship_group.loc[
    ~(ink_ship_group['MPa'].isin(['INK Flex PTP Malaysia',
                                  'INK Flex Zhuhai']))].copy()

# ADD PAST DATA FROM VELOCITY
ink_ship_group = pd.concat([past_ink_ship,ink_ship_group],ignore_index=False)

ink_por_group = ink_past_build_df.groupby(['CYCLE_WK_NM', 'LOC_FROM_NM',
                                           'BUS_UNIT_NM', 'PLTFRM_NM',
                                           'REGION',
                                           'PART_NR', 'CAL_WK_DT'])['QTY'].sum().reset_index()

por_cycle_unique = ink_por_group[['CYCLE_WK_NM']].drop_duplicates()
# TRY CROSS JOIN FIRST
ink_ship_group = ink_ship_group.merge(por_cycle_unique, how='cross')
ink_por_merge = ink_por_group.merge(
    ink_ship_group,
    how='outer', left_on=['CYCLE_WK_NM', 'LOC_FROM_NM', 'CAL_WK_DT', 'REGION', 'PART_NR'],
    right_on=['CYCLE_WK_NM', 'MPa', 'TPO_LA_CONF_DELIVERY_DATE_POR', 'REGION', 'SKU'])

# =============================================================================
# GET POR DATA (THOSE WITH SHIP ON THE SAME WEEK)
# =============================================================================
ink_por_ship_df = ink_por_merge.loc[
    (ink_por_merge['LOC_FROM_NM'].notnull()) &
    (ink_por_merge['MPa'].notnull())].copy()
# USE SHIPMENT PLATFORM AS PLATFORM MAY CHANGE NAME OVER TIME
ink_por_ship_df = ink_por_ship_df.drop(columns=['LOC_FROM_NM', 'BUS_UNIT_NM_x',
                                                'PLTFRM_NM', 
                                                'PART_NR', 
                                                'TPO_LA_CONF_DELIVERY_DATE_POR'])
ink_por_ship_df = ink_por_ship_df.rename(columns={'BUS_UNIT_NM_y': 'BUS_UNIT_NM'})

# =============================================================================
# GET POR DATA (THOSE WITH POR AND NO SHIPMENT ON THAT WEEK)
# =============================================================================
ink_por_df = ink_por_merge.loc[
    (ink_por_merge['LOC_FROM_NM'].notnull()) &
    (ink_por_merge['MPa'].isnull())].copy()
ink_por_df = ink_por_df.drop(columns=['MPa', 'TPO_LA_CONF_DELIVERY_DATE_POR',
                                      'BUS_UNIT_NM_y', 'PLATFORM',
                                      'SKU'])
# MERGE AND TAKE LATEST PLATFORM FROM SHIPMENT DATA
# NOVELLI BASE YET1 CHANGE TO NOVELLI BASE PAAS (REPLACE)
ink_por_df = ink_por_df.merge(Ink_POR_grouped, how='left', on='PART_NR')
ink_por_df.loc[
    (ink_por_df['PLTFRM_NM_x'] != ink_por_df['PLTFRM_NM_y']) &
    (ink_por_df['PLTFRM_NM_y'].notnull()), 'PLTFRM_NM_x'] = ink_por_df['PLTFRM_NM_y']

ink_por_df = ink_por_df.drop(columns=['LOC_FROM_NM_y', 'PLTFRM_NM_y',
                                      'BUS_UNIT_NM'])
ink_por_df = ink_por_df.rename(columns={'LOC_FROM_NM_x': 'MPa',
                                        'BUS_UNIT_NM_x': 'BUS_UNIT_NM',
                                        'PLTFRM_NM_x': 'PLATFORM',
                                        'PART_NR': 'SKU'})
ink_por_df['TPO_LA_QTY'] = ink_por_df['TPO_LA_QTY'].fillna(0)
# =============================================================================
# GET SHIPMENT DATA (THOSE WITH SHIPMENT AND NO POR)
# =============================================================================
ink_ship_no_por_df = ink_por_merge.loc[
    (ink_por_merge['LOC_FROM_NM'].isnull()) &
    (ink_por_merge['MPa'].notnull())].copy()

ink_ship_no_por_df = ink_ship_no_por_df.drop(columns=['LOC_FROM_NM', 'BUS_UNIT_NM_x',
                                                      'PLTFRM_NM', 
                                                      'PART_NR', 'CAL_WK_DT'])
ink_ship_no_por_df = ink_ship_no_por_df.rename(columns={'BUS_UNIT_NM_y': 'BUS_UNIT_NM',
                                                        'TPO_LA_CONF_DELIVERY_DATE_POR': 'CAL_WK_DT'})
ink_ship_no_por_df['QTY'] = ink_ship_no_por_df['QTY'].fillna(0)
# =============================================================================
# FIND THOSE WITH SHIPMENT BUT NOT IN POR DATE
# =============================================================================

# null_por_df = ink_por_merge.loc[ink_por_merge['CYCLE_WK_NM'].isnull()].copy()
# null_por_df = null_por_df[['MPa', 'TPO_LA_CONF_DELIVERY_DATE_POR',
#                            'BUS_UNIT_NM_y', 'PLATFORM',
#                            'REGION_y', 'DC_IC_PO_PLANT_y',
#                            'SKU', 'TPO_LA_QTY']].copy()

# null_por_df = null_por_df.merge(por_cycle_unique, how='cross')

# MERGE WITH POR START DATE AND REMOVE SHIPMENT BEFORE IT
# null_por_df = null_por_df.merge(por_start_df, how='left', on='CYCLE_WK_NM')
# null_por_df = null_por_df.loc[
#     null_por_df['TPO_LA_CONF_DELIVERY_DATE_POR'] >= null_por_df['POR_START']].copy()
# REMOVE MISSING CYCLE (FOUND IN SHIPMENT DATA ONLY)
# ink_por_merge = ink_por_merge.loc[ink_por_merge['CYCLE_WK_NM'].notnull()].copy(
# )
# ink_por_merge = ink_por_merge.drop(
#     columns=['MPa', 'TPO_LA_CONF_DELIVERY_DATE_POR', 'BUS_UNIT_NM_y',
#              'PLATFORM', 'DC_IC_PO_PLANT_y', 'REGION_y', 'SKU'])

# ink_por_merge = ink_por_merge.rename(columns={
#     'LOC_FROM_NM': 'MPa',
#     'BUS_UNIT_NM_x': 'BUS_UNIT_NM',
#     'PLTFRM_NM': 'PLATFORM',
#     'DC_IC_PO_PLANT_x': 'DC_IC_PO_PLANT',
#     'REGION_x': 'REGION',
#     'PART_NR': 'SKU',
# })

# ink_por_merge['TPO_LA_QTY'] = ink_por_merge['TPO_LA_QTY'].fillna(0)

# null_por_df = null_por_df.rename(columns={
#     'TPO_LA_CONF_DELIVERY_DATE_POR': 'CAL_WK_DT',
#     'BUS_UNIT_NM_y': 'BUS_UNIT_NM',
#     'DC_IC_PO_PLANT_y': 'DC_IC_PO_PLANT',
#     'REGION_y': 'REGION',
#     'PART_NR': 'SKU',
# })

# null_por_df['QTY'] = 0
# DROP EXTRA COLUMN
# null_por_df = null_por_df.drop(columns=['POR_START'])
ship_v_por_df = pd.concat([ink_por_ship_df, ink_por_df,
                           ink_ship_no_por_df], ignore_index=True)

# MERGE WITH POR START DATE AND REMOVE SHIPMENT BEFORE IT
ship_v_por_df = ship_v_por_df.merge(por_start_df, how='left', on='CYCLE_WK_NM')
ship_v_por_df = ship_v_por_df.loc[
    ship_v_por_df['CAL_WK_DT'] >= ship_v_por_df['POR_START']].copy()
ship_v_por_df = ship_v_por_df.drop(columns=['POR_START'])
ship_v_por_df = ship_v_por_df.merge(family_map, how='left',
                                    left_on=[
                                        'MPa', 'PLATFORM'],
                                    right_on=['MPA', 'PLTFRM_NM'])
ship_v_por_df = ship_v_por_df.drop(columns=['MPA', 'PLTFRM_NM'])

# %% PARQUET
# =============================================================================
# EXPORT
# =============================================================================
ink_month['QUARTER'] = ink_month['QUARTER'].astype(str)
WR_INK_EXE['QUARTER'] = WR_INK_EXE['QUARTER'].astype(str)
laser_month['QUARTER'] = laser_month['QUARTER'].astype(str)
def output_pq(Type,df,file_name):
    path_dir = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                               'PARQUET',Type)
    os.chdir(path_dir)
    df.to_parquet(file_name + '.parquet', engine='pyarrow', index=False)
    
def output_csv(Type,df,file_name):
    path_dir = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                               'CSV',Type)
    os.chdir(path_dir)
    df.to_csv(file_name + '.csv', index=False)
    
# SHIPMENT
output_pq('Shipment',TPO_Final,'Shipment')
# MPA PERFORMANCE
output_pq('MPA Performance',month_slice,'month_slice')
output_pq('MPA Performance',unique_month,'unique_month')
output_pq('MPA Performance',quarter_unique,'quarter_unique')
output_pq('MPA Performance',fy_unique,'fy_unique')
output_pq('MPA Performance',q_slice,'q_slice')
output_pq('MPA Performance',fy_slice,'fy_slice')
output_pq('MPA Performance',ink_mpa_slice_df,'ink_mpa_slice_df')
output_pq('MPA Performance',ink_region_slice_df,'ink_region_slice_df')
output_pq('MPA Performance',ink_plt_slice_df,'ink_plt_slice_df')
output_pq('MPA Performance',ink_fam_slice_df,'ink_fam_slice_df')
output_pq('MPA Performance',ink_month,'ink_month')
output_pq('MPA Performance',laser_mpa_slice_df,'laser_mpa_slice_df')
output_pq('MPA Performance',laser_region_slice_df,'laser_region_slice_df')
output_pq('MPA Performance',laser_plt_slice_df,'laser_plt_slice_df')
output_pq('MPA Performance',laser_fam_slice_df,'laser_fam_slice_df')
output_pq('MPA Performance',laser_cat_slice_df,'laser_cat_slice_df')
output_pq('MPA Performance',laser_month,'laser_month')
# SHIPMENT WITH POR
output_pq('Shipment With POR',WR_FINAL,'WR_FINAL')
output_pq('Shipment With POR',cycle_ref_df,'cycle_ref_df')
output_pq('Shipment With POR',WR_bu_df,'WR_bu_df')
# SHIPMENT VS POR
output_pq('Shipment Vs POR',ship_v_por_df,'ship_v_por_df')
# EXECUTIVE REPORT
output_pq('Ernest Ink MPA Performance',month_slice_exe,'month_slice')
output_pq('Ernest Ink MPA Performance',unique_month_exe,'unique_month')
output_pq('Ernest Ink MPA Performance',quarter_unique_exe,'quarter_unique')
output_pq('Ernest Ink MPA Performance',fy_unique_exe,'fy_unique')
output_pq('Ernest Ink MPA Performance',q_slice_exe,'q_slice')
output_pq('Ernest Ink MPA Performance',fy_slice_exe,'fy_slice')
output_pq('Ernest Ink MPA Performance',ink_mpa_slice_exe_df,'ink_mpa_slice_df')
output_pq('Ernest Ink MPA Performance',ink_region_slice_exe_df,'ink_region_slice_df')
output_pq('Ernest Ink MPA Performance',ink_plt_slice_exe_df,'ink_plt_slice_df')
output_pq('Ernest Ink MPA Performance',ink_fam_slice_exe_df,'ink_fam_slice_df')
output_pq('Ernest Ink MPA Performance',ink_sku_slice_exe_df,'ink_sku_slice_df')
output_pq('Ernest Ink MPA Performance',ink_month_exe,'ink_month')
# EXECUTIVE REPORT POR
output_pq('Ernest POR',WR_INK_EXE,'WR_INK')
output_pq('Ernest POR',ink_cycle,'cycle_ref_df')

# =============================================================================
# CSV VERSION
# =============================================================================
# SHIPMENT
output_csv('Shipment',TPO_Final,'Shipment')
# MPA PERFORMANCE
output_csv('MPA Performance',month_slice,'month_slice')
output_csv('MPA Performance',unique_month,'unique_month')
output_csv('MPA Performance',quarter_unique,'quarter_unique')
output_csv('MPA Performance',fy_unique,'fy_unique')
output_csv('MPA Performance',q_slice,'q_slice')
output_csv('MPA Performance',fy_slice,'fy_slice')
output_csv('MPA Performance',ink_mpa_slice_df,'ink_mpa_slice_df')
output_csv('MPA Performance',ink_region_slice_df,'ink_region_slice_df')
output_csv('MPA Performance',ink_plt_slice_df,'ink_plt_slice_df')
output_csv('MPA Performance',ink_fam_slice_df,'ink_fam_slice_df')
output_csv('MPA Performance',ink_month,'ink_month')
output_csv('MPA Performance',laser_mpa_slice_df,'laser_mpa_slice_df')
output_csv('MPA Performance',laser_region_slice_df,'laser_region_slice_df')
output_csv('MPA Performance',laser_plt_slice_df,'laser_plt_slice_df')
output_csv('MPA Performance',laser_fam_slice_df,'laser_fam_slice_df')
output_csv('MPA Performance',laser_cat_slice_df,'laser_cat_slice_df')
output_csv('MPA Performance',laser_month,'laser_month')
# SHIPMENT WITH POR
output_csv('Shipment With POR',WR_FINAL,'WR_FINAL')
output_csv('Shipment With POR',cycle_ref_df,'cycle_ref_df')
output_csv('Shipment With POR',WR_bu_df,'WR_bu_df')
# SHIPMENT VS POR
output_csv('Shipment Vs POR',ship_v_por_df,'ship_v_por_df')
# EXECUTIVE REPORT
output_csv('Ernest Ink MPA Performance',month_slice_exe,'month_slice')
output_csv('Ernest Ink MPA Performance',unique_month_exe,'unique_month')
output_csv('Ernest Ink MPA Performance',quarter_unique_exe,'quarter_unique')
output_csv('Ernest Ink MPA Performance',fy_unique_exe,'fy_unique')
output_csv('Ernest Ink MPA Performance',q_slice_exe,'q_slice')
output_csv('Ernest Ink MPA Performance',fy_slice_exe,'fy_slice')
output_csv('Ernest Ink MPA Performance',ink_mpa_slice_exe_df,'ink_mpa_slice_df')
output_csv('Ernest Ink MPA Performance',ink_region_slice_exe_df,'ink_region_slice_df')
output_csv('Ernest Ink MPA Performance',ink_plt_slice_exe_df,'ink_plt_slice_df')
output_csv('Ernest Ink MPA Performance',ink_fam_slice_exe_df,'ink_fam_slice_df')
output_csv('Ernest Ink MPA Performance',ink_sku_slice_exe_df,'ink_sku_slice_df')
output_csv('Ernest Ink MPA Performance',ink_month_exe,'ink_month')
# EXECUTIVE REPORT POR
output_csv('Ernest POR',WR_INK_EXE,'WR_INK')
output_csv('Ernest POR',ink_cycle,'cycle_ref_df')

end_run = datetime.now()
td = (end_run - start_run).total_seconds() /60
print('Code has completed running, Time taken {:.2f} minutes'.format(td))
# CHECKING
# df_parquet = pd.read_parquet('Shipment.parquet')
# df_parquet.head()
# TPO_Final.equals(df_parquet)  
