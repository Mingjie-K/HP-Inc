# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 10:21:49 2022

@author: kohm
"""

# %% Import Libraries
import pandas as pd
import numpy as np
import glob
from datetime import datetime
from dateutil.relativedelta import relativedelta
import importlib

import os
user = os.getenv('USERPROFILE')
# SharePoint Path
project_func_path = os.path.join(
    user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Code')
# Troubleshoot Path
# project_func_path = os.path.join(user, 'OneDrive - HP Inc\Projects\SAIL\Code')
os.chdir(project_func_path)

# Change directory to import neccessary module
import business  # nopep8
import function_map as fm  # nopep8
importlib.reload(fm)
pd.set_option('display.max_columns', None)

# GET ALL DATES
today, current_mon_date, current_mon_month = fm.get_date()
start_run = datetime.now()

# %% Define all list and dictionary

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
             'REGION': str}

dates = ['TPO_Created_On', 'TPO_Requested_Delivery_Date',
         'TPO_LA_Conf_Delivery_Date', 'TPO_PR_Creation_Date']

past_dates = dates[0:3]

past_dates_1 = dates[1:3]

ink_por_naming = fm.ink_por_naming

laser_por_naming = fm.laser_por_naming

# %% Functions to get data


def read_csv_s4(mpa_str):
    # READING CURRENT FACTORY PURCHASE ORDER REPORT
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
    # READING PAST PROCESSED FACTORY PURCHASE ORDER REPORT
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                        'PAST PROCESSED DATA', TYPE, mpa_str)
    os.chdir(path)
    past_csv_files = glob.glob(path + '/*' + '.csv')
    past_files = [os.path.basename(x) for x in past_csv_files]
    past_df = pd.DataFrame()
    for file in past_files:
        df = pd.read_csv(file,
                         dtype=data_type,
                         parse_dates=date_in,
                         converters={'Sub_Region': str})
        past_df = pd.concat([past_df, df], ignore_index=True)
    past_df[['DC_IC_PO_Plant', 'REGION', 'Sub_Region']] = \
        past_df[['DC_IC_PO_Plant', 'REGION', 'Sub_Region']].fillna('')

    return past_df


def read_por(mpa_str):
    # READING ALL POR DATA (SKUS) FOR FACTORY PURCHASE ORDER REPORT
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\POR',
                        mpa_str)
    os.chdir(path)
    por_csv_files = glob.glob(path + '/*' + '.csv')
    por_files = [os.path.basename(x) for x in por_csv_files]
    por_df = pd.DataFrame()
    for file in por_files:
        df = pd.read_csv(file)
        por_df = pd.concat([por_df, df], ignore_index=True)
    return por_df


# =============================================================================
# ETD DATA
# =============================================================================
canon_etd_path = os.path.join(
    user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data\CANON\ETD')

# =============================================================================
# SHIPMENT WITH POR DATA (ONLY LATEST POR)
# =============================================================================


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
        # READ LAST WEEK CSV FOR LASER
        week_num = 1
        last_mon_date = today - relativedelta(days=today.weekday(), weeks=1)
        filename = last_mon_date.strftime('%Y-W%V') + '.csv'
        csv_files = glob.glob(path + '/*' + '.csv')
        csv_files = [os.path.basename(x) for x in csv_files]
        while(filename not in csv_files):
            week_num = week_num + 1
            last_mon_date = today - \
                relativedelta(days=today.weekday(), weeks=week_num)
            filename = last_mon_date.strftime('%Y-W%V') + '.csv'
        build_df = pd.read_csv(filename,
                               parse_dates=['CAL_WK_DT'])
    return build_df

# =============================================================================
# SHIPMENT VS POR (READ ALL PAST BUILD)
# =============================================================================


def read_past_build(mpa_str, mon_date):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\BUILD',
                        mpa_str)
    os.chdir(path)
    csv_files = glob.glob(path + '/*' + '.csv')
    past_build_df = pd.DataFrame()
    for file in csv_files:
        past_build = pd.read_csv(file, parse_dates=['CAL_WK_DT'])
        past_build_df = pd.concat([past_build, past_build_df])
    past_build_df = past_build_df.loc[past_build_df['CAL_WK_DT']
                                      <= mon_date]
    past_build_df = past_build_df.rename(
        columns={'LOC_TO_CD': 'DC_IC_PO_Plant'})
    past_build_df = fm.map_region(past_build_df, region_map, sub_region_map)
    past_build_df.columns = past_build_df.columns.str.upper()
    return past_build_df


# =============================================================================
# TV DASHBOARD
# =============================================================================
def read_week_build(user, folder):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\BUILD',
                        folder)
    os.chdir(path)
    past_csv_files = glob.glob(path + '/*' + '.csv')
    por_files = [os.path.basename(x) for x in past_csv_files]
    por_df = pd.DataFrame()
    for file in por_files:
        df = pd.read_csv(file, parse_dates=['CAL_WK_DT'])
        por_df = pd.concat([por_df, df], ignore_index=True)
    return por_df

# %% Functions to export data


def output_pq(Type, df, file_name):
    path_dir = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                            'PARQUET', Type)
    os.chdir(path_dir)
    df.to_parquet(file_name + '.parquet', engine='pyarrow', index=False)


def output_csv(Type, df, file_name):
    path_dir = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                            'CSV', Type)
    os.chdir(path_dir)
    df.to_csv(file_name + '.csv', index=False)

# %% Business User Filtering Excel File


business_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput',
                             'Data Filtering')

os.chdir(business_path)
# =============================================================================
# MPA
# =============================================================================
mpa_map = business.mpa()

# GET ONLY INKJET MPA
ink_mpa_map = mpa_map.loc[mpa_map['BU'] == 'Inkjet'].copy()

# GET ONLY LASER MPA
laser_mpa_map = mpa_map.loc[mpa_map['BU'] == 'Laser'].copy()

# =============================================================================
# REGION
# =============================================================================
# REGION MAPPING
region_map = business.region()

# SUB-REGION MAPPING
sub_region_map = business.sub_region()

# FAMILY MAPPING
family_map = business.family()

# =============================================================================
# HPPS
# =============================================================================
# HPPS PROFIT CENTER
hpps_pc = business.hpps_pc()

# HPPS FAMILY MAPPING
hpps_family = business.hpps_family()

# HPPS SHIPPING POINT
hpps_ship_point, ship_point = business.hpps_ship_point()

# HPPS EXCLUDE INTERPLANT
hpps_ex_plant = business.hpps_ex_plant()

# =============================================================================
# FXN WH LASER (HPPS MOVE TO FXN WH LASER)
# =============================================================================
# FXN WH LASER FAMILY MAPPING (HPPS MOVE TO FXN WH LASER)
fxnwhl_family = business.fxnwhl_family()

# FXN WH LASER PROFIT CENTER (HPPS MOVE TO FXN WH LASER)
fxnwhl_pc = business.fxnwhl_pc()

# FXN WH LASER PL OVERWRITE (CHANGE IN PL BY PLANNING)
fxnwhl_pl = business.fxnwhl_pl()

# FXN WH LASER EXCLUDE INTERPLANT
fxnwhl_ex_plant = business.fxnwhl_ex_plant()

# CANON SITE MAPPING
canon_site_dict = business.canon_site()

# ONLY EXCLUDE FLEX SKUS
ex_skus = business.ex_skus()

# =============================================================================
# TV MAPPING
# =============================================================================
tv_family_map = business.tv_family()
# %% INKJET DATA
inkjet_list = ['NKG TH', 'NKG YY', 'FLEX PTP', 'FLEX ZH', 'FXN CQ', 'JWH INK',
               'FXN WH INK']

ink_dataset = pd.DataFrame()
for mpa in inkjet_list:
    ink_get = read_csv_s4(mpa)
    ink_dataset = pd.concat([ink_dataset, ink_get], ignore_index=True)

ink_por = pd.DataFrame()
for mpa in inkjet_list:
    ink_get = read_por(mpa)
    ink_por = pd.concat([ink_por, ink_get], ignore_index=True)

ink_por['LOC_FROM_NM'] = ink_por['LOC_FROM_NM'].map(ink_por_naming)

ink_dataset = fm.map_mpa(ink_dataset, ink_mpa_map)
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
    laser_por = pd.concat([laser_por, laser_get], ignore_index=True)

laser_por['LOC_FROM_NM'] = laser_por['LOC_FROM_NM'].map(laser_por_naming)

laser_dataset = fm.map_mpa(laser_dataset, laser_mpa_map)

# HPPS
hpps_dataset = read_csv_s4('HPPS')
hpps_por_df = read_por('HPPS')
hpps_por_df['LOC_FROM_NM'] = hpps_por_df['LOC_FROM_NM'].map(laser_por_naming)

hpps_dataset = fm.map_mpa(hpps_dataset, laser_mpa_map)

# FXN WH LASER
fxnwhl_dataset = read_csv_s4('FXN WH LASER')
fxnwhl_por_df = read_por('FXN WH LASER')
fxnwhl_por_df['LOC_FROM_NM'] = fxnwhl_por_df['LOC_FROM_NM'].map(
    laser_por_naming)

fxnwhl_dataset = fm.map_mpa(fxnwhl_dataset, laser_mpa_map)

# CANON
canon_list = ['CANON\EUROPA', r'CANON\USA SG']
canon_dataset = pd.DataFrame()
for mpa in canon_list:
    canon_get = read_csv_s4(mpa)
    canon_dataset = pd.concat([canon_dataset, canon_get], ignore_index=True)

canon_ffgi_por_df = read_por('CANON\CANON FFGI')
canon_ffgi_por_df['LOC_FROM_NM'] = 'Laser Canon'
canon_ffgi_por_df['CATEGORY'] = 'CANON FFGI'

canon_dataset = fm.map_mpa(canon_dataset, laser_mpa_map)

laser_df, laser_df_cum = fm.data_cleaning(
    laser_dataset, False, False, False, None)
ink_df, ink_df_cum = fm.data_cleaning(
    ink_dataset, False, False, False, None)
hpps_df, hpps_df_cum = fm.data_cleaning(
    hpps_dataset, True, False, True, ship_point)
fxnwhl_df, fxnwhl_df_cum = fm.data_cleaning(
    fxnwhl_dataset, True, False, False, None)
canon_df, canon_df_cum = fm.data_cleaning(
    canon_dataset, False, True, False, None)

# MAP REGION
laser_df = fm.map_region(laser_df, region_map, sub_region_map)
laser_df_cum = fm.map_region(
    laser_df_cum, region_map, sub_region_map)
ink_df = fm.map_region(ink_df, region_map, sub_region_map)
ink_df_cum = fm.map_region(ink_df_cum, region_map, sub_region_map)
hpps_df = fm.map_region(hpps_df, region_map, sub_region_map)
hpps_df_cum = fm.map_region(hpps_df_cum, region_map, sub_region_map)
fxnwhl_df = fm.map_region(fxnwhl_df, region_map, sub_region_map)
fxnwhl_df_cum = fm.map_region(
    fxnwhl_df_cum, region_map, sub_region_map)
canon_df = fm.map_region(canon_df, region_map, sub_region_map)
canon_df_cum = fm.map_region(
    canon_df_cum, region_map, sub_region_map)


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

# %% COMBINE DATA (FINAL DATA)
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
# EXCLUDE HPPS/FXNWHL INTERPLANT ITEMS
hpps_df_cum = hpps_df_cum.loc[~hpps_df_cum['DC_IC_PO_Plant'].isin(
    hpps_ex_plant)].copy()
fxnwhl_df_cum = fxnwhl_df_cum.loc[~fxnwhl_df_cum['DC_IC_PO_Plant'].isin(
    fxnwhl_ex_plant)].copy()
# OVERWRITE FXNWHL PL CHANGE
fxnwhl_df = fxnwhl_df.merge(fxnwhl_pl, how='left',
                            left_on=['TPO_PO_Vendor_Name', 'TPO_Material'],
                            right_on=['MPA', 'SKU'])
fxnwhl_df.loc[fxnwhl_df['SKU'].notnull(),
              'TPO_Profit_Center'] = fxnwhl_df['PL OVERWRITE']

fxnwhl_df_cum = fxnwhl_df_cum.merge(fxnwhl_pl, how='left',
                                    left_on=['TPO_PO_Vendor_Name',
                                             'TPO_Material'],
                                    right_on=['MPA', 'SKU'])

fxnwhl_df_cum.loc[fxnwhl_df_cum['SKU'].notnull(),
                  'TPO_Profit_Center'] = fxnwhl_df_cum['PL OVERWRITE']

fxnwhl_df_cum = fxnwhl_df_cum.drop(columns=['MPA', 'SKU'])

# ADDED DROP CATEGORY AND REPLACE
# fxnwhl_df_cum = fxnwhl_df_cum.merge(
#     fxnwhl_pc, how='left', on='TPO_Profit_Center')


# %% SHIPMENT DATA

TPO_Ink_Final, Ink_POR_grouped = fm.shipment_data(
    ink_df, ink_por, False, None, None, None, family_map)
TPO_Laser_Final, Laser_POR_grouped = fm.shipment_data(
    laser_df, laser_por, False, None, None, None, family_map)
TPO_HPPS_Final, HPPS_POR_grouped = fm.shipment_data(
    hpps_df, hpps_por_df, True, hpps_pc, hpps_family, hpps_ex_plant, None)
TPO_FXNWHL_Final, FXNWHL_POR_grouped = fm.shipment_data(
    fxnwhl_df, fxnwhl_por_df, True, fxnwhl_pc, fxnwhl_family, fxnwhl_ex_plant, None)

tpo_list = [TPO_Ink_Final, TPO_Laser_Final, TPO_HPPS_Final, TPO_FXNWHL_Final]
# =============================================================================
# COVERT TO MONDAY START WEEK
# =============================================================================
for df in tpo_list:
    df = fm.convert_date_week(df,
                              'TPO_LA_Conf_Delivery_Date_POR',
                              'TPO_LA_Conf_Delivery_Date')
    df = fm.convert_date_month(df,
                               'TPO_LA_Conf_Delivery_Month_POR',
                               'TPO_LA_Conf_Delivery_Date_POR')
    df = fm.convert_month_num(df, 'MONTH_NUM_POR',
                              'TPO_LA_Conf_Delivery_Month_POR')
    df = fm.convert_iso(df, 'ISO_POR',
                        'TPO_LA_Conf_Delivery_Date_POR')

TPO_Ink_Final = fm.shipment_por_skus(TPO_Ink_Final,
                                     Ink_POR_grouped,
                                     False)

# REMOVE FLEX PTP AND FLEX ZH FROM 2022 JULY
TPO_Ink_Final = TPO_Ink_Final.loc[
    ~((TPO_Ink_Final['MPa'].isin(['INK Flex PTP Malaysia',
                                  'INK Flex Zhuhai'])) &
      (TPO_Ink_Final['TPO_LA_CONF_DELIVERY_MONTH_POR'] >= '2022-07-01'))].copy()

TPO_Laser_Final = fm.shipment_por_skus(TPO_Laser_Final,
                                       Laser_POR_grouped,
                                       False)
TPO_HPPS_Final = fm.shipment_por_skus(TPO_HPPS_Final,
                                      HPPS_POR_grouped,
                                      False)
TPO_FXNWHL_Final = fm.shipment_por_skus(TPO_FXNWHL_Final,
                                        FXNWHL_POR_grouped,
                                        False)

# =============================================================================
# CANON SHIPMENT DATA
# =============================================================================
TPO_Canon_Final, Canon_POR_grouped = \
    fm.canon_shipment_data(canon_df, False,
                           canon_etd_path, canon_ffgi_por_df)

TPO_Cum_Canon, Canon_POR_grouped = \
    fm.canon_shipment_data(canon_df_cum, True,
                           canon_etd_path, canon_ffgi_por_df)

canon_tpo_list = [TPO_Canon_Final, TPO_Cum_Canon]
for df in canon_tpo_list:
    df = fm.convert_date_week(df,
                              'TPO_LA_Conf_Delivery_Date_POR',
                              'TPO_LA_Conf_Delivery_Date')
    df = fm.convert_date_month(df,
                               'TPO_LA_Conf_Delivery_Month_POR',
                               'TPO_LA_Conf_Delivery_Date_POR')
    df = fm.convert_month_num(df, 'MONTH_NUM_POR',
                              'TPO_LA_Conf_Delivery_Month_POR')
    df = fm.convert_iso(df, 'ISO_POR',
                        'TPO_LA_Conf_Delivery_Date_POR')

TPO_Canon_Final = fm.shipment_por_skus(TPO_Canon_Final,
                                       Canon_POR_grouped,
                                       False)

TPO_Cum_Canon = fm.shipment_por_skus(TPO_Cum_Canon,
                                     Canon_POR_grouped,
                                     True)

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

# %% FULL SHIPMENT DATA

TPO_Final = pd.concat([TPO_Ink_Final, TPO_Laser_Final, TPO_HPPS_Final, TPO_FXNWHL_Final,
                       TPO_Canon_Final], ignore_index=True)

# %% MONTH SHIPMENT VS TPO QTY
# =============================================================================
# FILTER POR SKUs AND ADD FAMILY
# =============================================================================
ink_added = fm.por_family(ink_df_cum, Ink_POR_grouped,
                          family_map, False)
laser_added = fm.por_family(laser_df_cum, Laser_POR_grouped,
                            family_map, False)
hpps_added = fm.por_family(hpps_df_cum, HPPS_POR_grouped,
                           hpps_family, True)
fxnwhl_added = fm.por_family(
    fxnwhl_df_cum, FXNWHL_POR_grouped, fxnwhl_family, True)

# MAKE A COPY FOR EXECUTIVE REPORT (INKJET)
# ADDING LA AND NA & CHINA
ink_added_exe = ink_added.copy()
ink_added_exe = fm.inkjet_region_mapping(ink_added_exe, 'Sub_Region',
                                         'REGION', 'NA')
ink_added_exe = fm.inkjet_region_mapping(ink_added_exe, 'Sub_Region',
                                         'REGION', 'LA')
ink_added_exe = fm.inkjet_sku_region_mapping(ink_added_exe,
                                             'TPO_Material',
                                             '#AB2',
                                             'REGION',
                                             'CHINA')


# =============================================================================
# GROUP INTO SHIPMENT AND PO
# =============================================================================
ink_po_df, ink_ship_df = fm.po_ship(ink_added)
# INKJET EXECUTIVE
ink_po_exe_df, ink_ship_exe_df = fm.po_ship(
    ink_added_exe)
laser_po_df, laser_ship_df = fm.po_ship(laser_added)
hpps_po_df, hpps_ship_df = fm.po_ship(hpps_added)
fxnwhl_po_df, fxnwhl_ship_df = fm.po_ship(fxnwhl_added)
canon_po_df, canon_ship_df = fm.po_ship(TPO_Cum_Canon)


po_list = [ink_po_df, ink_po_exe_df, laser_po_df,
           hpps_po_df, fxnwhl_po_df, canon_po_df]
ship_list = [ink_ship_df, ink_ship_exe_df, laser_ship_df,
             hpps_ship_df, fxnwhl_ship_df, canon_ship_df]

# =============================================================================
# CONVERT DATE TO MONDAY AND CHANGE TO MONTH
# =============================================================================
for po_data in po_list:
    po_data = fm.convert_date_week(po_data,
                                   'TPO_Requested_Delivery_Date',
                                   'TPO_Requested_Delivery_Date')
    po_data = fm.convert_date_month(po_data,
                                    'TPO_Requested_Delivery_Month_POR',
                                    'TPO_Requested_Delivery_Date')

for ship_data in ship_list:
    ship_data = fm.convert_date_week(ship_data,
                                     'TPO_LA_Conf_Delivery_Date',
                                     'TPO_LA_Conf_Delivery_Date')
    ship_data = fm.convert_date_month(ship_data,
                                      'TPO_LA_Conf_Delivery_Month_POR',
                                      'TPO_LA_Conf_Delivery_Date')


# GROUP BY MONTH AND WEEK


ink_month = fm.combine_po_ship(
    ink_po_df, ink_ship_df, today, 'Q-AUG')
# INKJET EXECUTIVE
ink_month_exe = fm.combine_po_ship(ink_po_exe_df, ink_ship_exe_df,
                                   today, 'Q-OCT')
laser_month = fm.combine_po_ship(
    laser_po_df, laser_ship_df, today, 'Q-AUG')
hpps_month = fm.combine_po_ship(
    hpps_po_df, hpps_ship_df, today, 'Q-AUG')
fxnwhl_month = fm.combine_po_ship(
    fxnwhl_po_df, fxnwhl_ship_df, today, 'Q-AUG')
canon_month = fm.combine_po_ship(
    canon_po_df, canon_ship_df, today, 'Q-AUG')
# Remove null MPA for CANON as no delivery
canon_month = canon_month.loc[~canon_month['MPA'].isnull()].copy()

# =============================================================================
# ADD CATEGORY FOR CANON, HPPS, FXNWHL
# =============================================================================
fxnwhl_df_cum = fxnwhl_df_cum.merge(fxnwhl_pc, how='left',
                                    on='TPO_Profit_Center')

fxnwhl_sku_cat = fm.unique_table(fxnwhl_df_cum, ['TPO_Material',
                                                 'CATEGORY'])
fxnwhl_sku_cat = fxnwhl_sku_cat.rename(columns={'TPO_Material': 'SKU'})

fxnwhl_month = fxnwhl_month.merge(fxnwhl_sku_cat, how='left', on='SKU')

hpps_sku_cat = fm.unique_table(hpps_df_cum, ['TPO_Material',
                                             'CATEGORY'])
hpps_sku_cat = hpps_sku_cat.rename(columns={'TPO_Material': 'SKU'})

hpps_month = hpps_month.merge(hpps_sku_cat, how='left', on='SKU')
canon_month['CATEGORY'] = 'CANON FFGI'
# FOR EXECUTIVE
ink_month_exe['TYPE'] = 'TPO_SHIP'

# =============================================================================
# CREATE COMBINED COLUMNS FOR RELATIONSHIP
# =============================================================================
laser_cat_list = [hpps_month, fxnwhl_month, canon_month]
for category in laser_cat_list:
    category = fm.combine_cols(category,
                               ['TPO_REQUESTED_DELIVERY_MONTH_STR',
                                'MPA',
                                'REGION',
                                'PLTFRM_NM',
                                'FAMILY', 'CATEGORY'],
                               'COMBINED')

ink_month = fm.combine_cols(ink_month,
                            ['TPO_REQUESTED_DELIVERY_MONTH_STR',
                             'MPA',
                             'REGION',
                             'PLTFRM_NM', 'FAMILY'],
                            'COMBINED')
laser_month = fm.combine_cols(laser_month,
                              ['TPO_REQUESTED_DELIVERY_MONTH_STR',
                               'MPA',
                               'REGION',
                               'PLTFRM_NM', 'FAMILY'],
                              'COMBINED')
ink_month_exe = fm.combine_cols(ink_month_exe,
                                ['MPA', 'REGION', 'PLTFRM_NM',
                                 'FAMILY', 'SKU', 'TYPE'],
                                'COMBINED')
laser_month = pd.concat(
    [laser_month, hpps_month, fxnwhl_month, canon_month], ignore_index=True)

# RECREATE COMBINED COLUMNS FOR LASER
laser_month = fm.combine_cols(laser_month,
                              ['TPO_REQUESTED_DELIVERY_MONTH_STR',
                               'MPA', 'REGION', 'PLTFRM_NM', 'FAMILY',
                               'CATEGORY'],
                              'COMBINED')

# =============================================================================
# CREATE TABLES FOR RELATIONSHIP (SLICER)
# =============================================================================
# INKJET SAIL DASHBOARD
unique_month = fm.create_unique_col(ink_month,
                                    'MONTH',
                                    'TPO_REQUESTED_DELIVERY_MONTH_POR').sort_values(by='MONTH')

month_slice = fm.unique_table(ink_month,
                              ['TPO_REQUESTED_DELIVERY_MONTH_POR',
                               'TPO_REQUESTED_DELIVERY_DATE']). \
    sort_values(by='TPO_REQUESTED_DELIVERY_DATE')

q_slice = fm.unique_table(ink_month,
                          ['TPO_REQUESTED_DELIVERY_MONTH_POR',
                           'QUARTER_NAME']). \
    sort_values(by='TPO_REQUESTED_DELIVERY_MONTH_POR')

fy_slice = fm.unique_table(ink_month,
                           ['TPO_REQUESTED_DELIVERY_MONTH_POR',
                            'FY_NAME']). \
    sort_values(by='TPO_REQUESTED_DELIVERY_MONTH_POR')

ink_mpa_slice_df = fm.unique_table(ink_month, ['MPA', 'COMBINED'])

ink_region_slice_df = fm.unique_table(
    ink_month, ['REGION', 'COMBINED'])

ink_plt_slice_df = fm.unique_table(
    ink_month, ['PLTFRM_NM', 'COMBINED'])

ink_fam_slice_df = fm.unique_table(ink_month, ['FAMILY', 'COMBINED'])

# INKJET EXECUTIVE DASHBOARD
unique_month_exe = fm.create_unique_col(ink_month_exe,
                                        'MONTH',
                                        'TPO_REQUESTED_DELIVERY_MONTH_POR').sort_values(by='MONTH')

month_slice_exe = fm.unique_table(ink_month_exe,
                                  ['TPO_REQUESTED_DELIVERY_MONTH_POR',
                                   'TPO_REQUESTED_DELIVERY_DATE']). \
    sort_values(by='TPO_REQUESTED_DELIVERY_DATE')

q_slice_exe = fm.unique_table(ink_month_exe,
                              ['TPO_REQUESTED_DELIVERY_MONTH_POR',
                               'QUARTER_NAME']). \
    sort_values(by='TPO_REQUESTED_DELIVERY_MONTH_POR')

fy_slice_exe = fm.unique_table(ink_month_exe,
                               ['TPO_REQUESTED_DELIVERY_MONTH_POR',
                                'FY_NAME']). \
    sort_values(by='TPO_REQUESTED_DELIVERY_MONTH_POR')

ink_mpa_slice_exe_df = fm.unique_table(
    ink_month_exe, ['MPA', 'COMBINED'])

ink_region_slice_exe_df = fm.unique_table(
    ink_month_exe, ['REGION', 'COMBINED'])

ink_plt_slice_exe_df = fm.unique_table(
    ink_month_exe, ['PLTFRM_NM', 'COMBINED'])

ink_fam_slice_exe_df = fm.unique_table(
    ink_month_exe, ['FAMILY', 'COMBINED'])

ink_sku_slice_exe_df = fm.unique_table(
    ink_month_exe, ['SKU', 'COMBINED'])

# LASER SAIL DASHBOARD
laser_unique_month = fm.create_unique_col(laser_month,
                                          'MONTH',
                                          'TPO_REQUESTED_DELIVERY_MONTH_POR').sort_values(by='MONTH')

laser_month_slice = fm.unique_table(laser_month,
                                    ['TPO_REQUESTED_DELIVERY_MONTH_POR',
                                     'TPO_REQUESTED_DELIVERY_DATE']). \
    sort_values(by='TPO_REQUESTED_DELIVERY_DATE')

laser_q_slice = fm.unique_table(laser_month,
                                ['TPO_REQUESTED_DELIVERY_MONTH_POR',
                                 'QUARTER_NAME']). \
    sort_values(by='TPO_REQUESTED_DELIVERY_MONTH_POR')

laser_fy_slice = fm.unique_table(laser_month,
                                 ['TPO_REQUESTED_DELIVERY_MONTH_POR',
                                  'FY_NAME']). \
    sort_values(by='TPO_REQUESTED_DELIVERY_MONTH_POR')

laser_mpa_slice_df = fm.unique_table(
    laser_month, ['MPA', 'COMBINED'])

laser_region_slice_df = fm.unique_table(
    laser_month, ['REGION', 'COMBINED'])

laser_plt_slice_df = fm.unique_table(
    laser_month, ['PLTFRM_NM', 'COMBINED'])

laser_fam_slice_df = fm.unique_table(
    laser_month, ['FAMILY', 'COMBINED'])

laser_cat_slice_df = fm.unique_table(
    laser_month, ['CATEGORY', 'COMBINED'])

# CREATE NAME FOR CUM CHART
fy_slice['FY_CUM_NAME'] = 'Cum to Date (' + fy_slice['FY_NAME'].str[6:8] + \
    fy_slice['FY_NAME'].str[-2:] + ')'

por_month_unique = fm.create_unique_col(ink_month,
                                        'TPO_REQUESTED_DELIVERY_MONTH_POR',
                                        'TPO_REQUESTED_DELIVERY_MONTH_POR')

quarter_unique = fm.create_unique_col(ink_month,
                                      'QUARTER_NAME',
                                      'QUARTER_NAME')

fy_unique = fm.create_unique_col(ink_month,
                                 'FY_NAME',
                                 'FY_NAME')

# INKJET EXECUTIVE
por_month_unique_exe = fm.create_unique_col(ink_month_exe,
                                            'TPO_REQUESTED_DELIVERY_MONTH_POR',
                                            'TPO_REQUESTED_DELIVERY_MONTH_POR')

quarter_unique_exe = fm.create_unique_col(ink_month_exe,
                                          'QUARTER_NAME',
                                          'QUARTER_NAME')

fy_unique_exe = fm.create_unique_col(ink_month_exe,
                                     'FY_NAME',
                                     'FY_NAME')


# %% SHIPMENT WITH POR

inkjet_b_list = ['NKG TH', 'NKG YY', 'FXN CQ', 'JWH INK', 'FXN WH INK']

ink_build_df = pd.DataFrame()
for mpa in inkjet_b_list:
    ink_b_get = read_build(mpa, True)
    ink_build_df = pd.concat([ink_build_df, ink_b_get], ignore_index=True)

laser_b_list = ['JABIL CUU', 'JWH LASER', 'FXN CZ']

laser_build_df = pd.DataFrame()
for mpa in laser_b_list:
    laser_b_get = read_build(mpa, False)
    laser_build_df = pd.concat(
        [laser_build_df, laser_b_get], ignore_index=True)
# FXN WH LASER (HW)
fxnwhl_build_df = read_build('FXN WH LASER', False)

# CANON
canon_jp_build = read_build('CANON\CANON FFGI\JP', False)
canon_ph_build = read_build('CANON\CANON FFGI\PH', False)
canon_vn_build = read_build('CANON\CANON FFGI\VN', False)
canon_zs_build = read_build('CANON\CANON FFGI\ZS', False)


# REPLACE INK BUILD PLAN NAME
ink_build_df['LOC_FROM_NM'] = ink_build_df['LOC_FROM_NM'].map(ink_por_naming)
# REPLACE LASER BUILD PLAN NAME
laser_build_df['LOC_FROM_NM'] = laser_build_df['LOC_FROM_NM'].map(
    laser_por_naming)
# REPLACE LASER BUILD PLAN NAME
fxnwhl_build_df['LOC_FROM_NM'] = fxnwhl_build_df['LOC_FROM_NM'].map(
    laser_por_naming)

# REPLACE CANON BUILD PLAN NAME
canon_jp_build['LOC_FROM_NM'] = 'Canon JP'
canon_ph_build['LOC_FROM_NM'] = 'Canon PH'
canon_vn_build['LOC_FROM_NM'] = 'Canon VN'
canon_zs_build['LOC_FROM_NM'] = 'Canon CN, Zhongshan'
canon_build_df = pd.concat([canon_jp_build, canon_ph_build,
                            canon_vn_build, canon_zs_build], ignore_index=True)

WR_INK = fm.combine_ship_build(
    ink_build_df, TPO_Ink_Final, today, current_mon_date,
    region_map, sub_region_map, mpa_map, hpps_family, family_map,
    False, None, False, False, False)
# INKJET EXECUTIVE
WR_INK_EXE = fm.combine_ship_build(
    ink_build_df, TPO_Ink_Final, today, current_mon_date,
    region_map, sub_region_map, mpa_map, hpps_family, family_map,
    False, None, False, True, False)
WR_LASER = fm.combine_ship_build(
    laser_build_df, TPO_Laser_Final, today, current_mon_date,
    region_map, sub_region_map, mpa_map, hpps_family, family_map,
    False, None, False, False, False)

WR_FXNWHL = fm.combine_ship_build(
    fxnwhl_build_df, TPO_FXNWHL_Final, today, current_mon_date,
    region_map, sub_region_map, mpa_map, hpps_family, family_map,
    True, 'Laser FXNWH HW', False, False, False)
WR_CANON = fm.combine_ship_build(
    canon_build_df, TPO_Canon_Final, today, current_mon_date,
    region_map, sub_region_map, mpa_map, hpps_family, family_map,
    False, None, True, False, False)
# LASER EXECUTIVE
WR_LASER_EXE = fm.combine_ship_build(
    laser_build_df, TPO_Laser_Final, today, current_mon_date,
    region_map, sub_region_map, mpa_map, hpps_family, family_map,
    False, None, False, False, True)

WR_FXNWHL_EXE = fm.combine_ship_build(
    fxnwhl_build_df, TPO_FXNWHL_Final, today, current_mon_date,
    region_map, sub_region_map, mpa_map, hpps_family, family_map,
    True, 'Laser FXNWH HW', False, False, True)
WR_CANON_EXE = fm.combine_ship_build(
    canon_build_df, TPO_Canon_Final, today, current_mon_date,
    region_map, sub_region_map, mpa_map, hpps_family, family_map,
    False, None, True, False, True)

# REMOVE WR_HPPS
WR_FINAL = pd.concat([WR_INK, WR_LASER, WR_FXNWHL,
                      WR_CANON], ignore_index=True)

# LASER EXECUTIVE
WR_LASER_FINAL = pd.concat([WR_LASER_EXE, WR_FXNWHL_EXE, WR_CANON_EXE],
                           ignore_index=True)
WR_LASER_FINAL.columns = WR_LASER_FINAL.columns.str.upper()
WR_LASER_FINAL.loc[WR_LASER_FINAL['FAMILY'].isnull(), 'FAMILY'] = ''
WR_LASER_FINAL = fm.convert_fy(WR_LASER_FINAL, 'QUARTER',
                               'TPO_REQUESTED_DELIVERY_MONTH_POR',
                               'Q-OCT', 'QUARTER_YEAR')

WR_LASER_FINAL['QUARTER_NAME'] = WR_LASER_FINAL['QUARTER'].astype(str)
WR_LASER_FINAL['QUARTER_NAME'] = 'F' + WR_LASER_FINAL['QUARTER_NAME'].str[-2:] + \
    """'""" + WR_LASER_FINAL['QUARTER_NAME'].str[2:4]
WR_LASER_FINAL['FY_NAME'] = 'ALL - FY' + WR_LASER_FINAL['QUARTER_YEAR']

# =============================================================================
# FOR INKJET EXECUTIVE
# =============================================================================
WR_INK_EXE.columns = WR_INK_EXE.columns.str.upper()
WR_INK_EXE = WR_INK_EXE.rename(
    columns={'TPO_LA_CONF_DELIVERY_DATE_POR': 'TPO_REQUESTED_DELIVERY_MONTH_POR',
             'PLATFORM': 'PLTFRM_NM'})

# Try for dynamic
WR_INK_EXE = fm.convert_fy(WR_INK_EXE, 'QUARTER',
                           'TPO_REQUESTED_DELIVERY_MONTH_POR',
                           'Q-OCT', 'QUARTER_YEAR')

WR_INK_EXE['QUARTER_NAME'] = WR_INK_EXE['QUARTER'].astype(str)
WR_INK_EXE['QUARTER_NAME'] = 'F' + WR_INK_EXE['QUARTER_NAME'].str[-2:] + \
    """'""" + WR_INK_EXE['QUARTER_NAME'].str[2:4]
WR_INK_EXE['FY_NAME'] = 'ALL - FY' + WR_INK_EXE['QUARTER_YEAR']
# COMBINED PORWSHIP FOR INK & LASER FOR EXECUTIVE
COMBINED_PORWSHIP = pd.concat([WR_INK_EXE, WR_LASER_FINAL], ignore_index=True)
COMBINED_PORWSHIP = fm.combine_cols(COMBINED_PORWSHIP,
                                    ['BU', 'MPA', 'REGION', 'FAMILY', 'PLTFRM_NM'],
                                    'COMBINED')

WR_INK_EXE['TYPE'] = 'POR'
cols = ['MPA', 'REGION', 'PLTFRM_NM', 'FAMILY', 'SKU', 'TYPE']
WR_INK_EXE['COMBINED'] = WR_INK_EXE[cols] \
    .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)

wr_mpa_slice_df = fm.unique_table(WR_INK_EXE, ['MPA', 'COMBINED'])
wr_region_slice_df = fm.unique_table(WR_INK_EXE, ['REGION', 'COMBINED'])
wr_plt_slice_df = fm.unique_table(WR_INK_EXE, ['PLTFRM_NM', 'COMBINED'])
wr_fam_slice_df = fm.unique_table(WR_INK_EXE, ['FAMILY', 'COMBINED'])
wr_sku_slice_df = fm.unique_table(WR_INK_EXE, ['SKU', 'COMBINED'])

ink_mpa_slice_exe_df = pd.concat([ink_mpa_slice_exe_df, wr_mpa_slice_df])
ink_region_slice_exe_df = pd.concat(
    [ink_region_slice_exe_df, wr_region_slice_df])
ink_plt_slice_exe_df = pd.concat([ink_plt_slice_exe_df, wr_plt_slice_df])
ink_fam_slice_exe_df = pd.concat([ink_fam_slice_exe_df, wr_fam_slice_df])
ink_sku_slice_exe_df = pd.concat([ink_sku_slice_exe_df, wr_sku_slice_df])

ink_cycle = fm.unique_table(ink_build_df, ['CYCLE_WK_NM', 'LOC_FROM_NM'])
ink_cycle['BU'] = 'Inkjet'
laser_cycle = fm.unique_table(laser_build_df, ['CYCLE_WK_NM', 'LOC_FROM_NM'])
laser_cycle['BU'] = 'Laser'
fxnwhl_cycle = fm.unique_table(fxnwhl_build_df, ['CYCLE_WK_NM', 'LOC_FROM_NM'])
fxnwhl_cycle['BU'] = 'Laser'
canon_cycle = fm.unique_table(canon_build_df, ['CYCLE_WK_NM', 'LOC_FROM_NM'])
canon_cycle['BU'] = 'Laser'
cycle_ref_df = pd.concat([ink_cycle, laser_cycle, fxnwhl_cycle, canon_cycle],
                         ignore_index=True)
cycle_ref_df['REF_POR_NAME'] = 'Reference to ' + \
    cycle_ref_df['CYCLE_WK_NM'] + ' POR'

cycle_ref_df = cycle_ref_df.rename(columns={'LOC_FROM_NM': 'MPa'})
WR_bu_df = fm.unique_table(cycle_ref_df, ['BU'])

# %% SHIPMENT VS POR

path_dir = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                        'PARQUET\Shipment')
os.chdir(path_dir)
past_ink_ship = pd.read_parquet('April to July 2021.parquet')


ink_past_build_df = pd.DataFrame()
for mpa in inkjet_b_list:
    ink_b_get = read_past_build(mpa, current_mon_date)
    ink_past_build_df = pd.concat(
        [ink_past_build_df, ink_b_get], ignore_index=True)

# REPLACE INK BUILD PLAN NAME
ink_past_build_df['LOC_FROM_NM'] = ink_past_build_df['LOC_FROM_NM'].map(
    ink_por_naming)
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
ink_ship_group = pd.concat([past_ink_ship, ink_ship_group], ignore_index=False)

ink_por_group = ink_past_build_df.groupby(['CYCLE_WK_NM', 'LOC_FROM_NM',
                                           'BUS_UNIT_NM', 'PLTFRM_NM',
                                           'REGION',
                                           'PART_NR', 'CAL_WK_DT'])['QTY'].sum().reset_index()

por_cycle_unique = fm.unique_table(ink_por_group, ['CYCLE_WK_NM'])
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
ink_por_ship_df = ink_por_ship_df.rename(
    columns={'BUS_UNIT_NM_y': 'BUS_UNIT_NM'})

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
ink_por_df = ink_por_df.merge(Ink_POR_grouped, how='left', on=[
                              'LOC_FROM_NM', 'PART_NR'])
ink_por_df.loc[
    (ink_por_df['PLTFRM_NM_x'] != ink_por_df['PLTFRM_NM_y']) &
    (ink_por_df['PLTFRM_NM_y'].notnull()), 'PLTFRM_NM_x'] = ink_por_df['PLTFRM_NM_y']

ink_por_df = ink_por_df.drop(columns=['PLTFRM_NM_y',
                                      'BUS_UNIT_NM'])
ink_por_df = ink_por_df.rename(columns={'LOC_FROM_NM': 'MPa',
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

# %% TV & EXECUTIVE REPORT

month_df = fm.create_month_df('Q-OCT')
current_fy = month_df.loc[
    month_df['Month'] == current_mon_month]['Year'].unique().item()

current_fy_text = 'FY' + current_fy

title_df = pd.DataFrame({'current_fy': [current_fy_text]})
ink_latest_cycle = cycle_ref_df.loc[
    cycle_ref_df['BU'] == 'Inkjet']['CYCLE_WK_NM'].unique().item()
laser_latest_cycle = cycle_ref_df.loc[
    cycle_ref_df['BU'] == 'Laser']['CYCLE_WK_NM'].unique().item()
title_df['Latest POR'] = ink_latest_cycle + ' POR'
title_df['BU'] = 'Inkjet'
laser_row = pd.DataFrame([{'current_fy': current_fy_text,
                           'Latest POR': laser_latest_cycle + ' POR',
                           'BU': 'Laser'}])
title_df = pd.concat([title_df, laser_row], ignore_index=True)

ink_month_exe.loc[ink_month_exe['FAMILY'].isnull(), 'FAMILY'] = ''
# COPY LASER MONTH EXECUTIVE
laser_month_exe = laser_month.copy()
laser_month_exe.loc[laser_month_exe['FAMILY'].isnull(), 'FAMILY'] = ''
laser_month_exe.loc[laser_month_exe['CATEGORY'].isnull(), 'CATEGORY'] = ''

# CONVERT LASER DATA TO FINANCIAL YEAR
laser_tv = fm.convert_fy(laser_month_exe, 'QUARTER',
                         'TPO_REQUESTED_DELIVERY_MONTH_POR',
                         'Q-OCT', 'QUARTER_YEAR')

laser_tv = fm.quarter_year_name(laser_tv, 'QUARTER', 'QUARTER_YEAR',
                                'QUARTER_NAME', 'FY_NAME')
# GET CURRENT FY DATA
laser_tv = laser_tv.loc[laser_tv['QUARTER_YEAR'] == current_fy].copy()
# REMOVE ACC AND TONER FOR LASER
laser_tv = laser_tv.loc[
    (laser_tv['CATEGORY'] != 'Laser FXNWH ACC') &
    (laser_tv['CATEGORY'] != 'Laser FXNWH TONER')].copy()

ink_tv = ink_month_exe.loc[ink_month_exe['QUARTER_YEAR'] == current_fy].copy()

# DROP COLUMNS NOT IN USE
laser_tv = laser_tv.drop(columns=['CATEGORY'])
ink_tv = ink_tv.drop(columns=['TYPE'])

# =============================================================================
# CREATE RELATIONSHIP
# =============================================================================
laser_tv = fm.combine_cols(laser_tv, ['BU', 'MPA', 'REGION', 'FAMILY', 'PLTFRM_NM'],
                           'COMBINED')
ink_tv = fm.combine_cols(ink_tv, ['BU', 'MPA', 'REGION', 'FAMILY', 'PLTFRM_NM'],
                         'COMBINED')
all_tv = pd.concat([ink_tv, laser_tv], ignore_index=True)

all_tv_rel = fm.unique_table(all_tv,
                             ['BU', 'MPA', 'REGION', 'FAMILY', 'PLTFRM_NM', 'COMBINED'])
COMBINED_PORWSHIP_rel = fm.unique_table(all_tv,
                                        ['BU', 'MPA', 'REGION', 'FAMILY', 'PLTFRM_NM', 'COMBINED'])
Combined_Relationship = pd.concat([all_tv_rel, COMBINED_PORWSHIP_rel],
                                  ignore_index=True)
Combined_Relationship = fm.unique_table(Combined_Relationship,
                                        ['BU', 'MPA', 'REGION', 'FAMILY', 'PLTFRM_NM', 'COMBINED'])
# =============================================================================
# POR ROLLUP
# =============================================================================
ink_build_dir = [r'INKJET WEEKLY BUILD\FXN CQ',
                 r'INKJET WEEKLY BUILD\FXN WH INK',
                 r'INKJET WEEKLY BUILD\JWH INK',
                 r'INKJET WEEKLY BUILD\NKG TH',
                 r'INKJET WEEKLY BUILD\NKG YY']

laser_build_dir = [r'FXN CZ', r'FXN WH LASER', r'JABIL CUU', r'JWH LASER']

# INKJET
ink_por_df = pd.DataFrame()
for folder in ink_build_dir:
    read_data = read_week_build(user, folder)
    ink_por_df = pd.concat([ink_por_df, read_data], ignore_index=True)

# LASER
laser_por_df = pd.DataFrame()
for folder in laser_build_dir:
    read_data = read_week_build(user, folder)
    laser_por_df = pd.concat([laser_por_df, read_data], ignore_index=True)

# CANON
canon_jp_build = read_week_build(user, r'CANON\CANON FFGI\JP')
canon_ph_build = read_week_build(user, r'CANON\CANON FFGI\PH')
canon_vn_build = read_week_build(user, r'CANON\CANON FFGI\VN')
canon_zs_build = read_week_build(user, r'CANON\CANON FFGI\ZS')

# REPLACE CANON BUILD PLAN NAME
canon_jp_build['LOC_FROM_NM'] = 'Canon JP'
canon_ph_build['LOC_FROM_NM'] = 'Canon PH'
canon_vn_build['LOC_FROM_NM'] = 'Canon VN'
canon_zs_build['LOC_FROM_NM'] = 'Canon CN, Zhongshan'

canon_por_df = pd.concat([canon_jp_build, canon_ph_build,
                          canon_vn_build, canon_zs_build], ignore_index=True)

date_df = fm.create_date_query()

ink_query_df = fm.query_por(ink_por_df, date_df)
laser_query_df = fm.query_por(laser_por_df, date_df)
canon_query_df = fm.query_por(canon_por_df, date_df)
latest_por_date = ink_query_df['CAL_WK_DT'].max()

# MAP INK MPA
ink_query_df['LOC_FROM_NM'] = ink_query_df['LOC_FROM_NM'].map(
    fm.ink_por_naming)
# MAP LASER MPA
laser_query_df['LOC_FROM_NM'] = laser_query_df['LOC_FROM_NM'].map(
    fm.laser_por_naming)

# REGION MAPPING
region_map = region_map.merge(sub_region_map, how='left', on='DC_IC_PO_Plant')
ink_region_map = region_map.copy()
ink_region_map.loc[
    (ink_region_map['Sub_Region'] == 'NA') |
    (ink_region_map['Sub_Region'] == 'LA'), 'REGION'] = \
    ink_region_map['Sub_Region']
ink_region_map = ink_region_map.drop(columns=['Sub_Region'])

ink_query_df = fm.map_por_region(ink_query_df, ink_region_map)
laser_query_df = fm.map_por_region(laser_query_df, region_map)
canon_query_df = fm.map_por_region(canon_query_df, region_map)

ink_query_df = fm.inkjet_sku_region_mapping(
    ink_query_df, 'PART_NR', '#AB2', 'REGION', 'CHINA')

ink_query_df = fm.group_por(ink_query_df)
laser_query_df = fm.group_por(laser_query_df)
canon_query_df = fm.group_por(canon_query_df)

# COMBINE ALL PORS
por_query_df = pd.concat([ink_query_df, laser_query_df, canon_query_df],
                         ignore_index=True)

all_tv_grouped = all_tv.groupby(
    ['BU',
     'MPA', 'TPO_REQUESTED_DELIVERY_DATE',
     'PLTFRM_NM', 'BUS_UNIT_NM',
     'REGION', 'SKU'], dropna=False)[['TPO_QTY', 'TPO_LA_QTY']].sum().reset_index()
# =============================================================================
# COMBINE POR AND SHIP
# =============================================================================
por_ship_df = por_query_df.merge(
    all_tv_grouped,
    how='outer',
    left_on=['LOC_FROM_NM', 'CAL_WK_DT', 'REGION', 'PART_NR'],
    right_on=['MPA', 'TPO_REQUESTED_DELIVERY_DATE', 'REGION', 'SKU'])

# CLEANUP BU COLUMN
por_ship_df.loc[por_ship_df['LOC_FROM_NM'].str.contains('INK', na=False),
                'BU'] = 'Inkjet'
por_ship_df.loc[por_ship_df['LOC_FROM_NM'].str.contains('Laser', na=False),
                'BU'] = 'Laser'
por_ship_df.loc[por_ship_df['LOC_FROM_NM'].str.contains('Canon', na=False),
                'BU'] = 'Laser'

por_ship_df.loc[por_ship_df.duplicated(['CAL_WK_DT', 'LOC_FROM_NM', 'REGION',
                                        'FAMILY_NM', 'PLTFRM_NM_x', 'BUS_UNIT_NM_x',
                                        'PART_NR', 'QTY']),
                ['CAL_WK_DT', 'LOC_FROM_NM',
                 'FAMILY_NM', 'PLTFRM_NM_x', 'BUS_UNIT_NM_x',
                 'PART_NR', 'QTY']] = np.nan

# =============================================================================
# THOSE WITH POR BUT NO SHIPMENT OVERWRITE DATA
# =============================================================================
por_ship_df.loc[por_ship_df['TPO_REQUESTED_DELIVERY_DATE'].isnull(),
                'TPO_REQUESTED_DELIVERY_DATE'] = \
    por_ship_df['CAL_WK_DT']

por_ship_df.loc[por_ship_df['MPA'].isnull(),
                'MPA'] = \
    por_ship_df['LOC_FROM_NM']

por_ship_df.loc[por_ship_df['PLTFRM_NM_y'].isnull(),
                'PLTFRM_NM_y'] = \
    por_ship_df['PLTFRM_NM_x']

por_ship_df.loc[por_ship_df['BUS_UNIT_NM_y'].isnull(),
                'BUS_UNIT_NM_y'] = \
    por_ship_df['BUS_UNIT_NM_x']

por_ship_df.loc[por_ship_df['SKU'].isnull(),
                'SKU'] = \
    por_ship_df['PART_NR']

# ONLY GET DATA TILL LATEST POR DATA
por_ship_df = por_ship_df.loc[
    por_ship_df['TPO_REQUESTED_DELIVERY_DATE'] <= current_mon_date].copy()

# FILL EMPTY QUANTITY WITH 0
por_ship_df[['QTY', 'TPO_QTY', 'TPO_LA_QTY']] = \
    por_ship_df[['QTY', 'TPO_QTY', 'TPO_LA_QTY']].fillna(0)

# DROP IRRELEVANT COLUMNS
por_ship_df = por_ship_df.drop(columns=['CAL_WK_DT', 'LOC_FROM_NM',
                                        'FAMILY_NM', 'PLTFRM_NM_x',
                                        'BUS_UNIT_NM_x', 'PART_NR'])

# STRIP _y AT THE RIGHT FOR COLUMN NAMING
por_ship_df.columns = por_ship_df.columns.str.rstrip('_y')

# COMBINE WITH FAMILY MAPPING
por_ship_df = por_ship_df.merge(
    family_map, how='left', on=['MPA', 'PLTFRM_NM'])

por_ship_df = fm.convert_date_month(por_ship_df,
                                    'TPO_REQUESTED_DELIVERY_MONTH_POR',
                                    'TPO_REQUESTED_DELIVERY_DATE')

por_ship_df = fm.convert_fy(por_ship_df,
                            'TPO_REQUESTED_DELIVERY_Q_POR',
                            'TPO_REQUESTED_DELIVERY_MONTH_POR',
                            'Q-OCT', 'FY_YEAR')
# PLANNING CATEGORIZATION (OEM PRODUCTS DOES NOT HAVE A CATEGORIZATION)
category_path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR',
                             'Data\PLANNING CATEGORIZATION')
os.chdir(category_path)
plan_cat_df = pd.read_csv('PLANNING CATEGORIZATION.csv',
                          usecols=[0, 1, 2, 3])

por_ship_df = por_ship_df.merge(plan_cat_df,
                                how='left',
                                left_on='SKU',
                                right_on='PART_NR')
# =============================================================================
# OVERWRITE CATEGORIZATION FOR INKJET
# =============================================================================
por_ship_df.loc[
    (por_ship_df['CAT_SUB'].str.contains('Oj pro', na=False, case=False)) &
    (por_ship_df['BU'] == 'Inkjet'),
    'CAT_SUB'] = 'OJ PRO'

# OVERWRITE FOR RUBY TOPAZ
por_ship_df.loc[
    (por_ship_df['PLTFRM_NM'].str.contains('ruby|topaz', na=False, case=False)) &
    (por_ship_df['BU'] == 'Inkjet'),
    'CAT_SUB'] = 'MOBILE'

# OVERWRITE FOR OEM
por_ship_df.loc[
    (por_ship_df['PLTFRM_NM'].str.contains('OEM', na=False, case=False)) &
    (por_ship_df['BU'] == 'Inkjet'),
    'CAT_SUB'] = 'OEM'

por_ship_df.loc[
    (por_ship_df['PLTFRM_NM'].str.contains('OEM', na=False, case=False)) &
    (por_ship_df['BU'] == 'Inkjet'),
    'CAT_NM'] = 'OEM'

# =============================================================================
# REMOVE THOSE WITH NO CATEGORIZATION AS THEY ARE ACCESSORIES AND OEM
# =============================================================================
por_ship_df = por_ship_df.loc[por_ship_df['CAT_SUB'].notnull()].copy()

# =============================================================================
# REMOVE CAT_SUB WITH EOL
# =============================================================================
por_ship_df = por_ship_df.loc[~(
    por_ship_df['CAT_SUB'].str.contains('EOL'))].copy()

# =============================================================================
# TV FAMILY MAPPING TO MAIN DATA por_ship_df
# =============================================================================
base = r'^{}'
expr = '(?:\s|^){}(?:\s|$)'

for bu, plt, fam in zip(tv_family_map['BU'], tv_family_map['PLTFRM_NM'],
                        tv_family_map['TV_FAMILY']):
    por_ship_df.loc[
        (por_ship_df['BU'] == bu) &
        (por_ship_df['PLTFRM_NM'].str.contains(
            base.format(''.join(expr.format(plt))))),
        'TV_FAMILY'] = fam


# =============================================================================
# REMOVE PLATFORM ACCESSORIES, AV- AND UNKNOWN FROM DATA
# =============================================================================
por_ship_df = por_ship_df.loc[
    ~por_ship_df['PLTFRM_NM'].str.contains('ACCESSOR|AV-|UNKNOWN')].copy()

# =============================================================================
# OVERWRITE CANON FACTORIES TO OVERALL CANON
# =============================================================================

# DROP PART_NR COLUMN
por_ship_df = por_ship_df.drop(columns=['PART_NR'])

# OUTPUT FIRST FOR MAP AS OVERWRITE WILL DELETE COORDINATES
output_csv('Executive', por_ship_df, 'POR_SHIP_TPO')
output_pq('Executive', por_ship_df, 'POR_SHIP_TPO')

por_ship_df['MPA'] = por_ship_df['MPA'].replace(fm.canon_short_naming)

# =============================================================================
# ADD COMBINED COLUMN FOR RELATIONSHIP
# =============================================================================
por_ship_df = fm.combine_cols(por_ship_df,
                              ['BU',
                               'BUS_UNIT_NM',
                               'CAT_NM',
                               'CAT_SUB', 'MPA', 'REGION',
                               'TV_FAMILY', 'PLTFRM_NM', 'SKU'],
                              'COMBINED')
# =============================================================================
# MTD DATA
# =============================================================================
mtd_por_ship_df = por_ship_df.loc[
    por_ship_df['TPO_REQUESTED_DELIVERY_MONTH_POR'] == current_mon_month].copy()

# INKJET
mtd_ink_por_ship_df = mtd_por_ship_df.loc[
    mtd_por_ship_df['BU'] == 'Inkjet'].copy()
# LASER
mtd_laser_por_ship_df = mtd_por_ship_df.loc[
    mtd_por_ship_df['BU'] == 'Laser'].copy()

# =============================================================================
# QTD DATA
# =============================================================================
# Get Current Quarter
current_quarter = month_df.loc[
    month_df['Month'] == current_mon_month]['Quarter'].unique().item()

qtd_por_ship_df = por_ship_df.loc[
    por_ship_df['TPO_REQUESTED_DELIVERY_Q_POR'] == current_quarter].copy()

# =============================================================================
# FUNCTION TO GROUP DATA AND CALCULATE SHIPMENT
# =============================================================================
ink_cat_df_grouped = fm.tv_fam_group(mtd_ink_por_ship_df)
laser_cat_df_grouped = fm.tv_fam_group(mtd_laser_por_ship_df)

# =============================================================================
# FACTORY ALERT (CURRENT MONTH) LASER
# =============================================================================
laser_alert = laser_cat_df_grouped.groupby(
    ['MPA', 'TV_FAMILY'])[['TPO_QTY', 'TPO_LA_QTY']].sum().reset_index()
laser_alert = fm.tv_pct(laser_alert)
# GET ONLY THOSE FAMILY THAT NEEDS ALERT
laser_alert = laser_alert.loc[laser_alert['Meet Target'] == 2].copy()
laser_alert['MPA'] = laser_alert['MPA'].replace(fm.laser_short_naming)
# GROUP BY FAMILY AND GIVE ALERT
alert_grouped = laser_alert.groupby(
    'TV_FAMILY')['MPA'].apply(lambda x: ', '.join(x)).reset_index() \
    .rename(columns={'MPA': 'MPA_AFFECTED'})
laser_cat_df_grouped = laser_cat_df_grouped.merge(
    alert_grouped, how='left', on='TV_FAMILY')

# %% PO THAT ARE OPEN AND NOT DONE DEAL
ink_shipped = fm.remain_po(ink_po_exe_df, ink_ship_exe_df, current_mon_month)
laser_shipped = fm.remain_po(laser_po_df, laser_ship_df, current_mon_month)
fxnwhl_shipped = fm.remain_po(fxnwhl_po_df, fxnwhl_ship_df, current_mon_month)
# ADD CATEGORY AND GET HW FOR FXNWHL
fxnwhl_shipped = fxnwhl_shipped.merge(fxnwhl_sku_cat, how='left', on='SKU')
fxnwhl_shipped = fxnwhl_shipped.loc[
    fxnwhl_shipped['CATEGORY'] == 'Laser FXNWH HW'].copy()
canon_shipped = fm.remain_po(canon_po_df, canon_ship_df, current_mon_month)
# Overwrite CANON MPA
canon_shipped['MPA'] = 'Canon'
remain_po_df = pd.concat([ink_shipped, laser_shipped, fxnwhl_shipped,
                          canon_shipped], ignore_index=True)

remain_po_df = remain_po_df.merge(plan_cat_df,
                                  how='left',
                                  left_on='SKU',
                                  right_on='PART_NR')

# =============================================================================
# OVERWRITE CATEGORIZATION FOR INKJET
# =============================================================================
remain_po_df.loc[
    (remain_po_df['CAT_SUB'].str.contains('Oj pro', na=False, case=False)) &
    (remain_po_df['BU'] == 'Inkjet'),
    'CAT_SUB'] = 'OJ PRO'

# OVERWRITE FOR RUBY TOPAZ
remain_po_df.loc[
    (remain_po_df['PLTFRM_NM'].str.contains('ruby|topaz', na=False, case=False)) &
    (remain_po_df['BU'] == 'Inkjet'),
    'CAT_SUB'] = 'MOBILE'

# OVERWRITE FOR OEM
remain_po_df.loc[
    (remain_po_df['PLTFRM_NM'].str.contains('OEM', na=False, case=False)) &
    (remain_po_df['BU'] == 'Inkjet'),
    'CAT_SUB'] = 'OEM'

remain_po_df.loc[
    (remain_po_df['PLTFRM_NM'].str.contains('OEM', na=False, case=False)) &
    (remain_po_df['BU'] == 'Inkjet'),
    'CAT_NM'] = 'OEM'

# =============================================================================
# REMOVE THOSE WITH NO CATEGORIZATION AS THEY ARE ACCESSORIES AND OEM
# =============================================================================
remain_po_df = remain_po_df.loc[remain_po_df['CAT_SUB'].notnull()].copy()

# =============================================================================
# REMOVE CAT_SUB WITH EOL
# =============================================================================
remain_po_df = remain_po_df.loc[~(
    remain_po_df['CAT_SUB'].str.contains('EOL'))].copy()

# ADD TV FAMILY MAPPING
for bu, plt, fam in zip(tv_family_map['BU'], tv_family_map['PLTFRM_NM'],
                        tv_family_map['TV_FAMILY']):
    remain_po_df.loc[
        (remain_po_df['BU'] == bu) &
        (remain_po_df['PLTFRM_NM'].str.contains(
            base.format(''.join(expr.format(plt))))),
        'TV_FAMILY'] = fam

# =============================================================================
# REMOVE PLATFORM ACCESSORIES, AV- AND UNKNOWN FROM DATA
# =============================================================================
remain_po_df = remain_po_df.loc[
    ~remain_po_df['PLTFRM_NM'].str.contains('ACCESSOR|AV-|UNKNOWN')].copy()

# DROP PART_NR COLUMN
remain_po_df = remain_po_df.drop(columns=['PART_NR', 'CATEGORY'])

# =============================================================================
# ADD COMBINED COLUMN FOR RELATIONSHIP
# =============================================================================
remain_po_df = fm.combine_cols(remain_po_df,
                               ['BU',
                                'BUS_UNIT_NM',
                                'CAT_NM',
                                'CAT_SUB', 'MPA', 'REGION',
                                'TV_FAMILY', 'PLTFRM_NM', 'SKU'],
                               'COMBINED')
remain_rel = remain_po_df[['BU', 'BUS_UNIT_NM', 'CAT_NM', 'CAT_SUB',
                           'MPA', 'REGION', 'TV_FAMILY', 'PLTFRM_NM',
                           'SKU', 'COMBINED']].copy()
por_ship_rel = por_ship_df[['BU', 'BUS_UNIT_NM', 'CAT_NM', 'CAT_SUB',
                           'MPA', 'REGION', 'TV_FAMILY', 'PLTFRM_NM',
                            'SKU', 'COMBINED']].copy()
por_ship_tpo_rel = pd.concat([remain_rel, por_ship_rel], ignore_index=True)

por_bu_slice_df = fm.unique_table(por_ship_tpo_rel, ['BU', 'COMBINED'])
por_bus_slice_df = fm.unique_table(
    por_ship_tpo_rel, ['BUS_UNIT_NM', 'COMBINED'])
por_catnm_slice_df = fm.unique_table(por_ship_tpo_rel, ['CAT_NM', 'COMBINED'])
por_catsub_slice_df = fm.unique_table(
    por_ship_tpo_rel, ['CAT_SUB', 'COMBINED'])
por_mpa_slice_df = fm.unique_table(por_ship_tpo_rel, ['MPA', 'COMBINED'])
por_region_slice_df = fm.unique_table(por_ship_tpo_rel, ['REGION', 'COMBINED'])
por_tv_slice_df = fm.unique_table(por_ship_tpo_rel, ['TV_FAMILY', 'COMBINED'])
por_plt_slice_df = fm.unique_table(por_ship_tpo_rel, ['PLTFRM_NM', 'COMBINED'])
por_sku_slice_df = fm.unique_table(por_ship_tpo_rel, ['SKU', 'COMBINED'])


# %% PARQUET

ink_month['QUARTER'] = ink_month['QUARTER'].astype(str)
WR_INK_EXE['QUARTER'] = WR_INK_EXE['QUARTER'].astype(str)
laser_month['QUARTER'] = laser_month['QUARTER'].astype(str)

# MPA PERFORMANCE NAMES
mpa_per_df_names = [month_slice, unique_month, quarter_unique, fy_unique,
                    q_slice, fy_slice, ink_mpa_slice_df, ink_region_slice_df,
                    ink_plt_slice_df, ink_fam_slice_df, ink_month,
                    laser_mpa_slice_df, laser_region_slice_df,
                    laser_plt_slice_df, laser_fam_slice_df, laser_cat_slice_df,
                    laser_month]
mpa_file_names = ['month_slice', 'unique_month', 'quarter_unique', 'fy_unique',
                  'q_slice', 'fy_slice', 'ink_mpa_slice_df', 'ink_region_slice_df',
                  'ink_plt_slice_df', 'ink_fam_slice_df', 'ink_month',
                  'laser_mpa_slice_df', 'laser_region_slice_df',
                  'laser_plt_slice_df', 'laser_fam_slice_df', 'laser_cat_slice_df',
                  'laser_month']
# SHIPMENT WITH POR NAMES
shipwpor_df_names = [WR_FINAL, cycle_ref_df, WR_bu_df]
shipwpor_file_names = ['WR_FINAL', 'cycle_ref_df', 'WR_bu_df']

# INKJET EXECUTIVE REPORT NAMES
ink_exe_df_names = [month_slice_exe, unique_month_exe, quarter_unique_exe,
                    fy_unique_exe, q_slice_exe, fy_slice_exe,
                    ink_mpa_slice_exe_df, ink_region_slice_exe_df,
                    ink_plt_slice_exe_df, ink_fam_slice_exe_df,
                    ink_sku_slice_exe_df, ink_month_exe]
ink_exe_file_names = ['month_slice', 'unique_month', 'quarter_unique',
                      'fy_unique', 'q_slice', 'fy_slice',
                      'ink_mpa_slice_df', 'ink_region_slice_df',
                      'ink_plt_slice_df', 'ink_fam_slice_df',
                      'ink_sku_slice_df', 'ink_month']

# TV EXECUTIVE REPORT NAMES AND EXECUTIVE REPORT
tv_names = [mtd_ink_por_ship_df, ink_cat_df_grouped, mtd_laser_por_ship_df,
            laser_cat_df_grouped, por_ship_df, qtd_por_ship_df, remain_po_df,
            por_bu_slice_df, por_bus_slice_df, por_catnm_slice_df,
            por_catsub_slice_df, por_mpa_slice_df, por_region_slice_df,
            por_tv_slice_df, por_plt_slice_df, por_sku_slice_df,
            title_df, all_tv, Combined_Relationship, COMBINED_PORWSHIP]
tv_file_names = ['Inkjet_Current_Mth', 'Inkjet_CAT', 'Laser_Current_Mth',
                 'Laser_CAT', 'W_POR_SHIP_TPO', 'QTD_POR_SHIP_TPO', 'Remain_PO',
                 'por_bu_slice', 'por_bus_slice', 'por_catnm_slice',
                 'por_catsub_slice', 'por_mpa_slice', 'por_region_slice',
                 'por_tv_slice', 'por_plt_slice', 'por_sku_slice',
                 'Combined Title', 'Combined Data', 'Combined Relationship',
                 'Combined PorwShip']

# SHIPMENT
output_pq('Shipment', TPO_Final, 'Shipment')
# MPA PERFORMANCE
for df, filename in zip(mpa_per_df_names, mpa_file_names):
    output_pq('MPA Performance', df, filename)
# SHIPMENT WITH POR
for df, filename in zip(shipwpor_df_names, shipwpor_file_names):
    output_pq('Shipment With POR', df, filename)
# SHIPMENT VS POR
output_pq('Shipment Vs POR', ship_v_por_df, 'ship_v_por_df')
# EXECUTIVE REPORT
for df, filename in zip(ink_exe_df_names, ink_exe_file_names):
    output_pq('Ernest Ink MPA Performance', df, filename)
# EXECUTIVE REPORT POR
output_pq('Ernest POR', WR_INK_EXE, 'WR_INK')
output_pq('Ernest POR', ink_cycle, 'cycle_ref_df')

# EXECUTIVE TV
for df, filename in zip(tv_names, tv_file_names):
    output_pq('Executive', df, filename)

# %% CSV
# SHIPMENT
output_csv('Shipment', TPO_Final, 'Shipment')
# MPA PERFORMANCE
for df, filename in zip(mpa_per_df_names, mpa_file_names):
    output_csv('MPA Performance', df, filename)
# SHIPMENT WITH POR
for df, filename in zip(shipwpor_df_names, shipwpor_file_names):
    output_csv('Shipment With POR', df, filename)
# SHIPMENT VS POR
output_csv('Shipment Vs POR', ship_v_por_df, 'ship_v_por_df')
# EXECUTIVE REPORT
for df, filename in zip(ink_exe_df_names, ink_exe_file_names):
    output_csv('Ernest Ink MPA Performance', df, filename)
# EXECUTIVE REPORT POR
output_csv('Ernest POR', WR_INK_EXE, 'WR_INK')
output_csv('Ernest POR', ink_cycle, 'cycle_ref_df')

# EXECUTIVE TV
for df, filename in zip(tv_names, tv_file_names):
    output_csv('Executive', df, filename)

end_run = datetime.now()
td = (end_run - start_run).total_seconds() / 60
print('Code has completed running, Time taken {:.2f} minutes'.format(td))

os.chdir(project_func_path)
with open("Runtime.txt", "a") as f:
    f.write('\n' + str(today.date()) + '\t {:.2f} minutes'.format(td) + '\t'
            + datetime.now().strftime('%H:%M:%S'))
# CHECKING
# df_parquet = pd.read_parquet('Shipment.parquet')
# df_parquet.head()
# TPO_Final.equals(df_parquet)
