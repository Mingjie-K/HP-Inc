# -*- coding: utf-8 -*-
"""
Created on Mon Feb 13 09:41:59 2023

@author: kohm
"""
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime
import os
import glob

ink_por_naming = {'JABIL WEIHAI': 'INK Jabil Weihai',
                  'NKG Thailand': 'INK NKG Thailand',
                  'NKG Yue Yang': 'INK NKG Yue Yang',
                  'Flex PTP Malaysia': 'INK Flex PTP Malaysia',
                  'Flex Zhuhai': 'INK Flex Zhuhai',
                  'Foxconn ChongQing': 'INK Foxconn ChongQing',
                  'FOXCONN WEIHAI': 'INK Foxconn Weihai'}

laser_por_naming = {'HP FOXCONN PARDUBICE MFG(SG31)': 'Laser Foxconn Czech',
                    'JABIL CHIHUAHUA': 'Laser Jabil Chihuahua',
                    'JABIL WEIHAI': 'Laser Jabil Weihai',
                    'HP Printing Shandong': 'Laser HPPS',
                    'FOXCONN WEIHAI': 'Laser Foxconn Weihai'}

laser_short_naming = {'Laser Foxconn Czech':'FXN CZ',
                      'Laser Foxconn Weihai':'FXN WH',
                      'Laser Jabil Chihuahua':'JCUU',
                      'Laser Jabil Weihai':'JWH'}

canon_short_naming = {'Canon CN, Zhongshan':'Canon',
                      'Canon JP':'Canon',
                      'Canon PH':'Canon',
                      'Canon VN':'Canon'}

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
    dataset[['REGION', 'Sub_Region']] = dataset[[
        'REGION', 'Sub_Region']].fillna('')
    return dataset

# %% S4_Data Cleaning


def data_cleaning(dataset, hpps_bool, canon_bool, ship_bool, hpps_ship_point):
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
            hpps_ship_point)].copy()
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

# %% SHIPMENT DATA CLEANING


def shipment_data(dataset, por_df, hpps_bool, pc, hpps_family, ex_plant, family_map):
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

    TPO_Final[['PLTFRM_NM', 'REGION', 'Sub_Region']] = \
        TPO_Final[['PLTFRM_NM', 'REGION', 'Sub_Region']].fillna('')

    if hpps_bool:
        TPO_Final = TPO_Final.merge(hpps_family, how='left',
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

# =============================================================================
# SHIPMENT GET POR SKUS
# =============================================================================


def shipment_por_skus(grouped_df, por_items, canon_cum_bool):
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
        TPO_Clean = pd.concat([TPO_Clean, find_df], ignore_index=True)

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

    TPO_Final[['PLTFRM_NM', 'REGION']] = TPO_Final[[
        'PLTFRM_NM', 'REGION']].fillna('')

    return TPO_Final, por_items

# %% CONVERT DATE FUNCTION


def get_date():
    today = datetime.today()
    current_mon_date = today - relativedelta(days=today.weekday())
    current_mon_date = current_mon_date.replace(
        hour=0, minute=0, second=0, microsecond=0)
    current_mon_month = current_mon_date.replace(day=1)
    lastwk_mon_date = current_mon_date - relativedelta(weeks=1)
    return today, current_mon_date, current_mon_month, lastwk_mon_date


def convert_date_week(df, new_col, col):
    df[new_col] = \
        df[col].dt.to_period('W').dt.start_time
    return df


def convert_date_month(df, new_col, col):
    df[new_col] = \
        df[col].apply(lambda dt: dt.replace(day=1, hour=0,
                                            minute=0, second=0,
                                            microsecond=0))
    return df


def convert_month_num(df, new_col, col):
    df[new_col] = df[col].dt.month
    return df


def convert_iso(df, new_col, col):
    df[new_col] = df[col].dt.strftime('%y %b W%V')
    return df


def convert_fy(df, q_col, date_col, fy, y_col):
    df[q_col] = \
        df[date_col].dt.to_period(fy)
    df[y_col] = df[q_col].astype(str)
    df[y_col] = df[y_col].str[0:4]
    return df


def create_month_df(fy_quarter):
    month_range = pd.date_range(start='10/10/2021', periods=480, freq='M')
    month_df = pd.DataFrame({'Month': month_range})
    month_df['Month'] = convert_date_month(month_df, 'Month', 'Month')
    month_df['Quarter'] = month_df['Month'].dt.to_period(fy_quarter)
    month_df['Quarter'] = month_df['Quarter'].astype(str)
    month_df['Year'] = month_df['Quarter'].str[0:4]
    return month_df


def get_cur_fy_df(df, year_col, cur_fy):
    df = df.loc[df[year_col] == cur_fy].copy()
    return df

# %% REGION MAPPING

# =============================================================================
# MAPPING INKJET REGION TO NA, LA, CHINA
# =============================================================================


def inkjet_region_mapping(df, sub_region_col, region_col, region_name):
    df.loc[df[sub_region_col] == region_name, region_col] = region_name
    return df

# =============================================================================
# MAPPING INKJET SKU TO REGION
# =============================================================================


def inkjet_sku_region_mapping(df, sku_col, sku_code, region_col, region_name):
    df.loc[df[sku_col].str.contains(sku_code), region_col] = region_name
    return df

# %% CREATING RELATIONSHIP

# =============================================================================
# CREATING COLUMNS FOR RELATIONSHIP
# =============================================================================


def combine_cols(df, col_list, new_col):
    cols = col_list

    df[new_col] = df[cols] \
        .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
    return df


def create_unique_col(df, new_col_name, col_name):
    unique = pd.DataFrame({new_col_name:
                           df[col_name].unique()
                           })
    return unique


def unique_table(df, col_list):
    df = df[col_list].drop_duplicates()
    return df


# %% MONTH SHIPMENT VS TPO QTY

def quarter_year_name(df, q_col, y_col, new_q_col, new_y_col):
    df[new_q_col] = df[q_col].astype(str)
    df[new_q_col] = 'F' + df[new_q_col].str[-2:] + \
        """'""" + df[new_q_col].str[2:4]
    df[new_y_col] = 'ALL - FY' + df[y_col]
    return df

# =============================================================================
# GET ONLY POR SKUS AND ADD FAMILY
# =============================================================================
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

# GROUP PO BASE ON TPO REQUESTED DELIVERY DATE AND SHIPMENT BASE ON TPO LA DELIVERY DATE


def po_ship(cum_month):
    # START AUGUST 2021
    cum_month = cum_month.loc[cum_month['TPO_Requested_Delivery_Date']
                              >= '2021-08-02'].copy()

    po_df = cum_month.groupby(['TPO_PO_Vendor_Name',
                               'Trade_PO',
                               'TPO_Requested_Delivery_Date',
                               'BU', 'PLTFRM_NM',
                               'BUS_UNIT_NM', 'FAMILY',
                               'REGION', 'PART_NR'],
                              dropna=False)['TPO_Qty'].sum().reset_index()

    ship_df = cum_month.groupby(['TPO_PO_Vendor_Name',
                                 'Trade_PO',
                                 'TPO_LA_Conf_Delivery_Date',
                                 'BU', 'PLTFRM_NM',
                                 'BUS_UNIT_NM', 'FAMILY',
                                 'REGION', 'PART_NR'],
                                dropna=False)['TPO_LA_Qty'].sum().reset_index()
    ship_df = ship_df.loc[ship_df
                          ['TPO_LA_Conf_Delivery_Date'].notnull()].copy(
    )

    return po_df, ship_df

# GROUP BY MONTH AND WEEK


def combine_po_ship(po_df, ship_df, today_date, fy_quarter, povpor_bool):
    # current_mon_date = today - relativedelta(days=today.weekday())
    # current_mon_date = current_mon_date.replace(
    #     hour=0, minute=0, second=0, microsecond=0)
    po_df = po_df.groupby(['TPO_PO_Vendor_Name',
                           'Trade_PO',
                           'TPO_Requested_Delivery_Date',
                           'TPO_Requested_Delivery_Month_POR',
                           'BU', 'PLTFRM_NM',
                           'BUS_UNIT_NM',
                           'FAMILY', 'REGION', 'PART_NR'],
                          dropna=False)['TPO_Qty'].sum().reset_index()
    ship_df = ship_df.groupby(['TPO_PO_Vendor_Name',
                               'Trade_PO',
                               'TPO_LA_Conf_Delivery_Date',
                               'TPO_LA_Conf_Delivery_Month_POR',
                               'BU', 'PLTFRM_NM',
                               'BUS_UNIT_NM',
                               'FAMILY', 'REGION', 'PART_NR'],
                              dropna=False)['TPO_LA_Qty'].sum().reset_index()
    po_ship_df = po_df.merge(ship_df, how='outer',
                             left_on=['TPO_PO_Vendor_Name',
                                      'Trade_PO',
                                      'TPO_Requested_Delivery_Date',
                                      'TPO_Requested_Delivery_Month_POR',
                                      'BU', 'PLTFRM_NM',
                                      'BUS_UNIT_NM',
                                      'FAMILY', 'REGION', 'PART_NR'],
                             right_on=['TPO_PO_Vendor_Name',
                                       'Trade_PO',
                                       'TPO_LA_Conf_Delivery_Date',
                                       'TPO_LA_Conf_Delivery_Month_POR',
                                       'BU', 'PLTFRM_NM',
                                       'BUS_UNIT_NM',
                                       'FAMILY', 'REGION', 'PART_NR'])

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
                                                  'INK Flex Zhuhai',
                                                  'Laser HPPS'])) &
          (po_ship_df['TPO_Requested_Delivery_Month_POR'] >= '2022-07-01'))].copy()
    # Start from August and get less than today (Last day of Month)
    if not povpor_bool:
        last_day_month = today_date.replace(
            day=1) + relativedelta(months=1) - relativedelta(days=1)
        po_ship_df = po_ship_df.loc[
            (po_ship_df['TPO_Requested_Delivery_Month_POR'] >= '2021-08-01') &
            (po_ship_df['TPO_Requested_Delivery_Date'] <= last_day_month)].copy()
    po_ship_df = po_ship_df.rename(columns={'TPO_PO_Vendor_Name': 'MPA'})

    po_ship_df = po_ship_df.rename(columns={'PART_NR': 'SKU'})
    po_ship_df.columns = po_ship_df.columns.str.upper()
    # Try for dynamic
    po_ship_df['TPO_REQUESTED_DELIVERY_MONTH_STR'] =  \
        po_ship_df['TPO_REQUESTED_DELIVERY_MONTH_POR'].dt.strftime('%Y_%m')
    # if ink_exe_bool:
    po_ship_df = convert_fy(po_ship_df, 'QUARTER',
                            'TPO_REQUESTED_DELIVERY_MONTH_POR',
                            fy_quarter,
                            'QUARTER_YEAR')
    po_ship_df = quarter_year_name(po_ship_df, 'QUARTER', 'QUARTER_YEAR', 
                                   'QUARTER_NAME', 'FY_NAME')

    return po_ship_df

# %% PO THAT ARE OPEN AND NOT DONE DEAL

def remain_po(po_df, ship_df, cur_month):
    shipped = ship_df.groupby([
        'TPO_PO_Vendor_Name','Trade_PO','PART_NR'],dropna=False) \
        ['TPO_LA_Qty'].sum().reset_index()
    shipped = po_df.merge(shipped,how='left',
                          on=['TPO_PO_Vendor_Name','Trade_PO','PART_NR'])
    shipped['TPO_LA_Qty'] = shipped['TPO_LA_Qty'].fillna(0)
    shipped['REMAINING_PO_QTY'] = shipped['TPO_Qty'] - shipped['TPO_LA_Qty']
    
    shipped = shipped.loc[
        (shipped['REMAINING_PO_QTY'] != 0)  &  
        (shipped['TPO_Requested_Delivery_Month_POR'] == cur_month)].copy()
    shipped = shipped.rename(columns={'TPO_PO_Vendor_Name':'MPA',
                                      'PART_NR':'SKU'})
    shipped.columns = shipped.columns.str.upper()
    return shipped


# %% SHIPMENT WITH POR


def combine_ship_build(df, ship_df, today_date, mon_date,
                       region, subregion, mpa,
                       hpps_fam, fam,
                       hpps_bool, hpps_hw_str, canon_bool, ink_exe_bool,
                       laser_exe_bool):
    min_por_date = df['CAL_WK_DT'].min()
    if mon_date > min_por_date:
        get_date = mon_date
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
                                 ], dropna=False)['TPO_LA_QTY'].sum().reset_index()

    if ink_exe_bool:
        month_df = create_month_df('Q-OCT')
        # Get this monday month
        current_month = mon_date.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0)
        current_fy = month_df.loc[month_df['Month']
                                  == current_month]['Year'].unique().item()
        WR_Final = convert_fy(WR_Final, 'Quarter',
                              'TPO_LA_CONF_DELIVERY_MONTH_POR', 'Q-OCT', 'Year')
        WR_Final = WR_Final.loc[WR_Final['Year'] == current_fy].copy()
        WR_Final = WR_Final.drop(columns=['Quarter', 'Year'])
        WR_Final['SHIP/POR'] = 'SHIP'
        # ADDING LA AND NA & CHINA
        WR_Final = inkjet_region_mapping(WR_Final, 'SUB_REGION',
                                         'REGION', 'NA')
        WR_Final = inkjet_region_mapping(WR_Final, 'SUB_REGION',
                                         'REGION', 'LA')
        WR_Final = inkjet_sku_region_mapping(WR_Final,
                                             'SKU',
                                             '#AB2',
                                             'REGION',
                                             'CHINA')
    elif laser_exe_bool:
        month_df = create_month_df('Q-OCT')
        # Get this monday month
        current_month = mon_date.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0)
        current_fy = month_df.loc[month_df['Month']
                                  == current_month]['Year'].unique().item()
        WR_Final = convert_fy(WR_Final, 'Quarter',
                              'TPO_LA_CONF_DELIVERY_MONTH_POR', 'Q-OCT', 'Year')
        WR_Final = WR_Final.loc[WR_Final['Year'] == current_fy].copy()
        WR_Final = WR_Final.drop(columns=['Quarter', 'Year'])
        WR_Final['SHIP/POR'] = 'SHIP'

    else:
        # GET ONLY CURRENT MONTH AND PAST 3 MONTHS OF SHIPMENT DATA
        three_months_back = mon_date.replace(day=1, hour=0, minute=0,
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
    df = map_region(df, region, subregion)

    if ink_exe_bool:
        # ADDING LA AND NA & CHINA
        df.loc[df['Sub_Region'] == 'NA', 'REGION'] = 'NA'
        df.loc[df['Sub_Region'] == 'LA', 'REGION'] = 'LA'
        df.loc[df['SKU'].str.contains('#AB2'), 'REGION'] = 'CHINA'

    # MAP BU, MPA SITE
    df = \
        df.merge(mpa, how='left', left_on='MPa', right_on='MPA')

    df = df.drop(columns=['MPA', 'MPA VENDOR CODE'])

    # ADD FAMILY MAPPING
    if hpps_bool:
        df = df.merge(hpps_fam, how='left',
                      left_on=['MPa',
                               'PLATFORM', 'SKU'],
                      right_on=['MPA', 'PLTFRM_NM', 'PART_NR'])
        df = df.drop(columns=['MPA', 'PLTFRM_NM', 'PART_NR'])
    else:
        df = df.merge(fam, how='left',
                      left_on=['MPa', 'PLATFORM'],
                      right_on=['MPA', 'PLTFRM_NM'])
        df = df.drop(columns=['MPA', 'PLTFRM_NM'])

    df = convert_date_month(df, 'TPO_LA_CONF_DELIVERY_MONTH_POR',
                            'TPO_LA_CONF_DELIVERY_DATE_POR')

    if ink_exe_bool or laser_exe_bool:
        # Get current Fiscal Year data
        df = convert_fy(df, 'Quarter',
                        'TPO_LA_CONF_DELIVERY_MONTH_POR', 'Q-OCT', 'Year')
        df = df.loc[df['Year'] == current_fy].copy()
        df = df.drop(columns=['Quarter', 'Year'])
    else:
        # Get 1 Year of future data
        one_year_fw = today_date.replace(day=1, hour=0, minute=0,
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

    if canon_bool:
        # CANON FAMILY NAMING TO FOLLOW ISAAC (REPLACE NAN WITH PLATFORM)
        WR_Final.loc[WR_Final['FAMILY'].isnull(
        ), 'FAMILY'] = WR_Final['PLATFORM']
        WR_Final['BU'] = WR_Final['BU'].fillna('Laser')
        WR_Final['MPA SITE'] = WR_Final['MPA SITE'].fillna('CANON')
    if ink_exe_bool or laser_exe_bool:
        WR_Final = WR_Final.rename(
            columns={'TPO_LA_CONF_DELIVERY_DATE_POR':'TPO_REQUESTED_DELIVERY_MONTH_POR',
                     'PLATFORM':'PLTFRM_NM'})
    return WR_Final

# %% TV DASHBOARD
def create_date_query():
    week_range = pd.date_range(start='11/8/2022', periods=480, freq='W')
    date_df = pd.DataFrame({'CAL_WK_DT': week_range})
    
    date_df = convert_date_week(date_df, 'CAL_WK_DT', 'CAL_WK_DT')
    
    date_df['CYCLE_WK_NM'] = date_df['CAL_WK_DT'].dt.isocalendar()['week']
    date_df['CYCLE_WK_NM'] = date_df['CYCLE_WK_NM'].astype(str)
    date_df['CYCLE_WK_NM'] = date_df['CYCLE_WK_NM'].str.zfill(2)
    date_df['CYCLE_WK_NM'] = date_df['CAL_WK_DT'].dt.year.astype(str) + '-W' + \
        date_df['CYCLE_WK_NM']
    
    # QUERY POR week only to get QUERY_POR_CYCLE
    date_df['QUERY_POR_WK'] = date_df['CAL_WK_DT'] - pd.DateOffset(weeks=1)
    date_df['QUERY_POR_WK_NUM'] = date_df['QUERY_POR_WK'].dt.isocalendar()['week']
    date_df['QUERY_POR_WK_NUM'] = date_df['QUERY_POR_WK_NUM'].astype(str)
    date_df['QUERY_POR_WK_NUM'] = date_df['QUERY_POR_WK_NUM'].str.zfill(2)
    date_df['QUERY_POR_CYCLE'] = date_df['QUERY_POR_WK'].dt.strftime('%Y-W') + \
        date_df['QUERY_POR_WK_NUM']
    return date_df

def query_por(por_df, date_df):
    por_query_df = date_df.merge(por_df, how='left',
                                 left_on=['QUERY_POR_CYCLE', 'CAL_WK_DT'],
                                 right_on=['CYCLE_WK_NM', 'CAL_WK_DT'])
    por_query_df = por_query_df.drop(columns=['CYCLE_WK_NM_y'])
    por_query_df = por_query_df.rename(
        columns={'CYCLE_WK_NM_x': 'CYCLE_WK_NM'})
    por_query_df = por_query_df.dropna()
    return por_query_df

def map_por_region(query_df, region_df):
    query_df = query_df.merge(region_df,
                              how='left',
                              left_on=['LOC_TO_CD'],
                              right_on=['DC_IC_PO_Plant'])
    return query_df

def group_por(query_df):
    query_df = query_df.groupby([
        'CAL_WK_DT', 'LOC_FROM_NM', 'REGION',
        'FAMILY_NM', 'PLTFRM_NM', 'BUS_UNIT_NM', 'PART_NR'],dropna=False)['QTY'].sum().reset_index()
    return query_df

def tv_pct(df):
    df['% TPO'] = df['TPO_QTY'] / \
        df['QTY']
    df['% Shipped'] = df['TPO_LA_QTY'] / \
        df['TPO_QTY']
    df[['% TPO','% Shipped']] = df[['% TPO','% Shipped']].fillna(0)
    # OVERWRITE MORE THAN 100% TO 100%
    df.loc[df['% TPO'] >= 1, '% TPO'] = 1
    # MEET TARGET
    df.loc[df['% TPO'] >= 0.9, 'Meet Target TPO'] = 1
    df.loc[df['% TPO'] < 0.5, 'Meet Target TPO'] = 2
    df.loc[df['Meet Target TPO'].isnull(),
                       'Meet Target TPO'] = 3

    # OVERWRITE MORE THAN 100% TO 100%
    df.loc[df['% Shipped'] >= 1, '% Shipped'] = 1
    # MEET TARGET
    df.loc[df['% Shipped'] >= 0.9, 'Meet Target Ship'] = 1
    df.loc[df['% Shipped'] < 0.5, 'Meet Target Ship'] = 2
    df.loc[df['Meet Target Ship'].isnull(),
                       'Meet Target Ship'] = 3
    return df
    
def tv_fam_group(df):
    # INKJET OEM PRODUCTS WITHOUT CAT_SUB BUT WITH TV_FAMILY, DROP THEM
    cat_df_grouped = df.groupby([
        'BU', 'MPA', 'PLTFRM_NM', 'TV_FAMILY', 'BUS_UNIT_NM',
        'CAT_NM', 'CAT_SUB'], dropna=False) \
        [['QTY', 'TPO_QTY', 'TPO_LA_QTY']].sum().reset_index()
    cat_df_grouped = tv_pct(cat_df_grouped)
    return cat_df_grouped

# =============================================================================
# COMBINE POR, TRADE PO AND SHIP
# =============================================================================
def combine_all_data(tpo_data, por_data, family_df, planning_df):
    tpo_grouped = tpo_data.groupby(
        ['BU',
         'MPA', 'TPO_REQUESTED_DELIVERY_DATE',
         'PLTFRM_NM', 'BUS_UNIT_NM',
         'REGION', 'SKU'], dropna=False)[['TPO_QTY', 'TPO_LA_QTY']].sum().reset_index()
    # =============================================================================
    # COMBINE POR AND SHIP
    # =============================================================================
    por_ship_df = por_data.merge(
        tpo_grouped,
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

    # por_ship_df.loc[por_ship_df.duplicated(['CAL_WK_DT', 'LOC_FROM_NM', 'REGION',
    #                                         'FAMILY_NM', 'PLTFRM_NM_x', 'BUS_UNIT_NM_x',
    #                                         'PART_NR', 'QTY']),
    #                 ['CAL_WK_DT', 'LOC_FROM_NM',
    #                  'FAMILY_NM', 'PLTFRM_NM_x', 'BUS_UNIT_NM_x',
    #                  'PART_NR', 'QTY']] = np.nan

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
        family_df, how='left', on=['MPA', 'PLTFRM_NM'])

    por_ship_df = convert_date_month(por_ship_df,
                                        'TPO_REQUESTED_DELIVERY_MONTH_POR',
                                        'TPO_REQUESTED_DELIVERY_DATE')

    por_ship_df = convert_fy(por_ship_df,
                                'TPO_REQUESTED_DELIVERY_Q_POR',
                                'TPO_REQUESTED_DELIVERY_MONTH_POR',
                                'Q-OCT', 'FY_YEAR')


    por_ship_df = por_ship_df.merge(planning_df,
                                    how='left',
                                    left_on='SKU',
                                    right_on='PART_NR')
    return por_ship_df

# %% LASER TRADE PO VS CRP
def crp_tpo_clean(df,lastwk_mon_date):
     df.loc[
         df['TPO_Requested_Delivery_Date'] < df['TPO_Created_On'],
         'TPO_Requested_Delivery_Date'] = df['TPO_Created_On']
     df = df.loc[df['TPO_Requested_Delivery_Date'] >= lastwk_mon_date].copy()
     return df