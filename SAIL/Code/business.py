# -*- coding: utf-8 -*-
"""
Created on Thu Feb  2 11:15:38 2023

@author: kohm
"""
import pandas as pd


def ex_skus():
    # ONLY EXCLUDE FLEX SKUS
    ex_skus = pd.read_excel('Data Filtering.xlsx', sheet_name='Exempted SKUs',
                            dtype={'MPA': str,
                                   'Exempted SKUs': str})
    return ex_skus


def mpa():
    mpa_map = pd.read_excel('Data Filtering.xlsx', sheet_name='MPA',
                            dtype={'MPA VENDOR CODE': str,
                                   'MPA': str})
    return mpa_map


def region():
    region_map = pd.read_excel('Data Filtering.xlsx', sheet_name='Region',
                               dtype={'DC_IC_PO_Plant': str,
                                      'Region': str})
    return region_map


def sub_region():
    sub_region_map = pd.read_excel('Data Filtering.xlsx', sheet_name='Sub Region',
                                   dtype={'DC_IC_PO_Plant': str,
                                          'Sub_Region': str},
                                   keep_default_na=False)
    return sub_region_map


def family():
    family_map = pd.read_excel('Data Filtering.xlsx', sheet_name='Family',
                               dtype={'MPA': str,
                                      'PLTFRM_NM': str,
                                      'FAMILY': str})
    # REMOVE SPACES AT THE BACK
    family_map[['PLTFRM_NM', 'FAMILY']] = \
        family_map[['PLTFRM_NM', 'FAMILY']].applymap(lambda x: x.strip())

    return family_map


def hpps_pc():
    # HPPS PROFIT CENTER
    hpps_pc = pd.read_excel('Data Filtering.xlsx', sheet_name='HPPS Profit Center',
                            dtype={'TPO_Profit_Center': str,
                                   'CATEGORY': str})
    return hpps_pc


def hpps_family():
    # HPPS FAMILY MAPPING
    hpps_family = pd.read_excel('Data Filtering.xlsx', sheet_name='HPPS Family',
                                dtype={'MPA': str,
                                       'PLTFRM_NM': str,
                                       'PART_NR': str,
                                       'FAMILY': str})

    hpps_family[['PLTFRM_NM', 'PART_NR', 'FAMILY']] = \
        hpps_family[['PLTFRM_NM', 'PART_NR', 'FAMILY']
                    ].applymap(lambda x: x.strip())
    return hpps_family


def hpps_ship_point():
    # HPPS SHIPPING POINT
    hpps_ship_point = pd.read_excel('Data Filtering.xlsx',
                                    sheet_name='Shipping Point',
                                    dtype={'MPA': str,
                                           'Shipping Point': str})
    ship_point = hpps_ship_point['Shipping Point'].unique()
    return hpps_ship_point, ship_point


def hpps_ex_plant():
    hpps_ex_plant = pd.read_excel('Data Filtering.xlsx',
                                  sheet_name='HPPS Exclude Interplant')
    hpps_ex_plant = hpps_ex_plant['Excluded DC_IC_PO_Plant'].unique()
    return hpps_ex_plant
# =============================================================================
# FXN WH LASER (HPPS MOVE TO FXN WH LASER)
# =============================================================================


def fxnwhl_family():
    # FXN WH LASER FAMILY MAPPING
    fxnwhl_family = pd.read_excel('Data Filtering.xlsx',
                                  sheet_name='FXN WH Laser Family',
                                  dtype={'MPA': str,
                                         'PLTFRM_NM': str,
                                         'PART_NR': str,
                                         'FAMILY': str})
    fxnwhl_family[['PLTFRM_NM', 'PART_NR', 'FAMILY']] = \
        fxnwhl_family[['PLTFRM_NM', 'PART_NR', 'FAMILY']
                      ].applymap(lambda x: x.strip())
    return fxnwhl_family


def fxnwhl_pc():
    # FXN WH LASER PROFIT CENTER
    fxnwhl_pc = pd.read_excel('Data Filtering.xlsx',
                              sheet_name='FXN WH Laser Profit Center',
                              dtype={'MPA': str,
                                     'PLTFRM_NM': str,
                                     'PART_NR': str,
                                     'FAMILY': str})
    return fxnwhl_pc


def fxnwhl_pl():
    # FXN WH LASER PL OVERWRITE (CHANGE IN PL BY PLANNING)
    fxnwhl_pl = pd.read_excel('Data Filtering.xlsx',
                              sheet_name='PL Change',
                              dtype={'MPA': str,
                                     'SKU': str,
                                     'PL OVERWRITE': str})
    return fxnwhl_pl


def fxnwhl_ex_plant():
    # FXN WH LASER EXCLUDE INTERPLANT
    fxnwhl_ex_plant = pd.read_excel('Data Filtering.xlsx',
                                    sheet_name='FXN WH Laser Exclude Interplant')
    fxnwhl_ex_plant = fxnwhl_ex_plant['Excluded DC_IC_PO_Plant'].unique()
    return fxnwhl_ex_plant


def canon_site():
    canon_site = pd.read_excel('Data Filtering.xlsx', sheet_name='Canon Site',
                               dtype={'TPO_LA_REFERENCE': str,
                                      'SITE': str})
    canon_site['TPO_LA_REFERENCE'] = canon_site['TPO_LA_REFERENCE'].str.strip()
    canon_site_dict = canon_site.set_index('TPO_LA_REFERENCE').to_dict()
    return canon_site_dict


def tv_family():
    tv_family = pd.read_excel('Data Filtering.xlsx',
                              sheet_name='TV_FAMILY',
                              dtype={'PLTFRM_NM': str,
                                     'TV_FAMILY': str})

    tv_family[['PLTFRM_NM', 'TV_FAMILY']] = \
        tv_family[['PLTFRM_NM', 'TV_FAMILY']].applymap(lambda x: x.strip())
    
    tv_family['PLTFRM_NM'] = tv_family['PLTFRM_NM'].str.upper()
    tv_family['TV_FAMILY'] = tv_family['TV_FAMILY'].str.upper()

    return tv_family
