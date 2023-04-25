# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 14:19:47 2022

@author: kohm
"""

import pandas as pd
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import win32com.client
import time
pd.set_option('display.max_columns', None)

user = os.getenv('USERPROFILE')

por_master_path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\ISAAC')

os.chdir(por_master_path)

file_name = 'Output_Final_ISAAC - EXTENDED_USER.xlsx'

por_file = os.path.join(por_master_path, file_name)

# REFRESH FILE
# xlapp = win32com.client.DispatchEx('Excel.Application')
# xlapp.Visible = True
# wb = xlapp.Workbooks.Open(por_file)

# wb.RefreshAll()

# xlapp.CalculateUntilAsyncQueriesDone()
# wb.Save()
# time.sleep(10)
# xlapp.Quit()


def refresh(file):
    xlapp = win32com.client.DispatchEx('Excel.Application')
    time.sleep(5)
    xlapp.Visible = True
    wb = xlapp.Workbooks.Open(file)
    time.sleep(5)
    wb.RefreshAll()

    xlapp.CalculateUntilAsyncQueriesDone()
    wb.Save()
    time.sleep(10)
    xlapp.Quit()
    del xlapp


refresh(por_file)

time.sleep(10)

# =============================================================================
# Set POR Output Directory
# =============================================================================


def por_out_path(mpa_str):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\POR',
                        mpa_str)
    return path

# =============================================================================
# Set Build Plan Output Directory
# =============================================================================


def build_out_path(mpa_str):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\BUILD',
                        mpa_str)
    return path

# =============================================================================
# Set BUS_UNIT_CATEGORIZATION Output Directory
# =============================================================================


# def bus_out_path(bu):
#     path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\BUS_UNIT CATEGORIZATION',
#                         bu)
#     return path

# =============================================================================
# Set Inkjet Weekly Build Plan Output Directory
# =============================================================================


def ink_wk_build_out_path(mpa_str):
    path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR\Data\BUILD\INKJET WEEKLY BUILD',
                        mpa_str)
    return path

# =============================================================================
# Set PLANNING_CATEGORIZATION Output Directory
# =============================================================================


plan_cat_path = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsxPOR',
                    'Data\PLANNING CATEGORIZATION')


# %% INKJET POR Output Directory
# fptp_output = por_out_path('FLEX PTP')
# fxn_cq_output = por_out_path('FXN CQ')
jwhi_output = por_out_path('JWH INK')
nkgth_output = por_out_path('NKG TH')
nkgyy_output = por_out_path('NKG YY')
fxnwhi_output = por_out_path('FXN WH INK')
# fzh_output = por_out_path('FLEX ZH')

# %% INKJET BUILD Output Directory
# fxn_cq_b_output = build_out_path('FXN CQ')
jwhi_b_output = build_out_path('JWH INK')
nkgth_b_output = build_out_path('NKG TH')
nkgyy_b_output = build_out_path('NKG YY')
fxnwhi_b_output = build_out_path('FXN WH INK')
# fptp_b_output = build_out_path('FLEX PTP')
# fzh_b_output = build_out_path('FLEX ZH')

# %% WEEKLY INKJET BUILD Output Directory
# fxn_cq_b_output_wk = ink_wk_build_out_path('FXN CQ')
jwhi_b_output_wk = ink_wk_build_out_path('JWH INK')
nkgth_b_output_wk = ink_wk_build_out_path('NKG TH')
nkgyy_b_output_wk = ink_wk_build_out_path('NKG YY')
fxnwhi_b_output_wk = ink_wk_build_out_path('FXN WH INK')

# %% INKJET BUS_UNIT Output Directory
# inkjet_bus_output = bus_out_path('Inkjet')

# %% LASER POR Output Directory
fxn_cz_output = por_out_path('FXN CZ')
jcuu_output = por_out_path('JABIL CUU')
jwhl_output = por_out_path('JWH LASER')
canon_ffgi_output = por_out_path('CANON\CANON FFGI')
canon_engine_output = por_out_path('CANON\CANON ENGINE AND TONER')
fxnwhl_output = por_out_path('FXN WH LASER')
# HPPS_output = por_out_path('HPPS')

# %% LASER BUILD Output Directory
# HPPS_b_output = build_out_path('HPPS')
fxn_cz_b_output = build_out_path('FXN CZ')
jcuu_b_output = build_out_path('JABIL CUU')
jwhl_b_output = build_out_path('JWH LASER')
# canon_ffgi_b_output = build_out_path('CANON\CANON FFGI')
fxnwhl_b_output = build_out_path('FXN WH LASER')
# CANON
canon_zs_b_output = build_out_path('CANON\CANON FFGI\ZS')
canon_ph_b_output = build_out_path('CANON\CANON FFGI\PH')
canon_jp_b_output = build_out_path('CANON\CANON FFGI\JP')
canon_vn_b_output = build_out_path('CANON\CANON FFGI\VN')

# %% Read POR Data
os.chdir(por_master_path)


def read_por_data(filename, sheet, skip):
    df = pd.read_excel(filename,
                       sheet_name=sheet,
                       dtype={'CYCLE_WK_NM': str,
                              'LOC_FROM_NM': str,
                              'FAMILY_NM': str,
                              'PLTFRM_NM': str,
                              'BUS_UNIT_NM': str,
                              'PART_NR': str},
                       skiprows=skip)
    return df


por_df = read_por_data(file_name, 'INKJET POR', 2)

fxncz_jcuu_por = read_por_data(file_name, 'LASER FXN CZ JCUU POR', 0)

jwhl_por = read_por_data(file_name, 'JABIL WH LASER POR', 2)

jwhi_por = read_por_data(file_name, 'JABIL WH INKJET POR', 3)

fxnwhi_por = read_por_data(file_name, 'FXN WH INKJET POR', 2)

canon_ffgi_por = read_por_data(file_name, 'CANON FFGI POR', 5)

canon_engine_por = read_por_data(file_name, 'CANON ENGINE TONER', 3)

fxnwhl_por = read_por_data(file_name, 'LASER FXN WH POR', 2)


# Get today date
today = datetime.today()
# INPUT FOR PREVIOUS DATA IF NECCESSARY
# today = datetime(2023, 3, 24)


def extract_mpa_por(df, today_date, mpa):
    current_week = today.strftime('%Y-W%V')
    find_por = df.loc[(df['CYCLE_WK_NM'] == current_week) &
                      (df['LOC_FROM_NM'] == mpa)].copy()
    week_num = 1
    while (len(find_por) == 0):
        get_date = today_date - relativedelta(weeks=week_num)
        get_week = get_date.isocalendar()[1]
        current_week = today_date.strftime('%Y-W'+f'{get_week:02}')
        find_por = df.loc[(df['CYCLE_WK_NM'] == current_week) &
                          (df['LOC_FROM_NM'] == mpa)].copy()
        week_num = week_num + 1
    return find_por, current_week


def extract_canon_por(df, today_date):
    current_week = today.strftime('%Y-W%V')
    find_por = df.loc[df['CYCLE_WK_NM'] == current_week].copy()
    week_num = 1
    while (len(find_por) == 0):
        get_date = today_date - relativedelta(weeks=week_num)
        get_week = get_date.isocalendar()[1]
        current_week = today_date.strftime('%Y-W'+f'{get_week:02}')
        find_por = df.loc[df['CYCLE_WK_NM'] == current_week].copy()
        week_num = week_num + 1
    return find_por, current_week

# %% Read Build Plan Data


def read_build_data(filename, sheet, skip):
    df = pd.read_excel(filename,
                       sheet_name=sheet,
                       dtype={'CYCLE_WK_NM': str,
                              'LOC_TO_CD': str,
                              'LOC_FROM_NM': str,
                              'FAMILY_NM': str,
                              'PLTFRM_NM': str,
                              'BUS_UNIT_NM': str,
                              'PART_NR': str},
                       skiprows=skip)
    df = df.fillna(0)
    Dates = df.columns[7:]
    Col = df.columns[0:7]

    latest_build = pd.melt(df,
                           id_vars=Col,
                           value_vars=Dates,
                           value_name='QTY',
                           var_name='CAL_WK_DT')
    latest_build['CAL_WK_DT'] = pd.to_datetime(latest_build['CAL_WK_DT'],
                                               format='%m/%d/%y')

    return latest_build


def extract_mpa_build(df, today_date, mpa, canon):
    current_week = today.strftime('%Y-W%V')
    if canon:
        find_por = df.loc[(df['CYCLE_WK_NM'] == current_week)].copy()
    else:
        find_por = df.loc[(df['CYCLE_WK_NM'] == current_week) &
                          (df['LOC_FROM_NM'] == mpa)].copy()
    week_num = 1
    while (len(find_por) == 0):
        get_date = today_date - relativedelta(weeks=week_num)
        get_week = get_date.isocalendar()[1]
        current_week = today_date.strftime('%Y-W'+f'{get_week:02}')
        if canon:
            find_por = df.loc[(df['CYCLE_WK_NM'] == current_week)].copy()
        else:
            find_por = df.loc[(df['CYCLE_WK_NM'] == current_week) &
                              (df['LOC_FROM_NM'] == mpa)].copy()
        week_num = week_num + 1
    # REMOVE 0 DATA
    find_por = find_por.loc[find_por['QTY'] != 0].copy()
    return find_por, current_week

# %% Read BUS_UNIT CATEGORIZATION Data

# def read_bus_data(filename, sheet, skip, cat):
#     df = pd.read_excel(filename,
#                        sheet_name=sheet,
#                        dtype={'LOC_FROM_NM': str,
#                               'PLTFRM_NM': str,
#                               'PART_NR': str},
#                        skiprows=skip)
#     df['CATEGORY_BUS'] = cat
#     return df

# %% READ PLANNING CATEGORIZATION DATA

def read_plan_category(filename, sheet, skip):
    df = pd.read_excel(filename,
                        sheet_name=sheet,
                        dtype={'CYCLE_WK_NM': str,
                               'CAT_NM': str,
                               'CAT_SUB': str,
                               'PROD_LINE_CD': str,
                               'PART_NR': str},
                        skiprows=skip)
    df = df.groupby([
        'CAT_NM','CAT_SUB','PROD_LINE_CD','PART_NR']) \
    ['PART_NR'].nunique().reset_index(name='nunique')
    df = df.drop_duplicates(
        subset=['PART_NR'], keep='first')
    df = df.drop(columns=['nunique'])
    # current_week = today.strftime('%Y-W%V')
    # find_cat = df.loc[(df['CYCLE_WK_NM'] == current_week)].copy()
    # week_num = 1
    # while (len(find_cat) == 0):
    #     get_date = today_date - relativedelta(weeks=week_num)
    #     get_week = get_date.isocalendar()[1]
    #     current_week = today_date.strftime('%Y-W'+f'{get_week:02}')
    #     find_cat = df.loc[(df['CYCLE_WK_NM'] == current_week)].copy()
    return df

# %% LASER LATEST BUILD PLAN
fxncz_jcuu_build = read_build_data(file_name, 'FXN CZ JCUU BUILD PLAN', 8)
fxncz_build, fxncz_b_wk = extract_mpa_build(
    fxncz_jcuu_build, today, 'HP FOXCONN PARDUBICE MFG(SG31)', False)
jcuu_build, jcuu_b_wk = extract_mpa_build(
    fxncz_jcuu_build, today, 'JABIL CHIHUAHUA', False)
jwhl_build = read_build_data(file_name, 'JWH LASER BUILD PLAN', 7)
jwhl_build, jwhl_b_wk = extract_mpa_build(
    jwhl_build, today, 'JABIL WEIHAI', False)
canon_zs_build = read_build_data(file_name, 'CANON ZS FFGI BUILD PLAN', 7)
canon_vn_build = read_build_data(file_name, 'CANON VN FFGI BUILD PLAN', 7)
canon_ph_build = read_build_data(file_name, 'CANON PH FFGI BUILD PLAN', 7)
canon_jp_build = read_build_data(file_name, 'CANON JP FFGI BUILD PLAN', 7)
canon_zs_build, canon_zs_b_wk = extract_mpa_build(
    canon_zs_build, today, None, True)
canon_vn_build, canon_vn_b_wk = extract_mpa_build(
    canon_vn_build, today, None, True)
canon_ph_build, canon_ph_b_wk = extract_mpa_build(
    canon_ph_build, today, None, True)
canon_jp_build, canon_jp_b_wk = extract_mpa_build(
    canon_jp_build, today, None, True)

# %% INKJET LATEST BUILD PLAN
ink_build = read_build_data(file_name, 'INKJET BUILD PLAN', 6)
# fxn_cq_build, fxn_cq_b_wk = extract_mpa_build(
#     ink_build, today, 'Foxconn ChongQing', False)
nkgth_build, nkgth_b_wk = extract_mpa_build(
    ink_build, today, 'NKG Thailand', False)
nkgyy_build, nkgyy_b_wk = extract_mpa_build(
    ink_build, today, 'NKG Yue Yang', False)
jwhi_build, jwhi_b_wk = extract_mpa_build(
    ink_build, today, 'JABIL WEIHAI', False)
# fptp_build, fptp_b_wk = extract_mpa_build(
#     ink_build, today, 'Flex PTP Malaysia', False)
fxnwhi_build, fxnwhi_b_wk = extract_mpa_build(
    ink_build, today, 'FOXCONN WEIHAI', False)
# fzh_build, fzh_b_wk = extract_mpa_build(ink_build, today, 'Flex Zhuhai')
# %% INKJET LATEST WEEKLY BUILD PLAN
ink_wk_build = read_build_data(file_name, 'INKJET BUILD PLAN WEEKLY', 5)
fxn_cq_build_wk, fxn_cq_b_wk_wk = extract_mpa_build(
    ink_wk_build, today, 'Foxconn ChongQing', False)
nkgth_build_wk, nkgth_b_wk_wk = extract_mpa_build(
    ink_wk_build, today, 'NKG Thailand', False)
nkgyy_build_wk, nkgyy_b_wk_wk = extract_mpa_build(
    ink_wk_build, today, 'NKG Yue Yang', False)
jwhi_build_wk, jwhi_b_wk_wk = extract_mpa_build(
    ink_wk_build, today, 'JABIL WEIHAI', False)
fxnwhi_build_wk, fxnwhi_b_wk_wk = extract_mpa_build(
    ink_wk_build, today, 'FOXCONN WEIHAI', False)
# %% HPPS HW LATEST BUILD PLAN
# hpps_build = read_build_data(file_name, 'HPPS HW BUILD PLAN', 5)
# hpps_build, hpps_b_wk = extract_mpa_build(
#     hpps_build, today, 'HP Printing Shandong')

# %% LASER FXN WH LATEST BUILD PLAN
fxnwhl_build = read_build_data(file_name, 'FXN WH HW BUILD PLAN', 5)
fxnwhl_build, fxnwhl_b_wk = extract_mpa_build(
    fxnwhl_build, today, 'FOXCONN WEIHAI', False)

# %% INKJET LATEST POR
# fptp_por, fptp_wk = extract_mpa_por(por_df, today, 'Flex PTP Malaysia')
# fzh_por, fzh_wk = extract_mpa_por(por_df, today, 'Flex Zhuhai')
# fxn_cq_por, fxn_cq_wk = extract_mpa_por(por_df, today, 'Foxconn ChongQing')
nkgth_por, nkgth_wk = extract_mpa_por(por_df, today, 'NKG Thailand')
nkgyy_por, nkgyy_wk = extract_mpa_por(por_df, today, 'NKG Yue Yang')
jwhi_por, jwhi_wk = extract_mpa_por(jwhi_por, today, 'JABIL WEIHAI')
fxnwhi_por, fxnwhi_wk = extract_mpa_por(fxnwhi_por, today, 'FOXCONN WEIHAI')

# %% LASER LATEST POR
jwhl_por, jwhl_wk = extract_mpa_por(jwhl_por, today, 'JABIL WEIHAI')
fxn_cz_por, fxn_cz_wk = extract_mpa_por(
    fxncz_jcuu_por, today, 'HP FOXCONN PARDUBICE MFG(SG31)')
jcuu_por, jcuu_wk = extract_mpa_por(fxncz_jcuu_por, today, 'JABIL CHIHUAHUA')
# hpps_por, hpps_wk = extract_mpa_por(por_df, today, 'HP Printing Shandong')
fxnwhl_por, fxnwhl_wk = extract_mpa_por(fxnwhl_por, today, 'FOXCONN WEIHAI')
canon_ffgi_por, canon_wk = extract_canon_por(canon_ffgi_por, today)
canon_engine_por, canon_eng_wk = extract_canon_por(canon_engine_por, today)
# %% INKJET BUS_UNIT CATEGORIZATION (OLD)
# ink_ops = read_bus_data(file_name, 'INKJET OPS', 3, 'OPS')
# ink_hps = read_bus_data(file_name, 'INKJET HPS', 3, 'HPS')
# ink_ciss = read_bus_data(file_name, 'INKJET CISS', 3, 'CISS')
# ink_category = pd.concat([ink_ops, ink_hps, ink_ciss], ignore_index=True)

# %% PLANNING CATEGORY CATEGORIZATION
plan_cat_df = read_plan_category(file_name, 'PLANNING CATEGORIZATION', 0)
plan_cat_df['CAT_NM'] = \
    plan_cat_df['CAT_NM'].str.upper() 
plan_cat_df['CAT_SUB'] = \
    plan_cat_df['CAT_SUB'].str.upper()
plan_cat_df['PROD_LINE_CD'] = \
    plan_cat_df['PROD_LINE_CD'].str.upper() 
plan_cat_df['PART_NR'] = \
    plan_cat_df['PART_NR'].str.upper()
# ADD ONE COLUMN TO COMBINE AND CHECK FOR INCREMENTAL UPDATE
plan_cat_df['CHECK'] = plan_cat_df.apply('_'.join, axis=1)

# READ PAST DATA AND COMPARE
os.chdir(plan_cat_path)
old_plan_cat = pd.read_csv('PLANNING CATEGORIZATION.csv')
missing_cat = old_plan_cat.loc[
    ~old_plan_cat['CHECK'].isin(plan_cat_df['CHECK'])].copy()

# COMBINE ALL DATA
plan_cat_df = pd.concat([plan_cat_df,missing_cat])
# %% OUTPUT TO CSV


def output_csv(path, df, df_wk):
    os.chdir(path)
    df.to_csv(df_wk + '.csv', index=False)


# INKJET POR
# output_csv(fptp_output, fptp_por, fptp_wk)
# output_csv(fzh_output, fzh_por, fzh_wk)
# output_csv(fxn_cq_output, fxn_cq_por, fxn_cq_wk)
output_csv(nkgth_output, nkgth_por, nkgth_wk)
output_csv(nkgyy_output, nkgyy_por, nkgyy_wk)
output_csv(jwhi_output, jwhi_por, jwhi_wk)
output_csv(fxnwhi_output, fxnwhi_por, fxnwhi_wk)

# LASER POR
output_csv(jwhl_output, jwhl_por, jwhl_wk)
output_csv(fxn_cz_output, fxn_cz_por, fxn_cz_wk)
output_csv(jcuu_output, jcuu_por, jcuu_wk)
output_csv(fxnwhl_output, fxnwhl_por, fxnwhl_wk)
output_csv(canon_ffgi_output, canon_ffgi_por, canon_wk)
output_csv(canon_engine_output, canon_engine_por, canon_eng_wk)

# INKJET BUILD PLAN
# output_csv(fxn_cq_b_output, fxn_cq_build, fxn_cq_b_wk)
output_csv(nkgth_b_output, nkgth_build, nkgth_b_wk)
output_csv(nkgyy_b_output, nkgyy_build, nkgyy_b_wk)
output_csv(jwhi_b_output, jwhi_build, jwhi_b_wk)
output_csv(fxnwhi_b_output, fxnwhi_build, fxnwhi_b_wk)
# output_csv(fptp_b_output, fptp_build, fptp_b_wk)
# output_csv(fzh_b_output, fzh_build, fzh_b_wk)

# INKJET WEEKLY BUILD PLAN
# output_csv(fxn_cq_b_output_wk, fxn_cq_build_wk, fxn_cq_b_wk_wk)
output_csv(nkgth_b_output_wk, nkgth_build_wk, nkgth_b_wk_wk)
output_csv(nkgyy_b_output_wk, nkgyy_build_wk, nkgyy_b_wk_wk)
output_csv(jwhi_b_output_wk, jwhi_build_wk, jwhi_b_wk_wk)
output_csv(fxnwhi_b_output_wk, fxnwhi_build_wk, fxnwhi_b_wk_wk)

# LASER BUILD PLAN
output_csv(jwhl_b_output, jwhl_build, jwhl_b_wk)
output_csv(fxn_cz_b_output, fxncz_build, fxncz_b_wk)
output_csv(jcuu_b_output, jcuu_build, jcuu_b_wk)
# output_csv(HPPS_b_output, hpps_build, hpps_b_wk)
output_csv(fxnwhl_b_output, fxnwhl_build, fxnwhl_b_wk)
# CANON
output_csv(canon_zs_b_output, canon_zs_build, canon_zs_b_wk)
output_csv(canon_vn_b_output, canon_vn_build, canon_vn_b_wk)
output_csv(canon_ph_b_output, canon_ph_build, canon_ph_b_wk)
output_csv(canon_jp_b_output, canon_jp_build, canon_jp_b_wk)

# INKJET BUS_UNIT CATEGORIZATION
# output_csv(inkjet_bus_output, ink_category, 'Inkjet_Categorization')

# INKJET PLANNING_CATEGORY_CATEGORIZATION
output_csv(plan_cat_path, plan_cat_df, 'PLANNING CATEGORIZATION')


# %% OUTPUT FOR NEW MPAS IF NEEDED
# os.chdir(canon_engine_output)
# for cycle in canon_engine_por['CYCLE_WK_NM'].unique():
#     find_df = canon_engine_por.loc[canon_engine_por['CYCLE_WK_NM'] == cycle].copy()
#     find_df.to_csv(cycle+'.csv',index=False)
