# -*- coding: utf-8 -*-
"""
Created on Thu Aug 24 10:44:17 2023

@author: kohm
"""
import pandas as pd
import numpy as np
import os
import re
import glob
from datetime import datetime
from dateutil.relativedelta import relativedelta
pd.set_option('display.max_columns', None)

user = os.getenv('USERPROFILE')

today = datetime.today()
today = today.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
# Change datetime if neccessary
# today = datetime(2023, 10, 1, 0, 0)
month_str = today.strftime("""%b'%y""")
# Path
plan_dir = os.path.join(user, 'HP Inc\Team Site - Cap Mgmt',
                        'New Site Cap\Change Request')
comments_dir = os.path.join(user, 'HP Inc\Team Site - Cap Mgmt',
                            'New Site Cap\Comments')
data_dir = os.path.join(user, 'HP Inc\Team Site - Cap Mgmt',
                        'New Site Cap\Data')
mapping_dir = os.path.join(user, 'HP Inc\Team Site - Cap Mgmt',
                           'New Site Cap\Mapping')
sending_dir = os.path.join(user, 'HP Inc\Team Site - Cap Mgmt',
                        'New Site Cap\Data\MPA Output')
os.chdir(plan_dir)
find_file = today.strftime('%m_%Y')
find_file = glob.glob(plan_dir + '/*' + find_file + '*.xlsx')

try:
    latest_sitecap_excel = max(find_file, key=os.path.getmtime)
    plan_filename = latest_sitecap_excel.split('\\')[-1]
except:
    pass
deduct_month = 1
while (len(find_file) == 0):
    print('No SiteCap Planning file for current month')
    last_site_month = today - relativedelta(months=deduct_month)
    today = last_site_month
    month_str = last_site_month.strftime("""%b'%y""")
    find_file = last_site_month.strftime('%m_%Y')
    find_file = glob.glob(plan_dir + '/*' + find_file + '*.xlsx')
    latest_sitecap_excel = max(find_file, key=os.path.getmtime)
    plan_filename = latest_sitecap_excel.split('\\')[-1]
    deduct_month = deduct_month + 1
# plan_filename = find_file[0].split('\\')[-1]
# month_year_str = today.strftime("""%b'%y""")
sheet_str = month_str + ' Change Request'

print('File used for current month SiteCap : ', plan_filename)
x = input('Proceed? Y/N:')
if x == 'Y':
    # %% Read Data
    os.chdir(comments_dir)
    comments_df = pd.read_csv('Comments.csv')
    # REMOVE CURRENT UPLOAD MONTH IF THERE IS
    comments_df = comments_df.loc[comments_df['POR MONTH'] != month_str].copy()
    # PAST SITECAP DATA
    os.chdir(data_dir)
    sitecap_df = pd.read_csv('IPS Capacity POR.csv',
                             parse_dates=['SITECAP_MONTH'])
    # CHECK IF CONTAINS LATEST MONTH
    # current_month_num = len(
    #     sitecap_df.loc[sitecap_df['UPLOAD_MONTH'].str.contains(month_str)])
    # if current_month_num != 0:
    #     latest_month = sitecap_df['SITECAP_MONTH'].max()
    # else:
    sitecap_df = sitecap_df.loc[sitecap_df['UPLOAD_MONTH'] != month_str].copy()
    latest_month = sitecap_df['SITECAP_MONTH'].max()
    # MAPPING DATA
    os.chdir(mapping_dir)
    family_mapping = pd.read_excel('Mapping.xlsx', sheet_name='Family Mapping')
    family_mapping['PRODUCT'] = family_mapping['PRODUCT'].str.strip()
    family_mapping['FAMILY'] = family_mapping['FAMILY'].str.strip()
    family_mapping['FAMILY'] = family_mapping['FAMILY'].str.upper()
    family_mapping = family_mapping[['PRODUCT', 'FAMILY']].drop_duplicates()
    # =============================================================================
    # SITECAP
    # =============================================================================

    def extract_last_ips(df, current_month):
        last_month = current_month - relativedelta(months=1)
        last_month_str = last_month.strftime("""%b'%y""")
        find_ips = df.loc[df['UPLOAD_MONTH'] == last_month_str].copy()
        month_num = 1
        while(len(find_ips) == 0):
            get_month = last_month - relativedelta(months=month_num)
            get_month_str = get_month.strftime("""%b'%y""")
            find_ips = df.loc[df['UPLOAD_MONTH'] == get_month_str].copy()
            month_num = month_num + 1
        return find_ips

    latest_sitecap = extract_last_ips(sitecap_df, today)

    # =============================================================================
    # PLANNING
    # =============================================================================
    os.chdir(plan_dir)
    plan = pd.read_excel(plan_filename,
                         sheet_name=sheet_str,
                         skiprows=12)
    comments = pd.read_excel(
        plan_filename,
        sheet_name='Summary Comments')

    # %% Data Cleaning

    def unpivot(df):
        # GET LAST COLUMN
        col_idx = df.columns.get_loc('Nominal Cap at SOR') + 1
        Cols = df.columns[0:col_idx]
        Dates = df.columns[col_idx:]
        df = pd.melt(df,
                     id_vars=Cols,
                     value_vars=Dates,
                     value_name='QTY (K)',
                     var_name='SITECAP_MONTH')
        df['QTY (K)'] = df['QTY (K)'].apply(pd.to_numeric, errors='coerce')
        df['QTY (K)'] = df['QTY (K)'].fillna(0)
        return df

    def text_data_clean(df, comments_df):
        df.columns = df.columns.str.strip()
        df.columns = df.columns.str.upper()

        comments_df.columns = comments_df.columns.str.strip()
        comments_df.columns = comments_df.columns.str.upper()
        comments_df = comments_df.apply(lambda x: x.str.strip())
        return df, comments_df

    def toolcol_cleanup(df, cols):
        for col in cols:
            df.loc[
                df[col].str.contains('>', na=False), col] = \
                df[col].str.rsplit('>').str[-1]
        cols.append('NOMINAL CAP AT SOR')
        df[cols] = df[cols].fillna('')
        df[cols] = df[cols].astype(str)
        str_cols = df.select_dtypes(['object'])
        df[str_cols.columns] = str_cols.apply(lambda x: x.str.strip())
        return df

    def remove_eol(df):
        # Get Second Column for NPI YR
        npi_yr_col = df.columns[1]
        npi_df = df.loc[df[npi_yr_col] != 'INTEGRATED'].copy()
        npi_df[npi_yr_col] = npi_df[npi_yr_col].str.replace("""'""", '')
        npi_df = npi_df[['PRODUCT', 'MPA', npi_yr_col]].drop_duplicates()
        npi_df = npi_df.rename(columns={npi_yr_col: 'NPI YEAR'})
        df = df.merge(npi_df, how='left', on=['PRODUCT', 'MPA'])
        df = df.drop(columns=[npi_yr_col])
        # REMOVE PRODUCT UNIQUE
        df = df.loc[df['CAPACITY TYPE'] != 'PRODUCT UNIQUE'].copy()
        products = df.groupby(['PRODUCT'])['QTY (K)'].sum().reset_index()
        eol_products = products.loc[products['QTY (K)'] == 0]['PRODUCT'].unique(
        )
        df = df.loc[~df['PRODUCT'].isin(eol_products)].copy()
        return df
    
    def clean_npi(df):
        # Get Second Column for NPI YR
        npi_yr_col = df.columns[1]
        npi_df = df.loc[df[npi_yr_col] != 'INTEGRATED'].copy()
        npi_df[npi_yr_col] = npi_df[npi_yr_col].str.replace("""'""", '')
        npi_df = npi_df[['PRODUCT', 'MPA', npi_yr_col]].drop_duplicates()
        npi_df = npi_df.rename(columns={npi_yr_col: 'NPI YEAR'})
        df = df.merge(npi_df, how='left', on=['PRODUCT', 'MPA'])
        df = df.drop(columns=[npi_yr_col])
        # REMOVE PRODUCT UNIQUE
        df = df.loc[df['CAPACITY TYPE'] != 'PRODUCT UNIQUE'].copy()
        return df

    def clean_merge(df, col, xy_bool):
        # Overwrite with Planning data
        if xy_bool:
            col_main = col + '_x'
            col_replace = col + '_y'
            df.loc[df['PRODUCT_y'].notnull(), col_main] = df[col_replace]
            df = df.drop(columns=[col_replace])
        else:
            col_replace = col + '_y'
            df.loc[df['PRODUCT_y'].notnull(), col] = df[col_replace]
            df = df.drop(columns=[col_replace])
        return df

    def combine_cols(df, col_list, new_col):
        cols = col_list

        df[new_col] = df[cols] \
            .apply(lambda row: '_'.join(row.values.astype(str)), axis=1)
        return df
    
    def pivot_output(df, current_month):
        df = df.loc[df['UPLOAD_MONTH'] == month_str].copy()
        # df['SITECAP_MONTH'] = pd.to_datetime(df['SITECAP_MONTH'],
        #                                      format='%b-%y')
        # df['SITECAP_MONTH'] = df['SITECAP_MONTH'].dt.strftime('%b-%y')
        ini_col = ['LAST MODIFIED', 'NPI YEAR', 'PL', 'PRODUCT', 'PLATFORM']
        tool_col = [col for col in df.columns if 'TOOLING' in col]
        tool_col_sort = sorted(tool_col, key=lambda x: int(x[14:16]))
        # TAKE LATEST 2 TOOLING CAP ONLY
        tool_col_sort = tool_col_sort[-2:]
        ini_col.extend(tool_col_sort)
        ini_col.extend(['MPA', 'FACTORY', 'CAPACITY TYPE', 
                        'NOMINAL CAP AT SOR'])
        df_pivot = df.pivot_table(
            index=ini_col,
            columns='SITECAP_MONTH', 
            values='QTY (K)')
        df_pivot = df_pivot.rename(
            columns = lambda x: x.strftime('%b-%y'))
        df_pivot = df_pivot.reset_index()
        return df_pivot
        

    # %% COMPARE LAST MONTH IPS SITECAP WITH LATEST PLANNING DATA
    # TO SEE IF NEW MONTH IS ADDED
    plan_latest_month = unpivot(plan)['SITECAP_MONTH'].max()
    plan_unique_months = unpivot(plan)['SITECAP_MONTH'].unique()

    if plan_latest_month > latest_month:
        copy_ips_latest = latest_sitecap.loc[
            latest_sitecap['SITECAP_MONTH'] == latest_month].copy()
        filtered_dates = [d for d in plan_unique_months if d > latest_month]
        for monthdate in filtered_dates:
            copy_ips_latest['SITECAP_MONTH'] = monthdate
            latest_sitecap = pd.concat([latest_sitecap, copy_ips_latest],
                                       ignore_index=True)
    # %% Planning Change Data
    # =============================================================================
    # Planning file (JUNE CHANGES)
    # =============================================================================

    def plan_change_cleanup(plan_change, comments,
                            month_year,
                            current_month,
                            latest_month):

        plan_change = unpivot(plan_change)

        # CLEAN TEXT DATA
        plan_change, comments = text_data_clean(plan_change, comments)

        # CLEAN UP TOOLING COLUMN
        tool_cols = [col for col in plan_change.columns if 'TOOLING' in col]
        for col in tool_cols:
            null_cells = plan_change[col].isnull()
            plan_change[col] = plan_change[col].astype(
                str).mask(null_cells, np.NaN)

        plan_change = toolcol_cleanup(plan_change, tool_cols)
        # GET ONLY THIS MONTH PLANNING MODIFIED AND COMMENTS
        plan_change = plan_change.loc[
            plan_change['LAST MODIFIED'] == month_year].copy()
        comments = comments.loc[
            comments['POR MONTH'] == month_year].copy()

        if len(plan_change) == 0:
            print('No Changes for current month')
        else:
            # GET ONLY CURRENT MONTH AND FUTURE, CURRENT MONTH COMMENTS
            plan_change = plan_change.loc[
                plan_change['SITECAP_MONTH'] >= current_month].copy()

            plan_change[tool_cols] = plan_change[tool_cols].replace('', np.nan)
            plan_change[tool_cols] = plan_change[tool_cols].replace(
                '-', np.nan)
            # remain_cols = list(set(plan_change.columns) - set(tool_cols))

            plan_change[tool_cols] = \
                plan_change.groupby(
                    ['PL', 'PRODUCT', 'PLATFORM',
                     'MPA',
                     'FACTORY'])[
                         tool_cols].fillna(method='ffill').fillna(0).astype(float)

            plan_change['QTY (K)'] = plan_change['QTY (K)'].fillna(0)
            # Remove EOL or 0 Qty
            # plan_change = remove_eol(plan_change)
            # Clean NPI Year Column
            plan_change = clean_npi(plan_change)
            
            # Rename columns for merging
            plan_change = plan_change.rename(
                columns={'PL': 'PL_y',
                         'PRODUCT': 'PRODUCT_y',
                         'PLATFORM': 'PLATFORM_y',
                         'MPA': 'MPA_y',
                         'FACTORY': 'FACTORY_y',
                         'CAPACITY TYPE': 'CAPACITY TYPE_y',
                         'SITECAP_MONTH': 'SITECAP_MONTH_y'
                         })
            # Rename NKG to Calcomp
            plan_change.loc[
                (plan_change['FACTORY_y'] == 'THAILAND')
                & (plan_change['MPA_y'].str.contains('NKG|Calcomp')),
                'MPA_y'] = 'CALCOMP TH'
            plan_change.loc[
                (plan_change['FACTORY_y'] == 'CHINA')
                & (plan_change['MPA_y'].str.contains('NKG|Calcomp')),
                'MPA_y'] = 'CALCOMP YY'
            # REMOVE FLEXTRONICS
            plan_change = plan_change.loc[
                plan_change['MPA_y'] != 'FLEXTRONICS'].copy()

        return plan_change, comments

    plan, comments = plan_change_cleanup(
        plan, comments,
        month_str, today, latest_month)

    # %% IPS Data
    # =============================================================================
    # IPS File (MAY File which is to change for July Plan Request)
    # =============================================================================

    def ips_cleanup(filename, month_year, current_month):
        ips_df = pd.read_excel(
            filename,
            sheet_name=month_year,
            skiprows=12)
        ips_comments = pd.read_excel(
            filename,
            sheet_name='Change History',
            skiprows=2)
        ips_df['Last Modified'] = ips_df['Last Modified'].fillna(
            method='ffill')

        ips_df = unpivot(ips_df)

        # CLEAN TEXT DATA
        ips_df, ips_comments = text_data_clean(ips_df, ips_comments)

        ips_comments['POR MONTH'] = ips_comments['POR MONTH'].fillna(
            method='ffill')

        # CLEAN UP TOOLING COLUMN
        tool_cols = [col for col in ips_df.columns if 'TOOLING' in col]
        ips_df = toolcol_cleanup(ips_df, tool_cols)

        # ADD ONE MONTH TO CURRENT MONTH FOR FILTERING DUE TO LAG
        ips_month = current_month + relativedelta(months=1)

        ips_df = ips_df.loc[
            ips_df['SITECAP_MONTH'] >= ips_month].copy()

        ips_df[tool_cols] = ips_df[tool_cols].replace('', np.nan)

        ips_df[tool_cols] = \
            ips_df.groupby(
                ['PL', 'PRODUCT', 'PLATFORM',
                 'MPA',
                 'FACTORY'])[
                     tool_cols].fillna(method='ffill').fillna(0).astype(float)

        ips_df['QTY (K)'] = ips_df['QTY (K)'].fillna(0)
        # Remove EOL or 0 Qty
        ips_df = remove_eol(ips_df)
        return ips_df, ips_comments

    # may_ips, may_comments = ips_cleanup(
    #     """IPS Capacity POR_May'23.xlsx""",
    #     may_str, may_date)

    # %% Combine Planning Data & IPS Data

    ips_df = latest_sitecap.loc[latest_sitecap['SITECAP_MONTH'] >= today].copy(
    )
    # ADD COMBINED TO CHECK
    # ips_df = combine_cols(ips_df, ['PRODUCT','PLATFORM','MPA',
    #                        'FACTORY'], 'COMBINED_x')
    # ips_combined = ips_df['COMBINED_x'].unique()

    # plan = combine_cols(plan, ['PRODUCT_y', 'PLATFORM_y','MPA_y',
    #                            'FACTORY_y'], 'COMBINED_y')
    # ONLY GET CURRENT MONTH MODIFIED AND IGNORE THOSE THAT IPS HAVE
    # plan = plan.loc[
    #     (~plan['COMBINED_y'].isin(ips_combined)) & \
    #     (plan['LAST MODIFIED'] == month_str)].copy()
    # DROP ALL COMBINED
    # ips_df = ips_df.drop(columns=['COMBINED_x'])
    # plan = plan.drop(columns=['COMBINED_y'])

    if len(plan) != 0:
        merged_df = ips_df.merge(plan,
                                 how='outer',
                                 left_on=['PL', 'PRODUCT',
                                          'PLATFORM', 'MPA',
                                          'FACTORY', 'CAPACITY TYPE',
                                          'SITECAP_MONTH'],
                                 right_on=['PL_y', 'PRODUCT_y',
                                           'PLATFORM_y', 'MPA_y',
                                           'FACTORY_y', 'CAPACITY TYPE_y',
                                           'SITECAP_MONTH_y'])
        # FIND PLANNING CHANGES
        # product_unique = plan['PRODUCT_y'].unique()
        # OVERWRITE QUANTITY CHANGES WHEREBY IPS AND PLANNING QTY IS NOT NULL
        # AND DIFFERENT
        merged_df.loc[
            (merged_df['QTY (K)_x'].notnull()) & \
            (merged_df['QTY (K)_y'].notnull()) & \
            (merged_df['QTY (K)_y'] != merged_df['QTY (K)_x']),
            'QTY (K)_x'] = merged_df['QTY (K)_y']

        os.chdir(data_dir)
        col_xy = ['LAST MODIFIED',
                  'NOMINAL CAP AT SOR',
                  'QTY (K)']
        col_main = ['PL', 'PLATFORM',
                    'MPA', 'FACTORY', 'CAPACITY TYPE',
                    'SITECAP_MONTH']
        for col in col_xy:
            merged_df = clean_merge(merged_df, col, True)
        # CLEAN NPI YEAR
        if 'NPI YEAR_y' in merged_df.columns:
            merged_df.loc[
                (merged_df['PRODUCT_y'].notnull()) &
                (merged_df['NPI YEAR_y'].notnull()),
                'NPI YEAR_x'] = merged_df['NPI YEAR_y']
            merged_df = merged_df.drop(columns=['NPI YEAR_y'])
        for col in col_main:
            merged_df = clean_merge(merged_df, col, False)
        toolcap = [col for col in merged_df.columns if 'TOOLING' in col]
        toolcap_x = [s for s in toolcap if '_x' in s]
        toolcap_x_sort = sorted(toolcap_x, key=lambda x: int(x[14:16]))

        toolcap_y = [s for s in toolcap if '_y' in s]
        toolcap_y_sort = sorted(toolcap_y, key=lambda x: int(x[14:16]))
        for x, y in zip(toolcap_x_sort, toolcap_y_sort):
            merged_df.loc[merged_df[x].isnull(), y] = merged_df[y]
        merged_df.loc[merged_df['PRODUCT'].isnull(),
                      'PRODUCT'] = merged_df['PRODUCT_y']
        merged_df = merged_df.loc[:, ~
                                  merged_df.columns.str.endswith('_y')]
        merged_df.columns = merged_df.columns.str.strip('_x')
        # TO CHANGE LOGIC BY PL, PRODUCT, PLATFORM, MPA & FACTORY EXCLUDE?
        float_cols = merged_df.select_dtypes(['float'])
        merged_df[float_cols.columns] = merged_df[float_cols.columns].fillna(0)
        merged_df['UPLOAD_MONTH'] = month_str
        # Comments
        comments_df = pd.concat([comments_df, comments], ignore_index=True)
        # Output and Overwrite Family Mapping
        os.chdir(data_dir)
        merged_df = pd.concat([sitecap_df, merged_df], ignore_index=True)
        merged_df = merged_df.drop(columns=['FAMILY'])
        merged_df = merged_df.merge(family_mapping, how='left', on='PRODUCT')
        # merged_df.loc[
        #     merged_df['CAPACITY TYPE'].str.contains('COMBINED PLAT'),
        #     'CAPACITY TYPE'] = 'PRODUCT UNIQUE'
        merged_df.to_csv('IPS Capacity POR.csv', index=False,
                         date_format='%Y-%m-%d')
        os.chdir(comments_dir)
        comments_df.to_csv('Comments.csv', index=False)
        # ADDED TO PIVOT AND SEND TO MPA
        cur_merged_df = pivot_output(merged_df, month_str)
        os.chdir(sending_dir)
        cur_merged_df.to_excel('IPS Capacity POR_' + month_str + '.xlsx',
                               index=False)
        for mpa in cur_merged_df['MPA'].unique():
            out_df = cur_merged_df.loc[cur_merged_df['MPA'] == mpa].copy()
            out_df.to_excel(month_str + '_' + mpa + '.xlsx',index=False)
        print('Site Cap Tool executed successfully')
    else:
        ips_df['UPLOAD_MONTH'] = month_str
        merged_df = pd.concat([sitecap_df, ips_df], ignore_index=True)
        # merged_df.loc[
        #     merged_df['CAPACITY TYPE'].str.contains('COMBINED PLAT'),
        #     'CAPACITY TYPE'] = 'PRODUCT UNIQUE'
        print('No update from planning for current month')
        os.chdir(data_dir)
        merged_df.to_csv('IPS Capacity POR.csv', index=False,
                         date_format='%Y-%m-%d')
        # ADDED TO PIVOT AND SEND TO MPA
        cur_merged_df = pivot_output(merged_df, month_str)
        os.chdir(sending_dir)
        cur_merged_df.to_excel('IPS Capacity POR_' + month_str + '.xlsx',
                               index=False)
        for mpa in cur_merged_df['MPA'].unique():
            out_df = cur_merged_df.loc[cur_merged_df['MPA'] == mpa].copy()
            out_df.to_excel(month_str + '_' + mpa + '.xlsx',index=False)
        print('Site Cap Tool executed successfully')
else:
    print('Please put planning file into correct directory')
