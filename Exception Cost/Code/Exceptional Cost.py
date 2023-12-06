# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 13:57:58 2022

@author: kohm
"""

import pandas as pd
import os
import glob
from datetime import datetime
# import numpy as np
from dateutil.relativedelta import relativedelta
import time
pd.set_option('display.max_columns', None)

user = os.getenv('USERPROFILE')
# =============================================================================
# Set Directory
# =============================================================================
pr_tracker_dir = os.path.join(user, 'HP Inc\Team Site - FY20 Cost Trackers')
db_pr_dir = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsx_PRTracker')
db_finance = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsx_Budget\Finance')
db_budget = os.path.join(user, 'HP Inc\PrintOpsDB - DBxlsx_Budget\Budget')
db_projected = os.path.join(
    user, 'HP Inc\PrintOpsDB - DBxlsx_Budget\Projected Exit')
os.chdir(pr_tracker_dir)

# MIGHT NEED TO CHANGE FOR LATEST EXCEL FILE
month_range = pd.date_range(start='10/10/2021', periods=480, freq='M')
month_df = pd.DataFrame({'Month': month_range})
month_df['Month'] = month_df['Month'].dt.strftime('%Y-%m-01')
month_df['Month'] = month_df['Month'].apply(pd.to_datetime)
month_df['Quarter'] = month_df['Month'].dt.to_period('Q-OCT')
month_df['File'] = month_df['Month'].dt.strftime('%b_%y')
month_df['Quarter'] = month_df['Quarter'].astype(str)
month_df['Quarter'] = 'FY' + month_df['Quarter']
month_df['Quarter'] = month_df['Quarter'].str[0:6] + \
    '_' + month_df['Quarter'].str[6:8]
# Add Financial Year
month_df['FY'] = month_df['Quarter'].str[0:6]
# Recover past data
# excel_details = datetime(2022, 7, 29).strftime('%b_%y')
excel_details = datetime.today().strftime('%b_%y')
PR_excel_files = glob.glob(pr_tracker_dir + '/*' + 'ombine*' + excel_details
                           + '*.xlsx')
deduct_month = 1

while (len(PR_excel_files) == 0):
    print('No PR Tracker for current month')
    last_pr_month = datetime.today() - relativedelta(months=deduct_month)
    excel_details = last_pr_month.strftime('%b_%y')
    PR_excel_files = glob.glob(pr_tracker_dir + '/*' + 'ombine*'
                               + excel_details + '*.xlsx')
    latest_pr_excel = max(PR_excel_files, key=os.path.getmtime)
    filename = latest_pr_excel.split('\\')[-1]
    deduct_month = deduct_month + 1

latest_pr_excel = max(PR_excel_files, key=os.path.getmtime)
filename = latest_pr_excel.split('\\')[-1]


# OLD FILES
# excel_details = datetime(2021,12,1).strftime('%b_%y')
# PR_excel_files = glob.glob(pr_tracker_dir + '/*' + 'combined*'
#                            + excel_details + '.xlsx')


print('File used for PR Tracker : ', filename)
x = input('Proceed? Y/N:')
# =============================================================================
# PR TRACKER
# =============================================================================
if x == 'Y':
    Exclude_Cost = pd.read_excel('ExceptionCost_Tool.xlsx',
                                 sheet_name='Mapping',
                                 skiprows=1,
                                 usecols='G')
    Exclude_Cost = Exclude_Cost.dropna()

    PR_df = pd.read_excel(filename, sheet_name='Sheet1')
    # Remove intercompany bill
    PR_df = PR_df.loc[PR_df['IC bill/Not Required/Reclass'].str.upper()
                      != 'IC BILL'].copy()
    # Remove status that are cancelled
    PR_df = PR_df.loc[PR_df['Status'].str.upper().isin(
        ['APPROVED FOR PAYMENT', 'CLOSED (INVOICE ISSUED)'])].copy()
    # uni_quarter = PR_df['Fiscal Quarter'].unique()

    def filter_df(df):
        # Get only relevant latest Fiscal Quarter and latest month
        # find_last = month_df.loc[month_df['Quarter'].isin(uni_quarter)]
        # find_last = find_last.iloc[-1,:]
        # current_quarter = find_last['Quarter']
        # Recover Past Data
        # current_month = datetime(2022, 12, 1)
        # current_month = datetime(2023, 10, 1)-relativedelta(months=1)
        current_month = datetime.today() - relativedelta(months=1)
        current_month = current_month.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0)
        current_month_date = current_month.strftime('%Y-%m-01')
        today = datetime.today().replace(day=1)
        today_str = today.strftime('%Y-%m-01')
        # IF INTER CROSS FY
        if (current_month.month == 10):
            # today = datetime.today().replace(day=1)
            # today_str = today.strftime('%Y-%m-01')
            current_FY = month_df.loc[month_df['Month'].isin(
                [today_str, current_month_date])]['FY'].unique()
            ori_fy = month_df.loc[month_df['FY'].isin(
                current_FY)]['Quarter'].unique()
            current_FY = month_df.loc[month_df['FY'].isin(
                current_FY)]['Quarter'].unique()
            budget_FY = current_FY[3:]
            previous_FY = current_FY[3]
            current_FY = current_FY[4:]
            df = df.loc[(df['Fiscal Quarter'].isin(current_FY))
                        & (df['Fiscal Quarter'].notnull())
                        ].copy()
            os.chdir(db_pr_dir)
            # CHECK FOR FILE
            check_name = current_month.strftime('%Y%m_') + 'PRTracker.xlsx'
            if os.path.exists(check_name):
                os.remove(check_name)
                time.sleep(1)
            xlsx_files = glob.glob(db_pr_dir + '/*' + '.xlsx')
            latest_xlsx = max(xlsx_files, key=os.path.getctime)
            filename = latest_xlsx.split('\\')[-1]
            hist_df = pd.read_excel(filename, sheet_name='Sheet1')
            hist_df = hist_df.loc[(hist_df['Fiscal Quarter'].isin([previous_FY]))
                                  & (hist_df['Fiscal Quarter'].notnull())
                                  ].copy()
            os.chdir(pr_tracker_dir)
            df = pd.concat([hist_df, df], ignore_index=True)
        elif (current_month.month >= 4 and current_month.month <= 6):
            current_FY = month_df.loc[month_df['Month'].isin(
                [current_month_date])]['FY'].unique()
            ori_fy = month_df.loc[month_df['FY'].isin(
                current_FY)]['Quarter'].unique()
            current_FY = month_df.loc[month_df['FY'].isin(
                current_FY)]['Quarter'].unique()
            budget_FY = current_FY
            previous_FY = current_FY[0:2]
            current_FY = current_FY[2:]
            df = df.loc[(df['Fiscal Quarter'].isin(current_FY))
                        & (df['Fiscal Quarter'].notnull())
                        ].copy()
            os.chdir(db_pr_dir)
            # CHECK FOR FILE
            check_name = current_month.strftime('%Y%m_') + 'PRTracker.xlsx'
            if os.path.exists(check_name):
                os.remove(check_name)
                time.sleep(1)
            xlsx_files = glob.glob(db_pr_dir + '/*' + '.xlsx')
            latest_xlsx = max(xlsx_files, key=os.path.getmtime)
            filename = latest_xlsx.split('\\')[-1]
            hist_df = pd.read_excel(filename, sheet_name='Sheet1')
            hist_df = hist_df.loc[(hist_df['Fiscal Quarter'].isin(previous_FY))
                                  & (hist_df['Fiscal Quarter'].notnull())
                                  ].copy()
            os.chdir(pr_tracker_dir)
            df = pd.concat([hist_df, df], ignore_index=True)
        elif (current_month.month >= 7 and current_month.month <= 9):
            current_FY = month_df.loc[month_df['Month'].isin(
                [current_month_date])]['FY'].unique()
            ori_fy = month_df.loc[month_df['FY'].isin(
                current_FY)]['Quarter'].unique()
            current_FY = month_df.loc[month_df['FY'].isin(
                current_FY)]['Quarter'].unique()
            budget_FY = current_FY
            previous_FY = current_FY[0:3]
            current_FY = current_FY[3:]
            df = df.loc[(df['Fiscal Quarter'].isin(current_FY))
                        & (df['Fiscal Quarter'].notnull())
                        ].copy()
            os.chdir(db_pr_dir)
            # CHECK FOR FILE
            check_name = current_month.strftime('%Y%m_') + 'PRTracker.xlsx'
            if os.path.exists(check_name):
                os.remove(check_name)
                time.sleep(1)
            xlsx_files = glob.glob(db_pr_dir + '/*' + '.xlsx')
            latest_xlsx = max(xlsx_files, key=os.path.getctime)
            filename = latest_xlsx.split('\\')[-1]
            hist_df = pd.read_excel(filename, sheet_name='Sheet1')
            hist_df = hist_df.loc[(hist_df['Fiscal Quarter'].isin(previous_FY))
                                  & (hist_df['Fiscal Quarter'].notnull())
                                  ].copy()
            os.chdir(pr_tracker_dir)
            df = pd.concat([hist_df, df], ignore_index=True)
        elif (current_month.month >= 1 and current_month.month <= 3):
            current_FY = month_df.loc[month_df['Month'].isin(
                [current_month_date])]['FY'].unique()
            ori_fy = month_df.loc[month_df['FY'].isin(
                current_FY)]['Quarter'].unique()
            current_FY = month_df.loc[month_df['FY'].isin(
                current_FY)]['Quarter'].unique()
            budget_FY = current_FY
            previous_FY = current_FY[0:1]
            current_FY = current_FY[1:]
            df = df.loc[(df['Fiscal Quarter'].isin(current_FY))
                        & (df['Fiscal Quarter'].notnull())
                        ].copy()
            os.chdir(db_pr_dir)
            # CHECK FOR FILE
            check_name = current_month.strftime('%Y%m_') + 'PRTracker.xlsx'
            if os.path.exists(check_name):
                os.remove(check_name)
                time.sleep(1)
            xlsx_files = glob.glob(db_pr_dir + '/*' + '.xlsx')
            latest_xlsx = max(xlsx_files, key=os.path.getctime)
            filename = latest_xlsx.split('\\')[-1]
            hist_df = pd.read_excel(filename, sheet_name='Sheet1')
            hist_df = hist_df.loc[(hist_df['Fiscal Quarter'].isin(previous_FY))
                                  & (hist_df['Fiscal Quarter'].notnull())
                                  ].copy()
            os.chdir(pr_tracker_dir)
            df = pd.concat([hist_df, df], ignore_index=True)
        else:
            current_FY = month_df.loc[month_df['Month']
                                      == current_month_date]['FY'].item()
            current_FY = month_df.loc[month_df['FY']
                                      == current_FY]['Quarter'].unique()
            budget_FY = current_FY
            df = df.loc[(df['Fiscal Quarter'].isin(current_FY))
                        & (df['Fiscal Quarter'].notnull())
                        ].copy()
            hist_df = pd.DataFrame()
            os.chdir(pr_tracker_dir)

        # # Exclude E&O Cost type
        for cost_type in Exclude_Cost['Exclude Cost Type that contains']:
            df = df.loc[~df['Cost Type'].str.contains(
                cost_type, na=False)].copy()

        df['UploadID'] = current_month.strftime('%Y %b')
        df['UploadFilter'] = df['UploadID'] + ' -' + ' ' + df['Fiscal Quarter']
        df['Cost Type'] = df['Cost Type'].str.rstrip()
        df['Cost Type'] = df['Cost Type'].str.upper()
        return df, ori_fy, current_month, hist_df, budget_FY

    PR_df, current_FY, current_month, hist_df, budget_FY = filter_df(PR_df)

    # =============================================================================
    # Exceptional Cost Tool Data
    # =============================================================================
    Cost_Cat = pd.read_excel('ExceptionCost_Tool.xlsx', sheet_name='Mapping',
                             skiprows=1,
                             usecols='A:B')
    Cost_Cat['Cost Type'] = Cost_Cat['Cost Type'].str.upper()

    MPA = pd.read_excel('ExceptionCost_Tool.xlsx', sheet_name='Mapping',
                        skiprows=1,
                        usecols='D:E')
    MPA = MPA.dropna()

    Finance = pd.read_excel('ExceptionCost_Tool.xlsx', sheet_name='Finance',
                            usecols='A:B')

    Projected_Exit = pd.read_excel('ExceptionCost_Tool.xlsx',
                                   sheet_name='Projected Exit')

    Budget_df = pd.read_excel('ExceptionCost_Tool.xlsx',
                              sheet_name='Budget', skiprows=1)
    Budget_df = Budget_df.loc[Budget_df['MPA'].notnull()].copy()
    Budget_df['Cost Type'] = Budget_df['Cost Type'].str.upper()

    Budget_Col = Budget_df.columns[2:]

    Budget_df = Budget_df.melt(id_vars=['MPA', 'Cost Type'], value_vars=Budget_Col,
                               var_name='Fiscal Quarter', value_name='Budget')

    Budget_df = Budget_df[['Fiscal Quarter', 'Budget', 'MPA', 'Cost Type']]
    Budget_df['Budget'] = Budget_df['Budget'].fillna(0)
    # Added to get current FY
    Budget_df = Budget_df.loc[Budget_df['Fiscal Quarter'].isin(budget_FY)]
    # filter_future = month_df.loc[month_df['Quarter'] == current_quarter]['Month'].min()
    # filter_future = month_df.loc[month_df['Month'] <= filter_future]['Quarter'].unique()
    # Budget_Current_df = Budget_df.loc[Budget_df['Fiscal Quarter'] == current_quarter].copy()
    # Budget_Future_df = Budget_df.loc[~Budget_df['Fiscal Quarter'].isin(filter_future)].copy()

    # =============================================================================
    # PLAN G
    # =============================================================================
    plan_g_df = pd.read_excel('ExceptionCost_Tool.xlsx',
                              sheet_name='Excluded Projects', skiprows=1)

    plan_g_df['Cost Type'] = plan_g_df['Cost Type'].str.upper()
    # plan_g_col = plan_g_df.columns[2:]
    # plan_g_df = plan_g_df.melt(id_vars=['MPA','Cost Type'],value_vars=plan_g_df,
    #                            var_name = 'Fiscal Quarter',value_name='Plan G')
    plan_g_df['UploadID'] = current_month.strftime('%Y %b')
    plan_g_df['UploadFilter'] = plan_g_df['UploadID'] + \
        ' -' + ' ' + plan_g_df['Fiscal Quarter']

    # MAP MPA
    for i in range(0, len(MPA)):
        PR_df.loc[PR_df['S/N'].str.startswith(MPA['MPA_PRTracker'][i], na=False), 'MPA'] \
            = MPA['MPA_ExcepCost'][i]

    cost_dict = Cost_Cat.set_index('Cost Type').to_dict()
    cost_dict = cost_dict['Cost Category']

    for i in range(0, len(plan_g_df)):
        plan_g_cost = plan_g_df['Cost'][i]
        claim = PR_df.loc[(PR_df['MPA'] == plan_g_df['MPA'][i])
                          & (PR_df['Cost Type'] == plan_g_df['Cost Type'][i])
                          & (PR_df['Fiscal Quarter'] == plan_g_df['Fiscal Quarter'][i])].head(1)
        if len(claim) != 0:
            index_num = claim.index[0]
            # print(index_num)
            claim = claim['Claim Amount'].item()
            deduct = claim - plan_g_cost
            PR_df.at[index_num, 'Claim Amount'] = deduct
        # else:
        #     break

    # =============================================================================
    # Get only latest projected exit (Fiscal Quarter)
    # =============================================================================
    Projected_Exit = Projected_Exit.loc[Projected_Exit['Fiscal Quarter'].isin(
        current_FY)]

    PR_df['Cost Category'] = PR_df['Cost Type'].map(cost_dict)

    Budget_df['Cost Category'] = Budget_df['Cost Type'].map(cost_dict)
    # Budget_Current_df['Cost Category'] = Budget_Current_df['Cost Type'].map(cost_dict)
    # Budget_Future_df['Cost Category'] = Budget_Future_df['Cost Type'].map(cost_dict)

    Finance['UploadID'] = current_month.strftime('%Y %b')
    Finance['UploadFilter'] = Finance['UploadID'] + \
        ' -' + ' ' + Finance['Fiscal Quarter']

    Projected_Exit['UploadID'] = current_month.strftime('%Y %b')
    Projected_Exit['UploadFilter'] = Projected_Exit['UploadID'] + \
        ' -' + ' ' + Projected_Exit['Fiscal Quarter']

    Budget_df['UploadID'] = current_month.strftime('%Y %b')
    Budget_df['UploadFilter'] = Budget_df['UploadID'] + \
        ' -' + ' ' + Budget_df['Fiscal Quarter']
    # Budget_Current_df['UploadID'] = current_month.strftime('%Y %b')
    # Budget_Current_df['UploadFilter'] = Budget_Current_df['UploadID'] + ' -' + ' '+ Budget_Current_df['Fiscal Quarter']
    # os.chdir(db_budget_fut)
    # Budget_Future_df['UploadID'] = current_month.strftime('%Y %b')
    # Budget_Future_df['UploadFilter'] = Budget_Future_df['UploadID'] + ' -' + ' '+ Budget_Future_df['Fiscal Quarter']
    # PR_df = pd.concat([hist_df,PR_df],ignore_index=True)

    # =============================================================================
    # Output Data To Location
    # =============================================================================
    os.chdir(db_pr_dir)
    file_header = current_month.strftime('%Y%m_')
    PR_df.to_excel(file_header + 'PRTracker.xlsx', index=False)
    # os.chdir(db_budget_cur)
    # Budget_Current_df.to_excel(file_header + 'Budget.xlsx',index=False)
    # os.chdir(db_budget_fut)
    # Budget_Future_df.to_excel(file_header + 'Budget.xlsx',index=False)
    os.chdir(db_budget)
    Budget_df.to_excel(file_header + 'Budget.xlsx', index=False)
    os.chdir(db_finance)
    Finance.to_excel(file_header + 'Finance.xlsx', index=False)
    os.chdir(db_projected)
    Projected_Exit.to_excel(file_header + 'Projected.xlsx', index=False)
    print('Exceptional Cost Tool executed successfully')
