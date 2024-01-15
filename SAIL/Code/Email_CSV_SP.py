# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import os
from win32com.client import Dispatch
from datetime import datetime, timedelta
import zipfile
import time
import shutil
import glob
import pandas as pd
pd.set_option('display.max_columns', None)

user = os.getenv('USERPROFILE')
os.startfile("outlook")
time.sleep(300)
# %% INKJET
# EXPORT DATA STARTS 1 MAR 2022 (CREATED ON)
# =============================================================================
# NKG YY
# =============================================================================
nkgyy_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                              'NKG YY\CSV')
nkgyy_data_path = os.path.join(user, 'OneDrive - HP Inc\Backup Data',
                               r'FACTORY REPORT\NKG YY')
nkgyy_subject = 'NKG YY Factory Purchase Order Report'
# =============================================================================
# NKG TH
# =============================================================================
nkgth_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                              'NKG TH\CSV')
nkgth_data_path = os.path.join(user, 'OneDrive - HP Inc\Backup Data',
                               r'FACTORY REPORT\NKG TH')
nkgth_subject = 'NKG TH Factory Purchase Order Report'
# =============================================================================
# JWH INK
# =============================================================================
jwhi_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'JWH INK\CSV')
jwhi_subject = 'JWH INK Factory Order Purchase Report'
# =============================================================================
# FXNWH INK
# =============================================================================
fxnwhi_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                               'FXN WH INK\CSV')
# %% LASER
# EXPORT DATA STARTS 1 MAR 2022 (CREATED ON)
# =============================================================================
# JWH LASER
# =============================================================================
jwhl_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'JWH LASER\CSV')
jwhl_data_path = os.path.join(user, 'OneDrive - HP Inc\Backup Data',
                              r'FACTORY REPORT\JWH')
jwhl_subject = 'JWH LASER Factory Purchase Order Report'
# =============================================================================
# FXN WH LASER (HPPS CHANGE TO THIS)
# =============================================================================
fxnwhl_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                               'FXN WH LASER\CSV')
fxnwhl_data_path = os.path.join(user, 'OneDrive - HP Inc\Backup Data',
                                r'FACTORY REPORT\FXN WH')
fxnwhl_subject = 'FXN WH Factory Purchase Order Report'
# =============================================================================
# JABIL CUU
# =============================================================================
jcuu_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'JABIL CUU\CSV')
jcuu_data_path = os.path.join(user, 'OneDrive - HP Inc\Backup Data',
                              r'FACTORY REPORT\JCUU')
jcuu_subject = 'JABIL CUU Factory Purchase Order Report'
# =============================================================================
# FXN CZ
# =============================================================================
fcz_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                            'FXN CZ\CSV')
fcz_data_path = os.path.join(user, 'OneDrive - HP Inc\Backup Data',
                             r'FACTORY REPORT\FXN CZ')
fcz_subject = 'FXN CZ Factory Purchase Order Report'
# =============================================================================
# CANON EUROPA
# =============================================================================
ceuro_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                              'CANON\EUROPA\CSV')
ceuro_data_path = os.path.join(user, 'OneDrive - HP Inc\Backup Data',
                               r'FACTORY REPORT\CANON EUROPA')
ceuro_subject = 'CANON EUROPA Factory Purchase Order Report'
# =============================================================================
# CANON USA SG
# =============================================================================
cusa_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'CANON', 'USA SG\CSV')
cusa_data_path = os.path.join(user, 'OneDrive - HP Inc\Backup Data',
                              r'FACTORY REPORT\CANON USA')
cusa_subject = 'CANON USA SG Factory Purchase Order Report'

# %% Function


def output_csv(csv_path, data_csv_path, subject):
    # =============================================================================
    # Set up connection to Outlook & Save to SP
    # =============================================================================
    outlook = Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder("6")
    # datafolder = inbox.Folders['SHIPMENT DATA']
    deleted = outlook.GetDefaultFolder("3")
    all_inbox = inbox.Items

    # received_dt = datetime.now() - timedelta(days=1)
    received_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    received_dt = received_dt - timedelta(days=1)
    received_dt = received_dt.strftime('%d-%m-%y')
    all_inbox = all_inbox.restrict("[ReceivedTime] >= '" + received_dt + "'")

    all_inbox = all_inbox.Restrict(
        "[Subject] = '{}'".format(subject))
    # for message in all_inbox:
    #     print(message.subject)
    sap_list = []
    for message in all_inbox:
        sap_list.append(message)
        if (len(sap_list) == 0):
            print(message + ' export has failed!')
        else:
            attachments = sap_list[-1].Attachments
            attachment = attachments.Item(1)
            attachment_name = str(attachment)
            csv_name = attachment_name[:-4] + '.csv'
            # Added export name and new path to dump
            export_name = attachment_name[11:]
            dump_path = os.path.dirname(csv_path)
            attachment.SaveASFile(dump_path + '\\' + export_name)
            time.sleep(1)
            if data_csv_path is not None:
                attachment.SaveASFile(data_csv_path + '\\' + attachment_name)
            print(attachment_name + ' has been exported successfully')
            time.sleep(1)
            os.chdir(dump_path)
            zip_file = zipfile.ZipFile(export_name)
            zip_file.extract('Factory Purchase Order Report_V.csv',
                             path=csv_path)
            time.sleep(1)
            os.chdir(csv_path)
            os.replace('Factory Purchase Order Report_V.csv',
                       'Factory Purchase Order Report.csv')
            if data_csv_path is not None:
                zip_file.extract('Factory Purchase Order Report_V.csv',
                                 path=data_csv_path + '\\CSV')
                zip_file.close()
                os.chdir(data_csv_path + '\\CSV')

                os.replace('Factory Purchase Order Report_V.csv', csv_name)
            else:
                zip_file.close()
            message.Unread = False
            message.move(deleted)
            print(attachment_name + ' has been extracted successfully')

    # for mail in reversed(datafolder.Items): # just tried deleting Items in reverse order
    #     mail.Delete()


# %% Output to respective directories
# FLEX ZH, FLEX PTP AND HPPS STOPPED
# =============================================================================
# INKJET
# =============================================================================
output_csv(nkgyy_csv_path, None, nkgyy_subject)
output_csv(nkgth_csv_path, None, nkgth_subject)
output_csv(jwhi_csv_path, None, jwhi_subject)
# =============================================================================
# LASER
# =============================================================================
output_csv(jwhl_csv_path, None, jwhl_subject)
output_csv(fxnwhl_csv_path, None, fxnwhl_subject)
output_csv(jcuu_csv_path, None, jcuu_subject)
output_csv(fcz_csv_path, None, fcz_subject)
output_csv(ceuro_csv_path, None, ceuro_subject)
output_csv(cusa_csv_path, None, cusa_subject)
# =============================================================================
# MOVE FXN WH FACTORY PURCHASE ORDER REPORT TO INKJET DIRECTORY
# =============================================================================
# ZIP
fxnwhl_zip_path = os.path.dirname(fxnwhl_csv_path)
os.chdir(fxnwhl_zip_path)
fxnwhl_zip_files = glob.glob(fxnwhl_zip_path + '/*' + '.zip')
fxnwhl_latest_zip = max(fxnwhl_zip_files, key=os.path.getctime)
fxnwhi_path = os.path.dirname(fxnwhi_csv_path)
shutil.copy(fxnwhl_latest_zip, fxnwhi_path)
os.chdir(fxnwhi_path)
fxnwhi_zip_file = zipfile.ZipFile(fxnwhl_latest_zip, mode='r')
fxnwhi_zip_file.extract('Factory Purchase Order Report_V.csv',
                        path=fxnwhi_csv_path)
os.chdir(fxnwhi_csv_path)
os.replace('Factory Purchase Order Report_V.csv',
           'Factory Purchase Order Report.csv')

time.sleep(60)
os.system('taskkill /im outlook.exe /f')
os.system('reg add HKEY_LOCAL_MACHINE\\SOFTWARE\\Microsoft\\Office\\16.0\\Outlook\\Security\\ /v ObjectModelGuard /t REG_DWORD /d 2 /f')
