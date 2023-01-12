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

user = os.getenv('USERPROFILE')

# %% INKJET
# EXPORT DATA STARTS 1 MAR 2022 (CREATED ON)
# =============================================================================
# NKG YY
# =============================================================================
nkgyy_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                              'NKG YY\ZIP')
nkgyy_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                              'NKG YY\CSV')
nkgyy_subject = 'NKG YY Factory Purchase Order Report'
# =============================================================================
# NKG TH
# =============================================================================
nkgth_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                              'NKG TH\ZIP')
nkgth_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                              'NKG TH\CSV')
nkgth_subject = 'NKG TH Factory Purchase Order Report'
# =============================================================================
# FLEX ZH
# =============================================================================
fzh_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                            'FLEX ZH\ZIP')
fzh_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                            'FLEX ZH\CSV')
fzh_subject = 'FLEX ZH Factory Purchase Order Report'
# =============================================================================
# FLEX PTP
# =============================================================================
fptp_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'FLEX PTP\ZIP')
fptp_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'FLEX PTP\CSV')
fptp_subject = 'FLEX PTP-Factory Purchase Order Report'
# =============================================================================
# FXN CQ
# =============================================================================
fcq_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                            'FXN CQ\ZIP')
fcq_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                            'FXN CQ\CSV')
fcq_subject = 'FXN CQ Factory Purchase Order Report'
# =============================================================================
# JWH INK
# =============================================================================
jwhi_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'JWH INK\ZIP')
jwhi_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'JWH INK\CSV')
jwhi_subject = 'JWH INK Factory Order Purchase Report'
# =============================================================================
# FXNWH INK
# =============================================================================
fxnwhi_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'FXN WH INK\ZIP')
fxnwhi_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'FXN WH INK\CSV')
# %% LASER
# EXPORT DATA STARTS 1 MAR 2022 (CREATED ON)
# =============================================================================
# JWH LASER
# =============================================================================
jwhl_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'JWH LASER\ZIP')
jwhl_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'JWH LASER\CSV')
jwhl_subject = 'JWH LASER Factory Purchase Order Report'
# =============================================================================
# HPPS
# =============================================================================
hpps_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'HPPS\ZIP')
hpps_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'HPPS\CSV')
hpps_subject = 'HPPS Factory Purchase Order Report'
# =============================================================================
# FXN WH LASER (HPPS CHANGE TO THIS)
# =============================================================================
fxnwhl_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                               'FXN WH LASER\ZIP')
fxnwhl_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                               'FXN WH LASER\CSV')
fxnwhl_subject = 'FXN WH Factory Purchase Order Report'
# =============================================================================
# JABIL CUU
# =============================================================================
jcuu_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'JABIL CUU\ZIP')
jcuu_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'JABIL CUU\CSV')
jcuu_subject = 'JABIL CUU Factory Purchase Order Report'
# =============================================================================
# FXN CZ
# =============================================================================
fcz_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                            'FXN CZ\ZIP')
fcz_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                            'FXN CZ\CSV')
fcz_subject = 'FXN CZ Factory Purchase Order Report'
# =============================================================================
# CANON EUROPA
# =============================================================================
ceuro_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                              'CANON\EUROPA\ZIP')
ceuro_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                              'CANON\EUROPA\CSV')
ceuro_subject = 'CANON EUROPA Factory Purchase Order Report'
# =============================================================================
# CANON USA SG
# =============================================================================
cusa_zip_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'CANON', 'USA SG\ZIP')
cusa_csv_path = os.path.join(user, 'HP Inc\PrintOpsDB - DB_DailyOutput\Data',
                             'CANON', 'USA SG\CSV')
cusa_subject = 'CANON USA SG Factory Purchase Order Report'

# %% Function


def output_csv(zip_path, csv_path, subject):
    # =============================================================================
    # Set up connection to Outlook & Save to SP
    # =============================================================================
    outlook = Dispatch("Outlook.Application").GetNamespace("MAPI")
    inbox = outlook.GetDefaultFolder("6")
    datafolder = inbox.Folders['SHIPMENT DATA']
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
            attachment.SaveASFile(zip_path + '\\' + attachment_name)
            print(attachment_name + ' has been exported successfully')
            time.sleep(1)
            os.chdir(zip_path)
            zip_file = zipfile.ZipFile(attachment_name, mode='r')
            zip_file.extract('Factory Purchase Order Report.csv',
                             path=csv_path)
            zip_file.close()
            message.Unread = False
            message.move(datafolder)
            print(attachment_name + ' has been extracted successfully')
            
    for mail in reversed(datafolder.Items): # just tried deleting Items in reverse order
        mail.Delete()
    

# %% Output to respective directories
# FLEX ZH, FLEX PTP AND HPPS STOPPED
# =============================================================================
# INKJET
# =============================================================================
output_csv(nkgyy_zip_path, nkgyy_csv_path, nkgyy_subject)
output_csv(nkgth_zip_path, nkgth_csv_path, nkgth_subject)
# output_csv(fzh_zip_path, fzh_csv_path, fzh_subject)
# output_csv(fptp_zip_path, fptp_csv_path, fptp_subject)
output_csv(fcq_zip_path, fcq_csv_path, fcq_subject)
output_csv(jwhi_zip_path, jwhi_csv_path, jwhi_subject)
# =============================================================================
# LASER
# =============================================================================
output_csv(jwhl_zip_path, jwhl_csv_path, jwhl_subject)
# output_csv(hpps_zip_path, hpps_csv_path, hpps_subject)
output_csv(fxnwhl_zip_path, fxnwhl_csv_path, fxnwhl_subject)
output_csv(jcuu_zip_path, jcuu_csv_path, jcuu_subject)
output_csv(fcz_zip_path, fcz_csv_path, fcz_subject)
output_csv(ceuro_zip_path, ceuro_csv_path, ceuro_subject)
output_csv(cusa_zip_path, cusa_csv_path, cusa_subject)
# =============================================================================
# MOVE FXN WH FACTORY PURCHASE ORDER REPORT TO INKJET DIRECTORY
# =============================================================================
# ZIP
os.chdir(fxnwhl_zip_path)
fxnwhl_zip_files = glob.glob(fxnwhl_zip_path + '/*' + '.zip')
fxnwhl_latest_zip = max(fxnwhl_zip_files, key=os.path.getctime)
shutil.copy(fxnwhl_latest_zip, fxnwhi_zip_path)
os.chdir(fxnwhi_zip_path)
fxnwhi_zip_file = zipfile.ZipFile(fxnwhl_latest_zip, mode='r')
fxnwhi_zip_file.extract('Factory Purchase Order Report.csv',
                 path=fxnwhi_csv_path)
