# Databricks notebook source
print("Healthy")

# COMMAND ----------

from re import search
from sqlalchemy import create_engine
import duckdb

import pandas as pd 
import numpy as np 
import mysql.connector


import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders


import warnings
warnings.filterwarnings('ignore')

# COMMAND ----------


import json
with open('/Workspace/Credentials/db_data.json', 'r') as fp:
    data = json.load(fp)

Dumpdb = mysql.connector.connect(
  host=data['Dumpdb']['host'],
  user=data['Dumpdb']['user'],
  passwd=data['Dumpdb']['passwd'],
  database = data['Dumpdb']['database']
)


Mifosdb = mysql.connector.connect(
  host=data['mifos']['host'],
  user=data['mifos']['user'],
  passwd=data['mifos']['passwd'],
  database = data['mifos']['database']
)


vbizdb = mysql.connector.connect(
  host=data['vbizdb']['host'],
  user=data['vbizdb']['user'],
  passwd=data['vbizdb']['passwd'],
  database = data['vbizdb']['database']
)

host = data['redshift']['host']
user = data['redshift']['user']
passwd = data['redshift']['passwd']
database = data['redshift']['database']

conn = create_engine(f"postgresql+psycopg2://{user}:{passwd}@{host}:5439/{database}")


# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)



from datetime import datetime, timedelta
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')

print(yesterday)


# COMMAND ----------


sql = f"""
SELECT 
b.vClientId AS ClientID, 
b.name AS BusinessName, 
a.nuban AS AccountNo,  
a.`type` AS BusinessType,
b.phone AS PhoneNumber,
b.primaryEmail AS EmailAddress,
b.bvn AS BVN,
a.createdAt AS DateCreated,
a.`status` AS OnboardingStatus,
b.address AS BusinessAddress,
a.updatedAt AS OnboardingCompletedDate,
a.vAccountId
FROM account a
LEFT JOIN business b ON b.id = a.businessId
WHERE LEFT(a.createdAt,10) = '{yesterday}';
"""
df = pd.read_sql_query(sql, vbizdb)
df.head()

# COMMAND ----------

#print(df['ClientID'][0])

# COMMAND ----------

if (len(df) > 0):
    if (len(df) == 1):
        sql = f"""
        SELECT cr.client_id as ClientID,
        cr.is_pep,
        cr.monitor,
        cr.name_searched,
        cr.profile_url,
        cr.search_id
        FROM comply_check_record cr
        WHERE cr.client_id = '{df['ClientID'][0]}'
        """
        df_aml = pd.read_sql_query(sql, Dumpdb)
        df_aml.head()

        sql = f"""
        SELECT msa.id AS vAccountId, msa.account_balance_derived AS Balance, IFNULL(mc.referred_by_id, '-')  AS ReferralID
        FROM m_savings_account msa
        LEFT JOIN m_client mc ON mc.id = msa.client_id
        WHERE msa.id = '{df['vAccountId'][0]}'
        """
        df_mifos = pd.read_sql_query(sql, Mifosdb)

        sql = f"""
        SELECT m.id AS ReferralID, m.display_name AS ReferralName
        FROM m_client m
        WHERE m.id = '{df_mifos['ReferralID'][0]}'
        """
        df_ref = pd.read_sql_query(sql, Mifosdb)

        df_ref['ReferralID'] = df_ref['ReferralID'].astype(str)
        df_mifos['ReferralID'] = df_mifos['ReferralID'].astype(str)

        df_mifos = df_mifos.merge(df_ref, on='ReferralID', how='left')


        df['vAccountId'] = df['vAccountId'].astype(str)
        df_mifos['vAccountId'] = df_mifos['vAccountId'].astype(str)

        df = df.merge(df_mifos, on='vAccountId', how='left')

        df['ClientID'] = df['ClientID'].astype(str)
        df_aml['ClientID'] = df_aml['ClientID'].astype(str)
        df = df.merge(df_aml, on='ClientID', how='left')
        df.head()

    else:
        sql = f"""
        SELECT cr.client_id as ClientID,
        cr.is_pep,
        cr.monitor,
        cr.name_searched,
        cr.profile_url,
        cr.search_id
        FROM comply_check_record cr
        WHERE cr.client_id IN ({','.join([str(x) for x in df['ClientID'].tolist()])})
        """
        df_aml = pd.read_sql_query(sql, Dumpdb)
        df_aml.head()


        sql = f"""
        SELECT msa.id AS vAccountId, msa.account_balance_derived AS Balance, IFNULL(mc.referred_by_id, '-')  AS ReferralID
        FROM m_savings_account msa
        LEFT JOIN m_client mc ON mc.id = msa.client_id
        WHERE msa.id IN ({','.join([str(x) for x in df['vAccountId'].tolist()])})
        """
        df_mifos = pd.read_sql_query(sql, Mifosdb)
        print(df_mifos)

        try: 
            sql = f"""
            SELECT m.id AS ReferralID, m.display_name AS ReferralName
            FROM m_client m
            WHERE m.id IN ({','.join([str(x) for x in df_mifos['ReferralID'][df_mifos['ReferralID'] != '-'].tolist()])})
            """
            df_ref = pd.read_sql_query(sql, Mifosdb)
            df_ref['ReferralID'] = df_ref['ReferralID'].astype(str)
            df_mifos['ReferralID'] = df_mifos['ReferralID'].astype(str)
            df_mifos = df_mifos.merge(df_ref, on='ReferralID', how='left')

            df['vAccountId'] = df['vAccountId'].astype(str)
            df_mifos['vAccountId'] = df_mifos['vAccountId'].astype(str)

            df = df.merge(df_mifos, on='vAccountId', how='left')

            df['ClientID'] = df['ClientID'].astype(str)
            df_aml['ClientID'] = df_aml['ClientID'].astype(str)
            df = df.merge(df_aml, on='ClientID', how='left')
            df.head()

        except Exception as e:
            print('No Ref')

            df_mifos['ReferralName'] = ''
            df['vAccountId'] = df['vAccountId'].astype(str)
            df_mifos['vAccountId'] = df_mifos['vAccountId'].astype(str)

            df = df.merge(df_mifos, on='vAccountId', how='left')

            df['ClientID'] = df['ClientID'].astype(str)
            df_aml['ClientID'] = df_aml['ClientID'].astype(str)
            df = df.merge(df_aml, on='ClientID', how='left')
            df.head()

    with pd.ExcelWriter(f"/Workspace/ReportDump/Vbiz_Opened_Acc/Vbiz_Opened_Acc_{yesterday}.xlsx") as writer:
        df.to_excel(writer, sheet_name = f'Report', index = False)
        
    # FILE TO SEND AND ITS PATH
    filename = f'Vbiz_Opened_Acc_{yesterday}.xlsx'
    SourcePathName  = "/Workspace/ReportDump/Vbiz_Opened_Acc/" + filename


    msg = MIMEMultipart()
    msg['From'] = 'auto-report@vfdtech.ng'
    msg['To'] = 'compliance@vfd-mfb.com, damilola.fatunmise@vfd-mfb.com, temitope.osinubi@vfd-mfb.com, bright.bassey@vfd-mfb.com'
    msg['Bcc'] = 'data-team@vfdtech.ng'
    msg['Subject'] = f'ACCOUNTS OPENED VIA VBIZ FOR {yesterday}'
    body = f"""
    Dear Team,

    Please find attached report for {yesterday}.

    Regards,
    DBR
    """
    msg.attach(MIMEText(body, 'plain'))


    ## ATTACHMENT PART OF THE CODE IS HERE
    attachment = open(SourcePathName, 'rb')
    part = MIMEBase('application', "octet-stream")
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
    msg.attach(part)

    server = smtplib.SMTP('smtp.office365.com', 587)  ### put your relevant SMTP here
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(username, passwd)  ### if applicable
    server.send_message(msg)
    server.quit()

else:
    msg = MIMEMultipart()
    msg['From'] = 'auto-report@vfdtech.ng'
    msg['To'] = 'compliance@vfd-mfb.com, damilola.fatunmise@vfd-mfb.com, temitope.osinubi@vfd-mfb.com, bright.bassey@vfd-mfb.com'
    msg['Bcc'] = 'data-team@vfdtech.ng'
    msg['Subject'] = f'ACCOUNTS OPENED VIA VBIZ FOR {yesterday}'
    body = f"""
    Dear Team,

    No Vbiz Account Opened {yesterday}.

    Regards
    Data Team (DBR),
    """
    msg.attach(MIMEText(body, 'plain'))
    server = smtplib.SMTP('smtp.office365.com', 587)  ### put your relevant SMTP here
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(username, passwd)  ### if applicable
    server.send_message(msg)
    server.quit()

# COMMAND ----------


