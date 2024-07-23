# Databricks notebook source
print("Healthy")

# COMMAND ----------

from re import search
from sqlalchemy import create_engine

import pandas as pd 
import numpy as np 
import mysql.connector


import warnings
warnings.filterwarnings('ignore')

from datetime import datetime, timedelta
today = datetime.today().strftime('%Y-%m-%d')
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
print(yesterday)

import json
with open('/Workspace/Credentials/db_data.json', 'r') as fp:
    data = json.load(fp)


Mifosdb = mysql.connector.connect(
  host=data['mifos']['host'],
  user=data['mifos']['user'],
  passwd=data['mifos']['passwd'],
  database = data['mifos']['database']
)


# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------

sql = f"""
SELECT msp.name AS ProductName, mc.display_name AS AccountName, msa.account_no, ai.`Email Address`, ai.`Mobile Number`, msa.activatedon_date, msa.total_deposits_derived,  c.referee_name, mc.is_sub_client, mc.parent_client_id, mc.id AS client_id
FROM m_savings_account msa
LEFT JOIN m_client mc ON mc.id = msa.client_id
LEFT JOIN m_savings_product msp ON msp.id = msa.product_id
LEFT JOIN `Address Info` ai ON ai.client_id = mc.id
LEFT JOIN (
	SELECT mcc.id, mcc.display_name AS referee_name
	FROM m_client mcc
) AS c ON c.id = mc.referred_by_id
WHERE msa.product_id = '26'
AND msa.approvedon_date = '{yesterday}'
AND mc.is_sub_client = '0' ;
"""
df_parent = pd.read_sql_query(sql, Mifosdb)
df_parent.head()

# COMMAND ----------

sql = f"""
SELECT msp.name AS ProductName, mc.display_name AS AccountName, msa.account_no, ai.`Email Address`, ai.`Mobile Number`, msa.activatedon_date, msa.total_deposits_derived,  c.referee_name, mc.is_sub_client, mc.parent_client_id, mc.id AS client_id
FROM m_savings_account msa
LEFT JOIN m_client mc ON mc.id = msa.client_id
LEFT JOIN m_savings_product msp ON msp.id = msa.product_id
LEFT JOIN `Address Info` ai ON ai.client_id = mc.id
LEFT JOIN (
	SELECT mcc.id, mcc.display_name AS referee_name
	FROM m_client mcc
) AS c ON c.id = mc.referred_by_id
WHERE msa.product_id = '26'
AND msa.approvedon_date = '{yesterday}'
AND mc.is_sub_client = '1' ;
"""
df_sub = pd.read_sql_query(sql, Mifosdb)
df_sub.head()

# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/Parent_Sub_Accounts_Opened_Backend/Parent_Sub_Accounts {yesterday}.xlsx") as writer:
    df_parent.to_excel(writer, sheet_name = f'Parent Accouts', index = False)
    df_sub.to_excel(writer, sheet_name = f'Sub Accounts', index = False)
    
# FILE TO SEND AND ITS PATH
filename = f'Parent_Sub_Accounts {yesterday}.xlsx'
SourcePathName  = "/Workspace/ReportDump/Parent_Sub_Accounts_Opened_Backend/" + filename


import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders



msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'samuel.adegbuyi@vfd-mfb.com'
msg['CC'] = 'bright.bassey@vfd-mfb.com, productteam@vfd-mfb.com'
msg['Bcc'] = 'data-team@vfdtech.ng'
msg['Subject'] = f'CORPORATE ACCOUNT OPENING {yesterday}'
body = f"""
Hi Team,

Please find attached fresh term deposit bookings made {yesterday}.


Regards,
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

# COMMAND ----------


