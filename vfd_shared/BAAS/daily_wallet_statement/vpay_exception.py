# Databricks notebook source
print("Healthy")

# COMMAND ----------

import pandas as pd 
import numpy as np 

import warnings
warnings.filterwarnings('ignore')

from datetime import datetime, timedelta
date = datetime.today() - timedelta(days = 1)
date = date.strftime('%Y-%m-%d')

date1 = datetime.today() - timedelta(days = 1)
date1 = date1.strftime('%Y-%m-%d 23')
date2 = datetime.today() - timedelta(days = 2)
date2 = date2.strftime('%Y-%m-%d 23')

print(date, date1, date2)
print('\n')
d = "'"+date+"'"
d1 = "'" + date1 + "'"
d2 = "'" + date2 + "'"
print(d, d1, d2, '\n')

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders


import mysql.connector
import json
with open('/Workspace/Credentials/db_data.json', 'r') as fp:
    data = json.load(fp)

DumpDB = mysql.connector.connect(
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


# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------

wl = [
('Vpay', '["alerts@minerva.ng", "kalu@vpay.africa", "odiong@vpay.africa"]'),
]

df_wallets = pd.DataFrame(wl, columns = ['wallet_name', 'email'])
df_wallets

# COMMAND ----------

sql = f"""
SELECT 
TWT.WALLET_NAME as wallet_name, 
TWT.TRANSACTION_ID AS 'Transaction Reference', 
TWT.STATEMENT_IDS AS 'Statement IDS', 
-- TWT.TRANSACTION_TYPE as transaction_type,
TWT.SESSION_ID AS 'Session ID', 
TWT.AMOUNT AS 'Transaction Amount', 
TWT.TRANSACTION_RESPONSE AS 'Transaction Response', 
TWT.TIME AS 'Transaction Date'
FROM TM_WALLET_TRANSACTIONS TWT
where TWT.WALLET_NAME like '%Vpay%'
AND TWT.TRANSACTION_TYPE = 'OUTFLOW'
AND LEFT(TWT.TIME, 13) >= {d2}
AND LEFT(TWT.TIME, 13) < {d1}
"""
out = pd.read_sql_query(sql, DumpDB)
out.head()

# COMMAND ----------

sql = f"""
SELECT 
TWT.WALLET_NAME as wallet_name, 
TWT.TRANSACTION_ID AS 'Transaction Reference', 
TWT.FROM_ACCOUNT_NO AS 'From Account No',
ALB.BANK_NAME AS 'From Bank',
TWT.TO_ACCOUNT_NO AS 'To Account No',
TWT.SESSION_ID AS 'Session ID', 
TWT.AMOUNT AS 'Amount', 
TWT.TIME AS 'Transaction Date'
FROM TM_WALLET_TRANSACTIONS TWT
LEFT JOIN AM_LOCAL_BANKS ALB ON ALB.NIP_CODE = TWT.FROM_BANK
WHERE TWT.TRANSACTION_TYPE = 'INFLOW'
AND TWT.WALLET_NAME like '%Vpay%'
AND LEFT(TWT.TIME, 13) >= {d2}
AND LEFT(TWT.TIME, 13) < {d1}
ORDER BY TWT.ID;
"""
inflow = pd.read_sql_query(sql, DumpDB)
inflow.head()

# COMMAND ----------

with pd.ExcelWriter(f'/Workspace/ReportDump/Wallet_Reports/Vpay Wallet Transactions {date}.xlsx') as writer:
    out.to_excel(writer,sheet_name = 'Outflow', index=False)
    inflow.to_excel(writer,sheet_name = 'Inflow', index=False) 

## FILE TO SEND AND ITS PATH
filename = f'Vpay Wallet Transactions {date}.xlsx'
SourcePathName  = '/Workspace/ReportDump/Wallet_Reports/' + filename

# COMMAND ----------

# b = recipient_mail
msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'alerts@minerva.ng, kalu@vpay.africa, odiong@vpay.africa'
msg['CC'] = 'Olugbenga.Paseda@vfdtech.ng, Victor.Nwaka@vfd-mfb.com'
msg['BCC'] = 'data-team@vfdtech.ng'

msg['Subject'] = f'Vpay Transactions for {date}'
body = f"""
Hello Vpay,

Your wallet transactions for {date} is attached for your review.

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


