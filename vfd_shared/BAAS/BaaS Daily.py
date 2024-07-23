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
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')

print(yesterday)

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

# COMMAND ----------

sql = f""" 
SELECT 
TWT.WALLET_NAME AS Wallet_Name,
COUNT( CASE WHEN TWT.TO_BANK = '999999' then TWT.ID END) AS Intra_Volume,
COUNT( CASE WHEN TWT.TO_BANK != '999999' then TWT.ID END) AS Inter_Volume,
IFNULL(SUM( CASE WHEN TWT.TO_BANK = '999999' then TWT.AMOUNT END),0) AS Intra_Value,
IFNULL(SUM( CASE WHEN TWT.TO_BANK != '999999' then TWT.AMOUNT END),0) AS Inter_Value
FROM TM_WALLET_TRANSACTIONS TWT 
WHERE LEFT(TWT.TIME,10) = '{yesterday}'
AND TWT.TRANSACTION_RESPONSE = '00'
AND TWT.TRANSACTION_TYPE = 'OUTFLOW'
AND TWT.WALLET_NAME != 'Victor Nwaka'
GROUP BY 1
ORDER BY 1;
"""
df_outflow = pd.read_sql_query(sql, Dumpdb)
df_outflow.head()

# COMMAND ----------

sql = f""" 
SELECT 
TWT.WALLET_NAME AS Wallet_Name,
COUNT( CASE WHEN TWT.TO_BANK = '999999' then TWT.ID END) AS Intra_Volume,
COUNT( CASE WHEN TWT.TO_BANK != '999999' then TWT.ID END) AS Inter_Volume,
IFNULL(SUM( CASE WHEN TWT.TO_BANK = '999999' then TWT.AMOUNT END),0) AS Intra_Value,
IFNULL(SUM( CASE WHEN TWT.TO_BANK != '999999' then TWT.AMOUNT END),0) AS Inter_Value
FROM TM_WALLET_TRANSACTIONS TWT 
WHERE  LEFT(TWT.TIME,10) = '{yesterday}'
AND TWT.TRANSACTION_RESPONSE = '00'
AND TWT.TRANSACTION_TYPE = 'INFLOW'
AND TWT.WALLET_NAME != 'Victor Nwaka'
GROUP BY 1
ORDER BY 1;
"""
df_inflow = pd.read_sql_query(sql, Dumpdb)
df_inflow.head()

# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/Wallet_Tranx/Wallet_Tranx_{yesterday}.xlsx") as writer:
    df_inflow.to_excel(writer, sheet_name = f'Inflow', index = False)
    df_outflow.to_excel(writer, sheet_name = f'Outflow', index = False)
    
# FILE TO SEND AND ITS PATH
filename = f'Wallet_Tranx_{yesterday}.xlsx'
SourcePathName  = "/Workspace/ReportDump/Wallet_Tranx/" + filename


import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders



msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'business@vfdtech.ng'
msg['Bcc'] = 'data-team@vfdtech.ng'
msg['Subject'] = f'Wallet Transactions for {yesterday}'
body = f"""
Hi Team,

Please find attached wallet transactions for {yesterday}.


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

# COMMAND ----------


