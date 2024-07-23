# Databricks notebook source
print("Healthy")

# COMMAND ----------

import warnings
warnings.filterwarnings('ignore')

import pandas as pd 
import numpy as np  
import duckdb
from sqlalchemy import create_engine

import mysql.connector

import json
with open('/Workspace/Credentials/db_data.json', 'r') as fp:
    data = json.load(fp)

Dumpdb = mysql.connector.connect(
  host=data['Dumpdb']['host'],
  user=data['Dumpdb']['user'],
  passwd=data['Dumpdb']['passwd'],
  database = data['Dumpdb']['database']
)

host = data['redshift']['host']
user = data['redshift']['user']
passwd = data['redshift']['passwd']
database = data['redshift']['database']

conn = create_engine(f"postgresql+psycopg2://{user}:{passwd}@{host}:5439/{database}")
pd.set_option('display.float_format', lambda x: '%.2f' % x)

engine = conn

# COMMAND ----------


from datetime import datetime, timedelta
start = datetime.today() - timedelta(days = 7)
s = start.strftime('%Y-%m-%d')

end = datetime.today() - timedelta(days = 1)
e = end.strftime('%Y-%m-%d') 


print(s)
print(e)


pd.set_option('display.float_format', lambda x: '%.2f' % x)


# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------

sql = f"""
SELECT *
FROM TM_WALLET_BILLS_TRANSACTIONS T
WHERE LEFT(T.TIME, 10) >= '{s}'
AND LEFT(T.TIME, 10) <= '{e}';
"""
df = pd.read_sql_query(sql, Dumpdb)
df.head()

# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/Wallet_Tranx/Wallet_Bills_Tranx_{s}_{e}.xlsx") as writer:
    df.to_excel(writer, sheet_name = f'Report', index = False)
    
# FILE TO SEND AND ITS PATH
filename = f'Wallet_Bills_Tranx_{s}_{e}.xlsx'
SourcePathName  = "/Workspace/ReportDump/Wallet_Tranx/" + filename


import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders



msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'faith.michael@vfdtech.ng, Business@vfdtech.ng'
msg['Bcc'] = 'data-team@vfdtech.ng'
msg['Subject'] = f'Wallet Bills Transactions for {s} To {e}'
body = f"""
Hi Team,

Please find attached wallet bills transactions for {s} To {e}.

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
