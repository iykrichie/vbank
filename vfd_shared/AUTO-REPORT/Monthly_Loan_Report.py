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

# COMMAND ----------

from datetime import datetime, timedelta
last_month =  (datetime.today() - timedelta(days = 15)).strftime('%Y-%m')
last_month_format2 =  (datetime.today() - timedelta(days = 15)).strftime('%b-%y')
print(last_month)
print(last_month_format2)

# COMMAND ----------

from re import search
from sqlalchemy import create_engine
import duckdb

import pandas as pd 
import numpy as np 
import mysql.connector


import warnings
warnings.filterwarnings('ignore')

pd.set_option('display.max_columns', None)


import json
with open('/Workspace/Credentials/db_data.json', 'r') as fp:
    data = json.load(fp)


Mifosdb = mysql.connector.connect(
  host=data['mifos']['host'],
  user=data['mifos']['user'],
  passwd=data['mifos']['passwd'],
  database = data['mifos']['database']
)


host = data['redshift']['host']
user = data['redshift']['user']
passwd = data['redshift']['passwd']
database = data['redshift']['database']

conn = create_engine(f"postgresql+psycopg2://{user}:{passwd}@{host}:5439/{database}")
pd.set_option('display.float_format', lambda x: '%.2f' % x)

# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------

sql = f'''
select 
dld.client_id ,dld.product_id , dld.product_name, dld.account_no, dld.approved_principal ,dld.disbursedon_date, dld.loan_status 
from dwh_loan_details dld
where LEFT(dld.disbursedon_date,7) = '{last_month}'
order by dld.product_id;
'''
df = pd.read_sql_query(sql, conn)
df.head(2)

# COMMAND ----------

display(df)

# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/Monthly_Loan_Report/LoanReport_{last_month_format2}.xlsx") as writer:
    df.to_excel(writer, sheet_name = f'{last_month_format2}', index = False)

# COMMAND ----------

# FILE TO SEND AND ITS PATH
filename = f'LoanReport_{last_month_format2}.xlsx'
SourcePathName  = "/Workspace/ReportDump/Monthly_Loan_Report/" + filename


# COMMAND ----------

import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# 

msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'victoria.faloye@vfdtech.ng, abimbola.osoba@vfd-mfb.com, samuel.atewe@vfdtech.ng'
msg['CC'] = 'data-team@vfdtech.ng'
msg['Subject'] = f'Loan Report {last_month_format2}'
body = f"""
Hello Team,

Please find attached report for {last_month_format2}.

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


