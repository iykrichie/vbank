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

from datetime import datetime, timedelta
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')

print(yesterday)

# COMMAND ----------

sql = f"""
SELECT msa.approvedon_date AS Date, COUNT(*) AS 'App Signup'
FROM m_savings_account msa
WHERE msa.approvedon_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
AND msa.approvedon_date < CURDATE()
AND msa.activatedon_userid = '31'
AND msa.product_id IN ('29','31','33')
GROUP BY 1
ORDER BY 1 DESC;
"""
df = pd.read_sql_query(sql, Mifosdb)
df.head()

# COMMAND ----------

df.to_csv('/Workspace/ReportDump/App_Signup_Report/data.csv', index =False)

with pd.ExcelWriter(f"/Workspace/ReportDump/App_Signup_Report/App_Signup_Report.xlsx") as writer:
    df.to_excel(writer, sheet_name = f'Report', index = False)
    
# FILE TO SEND AND ITS PATH
filename = f'App_Signup_Report.xlsx'
SourcePathName  = "/Workspace/ReportDump/App_Signup_Report/" + filename

import csv
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib


text = f"Please see below App Signup for the last 30 days." + """
Please see details below:

{table}

""" 
#th, td {{ padding: 5px; }}
html = """
<html>
<style> 
  tab, th, td {{ border: 1px solid black; border-collapse: collapse; }}
  th, td {{ padding: 3px; }}
</style>
<body>
<p>Dear Team, </br>
</br>
<p>Please see below App Signup for the last 30 days. 
</br>  </p>
</br>

{tab}
</br>


<p>Regards,</p>
</body>
</html>
"""


with open('/Workspace/ReportDump/App_Signup_Report/data.csv') as input_file:
    reader = csv.reader(input_file)
    data = list(reader)

text = text.format(table=tabulate(data, headers="firstrow", tablefmt="grid"))
html = html.format(tab=tabulate(data, headers="firstrow", tablefmt="html"))

message = MIMEMultipart(
    "alternative", None, [MIMEText(text), MIMEText(html,'html')])

print(message)


message['From'] = "auto-report@vfdtech.ng"

message['To'] = "rotimi.awofisibe@vfd-mfb.com, olumide.odewole@vfd-mfb.com, productteam@vfd-mfb.com"
message['BCC'] = "data-team@vfdtech.ng"

message['Subject'] = f'App Signup Report'


## ATTACHMENT PART OF THE CODE IS HERE
attachment = open(SourcePathName, 'rb')
part = MIMEBase('application', "octet-stream")
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', "attachment; filename= %s" % filename)
message.attach(part)


server = smtplib.SMTP('smtp-mail.outlook.com', 587)
server.ehlo()
server.starttls()
server.login(username, passwd)
server.send_message(message)
server.quit()

