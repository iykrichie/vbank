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
print(today)

import json
with open('/Workspace/Credentials/db_data.json', 'r') as fp:
    data = json.load(fp)

Dumpdb = mysql.connector.connect(
  host=data['Dumpdb']['host'],
  user=data['Dumpdb']['user'],
  passwd=data['Dumpdb']['passwd'],
  database = data['Dumpdb']['database']
)


# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']

# COMMAND ----------

import os

# Define the log file path and function for reading and writing
log_file_path = '/Workspace/ReportDump/Coralpay_Notification/logfile.txt'

# Function to write values to the log file
def write_to_log(value1, value2):
    with open(log_file_path, 'w') as file:
        file.write(f'{value1}\n')
        file.write(f'{value2}\n')

# Function to read values from the log file
def read_from_log():
    if not os.path.exists(log_file_path):
        print("Log file does not exist.")
        return None, None
    
    with open(log_file_path, 'r') as file:
        lines = file.readlines()
        if len(lines) < 2:
            print("Log file does not contain enough values.")
            return None, None
        
        value1 = lines[0].strip()
        value2 = lines[1].strip()
        
        return value1, value2

# COMMAND ----------

sql = f"""
SELECT CURDATE() AS DATE,  COUNT(*) AS 'TRANSACTION COUNT', IFNULL(SUM(M.AMOUNT),0) AS 'AMOUNT USED', 90000000 AS 'LIMIT AMOUNT', ROUND((IFNULL(SUM(M.AMOUNT),0)/90000000)*100,2) AS 'PERCENT UTILIZED'
FROM MM_BANK_SETTLEMENTS M
WHERE M.PARTNERS = 'CORALPAY_TRF'
AND M.RESPONSE_CODE = '00'
AND LEFT(M.POSTING_DATE, 10) = CURDATE();
"""
df = pd.read_sql_query(sql, Dumpdb)
df

# COMMAND ----------

PERCENT = df['PERCENT UTILIZED'][0]
ROUNDED_NUMBER = (PERCENT // 10) * 10

pd.options.display.float_format = '{:,.2f}'.format
df['TRANSACTION COUNT'] = df['TRANSACTION COUNT'].apply(lambda x: '{:,}'.format(x))
df['AMOUNT USED'] = df['AMOUNT USED'].apply(lambda x: '{:,}'.format(x))
df['LIMIT AMOUNT'] = df['LIMIT AMOUNT'].apply(lambda x: '{:,}'.format(x))
df['PERCENT UTILIZED'] = df['PERCENT UTILIZED'].apply(lambda x: '{:,}'.format(x))
df = df.T
df = df.reset_index()
df.rename(columns = {'index': 'Metrics', 0: 'Values'}, inplace = True)
df
df.head()

# COMMAND ----------

print(PERCENT, "% Utilized")
print(ROUNDED_NUMBER, "% Rounded")

# COMMAND ----------

# Read values from the log file
READ_DATE, READ_ROUND_UTILIZED = read_from_log()
print(f'Read values: {READ_DATE}, {READ_ROUND_UTILIZED}')

# COMMAND ----------

#Initiating the logs to today
if READ_DATE != today:
    write_to_log(today, 0)

# COMMAND ----------

#Cheching if notification will be sent
if (float(PERCENT) >= 50 and float(ROUNDED_NUMBER) != float(READ_ROUND_UTILIZED) and float(ROUNDED_NUMBER) < 100):
    print(True)

# COMMAND ----------

if (float(PERCENT) >= 50 and float(ROUNDED_NUMBER) != float(READ_ROUND_UTILIZED) and float(ROUNDED_NUMBER) < 100):

    df.to_csv('/Workspace/ReportDump/Coralpay_Notification/data.csv', index =False)

    with pd.ExcelWriter(f"/Workspace/ReportDump/Coralpay_Notification/Report.xlsx") as writer:
        df.to_excel(writer, sheet_name = f'Report', index = False)
        
    # FILE TO SEND AND ITS PATH
    filename = f'Report.xlsx'
    SourcePathName  = "/Workspace/ReportDump/Coralpay_Notification/" + filename



    import csv
    from tabulate import tabulate
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    import smtplib



    text = f"Please see below details" + """

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
        <p>Please see below details: 
        </br> </p>
        </br>

        {tab}
        </br>

        <p>Regards,</p>
    </body>
    </html>
    """


    with open('/Workspace/ReportDump/Coralpay_Notification/data.csv') as input_file:
        reader = csv.reader(input_file)
        data = list(reader)

    text = text.format(table=tabulate(data, headers="firstrow", tablefmt="grid"))
    html = html.format(tab=tabulate(data, headers="firstrow", tablefmt="html"))

    message = MIMEMultipart(
        "alternative", None, [MIMEText(text), MIMEText(html,'html')])

    print(message)


    message['From'] = 'auto-report@vfdtech.ng'
    #message['To'] = 'oladapo.omolaja@vfdtech.ng'
    message['To'] = "treasurynotification@vfd-mfb.com, AppSupport@vfdtech.ng, olugbenga.paseda@vfdtech.ng, victor.nwaka@vfdtech.ng, daniel.babatunde@vfdtech.ng, olawale.kareem@vfdtech.ng, oluwasijibomi.bamgboye@vfdtech.ng, ikechukwu.mbaliri@vfd-mfb.com, bukola.adedoyin@vfd-mfb.com, samuel.adegbuyi@vfd-mfb.com, ngozi.nwagbo@vfd-mfb.com"
    #message['CC'] = " "
    message['BCC'] = "data-team@vfdtech.ng"

    message['Subject'] = f'CoralPay Limit Notification - {PERCENT}% Utilized'


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


    #Update the log file
    write_to_log(today, ROUNDED_NUMBER)

# COMMAND ----------


