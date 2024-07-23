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
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
print(yesterday)

# COMMAND ----------

import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

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
SELECT
ml.disbursedon_date,
ml.id as loan_id,
ml.account_no AS loan_account_no,
replace(ifnull(mc.display_name, mg.display_name), 'null', '') as client_name,
-- ml.product_id,
mpl.name as product_name,
a.enum_message_property as loan_status,
ml.approved_principal,
c.enum_message_property as interest_method,
ml.maturedon_date,
ml.interest_charged_derived,
msa.account_no
from m_loan ml
left join m_client mc on mc.id = ml.client_id
left join m_group mg on mg.id = ml.group_id
left join m_product_loan mpl  on mpl.id = ml.product_id
LEFT JOIN m_savings_account msa ON msa.client_id = ml.client_id
left join (select enum_id, enum_message_property from r_enum_value where enum_name = 'loan_status_id') a on a.enum_id = ml.loan_status_id
left join (select enum_id, enum_message_property from r_enum_value where enum_name = 'interest_method_enum') c on c.enum_id = ml.interest_method_enum
where ml.product_id  = '47' 
and ml.disbursedon_date = '{yesterday}'
order BY ml.disbursedon_date DESC;
'''
df = pd.read_sql_query(sql, Mifosdb)
df.head()

# COMMAND ----------

df.shape

# COMMAND ----------

if (len(df) > 0):

    with pd.ExcelWriter(f"/Workspace/ReportDump/Daily_BaaS_Loan_Details/Liberty_{yesterday}.xlsx") as writer:
        df.to_excel(writer, sheet_name = f'{yesterday}', index = False)

    # FILE TO SEND AND ITS PATH
    filename = f'Liberty_{yesterday}.xlsx'
    SourcePathName  = "/Workspace/ReportDump/Daily_BaaS_Loan_Details/" + filename

    msg = MIMEMultipart()
    msg['From'] = 'auto-report@vfdtech.ng'
    msg['To'] = 'funmi@libertyng.com, mjohnson@libertyng.com, libertyassured@gmail.com, lukman@libertyng.com'
    msg['BCC'] = 'data-team@vfdtech.ng'
    msg['Subject'] = f'Liberty Assured Loan {yesterday}'
    body = f"""
    Hello Liberty Assured,

    Please find attached report for {yesterday}.

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

else:
    msg = MIMEMultipart()
    msg['From'] = 'auto-report@vfdtech.ng'
    msg['To'] = 'funmi@libertyng.com, mjohnson@libertyng.com, libertyassured@gmail.com, lukman@libertyng.com'
    msg['BCC'] = 'data-team@vfdtech.ng'
    msg['Subject'] = f'Liberty Assured Loan {yesterday}'
    body = f"""
    Hello Liberty Assured,

    No Loan disbursed for {yesterday}.

    Regards,
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


