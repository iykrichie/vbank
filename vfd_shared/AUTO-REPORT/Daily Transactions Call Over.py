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
select
SQL_CALC_FOUND_ROWS aud.id as id,
aud.resource_id as resourceId,
ev.enum_message_property as status,
o.name as office,
aud.made_on_date as made_On,
mk.username as maker,
aud.checked_on_date as checkedOnDate,
ck.username as checker,
aud.action_name as actionName,
aud.entity_name as Entity,
aud.client_id as clientId,
aud.loan_id as loanId,
aud.savings_account_id,
(
    select
    account_no
    from
    m_savings_account msa
    where
    id = aud.savings_account_id) as 'account number',
(
    select
    amount
    from
    m_savings_account_transaction msat
    where
    id = aud.resource_id) as amount,
(
    select
    msat.transaction_classification
    from
    m_savings_account_transaction msat
    where
    id = aud.resource_id) as narration
from
m_portfolio_command_source aud
left join m_appuser mk on mk.id = aud.maker_id
left join m_appuser ck on ck.id = aud.checker_id
left join m_office o on o.id = aud.office_id
left join r_enum_value ev on ev.enum_name = 'processing_result_enum' and ev.enum_id = aud.processing_result_enum
where mk.id = '274'
AND LEFT(aud.made_on_date, 10) = '{yesterday}'
order by aud.id DESC
 
"""
df = pd.read_sql_query(sql, Mifosdb)
df.head()

# COMMAND ----------

len(df)

# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/Tranx_Call_Over/Tranx_Call_Over {yesterday}.xlsx") as writer:
    df.to_excel(writer, sheet_name = f'Report', index = False)
    
# FILE TO SEND AND ITS PATH
filename = f'Tranx_Call_Over {yesterday}.xlsx'
SourcePathName  = "/Workspace/ReportDump/Tranx_Call_Over/" + filename


import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders



msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'financeops@vfd-mfb.com'
msg['Bcc'] = 'data-team@vfdtech.ng'
msg['Subject'] = f'Transaction Call Over {yesterday}'
body = f"""
Hi Team,

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

# COMMAND ----------


