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
today =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')

print(today)

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
pd.set_option('display.float_format', lambda x: '%.2f' % x)



# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------

# MAGIC %%time
# MAGIC a = '''
# MAGIC select
# MAGIC sa.account_no,
# MAGIC sa.account_balance_derived,
# MAGIC sa.overdraft_limit,
# MAGIC sa.overdraft_startedon_date as disbursement_date,
# MAGIC sa.overdraft_closedon_date as maturation_date,
# MAGIC ifnull(mc.referred_by_id, 0) as referred_by_id,
# MAGIC sa.approvedon_date
# MAGIC from m_savings_account sa
# MAGIC join m_client mc on mc.id = sa.client_id
# MAGIC where sa.account_balance_derived <= -1000
# MAGIC and !isnull(sa.overdraft_startedon_date)
# MAGIC order by sa.approvedon_date 
# MAGIC '''
# MAGIC df_msa = pd.read_sql_query(a, Mifosdb)
# MAGIC
# MAGIC refs = df_msa['referred_by_id'].astype(str)
# MAGIC refs = "'" +refs+"'"
# MAGIC
# MAGIC milo = f'''
# MAGIC     select
# MAGIC     replace(mc.display_name, 'null', ' ') as customer_name,
# MAGIC     sa.account_no,
# MAGIC     sp.name as product,
# MAGIC     sa.overdraft_limit as overdraft_amount,
# MAGIC     sa.nominal_annual_interest_rate_overdraft as interest_rate,
# MAGIC     sa.account_balance_derived as current_available_balance,
# MAGIC     sa.overdraft_limit + sa.account_balance_derived as overdrawn_amount,
# MAGIC     sa.overdraft_startedon_date as date_granted,
# MAGIC     sa.overdraft_closedon_date as expiry_date,
# MAGIC     d.referral_name as account_officer
# MAGIC
# MAGIC
# MAGIC     from m_savings_account sa
# MAGIC     join m_savings_product sp on sp.id = sa.product_id
# MAGIC     join m_client mc on mc.id = sa.client_id
# MAGIC     left join OverDraft od on od.CLIENTID = mc.id
# MAGIC     left join 
# MAGIC             (select c.id, replace(c.display_name, 'null', ' ') as referral_name from m_client c 
# MAGIC             where c.id in ({','.join([str(x) for x in refs.tolist()])})) d on d.id = mc.referred_by_id
# MAGIC     where sa.account_no in ({','.join([str(x) for x in df_msa['account_no'].tolist()])})
# MAGIC     and sa.overdraft_startedon_date = '{today}'
# MAGIC     '''
# MAGIC dfmilo = pd.read_sql_query(milo, Mifosdb)
# MAGIC
# MAGIC dfmilo

# COMMAND ----------

len(dfmilo)

# COMMAND ----------

if(len(dfmilo) > 0):
    display(dfmilo)

# COMMAND ----------

if (len(dfmilo) > 0):
    with pd.ExcelWriter(f"/Workspace/ReportDump/Overdraft_Disbursed/Overdraft_{today}.xlsx") as writer:
        dfmilo.to_excel(writer, sheet_name = f'Report', index = False)

    # FILE TO SEND AND ITS PATH
    filename = f'Overdraft_{today}.xlsx'
    SourcePathName  = "/Workspace/ReportDump/Overdraft_Disbursed/" + filename


    msg = MIMEMultipart()
    msg['From'] = 'auto-report@vfdtech.ng'
    msg['To'] = "internalauditcontrol@vfd-mfb.com"
    msg['cc'] = 'data-team@vfdtech.ng'
    msg['Subject'] = f'Overdraft Disbursed Report for {today}'
    body = f"""
    Hello Team,

    Please find attached report for {today}.

    Regards
    Data Team (DBR),
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
    msg['To'] = "internalauditcontrol@vfd-mfb.com"
    msg['cc'] = 'data-team@vfdtech.ng'
    msg['Subject'] = f'Overdraft Disbursed Report for {today}'
    body = f"""
    Hello Team,

    No Overdraft disbursed for {today}.

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


