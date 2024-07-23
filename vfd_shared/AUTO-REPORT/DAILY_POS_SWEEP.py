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

# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------

sql = f'''
SELECT DISTINCT L.TRANSIT_ACCOUNT_NUMBER 
FROM LUX_BULK_MERCHANT L
WHERE L.TRANSIT_ACCOUNT_NUMBER IS NOT NULL
AND L.ACCOUNT_NUMBER IS NOT NULL;
'''
df_transit = pd.read_sql_query(sql, Dumpdb)
df_transit.head(2)

# COMMAND ----------

from pyspark.sql.functions import lit
import dask
import dask.dataframe as dd
from sqlalchemy import create_engine

# MIFOS_DB
mifos_driver = "org.mariadb.jdbc.Driver"
mifos_database_host = data['mifos']['host']
mifos_database_port = "3306" # update if you use a non-default port
mifos_database_name = data['mifos']['database']
mifos_user=data['mifos']['user']
mifos_password = data['mifos']['passwd']
mifos_url = f"jdbc:mysql://{mifos_database_host}:{mifos_database_port}/{mifos_database_name}"

# Define function to extract data from dbs
def extract_function(query, url, user, password):
    df = spark.read \
                .format("jdbc") \
                .option("query", query) \
                .option("url", url) \
                .option("user", user) \
                .option("password", password) \
                .option("connectTimeout", "1000") \
                .option("treatEmptyValueAsNulls","true") \
                .option("maxRowsInMemory",200) \
                .load()
    return df


## For Mifos
sql = f'''
SELECT 
satt.created_date AS 'Transaction Date',
sa.account_no AS 'Beneficiary Acc No.',
sa.client_id AS 'Beneficiary ClientID',
ct.display_name AS 'Beneficiary Name',
sat.running_balance_derived AS 'Beneficiary Running Balance',
satt.amount AS Amount,
"VFD MFB" AS 'Beneficiary Bank',
saa.account_no AS 'Sender Acc No.',
saa.client_id AS 'Sender ClientID',
cc.display_name AS 'Sender Name',
satt.running_balance_derived AS 'Sender Running Balance',
satt.id AS ResourceID,
"VFD MFB" AS 'Sender Bank',
tr.description AS Narration
FROM m_account_transfer_details d
INNER JOIN m_account_transfer_transaction tr ON d.id =tr.account_transfer_details_id
INNER JOIN m_client cc ON cc.id =d.from_client_id
INNER JOIN m_client ct ON ct.id = d.to_client_id
INNER JOIN m_savings_account saa ON saa.id = d.from_savings_account_id
INNER JOIN m_savings_account_transaction satt ON saa.id=satt.savings_account_id AND satt.id=tr.from_savings_transaction_id
INNER JOIN m_savings_account sa ON sa.id = d.to_savings_account_id
INNER JOIN m_savings_account_transaction sat ON sa.id=sat.savings_account_id AND sat.id=tr.to_savings_transaction_id
INNER JOIN m_transaction_request mtr ON mtr.transaction_id = satt.id
WHERE tr.is_reversed=0 
AND LEFT(satt.created_date,10) = '{yesterday}' 
AND saa.account_no IN ({','.join(str(x) for x in df_transit['TRANSIT_ACCOUNT_NUMBER'])})
-- AND tr.description LIKE '%Bulk Settlement%'
'''       
# apply function
mifos_df = extract_function(query=sql, url=mifos_url, user=mifos_user, password=mifos_password)
mifos_df.persist()
df_report = mifos_df.toPandas()


# COMMAND ----------

display(df_report)

# COMMAND ----------

len(df_report)

# COMMAND ----------

sql = f'''
select vu.USERNAME ,vpal.`ACTION` , vpal.RESOURCE, vpal.DATE_UPDATED, TRIM(SUBSTRING(vpal.RESOURCE, 12, 11)) AS MainAccountNumber, TRIM(SUBSTRING(vpal.RESOURCE, 43, 11)) AS TransitAccountNumber
from VBAAS_PORTAL_AUDIT_LOG vpal 
left join VBAAS_USERS vu on vu.ID  = vpal.MAKER_USER_ID  
where vpal.API_URL  ='/baasadminsupport/lux/bulk-merchant' 
order by vpal.id desc ;
'''
df_users = pd.read_sql_query(sql, Dumpdb)

sql = f'''
SELECT msa.account_no AS MainAccountNumber, mc.display_name AS MainAccountName
FROM m_client mc
LEFT JOIN m_savings_account msa ON msa.client_id = mc.id
WHERE msa.account_no IN ({','.join(str(x) for x in df_users['MainAccountNumber'])})
'''
df_acc_names_main = pd.read_sql_query(sql, Mifosdb)

df_users = df_users.merge(df_acc_names_main, on = 'MainAccountNumber', how = 'left')

sql = f'''
SELECT msa.account_no AS TransitAccountNumber, mc.display_name AS TransitAccountName
FROM m_client mc
LEFT JOIN m_savings_account msa ON msa.client_id = mc.id
WHERE msa.account_no IN ({','.join(str(x) for x in df_users['TransitAccountNumber'])})
'''
df_acc_names_transit = pd.read_sql_query(sql, Mifosdb)

df_users = df_users.merge(df_acc_names_transit, on = 'TransitAccountNumber', how = 'left')
df_users.head(2)


# COMMAND ----------

#display(df_users)

# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/POS_SWEEP/Sweep.xlsx") as writer:
    df_report.to_excel(writer, sheet_name = f'{yesterday}', index = False)
    df_users.to_excel(writer, sheet_name = f'Sweep Username', index = False)

# COMMAND ----------

# FILE TO SEND AND ITS PATH
filename = f'Sweep.xlsx'
SourcePathName  = "/Workspace/ReportDump/POS_SWEEP/" + filename


# COMMAND ----------

import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

# 

msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'internalauditcontrol@vfd-mfb.com'
msg['CC'] = 'data-team@vfdtech.ng'
msg['Subject'] = f'POS Sweep Report for {yesterday}'
body = f"""
Hello Team,

Please find attached report for {yesterday}.

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


