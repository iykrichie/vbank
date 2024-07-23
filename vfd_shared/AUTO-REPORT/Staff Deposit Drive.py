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
#yesterday = 'Jun-05 to Jun-20'
yesterday_format2 =  (datetime.today() - timedelta(days = 1)).strftime('%b_%d')
print(yesterday)
print(yesterday_format2)

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

staff = pd.read_excel("/Workspace/ReportDump/Staff_List/Feb 24 Staff List.xlsx")
staff.head()

# COMMAND ----------

sql = f'''
SELECT
sa.account_no,
mc.display_name AS staff_name,
mc.id AS staff_client_id,
mc.referral_id
FROM m_savings_account sa
JOIN m_client mc ON mc.id = sa.client_id
WHERE sa.account_no IN ({','.join(str(x) for x in staff['Account Numbers'])})
AND mc.referral_id IS NOT NULL
GROUP BY mc.id
'''
staff = pd.read_sql_query(sql, Mifosdb)
staff.head(2)

# COMMAND ----------

display(staff)

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
mc.referred_by_id AS staff_client_id,
IFNULL(SUM(CASE WHEN st.transaction_type_enum = 1 THEN st.amount END),0) AS {yesterday_format2}_total_inflow,
IFNULL(SUM(CASE WHEN st.transaction_type_enum = 2 THEN st.amount END),0) AS {yesterday_format2}_total_outflow,
IFNULL(SUM(CASE WHEN sa.product_id = '36' AND st.transaction_type_enum = 1 THEN st.amount END),0) AS {yesterday_format2}_FD_inflow
FROM m_savings_account sa
JOIN m_client mc ON mc.id = sa.client_id
JOIN m_savings_product sp ON sp.id = sa.product_id
LEFT JOIN m_savings_account_transaction st ON st.savings_account_id = sa.id
WHERE st.transaction_type_enum IN (1, 2)
AND left(st.transaction_date,10) = '{yesterday}'
AND mc.referred_by_id IN ({','.join(str(x) for x in staff['staff_client_id'])})
GROUP BY 1
'''       
# apply function
mifos_df = extract_function(query=sql, url=mifos_url, user=mifos_user, password=mifos_password)
mifos_df.persist()
deposit = mifos_df.toPandas()

## For Mifos
sql = f'''
SELECT
CASE WHEN mc.referred_by_id IS NULL THEN 'others' ELSE mc.referred_by_id END AS staff_client_id,
IFNULL(SUM(CASE WHEN st.transaction_type_enum = 1 THEN st.amount END),0) AS {yesterday_format2}_total_inflow,
IFNULL(SUM(CASE WHEN st.transaction_type_enum = 2 THEN st.amount END),0) AS {yesterday_format2}_total_outflow,
IFNULL(SUM(CASE WHEN sa.product_id = '36' AND st.transaction_type_enum = 1 THEN st.amount END),0) AS {yesterday_format2}_FD_inflow
FROM m_savings_account sa
JOIN m_client mc ON mc.id = sa.client_id
JOIN m_savings_product sp ON sp.id = sa.product_id
LEFT JOIN m_savings_account_transaction st ON st.savings_account_id = sa.id
WHERE st.transaction_type_enum IN (1, 2)
AND left(st.transaction_date,10) = '{yesterday}'
AND mc.referred_by_id IS NULL
GROUP BY 1
'''       
# apply function
mifos_df_others = extract_function(query=sql, url=mifos_url, user=mifos_user, password=mifos_password)
mifos_df_others.persist()
deposit_others = mifos_df_others.toPandas()

deposit = deposit.append(deposit_others, ignore_index=True)

# COMMAND ----------

display(deposit)

# COMMAND ----------

other_staff = {
    'account_no': [''],
    'staff_name': [''],
    'staff_client_id': ['others'],
    'referral_id': [''],
}

other_staff = pd.DataFrame(other_staff)
staff = staff.append(other_staff, ignore_index=True)

# COMMAND ----------

display(staff)

# COMMAND ----------

full_df = staff[['staff_client_id', 'staff_name','referral_id']].merge(deposit, on = 'staff_client_id', how = 'left')
#full_df = full_df.sort_values(by=['staff_client_id'], ascending=False)
full_df = full_df.fillna(0)
full_df[f'{yesterday_format2}_total_inflow'] = full_df[f'{yesterday_format2}_total_inflow'].astype(float)
full_df[f'{yesterday_format2}_total_outflow'] = full_df[f'{yesterday_format2}_total_outflow'].astype(float)
full_df[f'{yesterday_format2}_FD_inflow'] = full_df[f'{yesterday_format2}_FD_inflow'].astype(float)
full_df.head(10)

# COMMAND ----------

full_df.dtypes

# COMMAND ----------

full_df.shape

# COMMAND ----------

display(full_df)

# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/Deposit_Drive/Deposit_Drive_{yesterday}.xlsx") as writer:
    full_df.to_excel(writer, sheet_name = f'{yesterday}', index = False)

# COMMAND ----------

# FILE TO SEND AND ITS PATH
filename = f'Deposit_Drive_{yesterday}.xlsx'
SourcePathName  = "/Workspace/ReportDump/Deposit_Drive/" + filename


# COMMAND ----------

import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

#rotimi.awofisibe@vfd-mfb.com, mercy.ejemudaro@vfd-mfb.com, ikechukwu.mbaliri@vfd-mfb.com, 

msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'rotimi.awofisibe@vfd-mfb.com, mercy.ejemudaro@vfd-mfb.com, ikechukwu.mbaliri@vfd-mfb.com'
msg['CC'] = 'data-team@vfdtech.ng'
msg['Subject'] = f'Staff Deposit Drive {yesterday}'
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


