# Databricks notebook source
print("Healthy")

# COMMAND ----------

from re import search
from sqlalchemy import create_engine

import pandas as pd 
import numpy as np 
import duckdb

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
  MIN(details.entity_id) entity_id,
  details.edate entry_date,
  details.acid1,
  details.report_header,
  details.reportid,
  details.branchname,
  MIN(details.debit_amount) debit_amount,
  MIN(details.credit_amount) credit_amount,
  MIN(details.aftertxn) aftertxn,
  MIN(details.transactionid) transactionid,
  MIN(details.manual_entry) manual_entry,
  details.id AS transtype,
  MIN(openingbalance) openingbalance,
  CASE
	  WHEN details.savings_transaction_id IS NOT NULL THEN CONCAT(details.account_name,'-',"(", (
	  SELECT CONCAT(s.account_no,'-',cl.display_name)
	  FROM m_savings_account s, m_client cl, m_savings_account_transaction ts
	  WHERE  ts.id=details.savings_transaction_id AND ts.savings_account_id=s.id AND cl.id=s.client_id),")")
	  WHEN details.loan_transaction_id IS NOT NULL THEN CONCAT(details.account_name,'-',"(", (
	  SELECT CONCAT(l.account_no,'-',cl.display_name)
	  FROM m_loan l, m_client cl,m_loan_transaction lt 
	  WHERE lt.id=details.loan_transaction_id AND lt.loan_id=l.id AND cl.id=l.client_id),")")
	  ELSE details.account_name END AS account_name,
  CASE
	  WHEN details.savings_transaction_id IS NOT NULL THEN
	  (select remarks from m_transaction_request r where r.transaction_id = details.savings_transaction_id)
	  ELSE details.description END AS 'description',
  ifnull(coalesce(details.savings_receipt,details.loan_receipt),"") receipt_number,
  MIN(details.created_date) created_date, 
  MIN(details.type_enum) type_enum
  FROM (
	  SELECT 
	  distinctrow je.account_id acid1,
	  gl.name AS report_header,
	  gl.classification_enum actype,
	  gl.gl_code AS reportid,
	  j1.entry_date edate,
	  gl1.name AS account_name,
	  if (j1.type_enum = 1, j1.amount, 0) AS debit_amount,
	  if (j1.type_enum = 2, j1.amount, 0) AS credit_amount,
	  j1.id,
	  j1.entity_id,
	  j1.office_id,
	  j1.transaction_id,
	  j1.type_enum,
	  j1.office_running_balance AS aftertxn,
	  j1.description AS description,
	  j1.transaction_id AS transactionid,
	  je.manual_entry,
	  j1.savings_transaction_id,
	  case
		  when j1.savings_transaction_id is not null then (
		  select mp.receipt_number
		  from m_payment_detail mp
		  join m_savings_account_transaction msat on mp.id = msat.payment_detail_id
		  where msat.id = j1.savings_transaction_id
		  )
		  else null
		  end as savings_receipt,
	  j1.loan_transaction_id,
	  case
		  when j1.loan_transaction_id is not null then (
		  select mpl.receipt_number
		  from m_payment_detail mpl
		  join m_loan_transaction mlt on mpl.id = mlt.payment_detail_id
		  where mlt.id = j1.loan_transaction_id
		  )
		  else null
		  end as loan_receipt,
	  j1.payment_details_id,
	  j1.created_date,
	  mo.name as branchname
	  FROM m_office mo
	  join acc_gl_journal_entry j1 on j1.office_id = mo.id
	  join acc_gl_account gl1 ON gl1.id = j1.account_id
	  join acc_gl_journal_entry je on je.transaction_id = j1.transaction_id
	  JOIN acc_gl_account gl ON gl.id = je.account_id
	  where mo.id = 1	  
	  AND LEFT(j1.entry_date,10) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) 
	  and gl1.id <> '360'
	  and je.account_id = '360'
	  AND LEFT(je.entry_date,10) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
	  ORDER BY j1.entry_date, j1.id
  ) details
  LEFT JOIN (
	  SELECT 
	  je.account_id acid2,
	  if(
		  aga1.classification_enum in (1, 5),
		  (
		  		SUM(if(je.type_enum = 2, IFNULL(je.amount, 0), 0)) - SUM(if(je.type_enum = 1, IFNULL(je.amount, 0), 0))
		  ),
		  (
		  		SUM(if(je.type_enum = 1, IFNULL(je.amount, 0), 0)) - SUM(if(je.type_enum = 2, IFNULL(je.amount, 0), 0))
		  )
	  ) openingbalance
	  FROM m_office o
	  JOIN acc_gl_journal_entry je ON je.office_id = o.id
	  JOIN acc_gl_account aga1 ON aga1.id = je.account_id
	  WHERE je.entry_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
	  AND je.office_running_balance IS NOT NULL
	  AND (o.id = 1)
	  AND je.account_id = '360'
	  GROUP BY je.account_id
	  ) opb ON opb.acid2 = details.acid1
  GROUP BY details.edate,
  details.acid1, details.report_header, details.reportid, details.account_name, details.branchname, details.id
  ORDER BY details.id
'''       
# apply function
mifos_df = extract_function(query=sql, url=mifos_url, user=mifos_user, password=mifos_password)
mifos_df.persist()
df_report = mifos_df.toPandas()


# COMMAND ----------

len(df_report)

# COMMAND ----------

df_report.head()

# COMMAND ----------

sql = """
SELECT account_name, COUNT(*) AS duplicate_occurence_count 
FROM df_report 
GROUP BY account_name 
HAVING COUNT(*) > 1 
ORDER BY COUNT(*) DESC"""
df_duplicate = duckdb.query(sql).to_df()
df_duplicate.head()

# COMMAND ----------

DUPLICATE = len(df_duplicate)
print(DUPLICATE)

# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/Finance_FD_ACCRUAL/Finance_FD_ACCRUAL.xlsx") as writer:
    df_duplicate.to_excel(writer, sheet_name = f'Duplicate_{yesterday}', index = False)
    df_report.to_excel(writer, sheet_name = f'Report_{yesterday}', index = False)

# COMMAND ----------

# FILE TO SEND AND ITS PATH
filename = f'Finance_FD_ACCRUAL.xlsx'
SourcePathName  = "/Workspace/ReportDump/Finance_FD_ACCRUAL/" + filename


# COMMAND ----------

import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders


msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'esther.nwafor@vfd-mfb.com, abimbola.osoba@vfd-mfb.com'
msg['CC'] = 'data-team@vfdtech.ng'
msg['Subject'] = f'Finance FD Accrual for {yesterday}'
body = f"""
Hello Team,

{DUPLICATE} duplicate found.

Please find attached report for {yesterday} with multiple sheets.

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


