# Databricks notebook source
print("Healthy")

# COMMAND ----------

from custom_packages.email_operations import EmailOperations

# COMMAND ----------

from re import search
from sqlalchemy import create_engine

import duckdb
import pandas as pd 
import numpy as np 

import mysql.connector

import warnings
warnings.filterwarnings('ignore')


pd.set_option('display.float_format', lambda x: '%.2f' % x)

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

# COMMAND ----------

from datetime import datetime, timedelta

today = datetime.today().strftime('%Y-%m-%d')
print(today)

# COMMAND ----------

# MAGIC %%time
# MAGIC def get_referrals(referral_code):
# MAGIC     rf = pd.read_sql_query(f'''select referral_id, mc.id, display_name, sa.account_no, sa.product_id, sp.name from m_client mc 
# MAGIC     left join m_savings_account sa on sa.client_id = mc.id
# MAGIC     left join m_savings_product sp on sp.id = sa.product_id
# MAGIC     where referral_id = '{referral_code}'
# MAGIC     ''', Mifosdb)
# MAGIC     
# MAGIC     referred_by_id = rf['id'][0]
# MAGIC     
# MAGIC     mif = f'''select mc.id as client_id, mc.display_name as client_name, sa.account_no, sa.id as account_id, sa.product_id, sp.name as product_name, 
# MAGIC     sa.account_balance_derived, sa.approvedon_date as activation_date,
# MAGIC     sa.closedon_date, referred_by_id
# MAGIC     from m_client mc 
# MAGIC     join m_savings_account sa on sa.client_id = mc.id 
# MAGIC     join m_savings_product sp on sp.id = sa.product_id
# MAGIC     where mc.referred_by_id = '{referred_by_id}'
# MAGIC     and isnull(sa.closedon_date)
# MAGIC     -- AND LEFT(sa.approvedon_date, 7) >= '2023-01'
# MAGIC     order by 7 desc
# MAGIC     '''
# MAGIC     
# MAGIC     dfmif = pd.read_sql_query(mif, Mifosdb)
# MAGIC     
# MAGIC     
# MAGIC     lo = f'''select mc.display_name as client_name, sa.account_no, sa.id as loan_id, sa.product_id, sp.name as product_name, 
# MAGIC     sa.principal_disbursed_derived as principal_disbursed, sa.principal_outstanding_derived as principal_outstanding, sa.disbursedon_date as disbursement_date,
# MAGIC     sa.closedon_date, referred_by_id
# MAGIC     from m_client mc 
# MAGIC     left join m_loan sa on sa.client_id = mc.id 
# MAGIC     join m_product_loan sp on sp.id = sa.product_id
# MAGIC     where mc.referred_by_id = '{referred_by_id}'
# MAGIC     -- and left(sa.disbursedon_date, 7) >= '2023-01'
# MAGIC     -- and isnull(sa.closedon_date)
# MAGIC     -- and !isnull(sa.disbursedon_date)
# MAGIC     order by 6 desc
# MAGIC     '''
# MAGIC     
# MAGIC     dflo = pd.read_sql_query(lo, Mifosdb)
# MAGIC     
# MAGIC     return rf, dfmif, dflo
# MAGIC
# MAGIC df = get_referrals('FSNH4') #('TVSKX') # , XAXFW, KB5QB, DTNVG') #, KB5QB, DTNVG') #TVSKX  5PL7O WK12A k7m4e
# MAGIC ref = df[0]['referral_id'][0]

# COMMAND ----------

staff_list = [
              #('Raphael', '5PL7O', 'raphael.oluwole@vfd-mfb.com'), 
              #('Joshua', 'K7M4E', 'joshua.vincent@vfd-mfb.com'),
              #('Simisola', 'VGFAZ',  'simisola.olanrewaju@vfd-mfb.com'),
              #('Vivian', 'FSNH4', 'vivian.ezeani@vfd-mfb.com')
              ]
             

# COMMAND ----------

staff_list.extend([
    #('Jonathan', 'VZJT2', 'Jonathan.Ozoekwem@vfd-mfb.com'),
    #('Nosa', 'MUJOX', 'Nosa.Osemwegie@vfd-mfb.com'),
    #('Patrick', 'JWZ8P', 'Patrick.Chukwurah@vfd-mfb.com'),
    #('Simisola', 'VGFAZ', 'Simisola.Olanrewaju@vfd-mfb.com'),
    #('Elizabeth', 'SEDFI', 'Elizabeth.Enifeni@vfd-mfb.com'),
    #('Catherine', 'ZXMH5', 'Catherine.Onelum@vfd-mfb.com'),
    #('Raphael', 'KKPBV', 'Raphael.Egbe@vfd-mfb.com'),
    #('Yonodu', 'MLMD8', 'Yonodu.Okeugo@vfd-mfb.com'),
    #('Stephen', '3NDR5', 'Stephen.Tewogbade@vfd-mfb.com'),
    #('Mayowa', 'WK12A', 'Mayowa.Francis@vfd-mfb.com'),
    #('Vivian', 'FSNH4', 'Vivian.Ezeani@vfd-mfb.com'),
    #('Blessing', 'TVSKX', 'Blessing.Idogei@vfd-mfb.com'),
    ('Omolola', '37QQX', 'omolola.eleduwe@vfd-mfb.com'),
    ('Kehinde', 'IPE5S', 'kehinde.eyiaromi@vfd-mfb.com'),
])


# COMMAND ----------

staff_list

# COMMAND ----------

# MAGIC %%time
# MAGIC for staff in staff_list[:]:
# MAGIC     df = get_referrals(staff[1]) #
# MAGIC     ref = df[0]['referral_id'][0]
# MAGIC     
# MAGIC     bal = df[1]
# MAGIC     ods = bal[bal['account_balance_derived'] < 0]
# MAGIC     deps = bal[bal['account_balance_derived'] >= 0]
# MAGIC     loans = df[2]
# MAGIC     with pd.ExcelWriter(f"C:\\Users\\babatunde.omotayo\\Downloads\\V_bank_tasks\\Raphael downlines\\{ref} accounts as at {today}.xlsx") as writer:
# MAGIC         deps.to_excel(writer, sheet_name = 'deposits', index = False)
# MAGIC         loans.to_excel(writer, sheet_name = 'loans', index = False)
# MAGIC         ods.to_excel(writer, sheet_name = 'overdrafts', index = False)
# MAGIC         
# MAGIC     # Initialize email object and its parameters
# MAGIC
# MAGIC     email = EmailOperations
# MAGIC
# MAGIC     filenames = [f"{ref} accounts as at {today}.xlsx"]
# MAGIC
# MAGIC     SourcePathName  = f"C:\\Users\\babatunde.omotayo\\Downloads\\V_bank_tasks\\Raphael downlines\\"  
# MAGIC
# MAGIC     subject =   f'Referral_id - {ref} Downline Accounts'
# MAGIC
# MAGIC     name = staff[0]
# MAGIC
# MAGIC     body = f'Your downline account deposits and loans is attached for your review.'
# MAGIC
# MAGIC     recipient = staff[2]
# MAGIC
# MAGIC     ## Send mail
# MAGIC     email.send_mail(filenames, SourcePathName, today, recipient, subject, name, body)

# COMMAND ----------

deps

# COMMAND ----------


