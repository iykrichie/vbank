# Databricks notebook source
print("Healthy")

# COMMAND ----------

from re import search
from sqlalchemy import create_engine
import duckdb


import warnings
warnings.filterwarnings('ignore')


import pandas as pd 
import numpy as np 
import mysql.connector

from datetime import datetime, timedelta
today = datetime.today().strftime('%Y-%m-%d')
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')

# COMMAND ----------

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



print(today)
print(yesterday)

# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------

#loan_accts = pd.read_excel("C:\\Users\\babatunde.omotayo\\Downloads\\V_bank_tasks\\projects\\special od report\\AUTOMATION OF OD REPORT.xlsx")

a = '''
select
sa.id as account_id,
sa.account_no,
sa.account_balance_derived,
sa.overdraft_limit,
sa.overdraft_startedon_date as disbursement_date,
sa.overdraft_closedon_date as maturation_date,
ifnull(mc.referred_by_id, 0) as referred_by_id
from m_savings_account sa
join m_client mc on mc.id = sa.client_id
where !isnull(sa.overdraft_startedon_date)
-- and sa.account_balance_derived <= -1000
'''
df_msa = pd.read_sql_query(a, Mifosdb)

refs = df_msa['referred_by_id'].astype(str)
refs = "'" +refs+"'"

## loan performance check on mifos

milo = f'''
    select
    sa.id as account_id,
    sa.account_no,
    replace(mc.display_name, 'null', ' ') as customer_name,
    sa.overdraft_limit as loan_amount,
    sa.nominal_annual_interest_rate_overdraft as interest_rate,
    sa.account_balance_derived as principal_balance,
    sa.overdraft_limit + sa.account_balance_derived as overdrawn_amount,
    sa.overdraft_startedon_date as disbursement_date,
    sa.overdraft_closedon_date as maturation_date,
    case when (timestampdiff(day, curdate(), sa.overdraft_closedon_date) > 60) then 'above 60 days'
        when (timestampdiff(day, curdate(), sa.overdraft_closedon_date) >= 31 and timestampdiff(day, curdate(), sa.overdraft_closedon_date) <= 60) then 'btw 31-60 days'
        when  (timestampdiff(day, curdate(), sa.overdraft_closedon_date) < 31) then '30 days and below' end as 'remaining_days_to_maturity',
    c.last_collection_date,
    ifnull(a.one_month_collection, 0) as one_month_collection,
    ifnull(concat(format((a.one_month_collection / sa.overdraft_limit) * 100, 2), '%'), 0) as CER_for_one_month,
    ifnull(b.three_month_collection, 0) as three_month_collection,
    ifnull(concat(format((b.three_month_collection / sa.overdraft_limit) * 100, 2), '%'), 0) as CER_for_three_months,
    ifnull(e.Sum_of_OD_Accrual_Interest_Yesterday, 0) as Sum_of_OD_Accrual_Interest_Yesterday,
    d.referral_name as account_officer
    
    from m_savings_account sa
    join m_client mc on mc.id = sa.client_id
    left join OverDraft od on od.CLIENTID = mc.id
    left join 
            (select st.savings_account_id as account_id, sum(st.amount) as 'one_month_collection' from m_savings_account_transaction st 
            -- join m_savings_account msa on st.savings_account_id = msa.id
            where st.savings_account_id in ({','.join([str(x) for x in df_msa['account_id'].tolist()])})
            and st.transaction_date between curdate() - interval 30 day and curdate()
            and st.is_reversed = 0
            and st.transaction_type_enum in ('1') 
            group by st.savings_account_id) a on a.account_id = sa.id
    left join 
            (select st.savings_account_id as account_id, sum(st.amount) as 'three_month_collection' from m_savings_account_transaction st 
            -- join m_savings_account msa on st.savings_account_id = msa.id
            where st.savings_account_id in ({','.join([str(x) for x in df_msa['account_id'].tolist()])})
            and st.transaction_date between curdate() - interval 90 day and curdate()
            and st.is_reversed = 0
            and st.transaction_type_enum in ('1') 
            group by st.savings_account_id) b on b.account_id = sa.id
    left join 
            (select st.savings_account_id as account_id, max(st.transaction_date) as 'last_collection_date' from m_savings_account_transaction st 
            -- join m_savings_account msa on st.savings_account_id = msa.id
            where st.savings_account_id in ({','.join([str(x) for x in df_msa['account_id'].tolist()])})
            and st.is_reversed = 0
            and st.transaction_type_enum in ('1') 
            group by st.savings_account_id) c on c.account_id = sa.id
        
    left join 
            (select c.id, replace(c.display_name, 'null', ' ') as referral_name from m_client c 
            where c.id in ({','.join([str(x) for x in refs.tolist()])})) d on d.id = mc.referred_by_id

    left join 
            (select st.savings_account_id as account_id, sum(st.amount) as 'Sum_of_OD_Accrual_Interest_Yesterday' 
				from m_savings_account_transaction st 
            where st.savings_account_id in ({','.join([str(x) for x in df_msa['account_id'].tolist()])})
            and st.is_reversed = 0
            and st.transaction_type_enum IN ('25') 
            AND st.transaction_date = '{yesterday}'
            GROUP BY st.savings_account_id ) e on e.account_id = sa.id
            
    where sa.id in ({','.join([str(x) for x in df_msa['account_id'].tolist()])})
    '''

milo_df = pd.read_sql_query(milo, Mifosdb)

milo_df.head()

# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/Ops_Overdraft_Report/Overdraft Report {today}.xlsx") as writer:
    milo_df.to_excel(writer, sheet_name = f'Report', index = False)
    
    
# FILE TO SEND AND ITS PATH
filename = f'Overdraft Report {today}.xlsx'
SourcePathName  = "/Workspace/ReportDump/Ops_Overdraft_Report/" + filename


import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders



msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
#msg['To'] = 'oladapo.omolaja@vfdtech.ng'
msg['To'] = 'victor.anyaegbu@vfd-mfb.com; productteam@vfd-mfb.com; patrick.chukwurah@vfd-mfb.com; creditops@vfd-mfb.com; rotimi.awofisibe@vfd-mfb.com'
msg['cc'] = 'olayinka.olatunji@vfd-mfb.com; samuel.ugochukwu@vfd-mfb.com'
msg['Bcc'] = 'data-team@vfdtech.ng'
msg['Subject'] = f'OVERDRAFT REPORT FOR SELECT ACCOUNTS AS AT {today}'
body = f"""
Hi Team,

Please find attached overdraft report as at {today}.


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

# COMMAND ----------


