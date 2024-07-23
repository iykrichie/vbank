# Databricks notebook source
print("Healthy")

# COMMAND ----------

from re import search
from sqlalchemy import create_engine
import duckdb

import pandas as pd 
import numpy as np 
import mysql.connector


import warnings
warnings.filterwarnings('ignore')

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


from datetime import datetime, timedelta
today = datetime.today().strftime('%Y-%m-%d')
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
print(today)
print(yesterday)

from datetime import datetime, timedelta
today = datetime.today() 
yesterday2 = datetime.today() - timedelta(days = 1)
today = today.strftime('%Y-%m-%d') 
yesterday2 = yesterday2.strftime('%Y-%m-%d')


df = pd.read_sql_query(f'''
    select
    msa.client_id,
    mc.daily_withdraw_limit as 'daily_withdraw_limit {today}',
    mc.max_transaction_limit as 'max_transaction_limit {today}'
    FROM m_savings_account msa
    LEFT JOIN m_client mc ON mc.id = msa.client_id
    AND msa.product_id in (29, 31, 33)
    AND !isnull(mc.id)
    AND isnull(msa.closedon_date)
    GROUP BY mc.id;
    ''', Mifosdb)

# COMMAND ----------

len(df)

# COMMAND ----------

#save to disk for tomorrow analysis
with pd.ExcelWriter(f'/Workspace/ReportDump/Compliance_Report/limit_report {today}.xlsx') as writer:
    df.to_excel(writer,sheet_name='Report', index=False)

#get yesterdays report
df_yesterday = pd.read_excel(f'/Workspace/ReportDump/Compliance_Report/limit_report {yesterday}.xlsx')


# COMMAND ----------

dfa = df.merge(df_yesterday, how='left', on='client_id')


df_upgrade = dfa[(dfa[f'daily_withdraw_limit {today}'] > dfa[f'daily_withdraw_limit {yesterday}']) |
                 (dfa[f'max_transaction_limit {today}'] > dfa[f'max_transaction_limit {yesterday}'])]



### details ####
dfc = pd.read_sql_query(f'''
    select mc.id client_id,
    min(msa.account_no) account_no,
    mc.display_name AS account_name, 
    CASE mc.client_level_cv_id
        WHEN  99 THEN 1
        WHEN 100 THEN 2
        WHEN 101 THEN 3 END AS tier,
    CASE msa.product_id 
        WHEN 29 then 'Individual Current Account'
        WHEN 31 then 'Staff Current Account'
        WHEN 33 then 'Universal Savings Account'
    END AS 'account_type',
    msa.account_balance_derived account_balance,
    a.`Email Address` email_address,
    a.`Mobile Number` mobile_number,
    max(msat.transaction_date) last_transaction_date
    FROM m_savings_account msa
    LEFT JOIN m_savings_account_transaction msat on msa.id = msat.savings_account_id
        AND msat.transaction_type_enum in (1, 2)
        AND msat.is_reversed = 0
    JOIN m_client mc ON mc.id = msa.client_id
    JOIN `Address Info` a on a.client_id = mc.id
    where mc.id in ({','.join(str(x) for x in df_upgrade.client_id)})
    group by 1
''', Mifosdb)


df_result = dfc.merge(df_upgrade, how='inner', on='client_id')

df_result.head()

# COMMAND ----------

### compliance Engine Check
dfd = pd.read_sql_query(f'''
SELECT m.client_id CLIENT_ID,
mc.activation_date as client_activation_date,
m.approvedon_date as account_opened_date,
min(m.account_no) account_no,
mc.display_name client_name, 
ai.`Email Address` as email_address,
ms.sub_status,
case when (ma.firstname != 'MobileApp' AND ma.firstname != 'user_pnd')
then CONCAT(ma.firstname, ' ', ma.lastname) else ma.firstname end as created_by, 
case when (maa.firstname != 'MobileApp' AND maa.firstname != 'user_pnd')
then CONCAT(maa.firstname, ' ', maa.lastname) else maa.firstname end as modified_by,
ms.block_narration_comment,
ms.created_date,
ms.lastmodified_date,
ms.end_date
FROM m_savings_account m
JOIN m_client mc ON mc.id = m.client_id
left join `Address Info` ai on ai.client_id = mc.id
LEFT JOIN m_savings_account_block_narration_history ms
 ON m.id = ms.account_id
 AND ms.sub_status IN ('BLOCK DEBIT', 'BLOCK CREDIT')
LEFT JOIN m_appuser ma ON ms.createdby_id = ma.id
LEFT JOIN m_appuser maa ON ms.lastmodifiedby_id = maa.id
WHERE mc.activation_date = '{yesterday2}'
AND m.product_id in (29, 31, 33)
GROUP BY 1;
''', Mifosdb)
dfd.head()

# COMMAND ----------

if len(dfd) > 0:
    ## account opening channel
    chann = pd.read_sql(f'''
            select
            account_no,
            activatedby_user_fullname as account_activation_channel
            from dwh_account_balances dab
            where account_no in ({','.join([str(x) for x in dfd['account_no'].tolist()])})
            ''', conn)

    chann['account_no'] = chann['account_no'].astype(str)
    dfd = pd.merge(dfd, chann, on = 'account_no', how = 'left')

    ## AML rating score 
    aml = pd.read_sql(f'''
                select
                client_id as CLIENT_ID,
                final_risk_score as aml_risk_score,
                case when final_risk_score <= 1.5 then 'Low risk'
                    when final_risk_score > 1.5  and final_risk_score < 4 then 'Medium risk'
                    when final_risk_score >= 4 then 'High risk' end as aml_risk_rating
                from aml_customer_risk_rating acr
                where client_id in ({','.join([str(x) for x in dfd['CLIENT_ID'].tolist()])})
                ''', Dumpdb)

    dfd = pd.merge(dfd, aml, on = 'CLIENT_ID', how = 'left')



    e = f'''
        SELECT *
        FROM UM_ACCOUNT_TIER_INFO U
        WHERE U.CLIENT_ID IN ({','.join(str(x) for x in dfd.CLIENT_ID)})
    '''
    dfe = pd.read_sql(e, Dumpdb)


    df_engine = dfd.merge(dfe, how='inner', on='CLIENT_ID')

else:
    df_engine = pd.DataFrame(columns = ['CLIENT_ID', 'account_opened_date', 'account_no', 'client_name',
       'sub_status', 'created_by', 'modified_by', 'block_narration_comment',
       'created_date', 'lastmodified_date', 'end_date',
       'account_activation_channel', 'aml_risk_score', 'aml_risk_rating'])
    


# COMMAND ----------

df_result.to_excel(f"/Workspace/ReportDump/Compliance_Report/Limit increase {today}.xlsx", index=False)
with pd.ExcelWriter(f"/Workspace/ReportDump/Compliance_Report/Compliance {today}.xlsx") as writer:
    df_engine.to_excel(writer, sheet_name='Completed Check', index=False)
    dfd.to_excel(writer, sheet_name='All Accounts', index=False)

# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)


#send report
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

#sender address
for item in [f'Limit increase {today}', f'Compliance {today}']:
    msg = MIMEMultipart()
    sender = "auto-report@vfdtech.ng"

    #receivers address
    receiver = "Temitope.Osinubi@vfd-mfb.com, internalauditcontrol@vfd-mfb.com, damilola.fatunmise@vfd-mfb.com, victor.anyaegbu@vfd-mfb.com, data-team@vfdtech.ng" # damilola.fatunmise@vfd-mfb.com"

    #subject of mail
    subject = f"{item} Report"

    #indexing using the msg object
    msg["From"] = sender
    msg["To"] = receiver
    msg["Subject"] = subject
    

    #body of mail
    body = f"""
        Dear Team,

        Please find attached report as at {today} 

        Best Regards.
    """
    #attach method to attach the body of the mail to the object msg
    msg.attach(MIMEText(body, 'plain'))

    #file name to be sent
    f = f'{item}.xlsx'

    #open the file path in read binary mode
    file = open("/Workspace/ReportDump/Compliance_Report/" + f, mode="rb")

    #mimebase object with maintype and subtype
    mb = MIMEBase('application', 'octet-stream')

    #to change the payload into encoded form
    mb.set_payload((file).read())

    #encode into base64
    encoders.encode_base64(mb)

    #attach object mimebase into mimemultipart object
    mb.add_header('Content-Disposition', f"attachment; filename = {f}")
    msg.attach(mb)

    #connecting to the mail protocol server
    s = smtplib.SMTP('smtp-mail.outlook.com',port=587)
    s.ehlo()

    #encrypting the mail
    s.starttls()
    s.ehlo()

    #authentication
    s.login(username, passwd)

    #sending the message
    s.send_message(msg)

#terminating the session
s.quit()

# COMMAND ----------


