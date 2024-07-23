# Databricks notebook source
import warnings
warnings.filterwarnings('ignore')

import pandas as pd 
import numpy as np  
import duckdb
from sqlalchemy import create_engine

import mysql.connector

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

engine = conn


# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------


from datetime import datetime, timedelta
start = datetime.today() - timedelta(days = 7)
s = start.strftime('%Y-%m-%d')
start = start.strftime('%Y-%m-%d') 
start = "'" + start + "'"


end = datetime.today() - timedelta(days = 1)
e = end.strftime('%Y-%m-%d') 
end = end.strftime('%Y-%m-%d') 
end = "'" + end + "'"


print(s)
print(e)

print(start)
print(end)



pd.set_option('display.float_format', lambda x: '%.2f' % x)


# COMMAND ----------

# spooling all logs for the last 7 days

alogs = '''
    SELECT 
    msc.ID AS id,
    msc.IP AS IP,
    msc.SUBSCRIBER_USERNAME AS subscriber,
    msc.API_RESOURCE_CONSUMED,
    case when msc.API_RESOURCE_CONSUMED IN ('/bvn/basic', '/bvn/premium') AND msc.RESOURCE_VALUE LIKE '%/%' then 
        CONCAT(msc.API_RESOURCE_CONSUMED,'-account') ELSE msc.API_RESOURCE_CONSUMED END AS resource_consumed,
    msc.RESOURCE_VALUE AS resource_value,
    msc.RESPONSE_STATUS AS response_status,
    msc.DATE,
    msc.API_NAME 
    FROM MS_SUBSCRIBERS_CALLS msc 
    WHERE LEFT(msc.DATE, 10) between  left(curdate(), 10) - interval 7 day and left(curdate(), 10) - interval 1 day
    and msc.SUBSCRIBER_USERNAME not in ('b.daniel','Victor Nwaka','v.nwaka','tunde_vfd_test')
    AND msc.RESPONSE_STATUS NOT IN ('Fail', 'Failed', 'FAIL', 'FAILED')
    '''
df_alogs = pd.read_sql_query(alogs, Dumpdb)
df_alogs 


# COMMAND ----------

df_alogs.dtypes

# COMMAND ----------

# total bvn by resource
bvn_by_rsrc = duckdb.query(f''' 
                            select 
                            da.resource_consumed,
                            -- da.resource_value,
                            count(*) as calls
                            from df_alogs da
                            where left(CAST(da.DATE AS TEXT), 10) between '{s}' and '{e}'
                            and da.response_status  not in ('Fail', 'Failed', 'FAIL', 'FAILED')
                            and da.subscriber not in ('b.daniel','Victor Nwaka','v.nwaka','tunde_vfd_test')
                            and da.resource_consumed in (
                                                        '/bvn/premium', '/transfer/inter', '/bvn/basic',
                                                       '/bvn/premium-account', '/transfer/intra', '/client',
                                                       '/bvn/basic-account', '/qrcode/generate', '/clientdetails',
                                                       '/corporateclient', '/verifybvn', '/create-disburse'
                                                        )
                            group by da.resource_consumed
                            ''').to_df()


# COMMAND ----------

bvn_by_rsrc.head()

# COMMAND ----------

# total bvn by resource
df_total_bvn = duckdb.query(f''' 
                            select 
                            -- da.resource_consumed,
                            -- da.resource_value,
                            count(*) as calls
                            from df_alogs da
                            where left(CAST(da.DATE AS TEXT), 10) between '{s}' and '{e}'
                            and da.response_status  not in ('Fail', 'Failed', 'FAIL', 'FAILED')
                            and da.subscriber not in ('b.daniel','Victor Nwaka','v.nwaka','tunde_vfd_test')
                            and da.resource_consumed in (
                                                        '/verifybvn',
                                                        '/bvn/basic',
                                                        '/bvn/premium',
                                                        '/bvn/basic-account',
                                                        '/bvn/premium-account',
                                                        '/image/match',
                                                        '/liveness/check'
                                                        )
                            ''').to_df()


df_total_bvn_calls = '{:,.0f}'.format(df_total_bvn['calls'][0])
print(df_total_bvn_calls)


# COMMAND ----------

print(df_total_bvn_calls)

# COMMAND ----------



# total bvn by resource
df_top_bvn = duckdb.query(f''' 
                            select 
                            da.subscriber as 'client',
                            -- da.resource_consumed,
                            -- da.resource_value,
                            count(*) as calls
                            from df_alogs da
                            where left(da.DATE, 10) between '{s}' and '{e}'
                            and da.response_status  not in ('Fail', 'Failed', 'FAIL', 'FAILED')
                            and da.subscriber not in ('b.daniel','Victor Nwaka','v.nwaka','tunde_vfd_test')
                            and da.resource_consumed in (
                                                        '/verifybvn',
                                                        '/bvn/basic',
                                                        '/bvn/premium',
                                                        '/bvn/basic-account',
                                                        '/bvn/premium-account',
                                                        '/image/match',
                                                        '/liveness/check'
                                                        )
                            group by da.subscriber
                            order by count(*) desc
                            limit 5
                            ''').to_df()

top_bvn_client = df_top_bvn['client'][0]
print(top_bvn_client)


# COMMAND ----------



# total bvn by resource
df_total_wallet = duckdb.query(f''' 
                            select 
                            -- da.resource_consumed,
                            -- da.resource_value,
                            count(*) as calls
                            from df_alogs da
                            where left(da.DATE, 10) between '{s}' and '{e}'
                            and da.response_status  not in ('Fail', 'Failed', 'FAIL', 'FAILED')
                            and da.subscriber not in ('b.daniel','Victor Nwaka','v.nwaka','tunde_vfd_test')
                            and da.resource_consumed in (
                                                        '/corporateclient',
                                                        '/clientdetails',
                                                        '/client'
                                                        )
                            ''').to_df()


df_total_wallet_calls = '{:,.0f}'.format(df_total_wallet['calls'][0])
print(df_total_wallet_calls)


# COMMAND ----------

df_top_wallet = duckdb.query(f''' 
                            select 
                            da.subscriber as client,
                            -- da.resource_consumed,
                            -- da.resource_value,
                            count(*) as calls
                            from df_alogs da
                            where left(da.DATE, 10) between '{s}' and '{e}'
                            and da.response_status  not in ('Fail', 'Failed', 'FAIL', 'FAILED')
                            and da.subscriber not in ('b.daniel','Victor Nwaka','v.nwaka','tunde_vfd_test')
                            and da.resource_consumed in (
                                                        '/corporateclient',
                                                        '/clientdetails',
                                                        '/client'
                                                        )
                            group by da.subscriber
                            order by count(*) desc
                            limit 5
                            ''').to_df()

df_top_wallet.head()


top_wallet_client = df_top_wallet['client'][0]
print(top_wallet_client)

# COMMAND ----------

df_all_api_calls = duckdb.query('''
                                select 
                                da.subscriber as customer,
                                da.resource_consumed as resource,
                                count(*) as calls
                                from df_alogs da
                                group by da.subscriber, da.resource_consumed
                                order by da.subscriber, da.resource_consumed
                                ''').to_df()


df_all_api_calls

# COMMAND ----------

df_total_qr = duckdb.query(f''' 
                            select 
                            da.subscriber as client,
                            count(*) as calls
                            from df_alogs da
                            where da.resource_consumed in (
                                                      '/qrcode/generate'
                                                        )
                            group by da.subscriber
                            ''').to_df()


df_total_qr_calls = '{:,.0f}'.format(df_total_qr['calls'][0])
print(df_total_qr_calls)

# COMMAND ----------

df_top_qr = duckdb.query(f''' 
                            select 
                            da.subscriber as client,
                            count(*) as calls
                            from df_alogs da
                            where da.resource_consumed in (
                                                      '/qrcode/generate'
                                                        )
                            group by da.subscriber
                            order by da.subscriber desc limit 1
                            ''').to_df()



top_qr_client = df_top_qr['client'][0]
print(top_qr_client)


# COMMAND ----------



df_total_lo = duckdb.query(f''' 
                            select 
                            da.subscriber as client,
                            count(*) as calls
                            from df_alogs da
                            where da.resource_consumed in (
                                                      '/create-disburse'
                                                        )
                            group by da.subscriber
                            order by da.subscriber desc limit 1
                            ''').to_df()


df_total_loan_calls = '{:,.0f}'.format(df_total_lo['calls'][0])
print(df_total_loan_calls)


# COMMAND ----------

df_top_lo = duckdb.query(f''' 
                            select 
                            da.subscriber as client,
                            count(*) as calls
                            from df_alogs da
                            where da.resource_consumed in (
                                                      '/create-disburse'
                                                        )
                            group by da.subscriber
                            order by da.subscriber desc limit 1
                            ''').to_df()



top_loan_client = df_top_lo['client'][0]
print(top_loan_client)


# COMMAND ----------


a = f"""
SELECT 
TWT.ID,
TWT.TRANSACTION_ID,
TWT.WALLET_NAME,
TWT.TRANSACTION_TYPE,
TWT.FROM_ACCOUNT_NO,
TWT.FROM_BANK,
TWT.FROM_CLIENT_ID,
TWT.TO_ACCOUNT_NO,
TWT.TO_BANK,
TWT.TO_CLIENT_ID,
TWT.TO_CLIENT,
cast(TWT.AMOUNT as unsigned) as AMOUNT,
TWT.TRANSACTION_RESPONSE,
TWT.STATEMENT_IDS,
TWT.TIME,
TWT.SESSION_ID
FROM TM_WALLET_TRANSACTIONS TWT 
WHERE LEFT(TWT.TIME,10) BETWEEN {start} AND {end}  
AND TWT.TRANSACTION_RESPONSE = '00'

AND TWT.TRANSACTION_TYPE in ('OUTFLOW', 'INFLOW')
AND TWT.WALLET_NAME != 'Victor Nwaka';
"""
df_twt = pd.read_sql_query(a, Dumpdb)
df_twt.head()

# COMMAND ----------

df_total_transfer = duckdb.query(f'''
                                select 
                                'Transfer API' as type,
                                count(*) as volume,
                                sum(cast(dt.AMOUNT as BIGINT)) as value
                                from df_twt dt
                                '''
                                ).to_df()


df_total_transfer



df_total_transfer_volume = '{:,.0f}'.format(df_total_transfer['volume'][0])
print(df_total_transfer_volume)

# ₦
df_total_transfer_value = 'N{:,.2f}'.format(df_total_transfer['value'][0])
print(df_total_transfer_value)

# COMMAND ----------


df_top_transfer = duckdb.query(f'''
                                select 
                                dt.WALLET_NAME as wallet_name,
                                'Transfer API' as type,
                                count(*) as volume,
                                sum(cast(dt.AMOUNT as BIGINT)) as value
                                from df_twt dt
                                where dt.TRANSACTION_TYPE = 'OUTFLOW'
                                group by dt.WALLET_NAME
                                order by count(*) desc limit 5
                                '''
                                ).to_df()


df_top_transfer



# COMMAND ----------


df_inter_out_transfer_api = duckdb.query(f'''
                                        select 
                                        dt.WALLET_NAME as customer,
                                        count(*) as volume,
                                        sum(dt.AMOUNT) as value
                                        from df_twt dt
                                        where dt.TRANSACTION_TYPE = 'OUTFLOW'
                                        -- and dt.TO_BANK != '999999'
                                        and dt.WALLET_NAME != 'Victor Nwaka'
                                        group by dt.WALLET_NAME
                                        order by dt.WALLET_NAME
                                        ''').to_df()


df_inter_out_transfer_api
total_outflow_value = 'N{:,.2f}'.format(df_inter_out_transfer_api['value'].sum())
total_outflow_volume = '{:,.0f}'.format(df_inter_out_transfer_api['volume'].sum())


df_inter_in_transfer_api = duckdb.query(f'''
                                        select 
                                        dt.WALLET_NAME as customer,
                                        count(*) as volume,
                                        sum(dt.AMOUNT) as value
                                        from df_twt dt
                                        where dt.TRANSACTION_TYPE = 'INFLOW'
                                        -- and dt.FROM_BANK != '999999'
                                        and dt.WALLET_NAME != 'Victor Nwaka'
                                        group by dt.WALLET_NAME
                                        order by dt.WALLET_NAME
                                        ''').to_df()

df_inter_in_transfer_api
total_inflow_value = 'N{:,.2f}'.format(df_inter_in_transfer_api['value'].sum())
total_inflow_volume = '{:,.0f}'.format(df_inter_in_transfer_api['volume'].sum())



# COMMAND ----------


baas = '''
SELECT
vbi.WALLET_NAME as 'Wallet Name',
vbw.DISBURSEMENT_ACCOUNT_NUMBER,
vbw.COLLECTION_ACCOUNT_NUMBER,
case when (vbw.DISBURSEMENT_ACCOUNT_NUMBER = '0' OR vbw.DISBURSEMENT_ACCOUNT_NUMBER = ' ' OR isnull(vbw.DISBURSEMENT_ACCOUNT_NUMBER)) 
				AND (vbw.COLLECTION_ACCOUNT_NUMBER != '0' OR !isnull(vbw.COLLECTION_ACCOUNT_NUMBER) OR vbw.COLLECTION_ACCOUNT_NUMBER != ' ')
				then vbw.COLLECTION_ACCOUNT_NUMBER
		when (vbw.DISBURSEMENT_ACCOUNT_NUMBER != '0' OR vbw.DISBURSEMENT_ACCOUNT_NUMBER != ' ' OR !isnull(vbw.DISBURSEMENT_ACCOUNT_NUMBER)) 
				AND (vbw.COLLECTION_ACCOUNT_NUMBER = '0' OR isnull(vbw.COLLECTION_ACCOUNT_NUMBER) OR vbw.COLLECTION_ACCOUNT_NUMBER = ' ')
				then vbw.DISBURSEMENT_ACCOUNT_NUMBER
		ELSE vbw.DISBURSEMENT_ACCOUNT_NUMBER END AS 'Account No'
FROM VBAAS_WALLET_IDENTIFIERS vbi
join VBAAS_WALLET_ACCOUNTS vbw ON vbw.WALLET_ID = vbi.ID
where vbw.DISBURSEMENT_ACCOUNT_NUMBER not in ('1000076436', '1023780592')
'''

df_baas = pd.read_sql_query(baas, Dumpdb)

df_baas = df_baas[df_baas['Account No'].isna() == False]

df_acc =  df_baas[['Wallet Name', 'Account No']]

accts = df_baas['Account No'].fillna(0)
accts = "'" + df_baas['Account No'] + "'"


# COMMAND ----------

stri = ','.join([str(x) for x in accts.tolist()])
a = f"""
SELECT mc.id AS 'client_id', mc.display_name AS 'client_name', msa.account_no AS 'account_no', 
SUM(msa.account_balance_derived) AS 'account_balance'
FROM m_client mc
JOIN m_savings_account msa ON msa.client_id = mc.id
WHERE ISNULL(msa.closedon_date)
AND msa.account_no IN (
{stri}
)
GROUP BY mc.id;
"""
df_mifos = pd.read_sql_query(a, Mifosdb)


a_accts = ','.join([str(x) for x in df_mifos['account_no'].tolist()])

# COMMAND ----------

from datetime import datetime, timedelta
today = datetime.today().strftime('%Y-%m-%d')
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
seven_days_ago = (datetime.today() - timedelta(days = 7)).strftime('%Y-%m-%d')
# year_start= datetime(datetime.today().year, 1, 1).strftime('%Y-%m-%d')
# year_end = datetime(datetime.today().year, 12, 31).strftime('%Y-%m-%d')

df_dates = pd.date_range(start=f"{seven_days_ago}", end=f"{yesterday}").tolist()

dates1 = [str(x)[:10] for x in df_dates]
dates2 = [str(x.strftime('%Y_%m_%d'))[:10] for x in df_dates]

yesterday2 = (datetime.today() - timedelta(days = 1)).strftime('%Y_%m_%d')
two_days_ago = (datetime.today() - timedelta(days = 2)).strftime('%Y_%m_%d')


print(yesterday)
print(yesterday2)
print(two_days_ago)
print(seven_days_ago)

# COMMAND ----------

dts = ['"'+x+'"' for x in dates2]
c = ','.join([str(x) for x in dts])

ttw = f'''
    select
    account_no,
    {c}  
    from twenty_twenty_two_deposit_balances tt2
    where tt2.account_no in ({a_accts})
    '''

df_ttw = pd.read_sql_query(ttw, conn)
df_ttw['account_no'] = df_ttw['account_no'].astype(str)


df_acc = pd.merge(df_mifos, df_ttw, on = 'account_no', how = 'left')


df_acc['Avg. Deposit'] = df_acc[[col for col in df_acc.columns[4:]]].mean(axis=1)

df_acc.head(10)


df_avg_deposit = df_acc['Avg. Deposit'].sum()
df_avg_deposit = 'N{:,.2f}'.format(df_avg_deposit)
print(df_avg_deposit)

# COMMAND ----------

with pd.ExcelWriter(f'/Workspace/ReportDump/Baas_Weekly_Report/baas_weekly_report {s} to {e}.xlsx') as writer:
    df_acc.to_excel(writer,sheet_name = 'Account Details', index=False)
    df_all_api_calls.to_excel(writer,sheet_name = 'API calls', index=False)
    df_inter_out_transfer_api.to_excel(writer,sheet_name = 'Inter Outward Trxn.', index=False)
    df_inter_in_transfer_api.to_excel(writer,sheet_name = 'Inter Inward Trxn.', index=False)


    
df_full = pd.DataFrame({'Reporting Metrics': ['Total BVN calls', 'Top BVN client', 'Total wallet calls',
                               'Top Wallet client', 'Total Loan calls', 'Top Loan client',
                               'Total QR calls', 'Top QR client', 'Total Transfer volume', 
                               'Total Transfer value',
                                'Cumulative Average deposit',
                                'Total outflow volume', 
                               'Total outflow value', 'Total Inflow volume', 'Total Inflow value'],
                  'Values': [f'{df_total_bvn_calls}', f'{top_bvn_client}', f'{df_total_wallet_calls}', 
                              f'{top_wallet_client}', f'{df_total_loan_calls}', f'{top_loan_client}', 
                              f'{df_total_qr_calls}', f'{top_qr_client}', f'{df_total_transfer_volume}', 
                              f'{df_total_transfer_value}', 
                              f'{df_avg_deposit}', 
                              f'{total_outflow_volume}', 
                              f'{total_outflow_value}', f'{total_inflow_volume}', f'{total_inflow_value}' ]
                  })

df_full.to_csv('/Workspace/ReportDump/Baas_Weekly_Report//baas.csv', index =False)


## FILE TO SEND AND ITS PATH
filename = f'baas_weekly_report {s} to {e}.xlsx'
SourcePathName  = '/Workspace/ReportDump/Baas_Weekly_Report/' + filename 

# COMMAND ----------



import csv
from tabulate import tabulate
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib



me = 'auto-report@vfdtech.ng'
you = 'toondex20@gmail.com' 

text = f"Reporting week {s} to {e}." + """
Please see details below:

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
<p>Dear All, </br>
</br>
<p>Please see below details for reporting week """+s+""" to """+e+""". 
</br> Also find attached sheet for breakdown.</p>
</br>

{tab}
</br>


<p>Regards,</p>
</body>
</html>
"""


with open('C:/Users/babatunde.omotayo/Downloads/V_bank_tasks/projects/baas_weekly_report/baas.csv') as input_file:
    reader = csv.reader(input_file)
    data = list(reader)

text = text.format(table=tabulate(data, headers="firstrow", tablefmt="grid"))
html = html.format(tab=tabulate(data, headers="firstrow", tablefmt="html"))

message = MIMEMultipart(
    "alternative", None, [MIMEText(text), MIMEText(html,'html')])

print(message)


message['From'] = 'auto-report@vfdtech.ng'
#message['To'] = 'Oladapo.Omolaja@vfdtech.ng'
message['To'] = 'Gbenga.Omolokun@vfdgroup.com, Olugbenga.Paseda@vfdtech.ng, Temitayo.Akerele@vfdtech.ng, Victor.Nwaka@vfdtech.ng, victoria.faloye@vfdtech.ng, rotimi.awofisibe@vfd-mfb.com'
message['Bcc'] =  'Oladapo.Omolaja@vfdtech.ng'
message['Subject'] = f'BaaS/Wallet Product Report {s} to {e}'


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

# COMMAND ----------


