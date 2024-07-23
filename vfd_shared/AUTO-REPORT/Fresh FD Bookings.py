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

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

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
		ELSE vbw.DISBURSEMENT_ACCOUNT_NUMBER END AS 'account_no'
FROM VBAAS_WALLET_IDENTIFIERS vbi
join VBAAS_WALLET_ACCOUNTS vbw ON vbw.WALLET_ID = vbi.ID
where vbw.DISBURSEMENT_ACCOUNT_NUMBER != '1000076436'
'''

df_baas = pd.read_sql_query(baas, Dumpdb)

df_baas = df_baas[(df_baas['account_no'].isna() == False) & (df_baas['account_no'] != '') & (df_baas['account_no'] != '0')]

df_acc =  df_baas[['Wallet Name', 'account_no']]

accts = df_baas['account_no'].fillna(0)
accts = "'" + df_baas['account_no'] + "'"

wallets = ','.join([str(x) for x in accts.tolist()])

# COMMAND ----------


co = '''
    select 
    mc.id as client_id,
    replace(mc.display_name , "null", " ") as client_name,
    concat("'", msa.account_no,  "'") as account_no,
    msp.name as product_name
    from m_savings_account msa 
    join m_client mc on mc.id = msa.client_id
    join m_savings_product msp on msp.id = msa.product_id
    where msp.id = 26
    and isnull(mc.closedon_date)
    and msa.account_no != 1002538503
    '''
df_co = pd.read_sql_query(co, Mifosdb)
# itables.show(df_co, maxBytes = 2**200)
corp = ','.join([str(x) for x in df_co['account_no'].tolist()])


# COMMAND ----------


# inflow transactions from mifos excluding intrabank 

inf = f'''
    select
    st.id as transaction_id,
    ifnull(mc.id, mg.id) as client_id,
    ifnull(replace(mc.display_name, "null", " "), mg.display_name) as client_name,
    msa.account_no,
    'VFD MFB' as beneficiary_bank,
    
    case when msa.account_no in ({wallets}) and msa.account_no in ({corp}) then 'wallet-corporate'
        when msa.account_no not in ({wallets}) and msa.account_no in ({corp}) then 'pure-corporate'
        when msa.account_no in (select sa.account_no from m_client cl join m_savings_account sa on sa.client_id = cl.id where !isnull(cl.image_id) and sa.product_id in (24,26,29,30,31,32,33)) then 'standard retail'
        when msa.account_no in (select s.account_no from m_savings_account s where s.product_id not in (24,26,29,30,31,32,33,36)) then 'wallet retail'
        else 'others' end as 'account_type',
    
    msa.product_id as product_id,
    msp.name as product_name,
    concat(format(msa.nominal_annual_interest_rate, 2), '%') as annual_interest_rate,
    mdat.deposit_period as 'FD_Tenor (days)',
    -- msa.original_interest_rate,
    st.amount,
    mpd.account_number as sender_account_number,
    mtr.transaction_brand_name as sender_name,
    
    case when msa.activatedon_userid = 31 then 'app booking' else 'manual booking' end as booking_channel,

    mpd.routing_code,
    mpt.value as channel,
    mpd.check_number,
    
    case st.is_reversed 
        when '0' then 'SUCCESS'
        else 'FAILED' end as 'transaction_status',
    
    st.created_date as transaction_date,
    case when (st.transaction_type_enum = '1' and mtr.remarks not like '%Reversal%')  then 'credit'
        when (st.transaction_type_enum = '2' and mtr.remarks not like '%Reversal%')  then 'debit' 
        when (st.transaction_type_enum = '3')  then 'interest-posting' 
        when mtr.remarks like '%Reversal%' then 'Reversal' end as 'transaction_type',
        
    case when st.transaction_type_enum = '3' then 'interest posting' else mtr.remarks end as 'narration'
    
    from m_savings_account_transaction st
    join m_savings_account msa on msa.id = st.savings_account_id
    join m_savings_product msp on msp.id = msa.product_id
    left join m_deposit_account_term_and_preclosure mdat on mdat.savings_account_id = msa.id
    left join m_client mc on mc.id = msa.client_id
    left join m_group mg on mg.id = msa.group_id
    left JOIN m_payment_detail mpd ON mpd.id = st.payment_detail_id
    left JOIN m_payment_type mpt ON mpt.id = mpd.payment_type_id
    left join m_transaction_request mtr on mtr.transaction_id = st.id
    where st.transaction_type_enum in ('1', '2', '3')
    and msa.product_id = 36
    and left(st.transaction_date, 10) = curdate() - interval 1 day
    '''

df_inf = pd.read_sql_query(inf, Mifosdb)


# COMMAND ----------

book = df_inf[df_inf['transaction_type'] == 'credit']
liq = df_inf[df_inf['transaction_type'] == 'debit']
mg = book.merge(liq[['client_id', 'narration', 'amount']], on = 'client_id', how ='left')
mg_fil = mg[mg['narration_y'].isna()]

ids = f'''
select
cl.id as id,
cl.display_name as referred_by_name,
cl.referral_id as referral_code,
a.id as client_id
from m_client cl

left join  (select
    mc.id,
    mc.referred_by_id
    from m_client mc 
    where mc.id in ({','.join([str(x) for x in mg_fil['client_id'].tolist()])})) a on a.referred_by_id = cl.id
where cl.id in
    (select
    mc.referred_by_id
    from m_client mc 
    where mc.id in ({','.join([str(x) for x in mg_fil['client_id'].tolist()])}))
'''
ids_df = pd.read_sql_query(ids, Mifosdb)
final_mg = pd.merge(mg_fil, ids_df, on = 'client_id', how = 'left')
final_mg = final_mg[['client_id', 'client_name', 'account_no', 'product_name', 'annual_interest_rate', 'FD_Tenor (days)',
       'amount_x', 'transaction_date', 'booking_channel', 'narration_x', 'referred_by_name',
       'referral_code']]

final_mg.rename(columns = {'narration_x': 'narration', 'amount_x': 'amount'}, inplace = True)

fresh = final_mg
fresh_manual = fresh[fresh['booking_channel'] == 'manual booking'].reset_index(drop = True)
fresh_app = fresh[fresh['booking_channel'] == 'app booking'].reset_index(drop = True)

# COMMAND ----------

# Adjusted to include all FD bookings on: 10th August 2023 
# all transactions related to term deposit accounts
alltd = pd.read_sql(f'''
    select
    dat.transaction_id,
    dat.client_id,
    dat.client_name,
    dat.account_no,
    dat.product_name,
    dab.nominal_annual_interest_rate as annual_interest_rate,
    dtd.deposit_period as FD_Tenor_days,
    dat.amount,
    dat.transaction_initializer,
    dab.activatedby_userid,
    dab.activatedby_username,
    case when dab.activatedby_userid = 31 then 'app booking' else 'manual booking' end as booking_channel,
    transaction_date,
    dab.approvedon_date,
    transaction_type,
    transaction_remarks
    from dwh_all_transactions dat
    left join dwh_account_balances dab on dab.account_no = dat.account_no
    left join dwh_term_deposit dtd on dtd.account_id = dat.account_id
    where dat.product_name = 'Term Fixed Deposit'
    and left(dat.transaction_date, 10) = '{yesterday}'
    and dat.transaction_type_enum = 1 -- (1,2,3)
    and dat.is_reversed = 'No'
    ''', conn)

# COMMAND ----------

ids = f'''
select
cl.id as id,
cl.display_name as referred_by_name,
cl.referral_id as referral_code,
a.id as client_id
from m_client cl
left join  (select
    mc.id,
    mc.referred_by_id
    from m_client mc 
    where mc.id in ({','.join([str(x) for x in alltd['client_id'].tolist()])})) a on a.referred_by_id = cl.id
where cl.id in
    (select
    mc.referred_by_id
    from m_client mc 
    where mc.id in ({','.join([str(x) for x in alltd['client_id'].tolist()])}))
'''
ids_df = pd.read_sql_query(ids, Mifosdb)
final_mg = pd.merge(alltd, ids_df, on = 'client_id', how = 'left')
final_mg = final_mg[['client_id', 'client_name', 'account_no', 'product_name', 'approvedon_date', 'annual_interest_rate', 'fd_tenor_days',
       'amount', 'transaction_date', 'booking_channel', 'activatedby_username', 'transaction_remarks', 'referred_by_name',
       'referral_code']]


final_mg.rename(columns = {'annual_interest_rate': 'annual_interest_rate_%'}, inplace = True)
final_mg['booking_type'] = np.where(final_mg['account_no'].isin(fresh['account_no'].tolist()), 'Fresh', 'Roll over')
mb = final_mg[final_mg['booking_channel'] == 'manual booking'].reset_index(drop = True)
ab = final_mg[final_mg['booking_channel'] == 'app booking'].reset_index(drop = True)


# COMMAND ----------

with pd.ExcelWriter(f"/Workspace/ReportDump/Fresh_FD_Bookings/Fresh FD Bookings {yesterday}.xlsx") as writer:
    mb.to_excel(writer, sheet_name = f'manual bookings', index = False)
    ab.to_excel(writer, sheet_name = f'app bookings', index = False)
    
# FILE TO SEND AND ITS PATH
filename = f'Fresh FD Bookings {yesterday}.xlsx'
SourcePathName  = "/Workspace/ReportDump/Fresh_FD_Bookings/" + filename


import csv, smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders



msg = MIMEMultipart()
msg['From'] = 'auto-report@vfdtech.ng'
msg['To'] = 'Esther.Nwafor@vfd-mfb.com, temitope.osinubi@vfd-mfb.com'
#msg['CC'] = 'charity.isaiah@vfd-mfb.com, mudiaga.eruotor@vfd-mfb.com, Olakunle.Salau@vfd-mfb.com, mercy.ejemudaro@vfd-mfb.com'
msg['Bcc'] = 'bright.bassey@vfd-mfb.com, data-team@vfdtech.ng'
msg['Subject'] = f'FRESH FD BOOKINGS FOR {yesterday}'
body = f"""
Hi Team,

Please find attached fresh term deposit bookings made {yesterday}.


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


