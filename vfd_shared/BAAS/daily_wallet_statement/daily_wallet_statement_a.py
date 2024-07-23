# Databricks notebook source
print("Healthy")

# COMMAND ----------

import pandas as pd 
import numpy as np 

import warnings
warnings.filterwarnings('ignore')

from datetime import datetime, timedelta
date = datetime.today() - timedelta(days = 1)
date = date.strftime('%Y-%m-%d')

date1 = datetime.today() - timedelta(days = 1)
date1 = date1.strftime('%Y-%m-%d 23')
date2 = datetime.today() - timedelta(days = 2)
date2 = date2.strftime('%Y-%m-%d 23')

print(date, date1, date2)
print('\n')
d = "'"+date+"'"
d1 = "'" + date1 + "'"
d2 = "'" + date2 + "'"
print(d, d1, d2, '\n')

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders


import mysql.connector
import json
with open('/Workspace/Credentials/db_data.json', 'r') as fp:
    data = json.load(fp)

DumpDB = mysql.connector.connect(
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


# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------

vbaas = f'''
SELECT
vbi.WALLET_NAME AS wallet_name,
vbi.IMPLEMENTATION AS implementation
FROM VBAAS_WALLET_IDENTIFIERS vbi
'''     

vbaas_id = pd.read_sql_query(vbaas, DumpDB)

#wl = [ ]

wl = [
('PocketApp-FAAS',
'["oj@piggyvest.com", "reconciliation@piggyvest.com", "ibk@piggyvest.com", "Accounting@piggyvest.com"]'),

('PocketApp',
'["oj@piggyvest.com", "reconciliation@piggyvest.com", "ibk@piggyvest.com", "Accounting@piggyvest.com"]'),
('Patronize',
'["oj@piggyvest.com", "Terry@patronize.co", "ibk@piggyvest.com", "Accounting@piggyvest.com", "reconciliation@piggyvest.com"]'),
('Egolepay',
'["payment@egolepay.com", "operations@egolepay.com", "Maruf.adebayo@egolepay.com", "adegbola.olasupo@egolepay.com"]'),
('EgoleAgent',
'["payment@egolepay.com", "operations@egolepay.com","Maruf.adebayo@egolepay.com", "adegbola.olasupo@egolepay.com"]'),
('Fusion',
'["oj@piggyvest.com", "Terry@patronize.co", "ibk@piggyvest.com", "Accounting@piggyvest.com", "reconciliation@piggyvest.com"]'),
('Xchangebox',
'["bank@xchangeboxng.com", "m.ismail@xchangeboxng.com", "r.suleiman@xchangeboxng.com", "m.sara@xchangeboxng.com"]'),
('GRUPP',
'["finance@mikro.africa", "lmd@mikro.africa", "comfort@mikro.africa", "olarogun@mikro.africa"]'),
('CICO',
'["accounts@cico.ng", "gbenga@cico.ng", "mathew@cico.ng", "raphael.oluwole@vfd-mfb.com"]'),
('Kobopay', 'leslie.emenalo@kobopay.com.ng'),
('Afriex',
'["hello@afriex.co", "tope@afriex.co", "john@afriex.co", "ugochukwu@afriex.co"]'),
('Errandpay',
'["ajibola@errandpay.com", "adebola@errandpay.com", "finance@errandpay.com"]'),
('ErrandpayAgent',
'["ajibola@errandpay.com", "adebola@errandpay.com", "finance@errandpay.com"]'),
('KVFD', '["info.epherag@gmail.com", "kvfdventures@gmail.com"]'),
('Veneta', 'support@mycashpot.co.uk'),
('Baxi', 'finance@capricorndigi.com'),

('CloudAfrica',
'["finance@cloud-africa.com", "accounts@cloud-africa.com", "d.amorioloye@cloud-africa.com", "o.afolabi@cloud-africa.com"]'),


('Paytap Technologies Limited', '["Contact@mypaytap.com", "kaytanko@gmail.com "]'),
('Payevery', '["emeka@paydestal.com", "maisha@paydestal.com"]'),
('TechMyriad', 'info@techmyriad.com') ,

('SystemSpecs', '["chris-mba@systemspecs.com.ng", "nwokolobia-agu@systemspecs.com.ng", "fawole@systemspecs.com.ng", "anyangbeso@systemspecs.com.ng", "Ogunbanwo@systemspecs.com.ng"]'),
('Muuv express', 'rahman.olalekan@vfdgroup.com'),



('Amali_inclusive', 'Support@amali.africa'),
('AmaliCredit', 'Support@amali.africa'),
('Redbiller', '["theredbiller@gmail.com", "dan@redbiller.email"]'),
('Voguepay', '["maria.izobemhe@voguepay.com", "andrew.edoja@voguepay.com"]'),
('XTRAPAY', '["xtratechglobalsolutions@gmail.com", "xtrapaycb@gmail.com"]'),
('Moneyinminutes',
'["finance@moneyinminutes.ng", "belinda.osayamwen@moneyinminutes.ng"]'),
('Fincra',
'["Finance@fincra.com", "bolu@fincra.com", "kehinde@fincra.com", "abel@fincra.com", "ajiri@fincra.com", "seun@fincra.com", "channelops@fincra.com"]'),
('Topos', '["b.omogbehin@topos.com.ng", "e.voit@topos.com.ng"]'),
('Settlapp', '["Ngozi@settl.me", "Finance@settl.me"]'),

('Cashex', '["treasury@cashex.app", "scott@cashex.app"]'),
('Liberty',
'["management@libertyng.com", "lukman@libertyng.com", "funmi@libertyng.com", "mjohnson@libertyng.com", "libertyassured@gmail.com"]'),
('Libertyassured',
'["funmi@libertyng.com", "mjohnson@libertyng.com", "libertyassured@gmail.com"]'),
]


df_wallets = pd.DataFrame(wl, columns = ['wallet_name', 'email'])


# COMMAND ----------

wall = pd.merge(df_wallets, vbaas_id, on= 'wallet_name', how = 'left')
wall.drop_duplicates(subset = 'wallet_name', inplace = True)
wall = wall.reset_index(drop = True)


from re import search
global pool_setup
def pool_setup():
    dfw = pd.read_sql_query(f"""
        SELECT 
        TWT.WALLET_NAME as wallet_name, 
        TWT.TRANSACTION_ID AS 'Transaction Reference', 
        TWT.STATEMENT_IDS AS 'Statement IDS', 
        -- TWT.TRANSACTION_TYPE as transaction_type,
        TWT.SESSION_ID AS 'Session ID', 
        TWT.AMOUNT AS 'Transaction Amount', 
        TWT.TRANSACTION_RESPONSE AS 'Transaction Response', 
        TWT.TIME AS 'Transaction Date'
        FROM TM_WALLET_TRANSACTIONS TWT
        -- WHERE TWT.FROM_ACCOUNT_NO = '1003016796'
        where TWT.WALLET_NAME like '%{row[1]}%'
        AND TWT.TRANSACTION_TYPE = 'OUTFLOW'
        AND LEFT(TWT.TIME, 13) >= {d2}
        AND LEFT(TWT.TIME, 13) < {d1}
        """, DumpDB)
    if len(dfw) > 0:
        dfw = dfw[dfw['wallet_name'] == f"{dfw['wallet_name'][0]}"]
    else:
        dfw = dfw
        
    oth = f'''
        select
        twt.WALLET_NAME as wallet_name,
        count(twt.TRANSACTION_ID) as count,
        twt.TRANSACTION_ID as Transaction_Reference,
        twt.STATEMENT_IDS as 'Statement IDS',
        twt.FROM_ACCOUNT_NO,
        alb.BANK_NAME as From_bank,
        twt.TO_ACCOUNT_NO,
        twt.SESSION_ID,
        twt.AMOUNT,
        twt.TIME as 'Transaction Date'
        from TM_WALLET_TRANSACTIONS twt
        left join AM_LOCAL_BANKS alb on alb.NIP_CODE = twt.FROM_BANK
        where twt.WALLET_NAME like '%{row[1]}%'
        AND LEFT(twt.TIME, 13) >= {d2}
        AND LEFT(twt.TIME, 13) < {d1}
        and twt.TRANSACTION_TYPE != 'INITIAL INFLOW'
        and twt.TRANSACTION_RESPONSE not in (02, 99)
        group by twt.TRANSACTION_ID
        having count(twt.TRANSACTION_ID) > 1
        '''

    df2 = pd.read_sql_query(oth, DumpDB)
    if len(df2) > 0:
        df2 = df2[df2['wallet_name'] == f"{df2['wallet_name'][0]}"]        
    else:
        df2 = df2
            
    df2.drop(columns = 'count', inplace = True)
        
    init = pd.read_sql_query(f'''
        SELECT 
        TWT.WALLET_NAME as wallet_name, 
        TWT.TRANSACTION_ID AS 'Transaction Reference', 
        TWT.TRANSACTION_TYPE,
        TWT.FROM_ACCOUNT_NO AS 'From Account No',
        ALB.BANK_NAME AS 'From Bank',
        TWT.TO_ACCOUNT_NO AS 'To Account No',
        TWT.SESSION_ID AS 'Session ID', 
        TWT.AMOUNT AS 'Amount', 
        TWT.TIME AS 'Transaction Date'
        FROM TM_WALLET_TRANSACTIONS TWT
        LEFT JOIN AM_LOCAL_BANKS ALB ON ALB.NIP_CODE = TWT.FROM_BANK
        WHERE TWT.TRANSACTION_TYPE = 'INITIAL INFLOW'
        -- and twt.TRANSACTION_RESPONSE not in (02, 99)
        AND TWT.WALLET_NAME like '%{row[1]}%'
        AND LEFT(TWT.TIME, 13) >= {d2}
        AND LEFT(TWT.TIME, 13) < {d1}
        ORDER BY TWT.ID;
        ''', DumpDB)
    
        
    return dfw, df2, init

global one_to_one
def one_to_one():
    ou = f"""
        SELECT 
        TWT.WALLET_NAME as wallet_name, 
        TWT.TRANSACTION_ID AS 'Transaction Reference', 
        TWT.STATEMENT_IDS AS 'Statement IDS', 
        -- TWT.TRANSACTION_TYPE as transaction_type,
        TWT.SESSION_ID AS 'Session ID', 
        TWT.AMOUNT AS 'Transaction Amount', 
        TWT.TRANSACTION_RESPONSE AS 'Transaction Response', 
        TWT.TIME AS 'Transaction Date'
        FROM TM_WALLET_TRANSACTIONS TWT
        -- WHERE TWT.FROM_ACCOUNT_NO = '1003016796'
        where TWT.WALLET_NAME like '%{row[1]}%'
        AND TWT.TRANSACTION_TYPE = 'OUTFLOW'
        AND LEFT(TWT.TIME, 13) >= {d2}
        AND LEFT(TWT.TIME, 13) < {d1}
        """
    dfw = pd.read_sql_query(ou, DumpDB)
    if len(dfw) > 0:
        dfw = dfw[dfw['wallet_name'] == f"{dfw['wallet_name'][0]}"]
    else:
        dfw = dfw
        
    oth = f"""
        SELECT 
        TWT.WALLET_NAME as wallet_name, 
        TWT.TRANSACTION_ID AS 'Transaction Reference', 
        TWT.FROM_ACCOUNT_NO AS 'From Account No',
        ALB.BANK_NAME AS 'From Bank',
        TWT.TO_ACCOUNT_NO AS 'To Account No',
        TWT.SESSION_ID AS 'Session ID', 
        TWT.AMOUNT AS 'Amount', 
        TWT.TIME AS 'Transaction Date'
        FROM TM_WALLET_TRANSACTIONS TWT
        LEFT JOIN AM_LOCAL_BANKS ALB ON ALB.NIP_CODE = TWT.FROM_BANK
        WHERE TWT.TRANSACTION_TYPE = 'INFLOW'
        -- and twt.TRANSACTION_RESPONSE not in (02, 99)
        AND TWT.WALLET_NAME like '%{row[1]}%'
        AND LEFT(TWT.TIME, 13) >= {d2}
        AND LEFT(TWT.TIME, 13) < {d1}
        ORDER BY TWT.ID;
        """
    df2 = pd.read_sql_query(oth, DumpDB)
    if len(df2) > 0:
        df2 = df2[df2['wallet_name'] == f"{df2['wallet_name'][0]}"] 
    else:
        df2 = df2
            
            
            
    init = pd.read_sql_query(f'''
        SELECT 
        TWT.WALLET_NAME as wallet_name, 
        TWT.TRANSACTION_ID AS 'Transaction Reference', 
        TWT.TRANSACTION_TYPE,
        TWT.FROM_ACCOUNT_NO AS 'From Account No',
        ALB.BANK_NAME AS 'From Bank',
        TWT.TO_ACCOUNT_NO AS 'To Account No',
        TWT.SESSION_ID AS 'Session ID', 
        TWT.AMOUNT AS 'Amount', 
        TWT.TIME AS 'Transaction Date'
        FROM TM_WALLET_TRANSACTIONS TWT
        LEFT JOIN AM_LOCAL_BANKS ALB ON ALB.NIP_CODE = TWT.FROM_BANK
        WHERE TWT.TRANSACTION_TYPE = 'INITIAL INFLOW'
        -- and twt.TRANSACTION_RESPONSE not in (02, 99)
        AND TWT.WALLET_NAME like '%{row[1]}%'
        AND LEFT(TWT.TIME, 13) >= {d2}
        AND LEFT(TWT.TIME, 13) < {d1}
        ORDER BY TWT.ID;
        ''', DumpDB)
    
    return dfw, df2, init
             

# COMMAND ----------

global save_and_send
def save_and_send(wallet_name, output, recipient_mail):
    if wallet_name == 'PocketApp' or wallet_name =='Fusion' or wallet_name == 'Patronize':
        with pd.ExcelWriter(f'/Workspace/ReportDump/Wallet_Reports/{wallet_name} Wallet Transactions {date}.xlsx') as writer:
            output[0].to_excel(writer,sheet_name = 'Outflow', index=False)
            output[1].to_excel(writer,sheet_name = 'Inflow', index=False) 
            output[2].to_excel(writer, sheet_name = 'initial inflow', index = False)
            
    else:
        with pd.ExcelWriter(f'/Workspace/ReportDump/Wallet_Reports/{wallet_name} Wallet Transactions {date}.xlsx') as writer:
            output[0].to_excel(writer,sheet_name = 'Outflow', index=False)
            output[1].to_excel(writer,sheet_name = 'Inflow', index=False) 
        
        

    ## FILE TO SEND AND ITS PATH
    filename = f'{row[1]} Wallet Transactions {date}.xlsx'
    SourcePathName  = '/Workspace/ReportDump/Wallet_Reports/' + filename
    
    if search('\[', recipient_mail) and search(']', recipient_mail):
        # print(row[2])
        b = recipient_mail
        b = b.replace('[', '')
        b = b.replace(']', '')
        b = ''.join([str(x) for x in b.split('"')])
    else:
        b = recipient_mail
        
    print(wallet_name, '-', ''.join([str(x) for x in recipient_mail.split('"')]))
        
    # b = recipient_mail
    msg = MIMEMultipart()
    msg['From'] = 'auto-report@vfdtech.ng'
    msg['To'] = f'{b}' #'bomotayo2020@outlook.com' 
    msg['CC'] = 'Olugbenga.Paseda@vfdtech.ng, Victor.Nwaka@vfd-mfb.com'
    msg['BCC'] = 'data-team@vfdtech.ng'

    msg['Subject'] = f'{wallet_name} Transactions for {date}'
    body = f"""
    Hello {wallet_name},

    Your wallet transactions for {date} is attached for your review.

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
    
    print(f"{wallet_name} report sent successfully. \n")

    
    

def merge_balance(dfout, dfin):
    tid = []
    if len(dfout) >= 1:
        for t in dfout['Statement IDS']:
            try:
                t = t.split('-')[0]
                tid.append("'"+str(t)+"'")
            except Exception as e:
                tid.append("'"+str(t)+"'")

        bal = pd.read_sql_query(f'''
                    select
                    concat("'",id,"'") as tid,
                    running_balance_derived as balance
                    from m_savings_account_transaction st
                    where id in ({','.join([str(x) for x in tid])})
                    ''', Mifosdb)
        bal['tid'] = bal['tid'].astype(str)

        dfout['tid'] = tid
        dfout = pd.merge(dfout, bal, on = 'tid', how = 'left')
        dfout.drop(columns = 'tid', inplace = True)    
    
    else:
        dfout = dfout
        
    tid = []
    
    if len(dfin) >= 1:
        for t in dfin['Statement IDS']:
            tid.append("'"+ str(t) +"'")

        bal = pd.read_sql_query(f'''
                    select
                    concat("'",id,"'") as tid,
                    running_balance_derived as balance
                    from m_savings_account_transaction st
                    where id in ({','.join([str(x) for x in tid])})
                    ''', Mifosdb)



        dfin['tid'] = tid
        dfin = pd.merge(dfin, bal, on = 'tid', how = 'left')
        dfin.drop(columns = 'tid', inplace = True)    
    
    else:
        dfin = dfin
        
    return dfout, dfin


from re import search
for row in wall[:].itertuples():
    if row[3] == 'POOL' and row[1] == 'Pennytree':
        output = pool_setup()
        out = output[0] 
        inf = output[1]
        init = output[2]
        result = merge_balance(out, inf)
        dfout = result[0]
        dfin = result[1]
        output = dfout, dfin, init
        
    elif row[3] == 'POOL' and row[1] != 'Pennytree':
        output = pool_setup()
        
    else:  #row[3] == '1-1':
        output = one_to_one()
        
        
    save_and_send(row[1], output, row[2])    

# COMMAND ----------


