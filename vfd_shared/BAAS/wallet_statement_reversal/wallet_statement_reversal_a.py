# Databricks notebook source
print("Healthy")

# COMMAND ----------

from IPython.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))

# COMMAND ----------

import pandas as pd 
import numpy as np 
import duckdb
from re import search

import warnings
warnings.filterwarnings('ignore')

from datetime import datetime, timedelta
d = datetime.today() - timedelta(days = 1)
d = d.strftime('%Y-%m-%d')

date = "'" + d + "'"
print(date)


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


# COMMAND ----------

username = data['auto-report']['email']
passwd = data['auto-report']['passwd']
print(username)

# COMMAND ----------

## Wallets list 

#('Sarepay', '1021486553', '["settlement@uniswitchng.com", "mary@uniswitchng.com"]'),
#('Techplus', '1020565895', 'tolulope@techplusnetwork.com')

wl = [
('ABEG TECHNOLOGIES LTD-PAYOUTS', '1032579523', '["abisola@piggyvest.com", "ifeanyi@piggyvest.com","reconciliation@piggyvest.com", "accounting@piggyvest.com"]') ,
('ABEG TECHNOLOGIES LTD- COLLECTIONS 1', '1032583597', '["abisola@piggyvest.com", "ifeanyi@piggyvest.com","reconciliation@piggyvest.com", "accounting@piggyvest.com"]'),
('AbegApp', '1003383267', '["oj@piggyvest.com","reconciliation@piggyvest.com", "ibk@piggyvest.com", "Accounting@piggyvest.com"]') ,
('AbegApp', '1022539683', '["oj@piggyvest.com", "reconciliation@piggyvest.com", "ibk@piggyvest.com", "Accounting@piggyvest.com"]') ,
('ABEG Settlement', '1026112260', '["oj@piggyvest.com", "reconciliation@piggyvest.com", "ibk@piggyvest.com", "Accounting@piggyvest.com"]') ,
('Patronize', '1007432974', '["oj@piggyvest.com", "Terry@patronize.co", "ibk@piggyvest.com", "Accounting@piggyvest.com", "reconciliation@piggyvest.com"]') ,
('JOLLOF BAY BUKA', '[1033587310,1033588582,1032678684]', '["management@jollofbaybuka.com", "Raphael.Oluwole@vfd-mfb.com"]') ,
('LIBERTYASSURED LIMITED', '1011974622', '["timileyin@libertyng.com, "funmi@libertyng.com", "libertyassured@gmail.com", "finance@libertyng.com"]') ,

('Veneta', '1016328952', 'support@mycashpot.co.uk'),
('Capricon', '1003571712', 'finance@capricorndigi.com') ,
('Vpay', '1024403306', '["alerts@minerva.ng", "kalu@vpay.africa", "odiong@vpay.africa"]') ,
('Vpay', '1024403296', '["alerts@minerva.ng", "kalu@vpay.africa", "odiong@vpay.africa"]') ,

('Fincra', '1008148029', '["Finance@fincra.com", "bolu@fincra.com", "kehinde@fincra.com", "abel@fincra.com", "ajiri@fincra.com", "seun@fincra.com", "channelops@fincra.com"]') ,
('Fincra Collections', '1023498132', '["Finance@fincra.com", "bolu@fincra.com", "kehinde@fincra.com", "abel@fincra.com", "ajiri@fincra.com", "seun@fincra.com", "channelops@fincra.com"]') ,

('BOARDROOM APARTMENTS', '1003725120', '["finance@boardroomapartments.com", "raphael.oluwole@vfd-mfb.com"]') ,


('Boardroom Apartment F&B', '1023100701', '["finance@boardroomapartments.com", "raphael.oluwole@vfd-mfb.com"]'),
('Korapay', '1010974515', 'payments@korapay.com') ,
('PayEvery Nigeria Limited', '1033775717', '["emeka@paydestal.com", "maisha@paydestal.com"]'),
('TechMyriad Limited', '1029448933', 'info@techmyriad.com') ,
('PAYTAP TECHNOLOGIES LIMITED', '1030817216', '["Contact@mypaytap.com", "kaytanko@gmail.com "]') ,
('GABSYN MICROFINANCE BANK LIMITED', '1032959909', '["Beamerbankltd@gmail.com", "finance@redbiller.email"]'),
('EASY PAY INTERNATIONAL SERVICES LTD', '1009840777', '["dike.akani@easypaybills.ng", "sorle.nyobah@easypaybills.ng", "michael.ekanem@easypaybills.ng"]'),
('SystemSpecs Technology Services Limited', '1030647761', '["chris-mba@systemspecs.com.ng", "nwokolobia-agu@systemspecs.com.ng", "fawole@systemspecs.com.ng", "anyangbeso@systemspecs.com.ng", "Ogunbanwo@systemspecs.com.ng"]'),
('Movis Logistics', '1027370199', '["rahman.olalekan@vfdgroup.com"]') ,
('Mikro', '1003106431', '["finance@mikro.africa", "lmd@mikro.africa", "comfort@mikro.africa", "olarogun@mikro.africa"]') ,
('Mikro', '1003172401', '["finance@mikro.africa", "lmd@mikro.africa", "comfort@mikro.africa", "olarogun@mikro.africa"]') ,
('Xchangebox', '1003016796', '["bank@xchangeboxng.com", "m.ismail@xchangeboxng.com", "r.suleiman@xchangeboxng.com", "m.sara@xchangeboxng.com"]') ,
('CICO', '[1001137813,1009932935]', '["accounts@cico.ng", "gbenga@cico.ng", "adeoye@cico.ng", "raphael.oluwole@vfd-mfb.com", "Catherine.Onelum@vfd-mfb.com"]') ,
('Kobopay', '1000107200', 'leslie.emenalo@kobopay.com.ng') ,
('Afriex', '1002730826', '["hello@afriex.co", "tope@afriex.co", "john@afriex.co", "ugochukwu@afriex.co"]') ,
('Errand pay', '[1006828912,1002706023,1003847035]', '["ajibola@errandpay.com", "adebola@errandpay.com", "finance@errandpay.com"]') ,
('Pherag', '1007849549', 'info.epherag@gmail.com') ,

('BOARDROOM APARTMENTS', '1000107169', '["finance@boardroomapartments.com", "raphael.oluwole@vfd-mfb.com"]') ,
('BOARDROOM APARTMENTS LTD. (PETTY)', '1002467236', '["finance@boardroomapartments.com", "raphael.oluwole@vfd-mfb.com"]') ,


('Cloud Africa', '1003858871', '["finance@cloud-africa.com", "accounts@cloud-africa.com"]') ,
('Egole', '[1003869277,1004419727]', '["payment@egolepay.com", "operations@egolepay.com", "Maruf.adebayo@egolepay.com", "adegbola.olasupo@egolepay.com"]') ,
('Amali', '1004577742', 'Support@amali.africa') ,
('Redbiller', '1005395200', '["theredbiller@gmail.com", "dan@redbiller.email", "finance@redbiller.email"]') ,
('Voguepay', '1003991457', '["maria.izobemhe@voguepay.com", "andrew.edoja@voguepay.com"]') ,
('Xtrapay', '1015068459', '["xtratechglobalsolutions@gmail.com", "xtrapaycb@gmail.com"]') ,
('Moneyinminutes', '1005097346', '["finance@moneyinminutes.ng", "belinda.osayamwen@moneyinminutes.ng"]') ,

('Topos', '1006941697', '["b.omogbehin@topos.com.ng", "e.voit@topos.com.ng"]') ,
('Settlapp', '[1004346559, 1004346559]', '["Ngozi@settl.me", "Finance@settl.me"]') ,
('Cashex', '1014894507', '["treasury@cashex.app", "scott@cashex.app"]') ,
('Fusion', '1015715179', '["oj@piggyvest.com", "Terry@patronize.co", "ibk@piggyvest.com", "Accounting@piggyvest.com", "reconciliation@piggyvest.com"]') ,
('Liberty', '1012864306', '["management@libertyng.com", "lukman@libertyng.com", "funmi@libertyng.com", "mjohnson@libertyng.com", "libertyassured@gmail.com", "lukman@libertyng.com", "management@libertyng.com", "lukman@libertyng.com", "dotun@libertyng.com"]') ,
('Libertyassured', '1011974622', '["funmi@libertyng.com", "mjohnson@libertyng.com", "libertyassured@gmail.com", "lukman@libertyng.com"]') ,
('LibertyPay', '1021680203', '["funmi@libertyng.com", "mjohnson@libertyng.com", "libertyassured@gmail.com", "lukman@libertyng.com"]') ,
('Pennytree', '1016428852', '["ayoogunlowo@gmail.com", "ayo@mypennytree.com", "abayomi@mypennytree.com", "support@mypennytree.com", "naomi@mypennytree.com"]') ,
('Pennytree', '1026313854', '["ayoogunlowo@gmail.com", "ayo@mypennytree.com", "abayomi@mypennytree.com", "support@mypennytree.com", "naomi@mypennytree.com"]') ,
('Traction', '1020736103', 'payment-operations@tractionapps.co') ,
('Ourpass', '1025385568', 'settlements@ourpass.co') ,
('Liquidspace', '1025567650', 'support@liquidspace.ng'),
('Bellbank', '1012418022', 'tayo.akintoye@bellmonie.com'),
('Squareroof', '1009756302', 'account@ceedvest.com'),
('Lumi', '1013105534', '["wale@enlumidata.com", "kayode@enlumidata.com", "ayotunde@enlumidata.com"]'),

('Jumpnpass', '1023338955', 'payments@jumpnpass.com'),
('Qraba', '1008837947',	'info@getflick.app'),
('Marasoft', '1023928640',	'["emmanuelm@marasoftbanking.vip", "payment-partners@marasoftbanking.vip"]'),
('Letshego', '1027772528', '["ayenim@letshego.com", "basseyj@letshego.com", "olugbengaa@letshego.com", "feyisayoa@letshego.com"]'),
('Shekel', '1023237728', 'sm.finance@shekel.africa'),

('Spleet', '1024405355', 'credit@spleet.africa, finance@spleet.africa'),
('Cashex', '1014894507', 'treasury@cashex.app'),
('Casha', '1022729503', 'team@casha.io'), 
('Casha', '1012176526', 'team@casha.io'),
('Bridger', '1011976970', 'bridger@bridger.africa'),
('Tiermoney', '1020086152',	'Patience@purse.money'),
]

df_wallets = pd.DataFrame(wl, columns = ['wallet_name', 'account_nos', 'email'])

# COMMAND ----------

t = []

for i in df_wallets['account_nos']:
    t.append((i[:11]))    
    
ti = ','.join([str(x) for x in t])
ti = ti.replace('[', '')

df_wallets['account_no'] = ti.split(',')

# COMMAND ----------

#df_wallets = df_wallets[33:34]
df_wallets.head()

# COMMAND ----------

Mifosdb = mysql.connector.connect(
  host=data['mifos']['host'],
  user=data['mifos']['user'],
  passwd=data['mifos']['passwd'],
  database = data['mifos']['database'],
  auth_plugin = 'mysql_native_password',
)

nam = f'''
    select
    mc.display_name as client_name,
    msa.account_no
    from m_client mc
    join m_savings_account msa on msa.client_id = mc.id
    where msa.account_no in ({ti})
    '''

df_nam = pd.read_sql_query(nam, Mifosdb)

df_wallets = pd.merge(df_wallets, df_nam, on = 'account_no', how = 'left')

df_wallets = df_wallets[['client_name', 'wallet_name', 'account_no', 'account_nos', 'email']]

df_wallets['show_beneficiary'] = ['Yes' if i == '1011974622' else 'No' for i in df_wallets['account_no']]
df_wallets.head()


# COMMAND ----------

def statement_spool(account_no, show_beneficiary):
    
    show_beneficiary = show_beneficiary.upper()
    
    if show_beneficiary == 'NO':
        comm = '-- '
    elif show_beneficiary == 'YES':
        comm = ''
    
    ptype = pd.read_sql_query(f'''
    select 
    sa.account_no,
    sa.product_id
    from m_savings_account sa 
    where sa.account_no in ({account_no})
    ''', Mifosdb)
    
    product_id = ptype['product_id'][0]
    
    inf = f'''
    select
    st.id as transaction_id,
    mpd.receipt_number as session_id,
    st.created_date as transaction_date,
    msa.account_no as account_no,
    msp.name as product,

    ifnull(replace(mc.display_name, "null", " "), mg.display_name) as client_name,
    
    renum.enum_value as transaction_type,
    {comm}mpd.account_number as beneficiary_account_no,
    
    ifnull(case when st.transaction_type_enum in (2,4,17,18,26)  then st.amount end ,0) as 'debit', 
    ifnull(case  when st.transaction_type_enum in (1,3) then st.amount end,0) as 'credit', 
    ifnull(st.running_balance_derived,0) as 'balance', 
    case st.is_reversed when 0 then 'No' else 'Yes' end as 'reversed',
     
    case when renum.enum_value like '%Stamp Duty%' then 'Stamp Duty' 
        when st.transaction_type_enum in (20, 21) then renum.enum_message_property else mtr.remarks end as 'narration'
    
        
    
    from m_savings_account_transaction st
    left join m_savings_account msa on msa.id = st.savings_account_id
    left join m_savings_product msp on msp.id = msa.product_id
    left join (SELECT
                rv.enum_name,
                rv.enum_id,
                rv.enum_message_property,
                rv.enum_value
                FROM r_enum_value rv
                WHERE rv.enum_name = 'savings_transaction_type_enum') renum on renum.enum_id = st.transaction_type_enum
 
    
    left join m_client mc on mc.id = msa.client_id
    left join m_group mg on mg.id = msa.group_id
    left JOIN m_payment_detail mpd ON mpd.id = st.payment_detail_id
    left JOIN m_payment_type mpt ON mpt.id = mpd.payment_type_id
    left join m_transaction_request mtr on mtr.transaction_id = st.id
    where st.transaction_type_enum in ('1', '2', '3', '4', '17', '18', '26') -- ('1', '2', '3', '5', '4', '17', '18', '26')
    and left(st.created_date, 10) = {date}  
    and msa.product_id = '{product_id}'
    and msa.account_no in ({account_no})
    -- and st.is_reversed = 0
    order by st.id
    '''

    df1 = pd.read_sql_query(inf, Mifosdb)

    import pymysql
    mifos_conn = pymysql.connect(user = data['mifos']['user'], 
                            password = data['mifos']['passwd'], 
                            database = data['mifos']['database'], 
                            host = data['mifos']['host'])
    mifos_cursor = mifos_conn.cursor()

    sql =f"""
        SELECT msat.id AS 'Tranaction ID', msa.account_no as 'Account No', msat.amount as 'Amount', 
        mtr.notes AS 'Transaction Remark', msat.created_date AS 'Transaction Date'
        FROM m_transaction_request mtr 
        JOIN m_savings_account_transaction msat ON msat.id = mtr.transaction_id
        JOIN m_savings_account msa ON msa.id = msat.savings_account_id
        WHERE msa.account_no in ({account_no})
        AND mtr.notes LIKE '%Reversal Of transactionId%'
        and left(msat.created_date, 10) = {date}
        ORDER BY msat.id ASC;
    """
    mifos_cursor.execute(sql)
    df2 = pd.DataFrame(mifos_cursor.fetchall())

    df2 = df2.rename(columns={
    0: "Tranaction ID",
    1: "Account No",
    2: "Amount",
    3: "Transaction Remark",
    4: "Transaction Date",
    })
    
    return df1, df2


# COMMAND ----------

# fetch transaction references from wallet table and merge with output
def merge_trf(output):
    t = output[0]
    s = t['session_id'].fillna(0).astype(str)
    s = "'"+s+"'"
    trf = pd.read_sql_query(f'''
                select
                distinct session_id,
                transaction_id as Transaction_reference
                from TM_WALLET_TRANSACTIONS
                where session_id in ('0', {','.join([str(x) for x in s.tolist()])})
                group by 1
                ''', DumpDB)

    trf = pd.merge(t, trf, on ='session_id', how = 'left')
    
    new_output = [trf, output[1]]
    
    return new_output

# COMMAND ----------

def save_send_statement(merchant_name, client_name, output):
    df1 = output[0]
    df2 = output[1]
    
    with pd.ExcelWriter(f'/Workspace/ReportDump/Wallet_Account_Statements/{merchant_name}_Bank_Statement_And_Reversal_{d}.xlsx') as writer:
        df1.to_excel(writer,sheet_name = 'Bank Statement', index = False)
        df2.to_excel(writer,sheet_name = 'Reversal', index = False)

        
    ## FILE TO SEND AND ITS PATH
    filename = f'{merchant_name}_Bank_Statement_And_Reversal_{d}.xlsx'
    SourcePathName  = '/Workspace/ReportDump/Wallet_Account_Statements/' + filename 

    
    msg = MIMEMultipart()
    msg['From'] = 'auto-report@vfdtech.ng'
    msg['To'] = f'{recipient}'
    msg['CC'] = 'Olugbenga.Paseda@vfdtech.ng, Victor.Nwaka@vfd-mfb.com'
    msg['BCC'] = 'data-team@vfdtech.ng'

    msg['Subject'] = 'V Account Statement'
    body = f"""
    Hello {client_name},

    Your bank statement and reversals for {d} is attached for your review.

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
    
    return (f'{merchant_name} statement sent successfully!')


for row in df_wallets[:].itertuples():
    
    merchant_name = row[2]
    client_name = row[1]
    show_beneficiary = row[6]
        
    if search('\[', row[4]) and search(']', row[4]):
        a = row[4]
        a = a.replace('[', '')
        a = a.replace(']', '')
        
    else:
        a = row[4]

    if search('\[', row[5]) and search(']', row[5]):
        recipient = row[5]
        recipient = recipient.replace('[', '')
        recipient = recipient.replace(']', '')
        recipient = ''.join([str(x) for x in recipient.split('"')])
        
    else:
        recipient = row[5]
    
    print(show_beneficiary)     
    print(a)
    print(recipient)
    print('\n')
       
    output = statement_spool(a, show_beneficiary)
    
    if merchant_name in ['Fusion', 'Sarepay']:
        
        new_output = merge_trf(output)
    
    else:
        
        new_output = output
    
    save_send_statement(merchant_name, client_name, new_output)
    print(f'{merchant_name} statement sent successfully!')

    


# COMMAND ----------


