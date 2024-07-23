# Databricks notebook source
print("Healthy")

# COMMAND ----------

import json
from datetime import datetime, timedelta
from pyspark.sql.functions import lit
import dask
import dask.dataframe as dd
from sqlalchemy import create_engine

# COMMAND ----------

from datetime import datetime, timedelta
today = datetime.today().strftime('%Y-%m-%d')
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
year =  (datetime.today() - timedelta(days = 1)).strftime('%Y')

print(year)
print(yesterday)

# COMMAND ----------

import pandas as pd

yesterday = datetime.now() - timedelta(1)
start_date = pd.to_datetime(yesterday.strftime('%Y-%m-%d 00:00:00'))
end_date = pd.to_datetime(yesterday.strftime('%Y-%m-%d 23:59:59'))

current_date = start_date
while current_date <= end_date:
      start_time = current_date.strftime('%Y-%m-%d %H:%M:%S')
      end_time = (current_date + pd.DateOffset(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
      print('RUNNING: ', start_time, ' to ', end_time)
      # Load in credentials
      with open('/Workspace/Credentials/db_data.json', 'r') as fp:
            data = json.load(fp)

      # MIFOS_DB
      mifos_driver = "org.mariadb.jdbc.Driver"
      mifos_database_host = data['mifos']['host']
      mifos_database_port = "3306" # update if you use a non-default port
      mifos_database_name = data['mifos']['database']
      mifos_user=data['mifos']['user']
      mifos_password = data['mifos']['passwd']
      mifos_url = f"jdbc:mysql://{mifos_database_host}:{mifos_database_port}/{mifos_database_name}"

      # REDSHIFT_DWH
      redshift_driver = "org.postgresql.Driver"
      redshift_host = data['redshift']['host']
      redshift_db_name = data['redshift']['database']
      redshift_user = data['redshift']['user']
      redshift_password = data['redshift']['passwd']
      redshift_url = f"jdbc:postgresql://{redshift_host}:5439/{redshift_db_name}"

      # DUMP_DB
      dump_driver = mifos_driver
      dump_host = data['Dumpdb']['host']
      dump_db_name = data['Dumpdb']['database']
      dump_user = data['Dumpdb']['user']
      dump_password = data['Dumpdb']['passwd']
      dump_url = f"jdbc:mysql://{dump_host}:3306/{dump_db_name}"

      # VBIZ_DB
      vbiz_driver = mifos_driver
      vbiz_host =data['vbizdb']['host']
      vbiz_db_name = data['vbizdb']['database']
      vbiz_user = data['vbizdb']['user']
      vbiz_password = data['vbizdb']['passwd']
      vbiz_url = f"jdbc:mysql://{vbiz_host}:3306/{vbiz_db_name}"

      # redshift connection for dask
      redshift_conn = f'postgresql://{redshift_user}:{redshift_password}@{redshift_host}:5439/{redshift_db_name}'

      # Specify the target Redshift table
      redshift_table = f'statement.{year}_acc_stmt'
      print(redshift_table)


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
      query_2 = f'''
      select
      st.id as transaction_id,
      ifnull(mc.id, mg.id) as client_id,
      ifnull(replace(mc.display_name, 'null', ''), mg.display_name) as client_name,
      st.savings_account_id as account_id,
      msa.account_no,
      msa.product_id,
      msp.name as product_name,
      st.amount, 
      st.overdraft_amount_derived as overdraft_amount, 
      st.running_balance_derived as running_balance, 
      st.cumulative_balance_derived as cumulative_balance, 
      st.created_date transaction_date,   
      case st.is_reversed_by_contra_entry
            when '0' then 'No'
            when '1' then 'Yes' end as is_reversed,
      case st.is_loan_disbursement
            when 0 then 'No'
            when 1 then 'Yes' end as is_transaction_loan_disbursement,
      st.transaction_type_enum, 
      renum.enum_value as transaction_type,
      mpd.account_number as third_party_account_no, 
      mpd.check_number as channel, 
      mpd.receipt_number session_id, 
      mpd.bank_number, 
      mpt.value as partners_description,
      mtr.notes transaction_notes, 
      mtr.remarks transaction_remarks
      from m_savings_account_transaction st
      left join m_savings_account msa on msa.id = st.savings_account_id
      left join m_savings_product msp on msp.id = msa.product_id
      left join m_client mc on mc.id = msa.client_id
      left join m_group mg on mg.id = msa.group_id
      left join (SELECT
            rv.enum_name,
            rv.enum_id,
            rv.enum_message_property,
            rv.enum_value
            FROM r_enum_value rv
            WHERE rv.enum_name = 'savings_transaction_type_enum') renum on renum.enum_id = st.transaction_type_enum
      left join m_appuser map on map.id = st.appuser_id
      left JOIN m_payment_detail mpd ON mpd.id = st.payment_detail_id
      left JOIN m_payment_type mpt ON mpt.id = mpd.payment_type_id
      left join m_transaction_request mtr on mtr.transaction_id = st.id
      where st.created_date >= '{start_time}' and st.created_date < '{end_time}'
      ORDER BY st.id
      '''       
      #                  
      # apply function
      mifos_df = extract_function(query=query_2, url=mifos_url, user=mifos_user, password=mifos_password)
      # Convert Pyspark DF to pandas DF, then to List
      print(mifos_df.count(), start_time, ' to ', end_time, ' is started')
      mifos_df.persist()
      mifos_pdf = mifos_df.toPandas()



      mifos_df = mifos_df.withColumn('load_date', lit(datetime.today().strftime('%Y-%m-%d %H:%M:%S')))

      # Load data to data warehouses
      mifos_pdf = mifos_df.toPandas()
      # Convert Pandas DataFrame to Dask DataFrame
      mifos_ddf = dd.from_pandas(mifos_pdf, npartitions=3)  # Specify the desired number of partitions


      # load data to redshift datawarehouse
      mifos_ddf.to_sql(redshift_table, redshift_conn, index=False, if_exists='append', parallel=True, method='multi')

      print(mifos_df.count(),  ' is Ended')

      #current_date += pd.DateOffset(days=1) 
      current_date += pd.DateOffset(minutes=10)

# COMMAND ----------

from IPython.display import clear_output

for i in range(10):
    clear_output(wait=True)
    print("Cleaned")

from IPython import get_ipython
get_ipython().magic('reset -sf') 
print('cleared')

# COMMAND ----------


