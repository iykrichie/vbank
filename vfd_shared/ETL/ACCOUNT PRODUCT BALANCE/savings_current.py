# Databricks notebook source
print("Healthy")

# COMMAND ----------

import json
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.functions import lit
import dask
import dask.dataframe as dd
from sqlalchemy import create_engine

# COMMAND ----------

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


# redshift connection for dask
redshift_conn = f'postgresql://{redshift_user}:{redshift_password}@{redshift_host}:5439/{redshift_db_name}'

# COMMAND ----------

#Specify the target Redshift table
redshift_table = 'balance.savings_current'


# COMMAND ----------

from datetime import datetime, timedelta
two_days_ago =  (datetime.today() - timedelta(days = 2)).strftime('%Y-%m-%d')
yesterday =  (datetime.today() - timedelta(days = 1)).strftime('%Y-%m-%d')
year =  (datetime.today() - timedelta(days = 1)).strftime('%Y')
print(year)
print(yesterday)
print(two_days_ago)

# COMMAND ----------

#yesterday = '2024-07-04' 
#two_days_ago = '2024-07-03' 

# COMMAND ----------

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
SELECT msa.id AS account_id, msa.account_no, msa.client_id, msa.product_id, '{yesterday}' AS date, CAST(0.0 AS FLOAT) AS closing_balance
FROM m_savings_account msa
WHERE msa.product_id <= 36
AND msa.approvedon_date <= '{yesterday}'
ORDER BY msa.id
'''       
#                  
# apply function
mifos_df = extract_function(query=sql, url=mifos_url, user=mifos_user, password=mifos_password)
# Convert Pyspark DF to pandas DF, then to List
mifos_df.persist()
mifos_pdf = mifos_df.toPandas()

# COMMAND ----------

mifos_pdf.count()

# COMMAND ----------

mifos_pdf.head()

# COMMAND ----------


# Load data to data warehouses
mifos_pdf = mifos_df.toPandas()
# Convert Pandas DataFrame to Dask DataFrame
mifos_ddf = dd.from_pandas(mifos_pdf, npartitions=20)  # Specify the desired number of partitions


# load data to redshift datawarehouse
mifos_ddf.to_sql(redshift_table, redshift_conn, index=False, if_exists='append', parallel=True, method='multi')

# COMMAND ----------

print(yesterday)
print(two_days_ago)

# COMMAND ----------

engine = create_engine(f'postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:5439/{redshift_db_name}')

sql = f'''
UPDATE "{redshift_table}" AS f
SET closing_balance = t.closing_balance
FROM (
	select stm.account_id, cast(stm.running_balance as FLOAT) as closing_balance
	from "statement.{year}_acc_stmt" stm
	where stm.transaction_id  in (
		select MAX(stm.transaction_id)
		from "statement.{year}_acc_stmt" stm
		where left(stm.transaction_date,10) <= '{yesterday}'
		and stm.account_id  IN (
			select account_id
			from "{redshift_table}"
			where date = '{yesterday}'
		)
		group by stm.account_id
)
) AS t
WHERE f.account_id = t.account_id
and f.date = '{yesterday}'
'''

with engine.begin() as con:     
    con.execute(sql)    

# COMMAND ----------

engine = create_engine(f'postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:5439/{redshift_db_name}')

sql = f'''
UPDATE "{redshift_table}" AS f
SET closing_balance = t.closing_balance
FROM (
    select account_id, cast(closing_balance as FLOAT) as closing_balance
    from "{redshift_table}"
    where date = '{two_days_ago}'
    ) AS t
WHERE f.account_id = t.account_id
and f.date = '{yesterday}'
AND f.closing_balance = 0
'''

with engine.begin() as con:     
    con.execute(sql)    

# COMMAND ----------

from IPython.display import clear_output

for i in range(10):
    clear_output(wait=True)
    print("Cleaned")

from IPython import get_ipython
get_ipython().magic('reset -sf') 
print('cleared')

# COMMAND ----------

# MAGIC %md
# MAGIC n = 7
# MAGIC while n >= 2:
# MAGIC
# MAGIC     from datetime import datetime, timedelta
# MAGIC     two_days_ago =  (datetime.today() - timedelta(days = n)).strftime('%Y-%m-%d')
# MAGIC     yesterday =  (datetime.today() - timedelta(days = n-1)).strftime('%Y-%m-%d')
# MAGIC     year =  (datetime.today() - timedelta(days = n)).strftime('%Y')
# MAGIC     print(year)
# MAGIC     print(yesterday)
# MAGIC     print(two_days_ago)
# MAGIC
# MAGIC     n -=1
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Where n is the last day in the transactions table
# MAGIC n = 8
# MAGIC while n >= 2:
# MAGIC     
# MAGIC     from datetime import datetime, timedelta
# MAGIC     two_days_ago =  (datetime.today() - timedelta(days = n)).strftime('%Y-%m-%d')
# MAGIC     yesterday =  (datetime.today() - timedelta(days = n-1)).strftime('%Y-%m-%d')
# MAGIC     year =  (datetime.today() - timedelta(days = n)).strftime('%Y')
# MAGIC     print(year)
# MAGIC     print(yesterday)
# MAGIC     print(two_days_ago)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC     # Define function to extract data from dbs
# MAGIC     def extract_function(query, url, user, password):
# MAGIC         df = spark.read \
# MAGIC                     .format("jdbc") \
# MAGIC                     .option("query", query) \
# MAGIC                     .option("url", url) \
# MAGIC                     .option("user", user) \
# MAGIC                     .option("password", password) \
# MAGIC                     .option("connectTimeout", "1000") \
# MAGIC                     .option("treatEmptyValueAsNulls","true") \
# MAGIC                     .option("maxRowsInMemory",200) \
# MAGIC                     .load()
# MAGIC         return df
# MAGIC
# MAGIC
# MAGIC     ## For Mifos
# MAGIC     sql = f'''
# MAGIC     SELECT msa.id AS account_id, msa.account_no, msa.client_id, msa.product_id, '{yesterday}' AS date, CAST(0.0 AS FLOAT) AS closing_balance
# MAGIC     FROM m_savings_account msa
# MAGIC     WHERE msa.product_id <= 36
# MAGIC     AND msa.approvedon_date <= '{yesterday}'
# MAGIC     ORDER BY msa.id
# MAGIC     '''       
# MAGIC     #                  
# MAGIC     # apply function
# MAGIC     mifos_df = extract_function(query=sql, url=mifos_url, user=mifos_user, password=mifos_password)
# MAGIC     # Convert Pyspark DF to pandas DF, then to List
# MAGIC     mifos_df.persist()
# MAGIC     mifos_pdf = mifos_df.toPandas()
# MAGIC
# MAGIC
# MAGIC     mifos_pdf.count()
# MAGIC
# MAGIC
# MAGIC     # Load data to data warehouses
# MAGIC     mifos_pdf = mifos_df.toPandas()
# MAGIC     # Convert Pandas DataFrame to Dask DataFrame
# MAGIC     mifos_ddf = dd.from_pandas(mifos_pdf, npartitions=20)  # Specify the desired number of partitions
# MAGIC
# MAGIC
# MAGIC     # load data to redshift datawarehouse
# MAGIC     mifos_ddf.to_sql(redshift_table, redshift_conn, index=False, if_exists='append', parallel=True, method='multi')
# MAGIC
# MAGIC
# MAGIC     print(yesterday)
# MAGIC     print(two_days_ago)
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC     engine = create_engine(f'postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:5439/{redshift_db_name}')
# MAGIC
# MAGIC     sql = f'''
# MAGIC     UPDATE "{redshift_table}" AS f
# MAGIC     SET closing_balance = t.closing_balance
# MAGIC     FROM (
# MAGIC         select stm.account_id, cast(stm.running_balance as FLOAT) as closing_balance
# MAGIC         from "statement.{year}_acc_stmt" stm
# MAGIC         where stm.transaction_id  in (
# MAGIC             select MAX(stm.transaction_id)
# MAGIC             from "statement.{year}_acc_stmt" stm
# MAGIC             where left(stm.transaction_date,10) <= '{yesterday}'
# MAGIC             and stm.account_id  IN (
# MAGIC                 select account_id
# MAGIC                 from "{redshift_table}"
# MAGIC                 where date = '{yesterday}'
# MAGIC             )
# MAGIC             group by stm.account_id
# MAGIC     )
# MAGIC     ) AS t
# MAGIC     WHERE f.account_id = t.account_id
# MAGIC     and f.date = '{yesterday}'
# MAGIC     '''
# MAGIC
# MAGIC     with engine.begin() as con:     
# MAGIC         con.execute(sql)    
# MAGIC
# MAGIC
# MAGIC     engine = create_engine(f'postgresql+psycopg2://{redshift_user}:{redshift_password}@{redshift_host}:5439/{redshift_db_name}')
# MAGIC
# MAGIC     sql = f'''
# MAGIC     UPDATE "{redshift_table}" AS f
# MAGIC     SET closing_balance = t.closing_balance
# MAGIC     FROM (
# MAGIC         select account_id, cast(closing_balance as FLOAT) as closing_balance
# MAGIC         from "{redshift_table}"
# MAGIC         where date = '{two_days_ago}'
# MAGIC         ) AS t
# MAGIC     WHERE f.account_id = t.account_id
# MAGIC     and f.date = '{yesterday}'
# MAGIC     AND f.closing_balance = 0
# MAGIC     '''
# MAGIC
# MAGIC     with engine.begin() as con:     
# MAGIC         con.execute(sql)    
# MAGIC
# MAGIC     print(yesterday, " ended")
# MAGIC     print('\n\n')
# MAGIC     n -=1
# MAGIC  
# MAGIC from IPython.display import clear_output
# MAGIC
# MAGIC for i in range(10):
# MAGIC     clear_output(wait=True)
# MAGIC     print("Cleaned")
# MAGIC
# MAGIC from IPython import get_ipython
# MAGIC get_ipython().magic('reset -sf') 
# MAGIC print('cleared')

# COMMAND ----------


