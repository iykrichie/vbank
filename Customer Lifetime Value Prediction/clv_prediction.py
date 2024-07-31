# -*- coding: utf-8 -*-
"""
Predicting Customer Lifetime Value (CLV)
"""

import os
import pandas as pd
import numpy as np
import warnings
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns
import json
from sqlalchemy import create_engine
from datetime import datetime, timedelta

!pip install lifetimes
from lifetimes import BetaGeoFitter, GammaGammaFitter
from lifetimes.plotting import plot_period_transactions
from pyspark.sql import SparkSession

# Suppress warnings
warnings.filterwarnings('ignore')

# Set pandas display options for better readability
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 80)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
pd.set_option('display.width', 500)

# Load database credentials
with open('/Workspace/Credentials/db_data.json', 'r') as fp:
    data = json.load(fp)

# Database connection details
host = data['redshift']['host']
user = data['redshift']['user']
passwd = data['redshift']['passwd']
database = data['redshift']['database']
conn = create_engine(f"postgresql+psycopg2://{user}:{passwd}@{host}:5439/{database}")

# Date variables
today = datetime.today().strftime('%Y-%m-%d')
yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
last_2_wks = (datetime.today() - timedelta(days=14)).strftime('%Y-%m-%d')
now = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
last_30_mins = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')

# Print date variables for debugging
print(today)
print(yesterday)
print('------------------------------------')
print(last_2_wks)
print(last_30_mins, 'to', now)

# Query to fetch customer transactions data
query = '''
SELECT
    dac.client_id,
    dac.client_category,
    COUNT(dat.transaction_id) AS frequency,
    SUM(dat.amount) AS monetary,
    DATEDIFF(DAYS, MAX(dat.transaction_date), CURRENT_DATE) AS recency,
    DATEDIFF(DAYS, MIN(dac.activation_date), CURRENT_DATE) AS T,
    CURRENT_DATE AS rundate
FROM
    dwh_all_transactions dat
LEFT JOIN
    dwh_all_clients dac ON dat.client_id = dac.client_id
LEFT JOIN
    dwh_all_accounts daa ON dac.client_id = daa.client_id
WHERE
    dat.transaction_type_enum IN (1, 2)
    AND UPPER(dac.client_category) NOT IN ('Unclassified')
    AND dac.client_status = 'Active'
    AND dat.transaction_date >= DATEADD(MONTH, -12, CURRENT_DATE)
GROUP BY
    dac.client_id,
    dac.client_category
HAVING COUNT(dat.transaction_id) > 0
'''

# Execute the query and load the result into a DataFrame
df = pd.read_sql_query(query, conn)

# Convert 'rundate' to datetime
df['rundate'] = df['rundate'].astype('datetime64[ns]')
df.info()

# Create CLV dataframe with required transformations
clv_df = pd.DataFrame()
clv_df["customer_id"] = df["client_id"]
clv_df["frequency"] = df["frequency"]
clv_df["T_weekly"] = df["t"] / 7
clv_df["recency_clv_weekly"] = df["recency"] / 7
clv_df["monetary_clv_avg"] = df["monetary"] / df["frequency"]

# Filter out customers who don't meet the criteria
clv_df = clv_df[clv_df["T_weekly"] >= 1]
clv_df = clv_df[clv_df["recency_clv_weekly"] >= 1]
clv_df = clv_df[clv_df["frequency"] >= 1]
clv_df = clv_df[clv_df["monetary_clv_avg"] >= 1]

# Display descriptive statistics
print(clv_df.describe().T)

# Fit the BG/NBD model
bgf = BetaGeoFitter(penalizer_coef=0.001)
bgf.fit(clv_df["frequency"], clv_df["recency_clv_weekly"], clv_df["T_weekly"])

# Predict number of transactions for the next 3 months
clv_df["exp_sales_3_month"] = bgf.predict(4 * 3, clv_df["frequency"], clv_df["recency_clv_weekly"], clv_df["T_weekly"])

# Predict number of transactions for the next 6 months
clv_df["exp_sales_6_month"] = bgf.predict(4 * 6, clv_df["frequency"], clv_df["recency_clv_weekly"], clv_df["T_weekly"])

# Fit the Gamma-Gamma model
ggf = GammaGammaFitter(penalizer_coef=0.01)
ggf.fit(clv_df['frequency'], clv_df['monetary_clv_avg'])
clv_df['predicted_average_profit'] = ggf.conditional_expected_average_profit(clv_df['frequency'], clv_df['monetary_clv_avg'])

# Calculate Customer Lifetime Value (CLV)
clv = ggf.customer_lifetime_value(
    bgf,
    clv_df["frequency"],
    clv_df["recency_clv_weekly"],
    clv_df["T_weekly"],
    clv_df["monetary_clv_avg"],
    time=6,  # 6 months
    freq='W',  # Tenure frequency in weeks
    discount_rate=0.01
)

clv_df["clv"] = clv

# Segment customers based on CLV
clv_df["segment"] = pd.qcut(clv_df["clv"], 4, labels=["D", "C", "B", "A"])
print(clv_df.groupby("segment").agg({"clv": ["mean", "sum", "count", "max", "min", "std"]}))

# Add current timestamp
clv_df["run_date"] = today

# Convert Pandas DataFrame to Spark DataFrame and save to Databricks table
spark = SparkSession.builder.getOrCreate()
spark_df = spark.createDataFrame(clv_df)
spark_df.write.mode("overwrite").saveAsTable("vfd_databricks.default.clv_prediction")

# Optionally write the DataFrame to Redshift
clv_df.to_sql(name='dwh_customer_lifetime_value', con=conn, if_exists='replace', index=False, chunksize=10000, method='multi')
