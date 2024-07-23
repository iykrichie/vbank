# Databricks notebook source
print("Healthy")

# COMMAND ----------

for i in range(1, 130):

    import json
    from datetime import datetime, timedelta
    from pyspark.sql.functions import lit
    import dask
    import dask.dataframe as dd
    from sqlalchemy import create_engine

    # Load in credentials
    with open('/Workspace/Credentials/db_data.json', 'r') as fp:
        data = json.load(fp)

    # REDSHIFT_DWH
    redshift_driver = "org.postgresql.Driver"
    redshift_host = data['redshift']['host']
    redshift_db_name = data['redshift']['database']
    redshift_user = data['redshift']['user']
    redshift_password = data['redshift']['passwd']
    redshift_url = f"jdbc:postgresql://{redshift_host}:5439/{redshift_db_name}"

    # DUMP_DB
    dump_driver = "org.mariadb.jdbc.Driver"
    dump_host = data['Dumpdb']['host']
    dump_db_name = data['Dumpdb']['database']
    dump_user = data['Dumpdb']['user']
    dump_password = data['Dumpdb']['passwd']
    dump_url = f"jdbc:mysql://{dump_host}:3306/{dump_db_name}"

    # redshift connection for dask
    redshift_conn = f'postgresql://{redshift_user}:{redshift_password}@{redshift_host}:5439/{redshift_db_name}'

    # Define function to extract data from dbs
    def extract_function(query, url, user, password):
        df = spark.read \
                    .format("jdbc") \
                    .option("query", query) \
                    .option("url", url) \
                    .option("user", user) \
                    .option("password", password) \
                    .option("connectTimeout", "100000") \
                    .option("treatEmptyValueAsNulls","true") \
                    .option("maxRowsInMemory",20) \
                    .load()
        return df



    ## For redshift last transaction
    sql = f'''
    select 
    max(id) as ltr,
    max(load_date) as last_date
    from "dump.am_verified_bvn"'''
    # apply the function
    redshift_lasttr_df = extract_function(query=sql, url=redshift_url, user=redshift_user, password=redshift_password )
    ltr_df = redshift_lasttr_df.toPandas()
    ltr_ls = ltr_df['ltr'].tolist()
    print(ltr_ls)


    # For dumpDB
    sql = f'''
    SELECT A.*,  NOW() AS LOAD_DATE
    FROM AM_VERIFIED_BVN A
    WHERE ID > {ltr_ls[0]}
    ORDER BY ID
    LIMIT 100000
    '''
    # apply function
    dump_df = extract_function(query=sql, url=dump_url, user=dump_user, password=dump_password)
    print(dump_df.count())
    dump_df.persist()
    dump_pdf = dump_df.toPandas()


    #dump_pdf = dump_pdf.toPandas()
    dump_pdf = dd.from_pandas(dump_pdf, npartitions=3)  # Specify the desired number of partitions

    # load data to redshift datawarehouse
    dump_pdf.to_sql('dump.am_verified_bvn', redshift_conn, index=False, if_exists='append', parallel=True, method='multi')

    print('\n\n')

    from IPython.display import clear_output

    for i in range(10):
        clear_output(wait=True)
        print("Cleaned")

    from IPython import get_ipython
    get_ipython().magic('reset -sf') 
    print('cleared')

# COMMAND ----------


