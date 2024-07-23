# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Negative Balance Daily Report
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 1: Connection information
# MAGIC
# MAGIC First define some variables to programmatically create these connections.
# MAGIC
# MAGIC Replace all the variables in angle brackets `<>` below with the corresponding information.

# COMMAND ----------

import sys
print(sys.version)

# COMMAND ----------

import os
print(os.getcwd())

# COMMAND ----------



import pandas as pd

import warnings
warnings.filterwarnings('ignore')

import json
# from custom_packages.email_operations import EmailOperations



# Load credentials from JSON file
with open('/Workspace/Credentials/db_data.json', 'r') as f:
    credentials = json.load(f)

mifos_credentials = credentials['mifos']

# Extract database connection details
database_host = mifos_credentials['host']
database_name = mifos_credentials['database']
user = mifos_credentials['user']
password = mifos_credentials['passwd']

# Database driver
driver = "org.mariadb.jdbc.Driver"

# Database port
database_port = "3306"  # Assuming default port for MySQL

# Construct URL
url = "jdbc:mysql://" + database_host + ":" + database_port + "/" + database_name

print(url)

# COMMAND ----------

# Set display format for floats
pd.set_option('display.float_format', '{:.2f}'.format)

# Get today and yesterday's dates efficiently using pandas
today = pd.Timestamp.today().strftime('%Y-%m-%d')
yesterday = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')

# Calculate date for last 2 weeks using pandas
last_2_weeks = (pd.Timestamp.today() - pd.Timedelta(days=14)).strftime('%Y-%m-%d')

print('------------------------------------')
print(last_2_weeks)

print('\n')

# Get timestamps efficiently using pandas
now = pd.Timestamp.today().strftime('%Y-%m-%d %H:%M:%S')
last_30_mins = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
trunc_last_30_mins = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime('%Y-%m-%d %H:%M')

print(f"{last_30_mins} to {now}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The full URL printed out above should look something like:
# MAGIC
# MAGIC ```
# MAGIC jdbc:mysql://localhost:3306/my_database
# MAGIC ```
# MAGIC
# MAGIC ### Check connectivity
# MAGIC
# MAGIC Depending on security settings for your MySQL database and Databricks workspace, you may not have the proper ports open to connect.
# MAGIC
# MAGIC Replace `<database-host-url>` with the universal locator for your MySQL implementation. If you are using a non-default port, also update the 3306.
# MAGIC
# MAGIC Run the cell below to confirm Databricks can reach your MySQL database.

# COMMAND ----------

# MAGIC %sh
# MAGIC nc -vz "172.32.6.213" 3306

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2: Reading the data
# MAGIC
# MAGIC Now that you've specified the file metadata, you can create a DataFrame. Use an *option* to infer the data schema from the file. You can also explicitly set this to a particular schema if you have one already.
# MAGIC
# MAGIC First, create a DataFrame in Python, referencing the variables defined above.

# COMMAND ----------

# Define the queries
agg_negative_balances_query = """
SELECT
    msp.name product_name,
    SUM(msa.account_balance_derived) AS total_principal_bal,
    CURRENT_DATE() AS run_date
FROM
    m_savings_account msa
LEFT OUTER JOIN m_savings_product msp 
ON
    msa.product_id = msp.id
WHERE
    msa.account_balance_derived < 0
GROUP BY
    msa.product_id
ORDER BY
    msa.product_id ASC
"""

negative_balances_query = """
SELECT
	T.*,
	msat.running_balance_derived AS Available_Balance
	-- Assuming all negative balances are loans
FROM
	(
	SELECT
		DISTINCT s.account_no AS Account_Number,
		c.id,
		COALESCE(c.display_name, g.display_name) AS display_name,
		coalesce(coalesce(concat(c.lastname, ' ', c.firstname, ' ', coalesce(c.middlename, '')), c.fullname), g.display_name) AS Account_Name,
		c.gender_cv_id,
		(
		SELECT
			v.code_value
		FROM
			m_code_value v
		WHERE
			code_id = 4
			AND v.id = c.gender_cv_id
    ) AS Gender,
		c.mobile_no,
		o.`Name` AS Branch,
		p.id AS product_id,
		UPPER(p.`Name`) AS Product,
		(
		SELECT
			MAX(sat1.id) AS running_transaction_id
		FROM
			m_savings_account_transaction sat1
		WHERE
			sat1.savings_account_id = s.id
			AND sat1.transaction_type_enum NOT IN (20, 21, 22, 25)
				AND sat1.transaction_date <= current_date
				AND sat1.is_reversed = 0
    ) AS 'transaction_id',
		s.overdraft_limit,
		s.nominal_annual_interest_rate_overdraft as interest_rate,
		s.account_balance_derived as principal_balance,
		s.overdraft_limit + s.account_balance_derived as overdrawn_amount,
		s.overdraft_startedon_date as disbursement_date,
		s.overdraft_closedon_date as maturity_date,
		st.display_name AS Account_Officer
	FROM
		m_savings_account s
	INNER JOIN m_savings_product p ON
		p.id = s.product_id
	LEFT JOIN m_client c ON
		c.id = s.client_id
	LEFT JOIN m_group g ON
		g.id = s.group_id
	LEFT JOIN m_staff st ON
		st.id = s.field_officer_id
	LEFT JOIN m_office o ON
		o.id = c.office_id
		) T
LEFT JOIN m_savings_account_transaction msat ON
	msat.id = T.transaction_id
WHERE
	msat.running_balance_derived < -1000
	
"""

# Read the queries into DataFrames
agg_negative_balances_df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", f"({agg_negative_balances_query}) AS agg_negative_balances") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", driver) \
    .load()

negative_balances_df = spark.read \
    .format("jdbc") \
    .option("url", url) \
    .option("dbtable", f"({negative_balances_query}) AS negative_balances") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", driver) \
    .load()

# Show the results (optional)
agg_negative_balances_df.show()
negative_balances_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 3: Writing the data to excel workbook & send report via mail
# MAGIC
# MAGIC The DataFrame defined and displayed above is a temporary connection to the remote database.
# MAGIC
# MAGIC

# COMMAND ----------

# %pip install openpyxl


# Convert Spark DataFrames to Pandas DataFrames
agg_negative_balances_pdf = agg_negative_balances_df.toPandas()
negative_balances_pdf = negative_balances_df.toPandas()

# Define the path for the Excel file
excel_path = f'/Workspace/ReportDump/Account_Product_Balances/negative_balances_report {today}.xlsx'

# Create a Pandas Excel writer using XlsxWriter as the engine
with pd.ExcelWriter(excel_path) as writer:
    agg_negative_balances_pdf.to_excel(writer, sheet_name='summarized_balances', index=False)
    negative_balances_pdf.to_excel(writer, sheet_name='OD_account_balances', index=False)

print(f"Excel workbook saved to {excel_path}")

# Define the email parameters
class EmailOperations:
    @staticmethod
    def send_mail(filenames, source_path_name, date, recipients, subject, name_of_recipient, body):
        import smtplib
        from email.mime.multipart import MIMEMultipart
        from email.mime.base import MIMEBase
        from email.mime.text import MIMEText
        from email import encoders

        msg = MIMEMultipart()
        msg['From'] = 'auto-report@vfdtech.ng'
        msg['To'] = recipients
        msg['Subject'] = subject
        email_body = f"""
        Dear {name_of_recipient} team,

        {body}

        Regards,
        """
        msg.attach(MIMEText(email_body, 'plain'))

        # Attach the file
        for file_name in filenames:
            file_path = os.path.join(source_path_name, file_name)
            with open(file_path, "rb") as file:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(file.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename={file_name}')
                msg.attach(part)

        # Send the email
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login('Babatunde.Omotayo@vfdtech.ng', 'wmglfvqvnhdqgrrc')
        server.send_message(msg)
        server.quit()
        print(f"Mail successfully sent to {name_of_recipient} on {now}!\n")

# Usage

# FILE TO SEND AND ITS PATH

today = pd.to_datetime('today').strftime('%Y-%m-%d')

filename = f'negative_balances_report {today}.xlsx'
source_path_name  = "/Workspace/ReportDump/Account_Product_Balances/"

EmailOperations.send_mail(
    filenames=[filename],
    source_path_name=source_path_name,
    date=today,
    recipients='finance@vfd-mfb.com, data-team@vfdtech.ng',
    subject=f'Overdraft (-ve) Balance Report {today}',
    name_of_recipient='Finance',
    body=f"Please find attached for negative balances summary and account breakdown as at {today}."
)


# COMMAND ----------


