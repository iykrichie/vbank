# Predicting Customer Lifetime Value (CLV)

This project aims to predict Customer Lifetime Value (CLV) using historical transaction data. Below is a detailed summary of the steps and the approach taken to determine CLV.

## Summary of the Code

### Environment Setup
- Import necessary libraries for data manipulation, visualization, and modeling.
- Configure pandas display settings for better readability.
- Suppress warnings for a cleaner output.

### Database Connection
- Load database credentials from a JSON file.
- Establish a connection to the database using SQLAlchemy.

### Date Variables
- Set up various date variables to be used later in the script for querying and debugging.

### Data Extraction
- Execute a SQL query to fetch customer transaction data for the past 12 months.
- Load the query result into a pandas DataFrame.

### Data Preprocessing
- Convert date columns to datetime objects.
- Create a new DataFrame (`clv_df`) for modeling, with calculated fields such as frequency, recency (in weeks), and average monetary value per transaction.
- Filter out customers who don't meet certain criteria (e.g., minimum frequency, recency, and average monetary value).

### Model Fitting
- **Beta-Geometric/NBD Model**: Fit the BG/NBD model to predict the number of transactions for the next 3 and 6 months.
- **Gamma-Gamma Model**: Fit the Gamma-Gamma model to predict the average monetary value of future transactions.

### Customer Lifetime Value Calculation
- Calculate the CLV for each customer using the fitted BG/NBD and Gamma-Gamma models.
- Segment customers into four categories (A, B, C, D) based on their CLV.

### Saving Results
- Convert the final DataFrame to a Spark DataFrame.
- Save the Spark DataFrame to a Databricks table.

## Approach to Determining Customer Lifetime Value (CLV)

### Data Collection
- Extract relevant customer transaction data from a database, including client ID, client category, transaction frequency, monetary value, recency, and tenure.

### Data Preparation
- Compute necessary metrics such as frequency (number of transactions), recency (time since the last transaction), T (time since the first transaction), and average monetary value per transaction.

### Modeling
- **Beta-Geometric/NBD Model**: Used to predict the number of future transactions over a specified period (e.g., 3 months and 6 months). This model estimates the probability of a customer making a purchase based on their transaction history.
- **Gamma-Gamma Model**: Used to predict the average monetary value of future transactions. This model assumes that the monetary value of transactions is independent of the transaction frequency.

### CLV Calculation
- Combine the predictions from the BG/NBD and Gamma-Gamma models to estimate the Customer Lifetime Value (CLV) over a specific time frame (e.g., 6 months).
- Adjust for a discount rate to reflect the time value of money.

### Customer Segmentation
- Segment customers into different groups (A, B, C, D) based on their CLV, allowing for targeted marketing strategies and resource allocation.

## Outcome
The final output includes a DataFrame with predicted CLV for each customer, which is saved to a Databricks table for further analysis and reporting. This enables the business to identify high-value customers and tailor their marketing efforts accordingly.
