import requests
import json
import pandas as pd
import datetime
import pyodbc

# Define API endpoint and parameters
endpoint = "https://data.cdc.gov/resource/9mfq-cb36.json"
params = {'$select': 'submission_date,state,tot_cases,tot_death',
          '$where': 'submission_date >= "2022-01-01T00:00:00.000"', '$limit': 100000}

# Make API request and parse response data
response = requests.get(endpoint, params=params)
data = json.loads(response.text)

# Transform data and load into database
df = pd.DataFrame(data)
df['timestamp'] = datetime.datetime.now()

# Connect to SQL Server database
conn = pyodbc.connect('Trusted_Connection=yes', DRIVER='{SQL Server}', SERVER='LEOBARTLETT', DATABASE='test')
cursor = conn.cursor()

# Check if table exists
table_exists = cursor.execute("SELECT COUNT(*) FROM sys.tables WHERE name='covid_data'").fetchone()[0]

# Create SQL Server table (if it doesn't exist)
if not table_exists:
    cursor.execute(
        "CREATE TABLE covid_data (submission_date datetime, state varchar(255), tot_cases int, tot_death int, timestamp datetime)")

# Insert data into SQL Server table
for index, row in df.iterrows():
    cursor.execute(
        "INSERT INTO covid_data (submission_date, state, tot_cases, tot_death, timestamp) VALUES (?, ?, ?, ?, ?)",
        row['submission_date'], row['state'], row['tot_cases'], row['tot_death'], row['timestamp'])
conn.commit()

# Close database connection
cursor.close()
conn.close()