"""
Title: COVID-19 Data Retrieval and Database Loading Script
Author: Leo Bartlett
Date: April 7, 2023
Version: 1.0

This Python script retrieves COVID-19 data from the CDC API and loads it into a SQL Server database.
It utilizes the 'requests', 'json', 'pandas', 'datetime', and 'pyodbc' libraries to make API requests,
parse response data, transform the data into a Pandas dataframe, and load it into a SQL Server database.
The script is specifically designed to run on the LeoBartlett server, which is a localhost server.
If you wish to run this script on a different server, you may need to modify the 'SERVER' and 'DATABASE' parameters in the 'pyodbc.connect'
function accordingly.
To update the data on the 'CovidData' table if new data is added or data is removed, the script truncates the table before inserting new data.

"""

import requests
import json
import pandas as pd
import datetime
import pyodbc

# Define API endpoint and parameters
endpoint = "https://data.cdc.gov/resource/9mfq-cb36.json"
params = {'$select': 'submission_date,state,tot_cases,tot_death',
          '$where': 'submission_date >= "2020-01-01T00:00:00.000"'}

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
table_exists = cursor.execute("SELECT COUNT(*) FROM sys.tables WHERE name='CovidData'").fetchone()[0]

# If table exists, truncate it to remove existing data
if table_exists:
    cursor.execute("TRUNCATE TABLE CovidData")
    print("Updating table!")
# If table does not exist, create it
else:
    cursor.execute(
        "CREATE TABLE CovidData (SubmissionDate datetime, state varchar(255), TotalCases int, TotalDeaths int, timestamp datetime)")
    print("Table Created Successfully!")
# Insert data into SQL Server table
for index, row in df.iterrows():
    cursor.execute(
        "INSERT INTO CovidData (SubmissionDate, state, TotalCases , TotalDeaths , timestamp) VALUES (?, ?, ?, ?, ?)",
        row['submission_date'], row['state'], row['tot_cases'], row['tot_death'], row['timestamp'])
conn.commit()

# Close database connection
cursor.close()
conn.close()
