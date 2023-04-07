# SQL Server Data Pipeline

This Python script utilizes the requests, json, pandas, datetime, and pyodbc libraries to retrieve COVID-19 data from the Centers for Disease Control and Prevention (CDC) API and load it into a SQL Server database.

This script is specifically designed to run on the LeoBartlett server, which is a localhost server. If you wish to run this script on a different server, you may need to modify the SERVER and DATABASE parameters in the pyodbc.connect function accordingly.

The script begins by defining the API endpoint and parameters to retrieve COVID-19 data from the CDC API. It then makes an API request using the requests library and parses the response data using the json library.

The retrieved data is then transformed into a Pandas dataframe and a timestamp is added to each row. This dataframe is then loaded into a SQL Server database using the pyodbc library.

Before inserting data into the SQL Server database, the script checks if the covid_data table already exists. If the table does not exist, the script creates the table using the CREATE TABLE SQL statement.

The script then uses a for loop to insert each row of the Pandas dataframe into the SQL Server database using the INSERT INTO SQL statement.
![image](https://user-images.githubusercontent.com/63686603/230542726-18c607dc-35c8-4fdf-a6b7-47a062a0b8c9.png)

Finally, the script closes the database connection using the cursor.close() and conn.close() functions.

Please note that this script may require additional modifications depending on your specific needs and server configuration.
