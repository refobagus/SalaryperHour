import os
import pandas as pd
import psycopg2

# insert your db connection
db_conn = psycopg2.connect(
    host='host',
    port='port',
    user='user',
    password='password',
    database='database'
)

schema_name = 'test'
employees_table = 'employees'
timesheets_table = 'timesheets'

# err handling if schema missing
with db_conn.cursor() as cursor:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
db_conn.commit()

csv_dir = './datafile'

# get all csv
csv_files = [file for file in os.listdir(csv_dir) if file.endswith('.csv')]

for file in csv_files:
    file_path = os.path.join(csv_dir, file)
    
    # get either employees or timesheet
    if 'employees' in file.lower():
        table_name = f'{schema_name}.{employees_table}'
    elif 'timesheets' in file.lower():
        table_name = f'{schema_name}.{timesheets_table}'
    else:
        # err handling if file is not employee or timesheet
        print(f"Skipping '{file}'")
        continue
    
    df = pd.read_csv(file_path)
    
    #use append info to satisfy req of only appends
    df.to_sql(table_name, db_conn, if_exists='append', index=False)

db_conn.close()
