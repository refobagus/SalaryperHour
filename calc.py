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
results_table = 'salary_per_hour'

with db_conn.cursor() as cursor:
    # get total hours and total salary and total employee for each month
    query = f"""
        SELECT e.branch_id, DATE_TRUNC('month', t.date) AS month,
            COUNT(DISTINCT e.employe_id) AS num_employees,
            SUM(EXTRACT(EPOCH FROM (t.checkout - t.checkin))) / 3600 AS total_hours,
            SUM(e.salary) AS total_salary
        FROM {schema_name}.{employees_table} e
        INNER JOIN {schema_name}.{timesheets_table} t ON e.employe_id = t.employee_id
        GROUP BY e.branch_id, month
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    # per requirement, it should overwrite the result
    cursor.execute(f"""
        DROP TABLE IF EXISTS {schema_name}.{results_table}
    """)
    cursor.execute(f"""
        CREATE TABLE {schema_name}.{results_table} (
            year INTEGER,
            month DATE,
            branch_id INTEGER,
            salary_per_hour NUMERIC
        )
    """)
    db_conn.commit()
    
    # grab results from query and do calculation of salary per hour
    for row in results:
        branch_id = row[0]
        month = row[1]
        num_employees = row[2]
        total_hours = row[3]
        total_salary = row[4]
        
        # err handling if theres total hours is 0
        if total_hours > 0:
            salary_per_hour = total_salary / total_hours
        else:
            salary_per_hour = 0
        
        year = month.year
        
        # insert all info to results
        cursor.execute(f"""
            INSERT INTO {schema_name}.{results_table}
            (year, month, branch_id, salary_per_hour)
            VALUES (%s, %s, %s, %s)
        """, (year, month, branch_id, salary_per_hour))
        db_conn.commit()

db_conn.close()
