# Load Dataset in Postgres DB
import requests
# import config
import pandas as pd




import psycopg2

with open('load_data.sql', 'r') as file:
    sql_query = file.read()

conn = psycopg2.connect(database = "test", 
                        user = "postgres", 
                        host= 'localhost',
                        password = "170545Kean",
                        port = 5432)

# Open a cursor to perform database operations
cur = conn.cursor()
# Execute a command: create datacamp_courses table
cur.execute(sql_query)
# Make the changes to the database persistent
conn.commit()
# Close cursor and communication with the database
cur.close()
conn.close()
