from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import timedelta
from airflow.utils.dates import days_ago

import pandas as pd
import chardet
import logging

EXTRACTED_ZONE = "/opt/airflow/dags/dataset/extracted_csv"
TABLE_ZONE = "/opt/airflow/dags/dataset/tables"

employee = f'{EXTRACTED_ZONE}/CompanyData.csv'

# function for transforming companydata file
def transform_company_data():
    input_file = f'{EXTRACTED_ZONE}/CompanyData.csv'
    with open(input_file, 'rb') as f:
        result = chardet.detect(f.read())
        encoding = result['encoding']
        logging.info(f"Detected encoding: {encoding}")

    df = pd.read_csv(input_file, encoding=encoding)

    df.loc[df["State"] == 'MP', "StateFull"] = 'Northern Mariana Islands'

    # state file
    state_df = df[["State", "StateFull"]]
    # include only unique values
    state_df = state_df.drop_duplicates()
    state_df = state_df.dropna()
    # exclude some entries that 'state' is not an abbreviation
    state_df = state_df[state_df["State"] != state_df["StateFull"]]
    state_df.to_csv(f'{TABLE_ZONE}/State.csv', index=False)

    # country file
    country_df = df[["Country", "CountryFull"]]
    # include only unique values
    country_df = country_df.drop_duplicates()
    country_df = country_df.dropna()
    country_df.to_csv(f'{TABLE_ZONE}/Country.csv', index=False)

    # employee file
    employee_df = df[['EmployeeID','First_Name','Surname','Age','DOB','StreetAddress','City','State','ZipCode','Country']]
    employee_df = employee_df.drop_duplicates()
    ## fix state for those that 'state' is not an abbreviation by retreived abbreviation form from State dataframe
    employee_df["State"] = employee_df["State"].apply(
    lambda x: state_df.loc[state_df["StateFull"] == x, "State"].values[0] if x in state_df["StateFull"].values else x)
    employee_df.to_csv(f'{TABLE_ZONE}/Employee.csv', index=False)
    
    # employment file
    employment_df = df[['EmployeeID','Office','Start_Date','Termination_Date','Office_Type','Currency','Salary','Active Status','Job_Profile']]
    employment_df = employment_df.drop_duplicates()
    # save file with generated employment id
    employment_df.to_csv(f'{TABLE_ZONE}/Employment.csv', index=True, index_label= 'EmploymentID')

# function for transforming jobprofile file
def transform_jobprofile():
    input_file_job = f'{EXTRACTED_ZONE}/JobProfileMapping.csv'
    with open(input_file_job, 'rb') as f:
        result_job = chardet.detect(f.read())
        encoding_job = result_job['encoding']
        logging.info(f"Detected encoding: {encoding_job}")

    job_df = pd.read_csv(input_file_job, encoding=encoding_job)
    job_df = job_df[['Job_Profile','Job_title','Department', ' Compensation ' ,'Level','Bonus %']]

    # adjust column name
    job_df = job_df.rename(columns={'Bonus %': 'Bonus', ' Compensation ': 'Compensation'})
    job_df['Compensation'] = job_df['Compensation'].apply(lambda x: float(x.replace(',', '')))

    job_df = job_df.drop_duplicates()
    job_df.to_csv(f'{TABLE_ZONE}/JobProfile.csv', index=False)
    
# function for creating table and loading data to PostgreSQL
def create_table_load_data():

    #load SQL script
    with open('/opt/airflow/dags/load_data.sql', 'r') as file:
        sql_load= file.read()
    
    # connect with PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost', schema='hrdb')
    conn = pg_hook.get_conn()
    cur = conn.cursor()

    # run SQL script
    cur.execute(sql_load)
    conn.commit()
    cur.close()
    conn.close()

# dag argument
default_args = {
    'owner': 'Nipphit Apisitpuwakul',
    'start_date': days_ago(0),
    'email': ['keanapisit@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# dag definition
with DAG('transform_load',
         schedule_interval='@daily',
         default_args=default_args,
         description='A simple data pipeline',
         catchup=False) as dag:

    # begin tasks
   
    # State, Country, Employee, JobProfile, and Employment tables
    t1 = PythonOperator(
        task_id='transform_company_data',
        python_callable=transform_company_data,
        dag=dag,
    ) 

    # JobProfile table
    t2 = PythonOperator(
        task_id='transform_jobprofile',
        python_callable=transform_jobprofile,
        dag=dag,
    )

    # create table and load data to PostgreSQL database
    t3 = PythonOperator(
        task_id='load_to_postgres',
        python_callable=create_table_load_data,
    )

    #pipeline
    t1 >> t2 >> t3