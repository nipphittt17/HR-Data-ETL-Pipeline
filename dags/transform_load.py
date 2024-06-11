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


def transform_company_data():
    input_file = f'{EXTRACTED_ZONE}/CompanyData.csv'
    with open(input_file, 'rb') as f:
        result = chardet.detect(f.read())
        encoding = result['encoding']
        logging.info(f"Detected encoding: {encoding}")

    df = pd.read_csv(input_file, encoding=encoding)

    df.loc[df["State"] == 'MP', "StateFull"] = 'Northern Mariana Islands'

    state_df = df[["State", "StateFull"]]
    # Only unique values
    state_df = state_df.drop_duplicates()
    state_df = state_df.dropna()
    # There's some entries that state is not an abbreviation
    # We alredy check that there alredy have its abbreviation in other entries
    # So we simply explude those
    state_df = state_df[state_df["State"] != state_df["StateFull"]]
    state_df.to_csv(f'{TABLE_ZONE}/State.csv', index=False)

    country_df = df[["Country", "CountryFull"]]
    # Only unique values
    country_df = country_df.drop_duplicates()
    country_df = country_df.dropna()
    country_df.to_csv(f'{TABLE_ZONE}/Country.csv', index=False)

    employee_df = df[['EmployeeID','First_Name','Surname','Age','DOB','StreetAddress','City','State','ZipCode','Country']]
    employee_df = employee_df.drop_duplicates()
    # employee_df = employee_df.dropna()

    ## Fix state for those with full form in 'State'
    employee_df["State"] = employee_df["State"].apply(
    lambda x: state_df.loc[state_df["StateFull"] == x, "State"].values[0] if x in state_df["StateFull"].values else x)
    employee_df.to_csv(f'{TABLE_ZONE}/Employee.csv', index=False)
    
    company_df = df[['EmployeeID','Office','Start_Date','Termination_Date','Office_Type','Currency','Salary','Active Status','Job_Profile']]
    company_df = company_df.drop_duplicates()
    company_df = company_df.dropna()
    company_df.to_csv(f'{TABLE_ZONE}/CompanyData.csv', index=True, index_label= 'CompanyData_ID')


def transform_jobprofile():
    input_file_job = f'{EXTRACTED_ZONE}/JobProfileMapping.csv'
    with open(input_file_job, 'rb') as f:
        result_job = chardet.detect(f.read())
        encoding_job = result_job['encoding']
        logging.info(f"Detected encoding: {encoding_job}")

    job_df = pd.read_csv(input_file_job, encoding=encoding_job)
    job_df = job_df[['Job_Profile','Job_title','Department', ' Compensation ' ,'Level','Bonus %']]

    job_df = job_df.rename(columns={'Bonus %': 'Bonus', ' Compensation ': 'Compensation'})
    job_df['Compensation'] = job_df['Compensation'].apply(lambda x: float(x.replace(',', '')))

    job_df = job_df.drop_duplicates()
    job_df = job_df.dropna()
    job_df.to_csv(f'{TABLE_ZONE}/JobProfile.csv', index=False)
    
def create_table_load_data():
    with open('/opt/airflow/dags/load_data.sql', 'r') as file:
        sql_load= file.read()
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost', schema='test')
    conn = pg_hook.get_conn()

    cur = conn.cursor()

    cur.execute(sql_load)
    conn.commit()

    cur.close()
    conn.close()

default_args = {
    'owner': 'Nipphit Apisitpuwakul',
    'start_date': days_ago(0),
    'email': ['keanapisit@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG('transform_load',
         schedule_interval=timedelta(minutes=10),
         default_args=default_args,
         description='A simple data pipeline',
         catchup=False) as dag:
    
    # State, Country, Employee, JobProfile, and CompanyData tables following the schema in ....
    t1 = PythonOperator(
        task_id='transform_company_data',
        python_callable=transform_company_data,
        dag=dag,
    ) 

    # JobProfile table following the schema in ....
    t2 = PythonOperator(
        task_id='transform_jobprofile',
        python_callable=transform_jobprofile,
        dag=dag,
    )

    t3 = PythonOperator(
        task_id='load_to_postgres',
        python_callable=create_table_load_data,
    )

    
    t1 >> t2 >> t3