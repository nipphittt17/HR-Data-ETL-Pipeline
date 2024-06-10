from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta
from airflow.utils.dates import days_ago

import pandas as pd
import chardet
import logging

EXTRACTED_ZONE = "/opt/airflow/dags/dataset/extracted_csv"
TABLE_ZONE = "/opt/airflow/dags/dataset/tables"

employee = f'{EXTRACTED_ZONE}/CompanyData.csv'


def companydata_file():
    input_file = f'{EXTRACTED_ZONE}/CompanyData.csv'
    with open(input_file, 'rb') as f:
        result = chardet.detect(f.read())
        encoding = result['encoding']
        logging.info(f"Detected encoding: {encoding}")

    df = pd.read_csv(input_file, encoding=encoding)

    state_df = df[["State", "StateFull"]]
    state_df.to_csv(f'{TABLE_ZONE}/State.csv', index=False)

    country_df = df[["Country", "CountryFull"]]
    country_df.to_csv(f'{TABLE_ZONE}/Country.csv', index=False)

    employee_df = df[['EmployeeID','First_Name','Surname','Age','DOB','StreetAddress','City','State','ZipCode','Country']]
    employee_df.to_csv(f'{TABLE_ZONE}/Employee.csv', index=False)
    
    company_df = df[['EmployeeID','Office','Start_Date','Termination_Date','Office_Type','Currency','Salary','Active Status','Job_Profile']]
    company_df.to_csv(f'{TABLE_ZONE}/CompanyData.csv', index=True, index_label= 'CompanyData_ID')


def jobprofile_file():
    input_file_job = f'{EXTRACTED_ZONE}/JobProfileMapping.csv'
    with open(input_file_job, 'rb') as f:
        result_job = chardet.detect(f.read())
        encoding_job = result_job['encoding']
        logging.info(f"Detected encoding: {encoding_job}")

    job_df = pd.read_csv(input_file_job, encoding=encoding_job)
    job_df = job_df[['Job_Profile','Job_title','Department', ' Compensation ' ,'Level','Bonus %']]
    job_df = job_df.rename(columns={'Bonus %': 'Bonus', ' Compensation ': 'Compensation'})
    job_df.to_csv(f'{TABLE_ZONE}/JobProfile.csv', index=False)

default_args = {
    'owner': 'Nipphit Apisitpuwakul',
    'start_date': days_ago(0),
    'email': ['keanapisit@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG('create_tables',
         schedule_interval=timedelta(minutes=2),
         default_args=default_args,
         description='A simple data pipeline',
         catchup=False) as dag:
    
    # State, Country, Employee, JobProfile, and CompanyData tables following the schema in ....
    t1 = PythonOperator(
        task_id='companydata_file',
        python_callable=companydata_file,
        dag=dag,
    ) 

    # JobProfile table following the schema in ....
    t2 = PythonOperator(
        task_id='jobprofile_file',
        python_callable=jobprofile_file,
        dag=dag,
    )

    ## EmployeeDiversity table following the schema in ....
    # t3 = PythonOperator(
    #     task_id='employee_table',
    #     python_callable=employee_table,
    #     dag=dag,
    # )

    t1 >> t2