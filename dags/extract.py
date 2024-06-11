from airflow import DAG
from airflow.operators.bash_operator import BashOperator

from datetime import timedelta
from airflow.utils.dates import days_ago


DATA_FOLDER = "/opt/airflow/dags/dataset"
EXTRACTED_ZONE = "/opt/airflow/dags/dataset/extracted_csv"

default_args = {
    'owner': 'Nipphit Apisitpuwakul',
    'start_date': days_ago(0),
    'email': ['keanapisit@hotmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG('extract_text_files',
         schedule_interval=timedelta(minutes=5),
         default_args=default_args,
         description='A simple data pipeline',
         catchup=False) as dag:

    t1 = BashOperator(
        task_id='extract_company_data',
        bash_command=f'tr "\\t" "," < {DATA_FOLDER}/CompanyData.txt \
        > {EXTRACTED_ZONE}/CompanyData.csv',
        dag=dag,
    )

    t2 = BashOperator(
        task_id='extract_diversity',
        bash_command=f'tr "\\t" "," < {DATA_FOLDER}/Diversity.txt \
        > {EXTRACTED_ZONE}/Diversity.csv',
        dag=dag,
    )

    t3 = BashOperator(
        task_id='extract_job_profile',
        bash_command=f"tr '\\t' ','  < {DATA_FOLDER}/2021.06_job_profile_mapping.txt \
        > {EXTRACTED_ZONE}/JobProfileMapping.csv",
        dag=dag,
    )


    t1 >> t2 >> t3
