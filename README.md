# HR-Analysis

The objective of this project was to design and implement an ETL (Extract, Transform, Load) pipeline that automates the processing of data from multiple text files, transforms the data according to business requirements and schemas constraint, and loads it into a PostgreSQL database. The workflow were automated and scheduled using Apache Airflow with python and sql scripts. The processed data is then visualized using Tableau to derive actionable insights

- [Extract](dags/exteact.py)
- [Transform & Load](dags/transform_load.py) with ... [SQL script](dags/load_data.sql)
- [Visualization]()

## Enviroment preparation

This project used **Apache Airflow** running via Docker.
[Docker compose file](docker-compose.yaml)
[Original source](https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml)

```
docker compose up
```

## Dataset
Two text files were downloaded from [this github repository](https://github.com/Koluit/The_Company_Data.git)

**1. CompanyData.txt**
<br> Columns: EmployeeID, First_Name, Surname, StreetAddress, City, State, StateFull, ZipCode, Country, CountryFull, Age, Office, Start_Date, Termination_Date, Office_Type, Department, Currency, Bonus_pct, Job_title, DOB, level, Salary, Active, Status, Job_Profile, Notes

**2. 2021.06_job_profile_mapping.txt**
<br> Columns: Department, Job_title, Job_Profile, Compensation, Level, Bonus %

## Data model
Based on the raw data, an entiry-relationship diagram was designed to ensure optimal organization and efficiency in data retrieval and analysis. The schema is depicted below:

<br><br> <img src="data_model.png" alt="Data_model"> <br>

**1. Fact table**
- Employment: This table stores comprehensive employment details of employees. This table captures the history of an employee's association with a company. The employment ID was generated, in case that the employee might change or have several positions within the organizationn. Additionally, this table is connected to the Employee and JobProfile tables via foreign keys.

**2. Dimension tables**
- Employee: This table contains personal and demographic information for each employee. The state and country names are stored only in abbreviated forms and connected to the State and Country tables via foreign keys.
- State: This table contains state names in both abbreviated and full forms.
- Country: This table contains country names in both abbreviated and full forms.
- JobProfile: This table provides details about each job profile, including job titles, department, and other relevant attributes.

## Extract-Transform-Load
**1. Extract**
- The initial step involved extracting text files and converting them into .csv format using the BashOperator in Apache Airflow.
- [Extraction script](dags/exteact.py).

**2. Transform**
- After extraction, the transformation process was executed using the PythonOperator.
- This step focused on eliminating duplications and rearranging the columns of each table to match the designed schema.
- [Detailed data transformation process](dags/transform_load.py).

**3. Load**
- Once the data was transformed, it was ready to be loaded into the PostgreSQL database.
- The PostgresHook was utilized to manage database connections and operations.
- Python scripts established the connection and executed SQL commands to load the .csv files into the database.
- The SQL script, which includes table creation and data loading commands, ensures that the data adheres to the specified schema and constraints, thereby maintaining data integrity and consistency.
- [Detailed data loading process](dags/transform_load.py), with [SQL script](dags/load_data.sql).


