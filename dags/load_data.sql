BEGIN;

DROP TABLE IF EXISTS public."CompanyData";
DROP TABLE IF EXISTS public."Employee";
DROP TABLE IF EXISTS public."JobProfile";
DROP TABLE IF EXISTS public."State";
DROP TABLE IF EXISTS public."Country";


CREATE TABLE public."State"(
    state char(2) NOT NULL,
    state_full varchar(50) NOT NULL,
    PRIMARY KEY (state)
);
COPY public."State" FROM '/Users/nipphittt/automating-your-data-pipeline-with-apache-airflow/dags/dataset/tables/State.csv' WITH (FORMAT csv, HEADER);


CREATE TABLE public."Country"(
    country char(2) NOT NULL,
    country_full varchar(20) NOT NULL,
    PRIMARY KEY (country)
);
COPY public."Country" FROM '/Users/nipphittt/automating-your-data-pipeline-with-apache-airflow/dags/dataset/tables/Country.csv' WITH (FORMAT csv, HEADER);


CREATE TABLE public."Employee"
(
    employee_id INTEGER NOT NULL,
    firstname varchar(50) NOT NULL,
    lastname varchar(50) NOT NULL,
    age INTEGER NOT NULL,
    dob date NOT NULL,
    street_address varchar(50),
    city varchar(50),
    state char(2),
    zipcode varchar(8),
    country char(2) NOT NULL,

    PRIMARY KEY(employee_id),
    FOREIGN KEY(state) REFERENCES public."State" (state),
    FOREIGN KEY(country) REFERENCES public."Country" (country)
);
COPY public."Employee" FROM '/Users/nipphittt/automating-your-data-pipeline-with-apache-airflow/dags/dataset/tables/Employee.csv' WITH (FORMAT csv, HEADER);



CREATE TABLE public."JobProfile"
(
    job_profile char(7) NOT NULL,
    job_title varchar(50) NOT NULL,
    department varchar(20) NOT NULL,
    compensation FLOAT NOT NULL,
    level varchar(50) NOT NULL,
    bonus_pct FLOAT NOT NULL,
    PRIMARY KEY (job_profile)
);

COPY public."JobProfile" FROM '/Users/nipphittt/automating-your-data-pipeline-with-apache-airflow/dags/dataset/tables/JobProfile.csv' WITH (FORMAT csv, HEADER);

-- DROP TABLE IF EXISTS public."Bill";
CREATE TABLE public."CompanyData"
(
    company_id INTEGER NOT NULL,
    employee_id INTEGER NOT NULL,
    office varchar(10) NOT NULL,
    start_date date NOT NULL,
    termination_date date NOT NULL,
    office_type varchar(10) NOT NULL,
    currency char(3) NOT NULL,
    salary FLOAT NOT NULL,
    active_status INTEGER NOT NULL,
    job_profile char(7) NOT NULL,

    PRIMARY KEY(company_id),
    FOREIGN KEY(employee_id) REFERENCES public."Employee" (employee_id),
    FOREIGN KEY(job_profile) REFERENCES public."JobProfile" (job_profile)
);

COPY public."CompanyData" FROM '/Users/nipphittt/automating-your-data-pipeline-with-apache-airflow/dags/dataset/tables/CompanyData.csv' WITH (FORMAT csv, HEADER);

END;