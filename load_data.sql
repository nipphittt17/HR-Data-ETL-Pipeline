BEGIN;

CREATE TABLE public."State"(
    state "char" NOT NULL,
    state_full "char" NOT NULL,
    PRIMARY KEY (state)
);
COPY public."State" FROM '' WITH (FORMAT csv, HEADER);

CREATE TABLE public."Country"(
    country "char" NOT NULL,
    country_full "char" NOT NULL,
    PRIMARY KEY (country)
);
COPY public."Country" FROM '' WITH (FORMAT csv, HEADER);

CREATE TABLE public."Employee"
(
    employee_id INTEGER NOT NULL,
    firstname "char" NOT NULL,
    lastname "char" NOT NULL,
    age INTEGER NOT NULL,
    dob date NOT NULL,
    street_address "char" NOT NULL,
    city "char" NOT NULL,
    state "char" NOT NULL,
    zipcode INTEGER NOT NULL,
    country "char" NOT NULL,

    PRIMARY KEY(employee_id),
    FOREIGN KEY(state) REFERENCES public."State" (state),
    FOREIGN KEY(country) REFERENCES public."Country" (country)
);
COPY public."Employee" FROM '' WITH (FORMAT csv, HEADER);

CREATE TABLE public."JobProfile"
(
    jobprofile "char" NOT NULL,
    jobtitle "char" NOT NULL,
    department "char" NOT NULL,
    compensation FLOAT NOT NULL,
    level "char" NOT NULL,
    bonus_pct FLOAT NOT NULL,

    PRIMARY KEY(jobprofile),
);
COPY public."JobProfile" FROM '' WITH (FORMAT csv, HEADER);

-- DROP TABLE IF EXISTS public."Bill";
CREATE TABLE public."CompanyData"
(
    id??
    employee_id INTEGER NOT NULL,
    office "char" NOT NULL,
    start_date date NOT NULL,
    termination_date date NOT NULL,
    office_type "char" NOT NULL,
    currency "char" NOT NULL,
    salary INTEGER NOT NULL,
    active_status INTEGER NOT NULL,
    job_profile "char" NOT NULL,

    FOREIGN KEY(employee_id) REFERENCES public."Employee" (employee_id),
    FOREIGN KEY(job_profile) REFERENCES public."JobProfile" (job_profile)
);

COPY public."CompanyData" FROM '' WITH (FORMAT csv, HEADER);

END;