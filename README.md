# HR-Analysis

[Dataset as text files](https://github.com/Koluit/The_Company_Data.git)
- CompanyData.txt
- 2021.06_job_profile_mapping.txt

Run **Apache Airflow** via Docker
[Docker compose file](docker-compose.yaml)
[Original source](https://airflow.apache.org/docs/apache-airflow/2.9.1/docker-compose.yaml)

```
docker compose up
```

- [Extract](dags/exteact.py)
- [Transform & Load](dags/transform_load.py) with ... [SQL script](dags/load_data.sql)
- [Visualization]()
