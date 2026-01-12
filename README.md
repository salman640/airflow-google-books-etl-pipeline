 Google Books ETL Pipeline using Apache Airflow

This project demonstrates a simple but complete ETL pipeline built with Apache Airflow.  
The pipeline extracts book data from the Google Books API, applies basic transformations, and loads the data into a PostgreSQL database.

The goal of this project is to show how Airflow can be used to orchestrate data workflows using Dockerized services.

Tech Stack

- Apache Airflow (CeleryExecutor)
- PostgreSQL
- Redis
- Docker & Docker Compose
- Python



What the Pipeline Does

The DAG runs three logical steps:

1. Extract:
   
   Fetches book data from the Google Books API.

2. Transform:
   
   Cleans and structures the raw API response into tabular format.

3. Load:
   
   Stores the transformed data into a PostgreSQL table named `books`.

   
<img width="1480" height="810" alt="load_Table" src="https://github.com/user-attachments/assets/e1395f20-00a4-4f28-827d-bf7b8a366760" />




