# Data Quality Framework - ETL

## Overview

This repository contains the skeleton code for implementing custom ETL  (Extract, Transform, Load) pipelines with auditing and logging in place. Alongside the skeleton pipeline logic, a couple of datasets are processed using the framework. 


### Setup & Installation

1. Use Python version 3.12+ and install the required packages using:
    ```shell
        pip install -r requirements
    ```
2. Create a new file with the name `.env` by taking a copy of `.env.template` file.
3. Create the framework database on MS SQL Server, by running the SQL code located in `docker/create-db.sql`
4. Update the MS SQL Server connection details in the `.env` file, these values are used to connect to the Framework
   database and load the values

### Directory Structure

```
├── README.md
├── docker
│   ├── Dockerfile
│   ├── create-db.sql
│   ├── entrypoint.sh
│   ├── run-init.sh
│   └── sqlserver_data
├── docker-compose.yml
├── requirements.txt
├── sql_scripts
│   └── create_schema
│       ├── crime_incidents.sql
│       ├── parking_meters.sql
│       └── traffic_crashes.sql
└── src
    ├── cleaning_and_validation
    │   ├── base_etl.py
    │   ├── db_utils.py     -- Utility code required to interact with DB
    │   ├── etl_mini_social_media.py
    │   ├── etl_parking_meters.py
    │   └── etl_social_media.py
    ├── normalization
    │   └── normalization_social_media.py
    └── resources
        ├── clean
        ├── normalized
        └── raw

```

## Instructions to run programs

1. To run the ETL pipeline for any of the datasets, choose a dataset from the available ones in `src/resources/raw`
2. The corresponding ETL pipeline is created in `src/cleaning/`, and run the file using the command `python3 -m src.cleaning.etl_social_media` (for running the ETL for `social_media_entertainment_data.csv`)
3. The same command can be used for other datasets and corresponding ETL pipelines
4. The output of the ETL pipeline will be stored in `src/resources/clean` folder with the format `processed_<dataset_name>` in CSV file format

## MS SQL Server Backup Commands
- If the SQL Server is running on Docker, use the following to backup the database and copy it on to host filesystem
- ```shell
   docker ps # To find container ID
   docker exec -it 3fa85b3065b1 /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'password123!' -Q "BACKUP DATABASE framework TO DISK = N'/var/opt/mssql/backup/framework.bak' WITH NOFORMAT, NOINIT, NAME = 'framework', SKIP, NOREWIND, NOUNLOAD, STATS = 10"
   docker cp 3fa85b3065b1:/var/opt/mssql/backup/framework.bak ./framework.bak
```