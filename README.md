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
.
├── artifacts
│   ├── graph
│   │   ├── parking_meters
│   │   └── traffic_crashes
│   └── relational
│       └── create_schema
├── docker
└── src
    ├── cleaning         -- The data cleansing, transformation decisions for each dataset
    ├── core             -- The core ETL logic
    ├── normalization
    └── resources        -- Contains all the datasets
        ├── clean
        ├── normalized
        └── raw

```

## Instructions to run all the ETLs

1. Run the `runner.py` in the root directory which can invoke and run all the ETL programs using `py runner.py`


## Instructions to run specific dataset ETL

1. To run the ETL pipeline for any of the datasets, choose a dataset from the available ones in `src/resources/raw`
2. The corresponding ETL pipeline is created in `src/cleaning/`, and run the file using the command `python3 -m src.cleaning.etl_social_media` (for running the ETL for `social_media_entertainment_data.csv`)
3. The same command can be used for other datasets and corresponding ETL pipelines
4. The output of the ETL pipeline will be stored in `src/resources/clean` folder with the format `processed_<dataset_name>` in CSV file format

## MS SQL Server Backup Commands
- If the SQL Server is running on Docker, use the following to back up the database and copy it on to host filesystem
- ```shell
   docker ps # To find container ID if done manually
   docker exec -it $(docker ps -aqf "name=dataqualityframework-db") /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P 'password123!' -Q "BACKUP DATABASE framework TO DISK = N'/var/opt/mssql/backup/framework.bak' WITH NOFORMAT, NOINIT, NAME = 'framework', SKIP, NOREWIND, NOUNLOAD, STATS = 10" && docker cp $(docker ps -aqf "name=dataqualityframework-db"):/var/opt/mssql/backup/framework.bak ./framework.bak
```