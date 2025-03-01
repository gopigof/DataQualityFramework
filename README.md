# ETL Parking Meters Repository

## Overview

This repository contains a set of scripts designed to perform ETL (Extract, Transform, Load) operations specifically for parking meters data. The primary entrypoint for this project is located in `etl_parking_meters.py`.

### Setup & Installation
1. Use Python version 3.12+ and install the required packages using:
    ```shell
        pip install -r requirements
    ```
2. Create a new file with the name `.env` by taking a copy of `.env.template` file.
3. Create the framework database on MS SQL Server, by running the SQL code located in `docker/create-db.sql`
4. Update the MS SQL Server connection details in the `.env` file, these values are used to connect to the Framework database and load the values

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

### Quickstart Instructions
1. A minified version of the "Social Media Entertainment" dataset was created and is available at `src/resources/raw/mini_social.csv`
2. The cleaning and validation for the minified social media dataset is at `src/cleaning/etl_mini_social_media.py`
3. Run the python file using the IDE runner or the command `python3 etl_mini_social_media.py`
