# ETL Parking Meters Repository

## Overview

This repository contains a set of scripts designed to perform ETL (Extract, Transform, Load) operations specifically for parking meters data. The primary entrypoint for this project is located in `etl_parking_meters.py`.

### Setup & Installation
1. Use Python version 3.12+ and install the required packages using:
    ```shell
        pip install -r requirements
    ```
2. Create a new file with the name `.env` by taking a copy of `.env.template` file.
3. Update the MS SQL Server connection details in the `.env` file, these values are used to connect to the Framework database and load the values

### Directory Structure
```
├── README.md
├── base_etl.py  -- File containing the main Data Quality Framework pipeline code
├── data_load.py -- Outdated and no longer required 
├── db_utils.py  -- Utility code required to interact with DB
├── docker       -- Docker related components, to setup DB through Docker
│   ├── Dockerfile
│   ├── create-db.sql
│   ├── entrypoint.sh
│   ├── run-init.sh
│   └── sqlserver_data
├── docker-compose.yml
├── etl_parking_meters.py  -- Sample implementation for each dataset, replicate using this file
├── requirements.txt
└── resources
    ├── clean
    │   └── processed_ParkingMeters.csv
    └── raw
        └── ParkingMeters.csv
```