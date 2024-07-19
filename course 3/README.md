# Data Warehouse ETL Project
This project involves setting up a data warehouse on AWS Redshift and performing ETL (Extract, Transform, Load) operations to move data from S3 to staging tables on Redshift and then transform it into a set of dimensional tables for analysis.

## Project Structure
sql_queries.py: Contains all SQL queries used in the project including table creation, table dropping, data copying from S3, and data insertion into the final tables.
create_tables.py: Script for setting up the database schema. It drops existing tables and recreates them.
etl.py: Script for performing the ETL process. It loads data from S3 into staging tables and then inserts the data into the final dimensional tables.
dwh.cfg: Configuration file containing AWS credentials and Redshift cluster details.

## Configuration
Before running the scripts, ensure that the dwh.cfg file is correctly configured with your AWS and Redshift cluster details.

## Usage

### Step 1: Create Tables
To create the tables in the Redshift cluster, run the create_tables.py script. This will drop any existing tables and create new ones based on the queries defined in sql_queries.py.

```
python create_tables.py
```

### Step 2: Run the ETL Process
To execute the ETL process and load data into the tables, run the etl.py script. This script will copy data from S3 to the staging tables and then insert it into the final tables.

```
python etl.py
```

