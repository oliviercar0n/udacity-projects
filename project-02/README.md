# Project: Data Warehouse

## Overview

This project contains the necessary files and documentation to run an ETL pipline to ingest data into a data warehouse for Sparkify hosted on Amazon Redshift. This data warehouse will enable Sparkify to aggregate their data, which currently resides in json files stored on Amazon S3, and will enable them to perform analytics and derive insight  on what their users are listening to. The pipeline is written in Python and SQL. 

## Design

The data warehouse was designed to follow a star schema design with users, songs, artists and time dimensions tables, optimized for song play analysis.

## Configuration

The postgres database is hosted on an Amazon Redshift cluster and ingests data from 2 S3 buckets. To set up the cluster, an IAM role first needs to be defined, with attached policies. The endpoint of the cluster and IAM role ARN are added to the ETL config in order to establish connection with the data warehouse.

## Process

The database tables are created and configured via a python script which calls stoed SQL sueries. The ETL pipeline must then connect to each Amazon S3 bucket, copy the data from each file into a staging table on the Amazon Redshift database, and finally execute `INSERT` statements to transform and load the data into the data warehouse tables.

## Project Structure

* `sql_queries.py` - This files contains allrelevant queries .
* `dwh.cfg` -  Contains key config variables for the Redshift cluster and S3 buckets 
* `create_table.py` - Contains code required to connect to the database, drop any existing table if they already exist and create all relevant staging and data warehouse tables. 
* `etl.py` - Contains  code required to connect to the Redshift database, copy song and log data from S3 buckets to the staging tables created in step 1. It then runs the insert queries to populate the final tables, removing duplicates where appropriate

## Runtime Step

1. Run command `python create_tables.py`
2. Run command `python etl.py`