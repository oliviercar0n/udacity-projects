# Project: Data Warehouse

## Overview

This project contains the necessary files and documentation to run an ETL pipline to ingest data into a data lake for Sparkify hosted on Amazon S3. This data lake will enable Sparkify to aggregate their data, which currently resides in json files stored on Amazon S3, and will enable them to perform analytics and derive insight on what their users are listening to. The pipeline is written in Python using PySpark.

## Design

The data lake parquet files were designed to follow a star schema design with users, songs, artists and time dimensions files, optimized for song play analysis.

## Configuration

The data lake is hosted on an Amazon S3  ingests data from 2 S3 buckets. The ETL script runs on an Amazon EMR cluster instance.

## Process

The ETL pipeline connects to each Amazon S3 bucket, reads the data from each file into Spark dataframes, wrangles the data, and finally writes the final data back to S3 in Parquet format

## Project Structure

* `elt.py` - This single file contains all the code to handle the process described below

## Runtime Step

1. Run command `python etl.py`
