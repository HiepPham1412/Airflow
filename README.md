
## Intro

This project aims to use Airflow as an orchestrator tool to easily mornitor and schedule an ETL pipeline from S3 to Redshift for an online music streaming company call Spakify.

## Data set

- Log data: s3://udacity-dend/log_data. This S3 bucket contains historical data of song played by Sparkify user 
- Song data: s3://udacity-dend/song_data. This data contains all information associated to a song

## ETL process

- Data is loaded from json files in S3 bucket to staging tables in Redshift
- Relevant data is extracted from staging tables, transformed and loaded to fact and dimention tables in Redshift


## Airflow operators and DAGs setting


![Dag](dag.png)



- Stage Operator: load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table. 

- Fact and Dimension Operators: extract and transform data from staging tables to relevant facts and dimention table.

- Data Quality Operator: Check the general quality of the data such as the number of records.

- Dag settings: 

	- Dags are run on 1 minute interval
    - The DAG does not have dependencies on past runs
    - On failure, the task are retried 3 times
    - Retries happen every 5 minutes
    - Catchup is turned off
    - Do not email on retry

## How to run:

1. Run the bash script `start.sh`


2. In Airflow UI, add following connections:

+ `aws_credentials`: contains user's aws login and secrets
+ `redshift`: contains all information to access a redshift cluser


3. Turn on the dag




