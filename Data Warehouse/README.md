# Warehouse
I am tasked with building a data warehouse for the analytics team, I will basically build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transform the data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Steps 
1. Configure and launch a Redshift cluster on AWS
2. Create tables on Redshift database
3. Stage data from S3 into Redshift staging tables
5. ETL on stagging tables to Star Dimension tables

### Configure and launch a Redshift cluster on AWS
The `configure.ipynb` notebook file contains step to configure and launch Redshift cluster on AWS. 

#### Requirements
* An AWS account 
* An AWS programmatic user with Amazon[IAM, S3, Redshift, EC2, EMR] permissions
* `Boto3` AWS SDK for python installed on your machine.

### Stage data from S3 into Redshift staging tables
At this stage, we use the COPY command to stage data into Redshift as against the INSERT command (which can also be used but is much slower)

The COPY command leverages the Amazon Redshift massively parallel processing (MPP) architecture to read and load data in parallel from a file or multiple files in an Amazon S3 bucket. 

You can take maximum advantage of parallel processing by splitting your data into multiple files, in cases where the files are compressed. You can also take maximum advantage of parallel processing by setting distribution keys on your tables

You'll find the COPY command in the `sql_queries.py` file.

### ETL on stagging tables to Star Dimension tables
Finally, run `etl.py` script. This script actually stages the data from s3 to Redshift, afterwards, it performs ETL on the staged tables tranforming them into Star dimension.

## Queries
The queries ran for all processes are saved in the `sql_queries.py` file.

### Creating Star Dimension tables
* The facts table, `songplays`, is distributed with the `DISTKEY` on `song_id` and `SORTKEY` on `start_time` columns. 
 In a bid to eleminate SHUFFLING, This distribution style is choosen for this table which would link to the `songs` table on the `song_id` column because the songs dimension table is quite large and a good number of joins is going to be happening between the two tables. 
 We want both tables to be distributed on the same key.

 * All other dimension tables have a `DISTSTYLE ALL` distribution style as they are not as much as the `songs` table and we can afford to replicate data.

### ETL Queries
The ETL queries are the `INSERT INTO <table_name>` queries in the `sql_queries.py` file. This queries perform some sql DQL task on the stagging tables to transform them into the needed data for the dimensions tables.