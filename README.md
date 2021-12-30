# Project Sparkify
Sparkify is a music streaming startup with an impressive userbase growth _(their marketing team must be doing one hell of a job)_ and is looking to move their processes and data to the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked with building a data warehouse for the analytics team, I will basically build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transform the data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

## Steps 
1. Configure and launch a Redshift cluster on AWS
2. Create tables on Redshift database
3. Configure and launch Spark cluster with AWS EMR
4. Stage data from S3 to Redshift stagging tables with Spark-Redshift connector
5. ETL on stagging tables to Star Dimension tables

### Configure and launch a Redshift cluster on AWS
The `configure.ipynb` notebook file contains step to configure and launch Redshift cluster on AWS. 

#### Requirements
* An AWS account 
* An AWS programmatic user with Amazon[IAM, S3, Redshift, EC2, EMR] permisions
* `Boto3` AWS SDK for python installed on your machine.

### Create tables on Redshift database
Once Redshift cluster is launched, you'll need to get the and save the  
* IAM role arn
* Redshift endpoint 
* Redshift port 
* Redshift database name
* database username 
* and database password
in the `dwh.cfg` file.

Once this is done, fire up the `create_table.py` script to create all necessary table for this project.

### Configure and launch Spark cluster with AWS EMR
The datasource file was stored in a rather complex and difficult to read way (files nested within files). This was taking too long for redshift to load using the `COPY` command.

To ease the task, A spark cluster can be used to load the file to Redshift using `spark-redshift` connector. This was the plan, but due to some minor configuration issue with `spark-redshift` connector I improvised. (Definitely an issue to file and will get back on it)

1. Fire up a spark cluster on AWS using AWS EMR
2. Run the `spark_stagging.py` script that stage the file to an S3 bucket in a better way not nested within files.


### ETL on stagging tables to Star Dimension tables
Finally, run `etl.py` script to stage data from new S3 path to Redshift staging tables.

The run will also perform ETL on the stagging tables into Star dimensions tables.


## Queries
The queries ran for all processes are saved in the `sql_queries.py` file.

### Creating Star Dimension tables
* The facts table, `songplays`, is distributed with the `DISTKEY` on `song_id` and `SORTKEY` on `start_time` columns. 
 In a bid to eleminate SHUFFLING, This distribution style is choosen for this table which would link to the `songs` table on the `song_id` column because the songs dimension table is quite large and a good number of joins is going to be happening between the two tables. 
 We want both tables to be distributed on the same key.

 * All other dimension tables have a `DISTSTYLE ALL` distribution style as the a not as much as the `songs` table and we can afford to replicate data.

### ETL Queries
The ETL queries are the `INSERT INTO <table_name>` queries in the `sql_queries.py` file. This queries perform some sql DQL task on the stagging tables to transform them into the needed data for the dimensions tables.