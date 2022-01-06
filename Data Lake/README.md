# Project Sparkify
Sparkify is a music streaming startup with an impressive userbase growth _(their marketing team must be doing one hell of a job)_ and is looking to move their processes and data to the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, I am task to build an ETL pipeline for a data lake hosted on S3. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Requirements
* An AWS account 
* An AWS programmatic user with AmazonS3 permissions

## ETL
* The `songs` and `artists` tables are extracted from the `song_data` on s3. The `songs` table contain quite a lot of record so when storing back into S3, it is partitioned by `partitionBy=["year", "artist_id"]`.

* The `users` and the `time` table are both extracted from the `log_data` on S3. The `time` data is also partitioned before storing into S3 to improve query time.

* The `songplays` join both `song_data` and `log_data` to extract it's fields and is partitioned before being stored back into s3.

* Partitioning the data before storing improves query time as it limits the volume of data being scanned.

* Also all data are stored in `parquet` files. This an efficient way of storing data as it a columnar store which reads data in less time and minimizes latency.

## dl.cfg 
The `dl.cfg` file will contain you user authentication information in form of AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

* Create the file `dl.cfg`.
* Create a user with AmazonS3 permision and give it programatic access.
* Get user AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
* Inside empty `dl.cfg` file, save keys as:
    ```
    [USER]
    AWS_ACCESS_KEY_ID=''
    AWS_SECRET_ACCESS_KEY=''
    ```

## Running python script
1. Configure and launch an AWS EMR clauster with spark installed.
2. Open terminal on local mechine and copy files to EMR cluster.
```
scp -i myKeyPair.pem <path_to_file>/etl.py hadoop@ec2-3-83-32-211.compute-1.amazonaws.com:~/data/

scp -i myKeyPair.pem <path_to_file>/dl.cfg hadoop@ec2-3-83-32-211.compute-1.amazonaws.com:~/data/
```
3. SSH into master EC2 instance - `ssh -i myKeyPair.pem hadoop@ec2-3-83-32-211.compute-1.amazonaws.com`
4. `sudo pip install configparser` on terminal
5. `cd data` on terminal
6. `spark-submit etl.py`