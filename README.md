# Project Sparkify
Sparkify is a music streaming startup with an impressive userbase growth _(their marketing team must be doing one hell of a job)_ and is looking to move their processes and data to the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

I am tasked with building a data warehouse for the analytics team, I will basically build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transform the data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

