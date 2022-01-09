# Project Sparkify
Sparkify is a music streaming startup with an impressive userbase growth _(their marketing team must be doing one hell of a job)_. They looking to improve the capabilities of their analytical and data team and have decided to introduce more automation and monitoring to their data warehouse ETL pipeline and have come to the conclusion that the best tool to achieve this is Apache Airflow.

In this project I am tasked to build a high grade data pipeline that is dynamic and built from reusable task, can be monitored and allow easy backfills. It should also be noted that data quality plays a big part in the analysis process for this project.

## Steps
1. Install and set up Airflow
2. Create an AWS user
3. Launch an AWS Redshift cluster
4. Launch Airflow and open Airflow UI on browser
5. Configure connection info on Airflow
6. Run DAG 

### Install and set up Airflow
There are a number of ways to install and set up airflow on your machine. For a guide to setting up airflow on your machine follow this link: [Airflow installation Guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#)

### Create an AWS user
For this project, you need to grant Redshift programatic access to read data from S3. 

To do this, you create an AWS IAM user with S3ReadOnlyPermission. Obtain user `aws_access_key_id` and `aws_secret_access_key` which would be used later.

### Launch an AWS Redshift cluster
You'll need to configure and launch a Redshift cluster that would serve as the home for the data warehouse.

Go to AWS Redshift console and click on create cluster. Enter the necessary information and you are good to go.

**Important**: S3 data is store in the `us-west-2` region so it's best to create the cluster in that region as well.

![redshift_cluster](https://raw.githubusercontent.com/mathias-mike/Project-Sparkify/master/Pipeline/redshift_cluster.png)

You'll need the information enclosed in red boxes later.

### Launch Airflow and open Airflow UI on browser
Depending on your installation and setup of airflow on you machine, this procedure might be different.

I used docker-image for my airflow setup locally, so to launch airflow, I simply enter the command; \
`docker-compose up` \
in my terminal.

To navigate to the UI, I open up a web browser to my local host web address on port 8080: `http://localhost:8080/` \
You'll be greeted by the DAG screen where you can run your dags. Before this you need to configure some connection info.

### Configure connection info on Airflow
1. Go to `Admin -> Connections` \
![admin-connections.png](https://raw.githubusercontent.com/mathias-mike/Project-Sparkify/master/Pipeline/admin-connections.png)

2. Under **Connections**, select **Create** \
![create-connections.png](https://raw.githubusercontent.com/mathias-mike/Project-Sparkify/master/Pipeline/create-connection.png)

3. On the create connection page, enter the following:
    * **Conn Id**: Enter `aws_credentials`.
    * **Conn Type**: Enter `Amazon Web Services`.
    * **Login**: Enter the `aws_access_key_id` from the IAM User credentials you created earlier.
    * **Password**: Enter the `aws_secret_access_key` from the IAM User credentials you created earlier.\
Once you've entered these values, select **Save and Add Another**.
![connection-aws-credentials.png](https://raw.githubusercontent.com/mathias-mike/Project-Sparkify/master/Pipeline/connection-aws-credentials.png)

4. On the next create connection page, enter the following values:

    * **Conn Id**: Enter `redshift`.
    * **Conn Type**: Enter `Postgres`.
    * **Host**: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See the screenshot above in **step 3**. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.
    * **Schema**: Enter `dev`. This is the Redshift database you want to connect to.
    * **Login**: Enter `awsuser`.
    * **Password**: Enter the password you created when launching your Redshift cluster.
    * **Port**: Enter `5439`. \
Once you've entered these values, select **Save**.
![connection-redshift.png](https://raw.githubusercontent.com/mathias-mike/Project-Sparkify/master/Pipeline/connection-redshift.png)


Awesome, now you are done with the configuration;

### Run DAG
Navigate back to the DAG's page, you'll notice 2 DAGs here; **sparkify** and **sparkify_with_subdag**. Both do the same thing in different code definitions.

* **sparkify**: Performs the task without a SubDAG
* **sparkify_with_subdag**: Performs the task with a SubDAG for the ETL on the Dimensions table.

So their **DAG Graph** would be different but they basically do the same thing.

