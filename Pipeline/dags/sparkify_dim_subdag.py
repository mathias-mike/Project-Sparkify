from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

# Old version
# from airflow.operators import (
#     StageToRedshiftOperator, 
#     LoadFactOperator,
#     LoadDimensionOperator, 
#     DataQualityOperator
# )

# Version 2
from operators import *
from helpers import SqlQueries
from load_dim_subdag import get_load_dim_subdag

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'mike',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2019, 1, 12),
}

dag = DAG (
    'sparkify_with_subdag',
    default_args=default_args,
    catchup=False,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)

start_operator = PostgresOperator (
    task_id='Begin_execution',
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id='redshift'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    et_query=SqlQueries.songplay_table_insert,
)

load_dimension_tables = SubDagOperator(
    task_id="load_dimension_tables_subdag",
    dag=dag,
    subdag=get_load_dim_subdag(
        "sparkify_with_subdag",      
        "load_dimension_tables_subdag",
        "redshift",
        default_args
    )
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {"test_query": SqlQueries.check_songplay_table, "expected_result":0},
        {"test_query": SqlQueries.check_user_table, "expected_result":0},
        {"test_query": SqlQueries.check_song_table, "expected_result":0},
        {"test_query": SqlQueries.check_artist_table, "expected_result":0}
    ]
)

# Operator that does literally nothing. It can be used to group tasks in a DAG.
# The task is evaluated by the scheduler but never processed by the executor.
end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_dimension_tables

load_dimension_tables >> run_quality_checks

run_quality_checks >> end_operator



