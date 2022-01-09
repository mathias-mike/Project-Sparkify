from airflow import DAG
from operators import LoadDimensionOperator
from helpers import SqlQueries

def get_load_dim_subdag (
        parent_dag_name,      
        task_id,
        redshift_conn_id,
        default_args,
        *args, **kwargs):

    dag = DAG(
        dag_id=f"{parent_dag_name}.{task_id}",
        default_args=default_args,
        schedule_interval='0 * * * *'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="users",
        et_query=SqlQueries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="songs",
        et_query=SqlQueries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="artists",
        et_query=SqlQueries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table="time",
        et_query=SqlQueries.time_table_insert
    )

    load_user_dimension_table
    load_song_dimension_table
    load_artist_dimension_table
    load_time_dimension_table

    return dag


