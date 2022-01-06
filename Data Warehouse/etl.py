import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    '''
    Stage data from S3 to Redshift staging tables (staging_events, staging_song). 

    Parameters
    ----
    cur: psycopg2.extensions.cursor
        Allows python code to execute PostgreSQL command in Redshift.
    conn: psycopg2.extensions.connection
        Connection to a PostgreSQL database instance (In this case, Redshift)
    '''
    for query in copy_table_queries:
        table_name = query[query.index('"') + 1 : query.index('" ')]
        print('Loading {} ...'.format(table_name))

        cur.execute(query)
        conn.commit()

        print('{} Loaded!\n'.format(table_name))


def insert_tables(cur, conn):
    '''
    Performs SQL to SQL ETL from staging tables to Star Dimension tables
    for easy analysis. 

    Parameters
    ----
    cur: psycopg2.extensions.cursor
        Allows python code to execute PostgreSQL command in Redshift.
    conn: psycopg2.extensions.connection
        Connection to a PostgreSQL database instance (In this case, Redshift)
    '''
    for query in insert_table_queries:
        table_name = query[query.index('"') + 1 : query.index('" ')]
        print('Inserting into {} ...'.format(table_name))

        cur.execute(query)
        conn.commit()

        print('Insertion complete!\n'.format(table_name))


def main():
    # Loading configuration from 'dwh.cfg' file.
    # Open 'configuration.ipynb' to set up configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connects to PostgreSQL database instance(Redshift) and get cursor.
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()