import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drops all existing tables in the Redshift cluster. 
    Runs "sql_queries.drop_table_queries" : A list of PostgreSQL DROP TABLE queries

    Parameters
    ----
    cur: psycopg2.extensions.cursor
        Allows python code to execute PostgreSQL command in Redshift.
    conn: psycopg2.extensions.connection
        Connection to a PostgreSQL database instance (In this case, Redshift)
    '''

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Create all tables in the Redshift cluster. 
    Runs "sql_queries.create_table_queries" : A list of PostgreSQL CREATE TABLE queries

    Parameters
    ----
    cur: psycopg2.extensions.cursor
        Allows python code to execute PostgreSQL command in Redshift.
    conn: psycopg2.extensions.connection
        Connection to a PostgreSQL database instance (In this case, Redshift)
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Loading configuration from 'dwh.cfg' file.
    # Open 'configuration.ipynb' to set up configuration
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connects to PostgreSQL database instance(Redshift) and get cursor.
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()