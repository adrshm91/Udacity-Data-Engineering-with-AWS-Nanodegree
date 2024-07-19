"""
ETL Script for Data Warehouse ETL Project

This script loads data from S3 into staging tables on Redshift and then inserts the data into the final tables.
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 into staging tables on Redshift.

    Parameters:
    - cur: psycopg2 cursor object
    - conn: psycopg2 connection object
    """
    for query in copy_table_queries:
        print(f"Executing {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables into final tables on Redshift.

    Parameters:
    - cur: psycopg2 cursor object
    - conn: psycopg2 connection object
    """
    for query in insert_table_queries:
        print(f"Executing {query}")
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to load data into staging tables and insert into final tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()