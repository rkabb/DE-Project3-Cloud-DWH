import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
     This function will take DB connection as parameter and calls the LOAD statements defined in sql_queries module.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This will will take DB connection as parameter and calls the INSERT statements defined in sql_queries module.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
     Main function. This will establish DB connection and calls functions to
     load stage tables and other table sin star schema
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    print('connected')
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()