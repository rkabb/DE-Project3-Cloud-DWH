import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
     This function drops any tables.
     It takes connection  to database as parameter.
     All the DDL and list of tables that need to be deleted are defined in module sql_queries.
    """
    for query in drop_table_queries:
        logger.info('Running DDL: '+ query )
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
     This function creates new tables.
     It takes connection  to database as parameter.
     All the DDL and list of tables that need to be created are defined in module sql_queries.
    """
    for query in create_table_queries:
        logger.info('Running DDL: '+ query )
        cur.execute(query)
        conn.commit()


def main():
    """
     This is the main program which will call the functions to drop and create tables.
     It establishes connection with DB and then calls other functions
    """
    logger.info('Beginning of create tables script')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    logger.info('Ready to connect to Redshift database')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logger.info('Sucessfully connected Redshift database')

    logger.info('Drop any existing tables')
    drop_tables(cur, conn)
    logger.info('Drop was sucessful')

    logger.info('Creating new tables')
    create_tables(cur, conn)
    logger.info('Create  tables  sucessful')

    logger.info('End of create tables script')

    conn.close()


if __name__ == "__main__":
    logger = logging.getLogger('Create_tables')
    logging.basicConfig(level=logging.INFO)
    logger.info('main')
    main()