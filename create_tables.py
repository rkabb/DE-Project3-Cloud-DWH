import configparser
import psycopg2
import logging
from sql_queries import create_table_queries, drop_table_queries, drop_schemas_queries ,create_schemas_queries


def drop_schemas(cur, conn):
    """
    Function to drop schemas. This function uses the variable 'drop_schemas_queries' defined in the 'sql_queries.py' file.
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    """
    for query in drop_schemas_queries:
        cur.execute(query)
        conn.commit()


def create_schemas(cur, conn):
    """
    Function to create schemas. This function uses the variable 'create_schemas_queries' defined in the 'sql_queries.py' file.
    Parameters:
        - curr: Cursor for a database connection
        - conn: Database connection
    Outputs:
        None
    """
    for query in create_schemas_queries:
        cur.execute(query)
        conn.commit()


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

    logger.info('Drop any existing schemas')
    drop_schemas(cur, conn)
    logger.info('Drop Schema was sucessful')

    logger.info('Creating new schema')
    create_schemas(cur, conn)
    logger.info('Create  schema sucessful')

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