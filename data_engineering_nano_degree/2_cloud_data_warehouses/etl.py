import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Function which loops through the load_staging statements before executing each create statement using the psycopg2 client (conn)
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Function which loops through the insert statements before executing each create statement using the psycopg2 client (conn)
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Main function which:
        - Opens the config file to fetch arguments
        - Create database connection via psycopg2 using the arguments from config file
        - Call both the load_staging_tablese and insert_tables functions passing the psycopg2 as arguments
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(f"host={config.get('CLUSTER', 'dwh_host')} dbname={config.get('CLUSTER', 'db_name')} user={config.get('CLUSTER', 'db_user')} password={config.get('CLUSTER', 'db_password')} port={config.get('CLUSTER', 'db_port')}")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()