import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    """
    Function which loops through the drop_table_queries before executing each drop statement usinig the psycopg2 client (conn)
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Function which loops through the create_table_queries before executing each create statement using the psycopg2 client (conn)
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Main function which:
        - Opens the config file to fetch arguments
        - Create database connection via psycopg2 using the arguments from config file
        - Call both the drop_table and create_table functions passing the psycopg2 as arguments
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    conn = psycopg2.connect(f"host={config.get('CLUSTER', 'dwh_host')} dbname={config.get('CLUSTER', 'db_name')} user={config.get('CLUSTER', 'db_user')} password={config.get('CLUSTER', 'db_password')} port={config.get('CLUSTER', 'db_port')}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
                            
                            
                            
                        
                        