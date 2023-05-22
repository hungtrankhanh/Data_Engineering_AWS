import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    '''Load json data from S3 into staging tables in Redshift'''
    
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    '''Extract data from staging tables, then transform and load them into analyzed tables with star schema in Redshift'''
    
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        print("Rows Added  = ", cur.rowcount)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    conn.close()
    print("DONE")


if __name__ == "__main__":
    main()