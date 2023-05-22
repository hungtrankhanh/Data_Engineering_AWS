import configparser
import psycopg2
from prettytable import PrettyTable
from sql_queries import query_session_with_num_of_user, query_on_song_artist_location, query_on_song_artist_latitude, query_on_song_artist_location_with_female, query_on_song_artist_location_with_female_paid


def get_session_with_num_of_user(cur, conn):
    '''Query number of user for each session group by session_id. The query result is sorted following to num_of_user descending'''
    
    cur.execute(query_session_with_num_of_user)
    rows = cur.fetchall()
    
    result_query = PrettyTable()
    result_query.field_names = ["session_id", "num_of_user"]
    for row in rows:
        result_query.add_row([row[0], row[1]])
    print("query_session_with_num_of_user")
    print(result_query)

def get_by_song_artist_location(cur, conn):
    '''Query number of user group by song_title, artist_name, location. The query result is sorted following to num_of_user descending '''
    
    cur.execute(query_on_song_artist_location)
    rows = cur.fetchall()
    
    result_query = PrettyTable()
    result_query.field_names = ["song_title", "artist_name", "location", "num_of_user"]
    for row in rows:
        result_query.add_row([row[0], row[1], row[2], row[3]])
    print("query_on_song_artist_location")
    print(result_query)


def get_by_song_artist_latitude(cur, conn):
    '''Query number of user group by song_title, artist_name, latitude. The query result is sorted following to num_of_user descending'''
    
    cur.execute(query_on_song_artist_latitude)
    rows = cur.fetchall()
    
    result_query = PrettyTable()
    result_query.field_names = ["song_title", "artist_name", "latitude", "num_of_user"]
    for row in rows:
        result_query.add_row([row[0], row[1], row[2], row[3]])
    print("query_on_song_artist_latitude")
    print(result_query)
                              

def get_by_song_artist_location_with_female(cur, conn):
    '''Query number of user where gender is female group by song_title, artist_name, location. The query result is sorted following to num_of_user descending'''
    
    cur.execute(query_on_song_artist_location_with_female)
    rows = cur.fetchall()
    
    result_query = PrettyTable()
    result_query.field_names = ["song_title", "artist_name", "location", "num_of_user"]
    for row in rows:
        result_query.add_row([row[0], row[1], row[2], row[3]])
    print("query_on_song_artist_location_with_female")
    print(result_query)


def get_by_song_artist_location_with_female_paid(cur, conn):
    '''Query number of user where gender is female and level is paid group by song_title, artist_name, location. The query result is sorted following to num_of_user descending'''
    
    cur.execute(query_on_song_artist_location_with_female_paid)
    rows = cur.fetchall()
    
    result_query = PrettyTable()
    result_query.field_names = ["song_title", "artist_name", "location", "num_of_user"]
    for row in rows:
        result_query.add_row([row[0], row[1], row[2], row[3]])
    print("query_on_song_artist_location_with_female_paid")
    print(result_query)
                              
    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    get_session_with_num_of_user(cur, conn)
    get_by_song_artist_location(cur, conn)
    get_by_song_artist_latitude(cur, conn)
    get_by_song_artist_location_with_female(cur, conn)
    get_by_song_artist_location_with_female_paid(cur, conn)

    conn.close()
    print("DONE")


if __name__ == "__main__":
    main()