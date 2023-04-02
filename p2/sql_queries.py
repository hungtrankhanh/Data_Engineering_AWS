import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_data"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_data"
songplay_table_drop = "DROP TABLE IF EXISTS song_plays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABlE staging_events_data ( 
        event_id int IDENTITY(0,1), 
        artist varchar(255), 
        auth varchar(255),  
        firstName varchar(255), 
        gender varchar(2), 
        itemInSession int, 
        lastName varchar(255), 
        length double precision, 
        level varchar(10), 
        location varchar(255), 
        method varchar(10), 
        page varchar(20), 
        registration varchar(50), 
        sessionId int, 
        song varchar(255), 
        status smallint, 
        ts bigint, 
        userAgent varchar(255), 
        userId int)
    """)

staging_songs_table_create = ("""
    CREATE TABlE staging_songs_data ( 
        num_songs int, 
        artist_id varchar(50), 
        artist_latitude double precision, 
        artist_longitude double precision, 
        artist_location varchar(255), 
        artist_name varchar(255), 
        song_id varchar(50), 
        title varchar(255), 
        duration double precision, 
        year smallint ) 
    """)
    

songplay_table_create = ("""
    CREATE TABlE song_plays ( 
        songplay_id int IDENTITY(0,1) PRIMARY KEY sortkey, 
        start_time datetime NOT NULL REFERENCES time(start_time), 
        user_id int NOT NULL REFERENCES users(user_id) distkey, 
        level varchar(10), 
        song_id varchar(50) NOT NULL REFERENCES songs(song_id), 
        artist_id varchar(50) NOT NULL REFERENCES artists(artist_id), 
        session_id int , 
        location varchar(255), 
        user_agent varchar(255) 
    )
    """)

user_table_create = ("""
    CREATE TABlE users ( 
        user_id int PRIMARY KEY sortkey distkey, 
        first_name varchar(255) NOT NULL, 
        last_name varchar(255) NOT NULL, 
        gender varchar(2), 
        level varchar(10)) 
     """)

song_table_create = ("""
    CREATE TABLE songs ( 
        song_id varchar(50) PRIMARY KEY distkey, 
        title varchar(255), 
        artist_id varchar(50), 
        year int, 
        duration double precision) 
     """)

artist_table_create = ("""
    CREATE TABLE artists ( 
        artist_id varchar(50) PRIMARY KEY distkey, 
        name varchar(255) , 
        location varchar(255), 
        latitude double precision, 
        longitude double precision)
    """)

time_table_create = ("""
    CREATE TABLE time ( 
        start_time datetime PRIMARY KEY, 
        hour smallint NOT NULL, 
        day smallint NOT NULL, 
        week smallint NOT NULL, 
        month smallint NOT NULL, 
        year smallint NOT NULL, 
        weekday smallint NOT NULL) 
    """)

# STAGING TABLES

staging_events_copy = ("""
    copy  staging_events_data from {}
    credentials 'aws_iam_role={}' 
    format as json {} compupdate off region 'us-west-2';
""").format(config.get("S3","LOG_DATA"), config.get("IAM_ROLE","ARN"), config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
     copy  staging_songs_data from {}
     credentials 'aws_iam_role={}' 
     json 'auto' compupdate off region 'us-west-2';
""").format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO song_plays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
        date_add('ms',e.ts,'1970-01-01') as start_time,
        e.userId as user_id,
        e.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        e.sessionId as session_id,
        e.location as location,
        e.userAgent as user_agent
    FROM staging_events_data e 
    JOIN staging_songs_data s ON (e.song = s.title AND e.length=s.duration)

""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        e.userId as user_id,
        e.firstName as first_name,
        e.lastName as last_name,
        e.gender as gender,
        e.level as level
    FROM staging_events_data e
    WHERE user_id is not NULL
    
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, year, artist_id, duration)
    SELECT DISTINCT
        s.song_id as song_id,
        s.title as title,
        s.year as year,
        s.artist_id as artist_id,
        s.duration as duration
    FROM staging_songs_data s
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        s.artist_id as artist_id,
        s.artist_name as name,
        s.artist_location as location,
        s.artist_latitude as latitude,
        s.artist_longitude as longitude
    FROM staging_songs_data s
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
         date_add('ms',e.ts,'1970-01-01') as start_time,
         EXTRACT(hour FROM start_time)   as hour,
         EXTRACT(day FROM start_time)  as day,
         EXTRACT(week FROM start_time) as week,
         EXTRACT(month FROM start_time) as month,
         EXTRACT(year FROM start_time) as year,
         EXTRACT(weekday FROM start_time) as weekday
    FROM staging_events_data e
""")


## QUERY INSIGHT FROM ANALYSED TABLES

query_session_with_num_of_user = """
    SELECT
        t1.session_id as session_id ,
        count(*) as num_of_user
    FROM song_plays t1
    JOIN users t2 ON (t1.user_id=t2.user_id)
    GROUP BY  t1.session_id
    ORDER BY num_of_user desc
    LIMIT 10
"""

query_on_song_artist_location = """
    SELECT
        t3.title as song_title,
        t4.name as artist_name,
        t4.location as location,
        count(t2.user_id) as num_of_user
    FROM song_plays t1
    JOIN users t2 on (t1.user_id=t2.user_id)
    JOIN songs t3 on (t1.song_id=t3.song_id)
    JOIN artists t4 on (t1.artist_id=t4.artist_id)
    GROUP BY t3.title, t4.name, t4.location
    ORDER BY num_of_user desc
    LIMIT 10 
"""

query_on_song_artist_latitude = """
    SELECT
        t3.title as song_title,
        t4.name as artist_name,
        t4.latitude as latitude,
        count(t2.user_id) as num_of_user
    FROM song_plays t1
    JOIN users t2 on (t1.user_id=t2.user_id)
    JOIN songs t3 on (t1.song_id=t3.song_id)
    JOIN artists t4 on (t1.artist_id=t4.artist_id)
    GROUP BY t3.title, t4.name, t4.latitude
    ORDER BY num_of_user desc
    LIMIT 10 
"""

query_on_song_artist_location_with_female = """
    SELECT
        t3.title as song_title,
        t4.name as artist_name,
        t4.location as location,
        count(t2.user_id) as num_of_user
    FROM song_plays t1
    JOIN users t2 on (t1.user_id=t2.user_id)
    JOIN songs t3 on (t1.song_id=t3.song_id)
    JOIN artists t4 on (t1.artist_id=t4.artist_id)
    WHERE t2.gender ='F'
    GROUP BY t3.title, t4.name, t4.location
    ORDER BY num_of_user desc
    LIMIT 10 
"""

query_on_song_artist_location_with_female_paid = """
    SELECT
        t3.title as song_title,
        t4.name as artist_name,
        t4.location as location,
        count(t2.user_id) as num_of_user
    FROM song_plays t1
    JOIN users t2 on (t1.user_id=t2.user_id)
    JOIN songs t3 on (t1.song_id=t3.song_id)
    JOIN artists t4 on (t1.artist_id=t4.artist_id)
    JOIN time t5 on (t1.start_time=t5.start_time)
    WHERE t2.gender='F' AND t2.level='paid'
    GROUP BY t3.title, t4.name, t4.location
    ORDER BY num_of_user desc
    LIMIT 10 
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]