import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
          artist VARCHAR
        , auth VARCHAR
        , first_name VARCHAR
        , gender VARCHAR
        , item_in_session INT
        , last_name VARCHAR
        , length FLOAT
        , level VARCHAR
        , location VARCHAR
        , method VARCHAR
        , page VARCHAR
        , registration BIGINT
        , session_id INT
        , song VARCHAR
        , status INT
        , ts BIGINT
        , user_agent VARCHAR
        , user_id INT
    )
""")


staging_songs_table_create = ("""
 CREATE TABLE staging_songs (
          artist_id VARCHAR
        , artist_location VARCHAR
        , artist_latitude FLOAT
        , artist_longitude FLOAT
        , artist_name VARCHAR
        , duration FLOAT
        , num_songs INT
        , song_id VARCHAR
        , title VARCHAR
        , year INT
    );
""")


songplay_table_create = ("""
    CREATE TABLE songplays (
          songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY
        , start_time TIMESTAMP NOT NULL
        , user_id INT NOT NULL
        , level VARCHAR NOT NULL
        , song_id VARCHAR NOT NULL
        , artist_id VARCHAR NOT NULL
        , session_id INT NOT NULL
        , location VARCHAR
        , user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE users (
      user_id INT NOT NULL PRIMARY KEY
    , first_name VARCHAR
    , last_name VARCHAR
    , gender VARCHAR
    , level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE songs (
      song_id VARCHAR NOT NULL PRIMARY KEY
    , title VARCHAR NOT NULL
    , artist_id VARCHAR NOT NULL
    , year INT 
    , duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE artists (
          artist_id VARCHAR NOT NULL PRIMARY KEY
        , name VARCHAR NOT NULL
        , location VARCHAR
        , latitude FLOAT
        , longitude FLOAT
    )

""")

time_table_create = ("""
    CREATE TABLE time (
          start_time TIMESTAMP NOT NULL PRIMARY KEY
        , hour INT NOT NULL
        , day INT NOT NULL
        , week INT NOT NULL
        , month INT NOT NULL
        , year INT NOT NULL
        , weekday INT NOT NULL
    )        
""")






# STAGING TABLES

staging_events_copy = (
f"""
    COPY staging_events
    FROM {config.get('S3','log_data')}
    credentials 'aws_iam_role={config.get('IAM_ROLE','arn')}'
    JSON {config.get('S3', 'log_jsonpath')};
"""
)


staging_songs_copy = (
f"""
    COPY staging_songs
    FROM {config.get('S3','song_data')}
    credentials 'aws_iam_role={config.get('IAM_ROLE','arn')}'
    FORMAT AS JSON 'auto';
"""
)



# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select distinct
          timestamp 'epoch' + (se.ts / 1000) * INTERVAL '1 Second' as start_time
        , se.user_id
        , se.level
        , ss.song_id
        , ss.artist_id
        , se.session_id
        , se.location
        , se.user_agent
    from staging_events se
    join staging_songs ss on se.artist = ss.artist_name and se.song = ss.title
    where se.page = 'NextSong'
    
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) 
    select distinct
          se.user_id
        , se.first_name
        , se.last_name
        , se.gender
        , se.level
    from staging_events se
    where se.user_id is not null
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
    select distinct
          ss.song_id
        , ss.title
        , ss.artist_id
        , ss.year
        , ss.duration
    from staging_songs ss
    where ss.song_id is not null
""")


artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
    select distinct
          ss.artist_id
        , ss.artist_name
        , ss.artist_location
        , ss.artist_latitude
        , ss.artist_longitude
    from staging_songs ss
    where ss.artist_id is not null
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    select distinct 
          timestamp 'epoch' + (se.ts / 1000) * interval '1 second ' as ts_timestamp
        , extract(hour from ts_timestamp) as hour
        , extract(day from ts_timestamp) as day
        , extract(week from ts_timestamp) as week
        , extract(month from ts_timestamp) as month
        , extract(year from ts_timestamp) as year
        , extract(dow from ts_timestamp) as weekday
    from staging_events se
""")

# QUERY LISTS

create_table_queries = [
      staging_events_table_create
    , staging_songs_table_create
    , songplay_table_create
    , user_table_create
    , song_table_create
    , artist_table_create
    , time_table_create
]

drop_table_queries = [
      staging_events_table_drop
    , staging_songs_table_drop
    , songplay_table_drop
    , user_table_drop
    , song_table_drop
    , artist_table_drop
    , time_table_drop
]

copy_table_queries = [
      staging_events_copy
    , staging_songs_copy
]

insert_table_queries = [
      songplay_table_insert
    , user_table_insert
    , song_table_insert
    , artist_table_insert
    , time_table_insert
]
