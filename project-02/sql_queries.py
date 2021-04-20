import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')

# VARIABLE ASSIGNMENT

S3_LOG = config.get('S3','LOG_DATA')
S3_LOG_PATH = config.get('S3','LOG_JSONPATH')
S3_SONGS = config.get('S3','SONG_DATA')
ROLE_ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
        artist VARCHAR NULL,
        auth VARCHAR NOT NULL,
        firstName VARCHAR NULL,
        gender VARCHAR NULL,
        itemInSession INT NOT NULL,
        lastName VARCHAR NULL,
        length FLOAT NULL,
        level VARCHAR NOT NULL,
        location VARCHAR NULL,
        method VARCHAR NOT NULL,
        page VARCHAR NOT NULL,
        registration FLOAT NULL ,
        sessionId INT NOT NULL,
        song VARCHAR NULL ,
        status INT NOT NULL,
        ts BIGINT NOT NULL,
        userAgent VARCHAR NULL,
        userId VARCHAR NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id VARCHAR NOT NULL,
    artist_latitude DECIMAL(9,6) NULL,
    artist_location VARCHAR NULL,
    artist_longitude DECIMAL(9,6) NULL,
    artist_name VARCHAR NOT NULL,
    duration FLOAT NOT NULL,
    num_songs INT NOT NULL,
    song_id VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    year INT NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id INT PRIMARY KEY IDENTITY(0,1),
    start_time TIMESTAMP NOT NULL, 
    user_id VARCHAR, 
    level VARCHAR NOT NULL,
    song_id VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    session_id INT NOT NULL, 
    location VARCHAR NOT NULL, 
    user_agent VARCHAR NOT NULL
)
""")

user_table_create = ("""
CREATE TABLE users (
    user_id VARCHAR PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender VARCHAR NOT NULL, 
    level VARCHAR NOT NULL
)
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id VARCHAR PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year INT NOT NULL, 
    duration FLOAT NOT NULL
)
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR NOT NULL, 
    location VARCHAR NULL, 
    latitude DECIMAL(9,6) NULL,
    longitude DECIMAL(9,6) NULL
)
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP NOT NULL, 
    hour INT NOT NULL, 
    day INT NOT NULL, 
    week INT NOT NULL, 
    month INT NOT NULL, 
    year INT NOT NULL, 
    weekday INT NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json {};
""").format(S3_LOG, ROLE_ARN, S3_LOG_PATH)
                       
staging_songs_copy = ("""
    copy staging_songs
    from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    json 'auto';
""").format(S3_SONGS, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
        e.userid,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events e
    JOIN staging_songs s
        ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration
    WHERE e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT 
        userid, 
        firstname, 
        lastname, 
        gender, 
        level
    FROM staging_events
    WHERE page = 'NextSong'
    and userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT
        start_time,
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time),
        extract(month from start_time),
        extract(year from start_time),
        extract(dayofweek from start_time)
    FROM songplay
    WHERE start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
