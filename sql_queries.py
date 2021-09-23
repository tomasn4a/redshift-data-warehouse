import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplay;"
user_table_drop           = 'DROP TABLE IF EXISTS "user";'
song_table_drop           = "DROP TABLE IF EXISTS song;"
artist_table_drop         = "DROP TABLE IF EXISTS artist;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id           INT IDENTITY(0, 1) PRIMARY KEY,
        artist             VARCHAR,
        auth               VARCHAR,
        firstName          VARCHAR,
        gender             CHAR(1),
        itemInSession      SMALLINT,
        lastName           VARCHAR,
        length             FLOAT,
        level              VARCHAR,
        location           VARCHAR,
        method             VARCHAR,
        page               VARCHAR,
        registration       FLOAT,
        sessionId          SMALLINT,
        song               VARCHAR,
        status             SMALLINT,
        ts                 BIGINT,
        userAgent          VARCHAR,
        userId             SMALLINT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id            VARCHAR,
        title              VARCHAR,
        duration           NUMERIC,
        year               SMALLINT,
        artist_id          VARCHAR,
        artist_name        VARCHAR,
        artist_location    VARCHAR,
        artist_latitude    NUMERIC(8, 5),
        artist_longitude   NUMERIC(8, 5),
        num_songs          BOOL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id        INT IDENTITY(0, 1) NOT NULL PRIMARY KEY,
        start_time         TIMESTAMP NOT NULL,
        user_id            SMALLINT NOT NULL,
        level              VARCHAR,
        song_id            VARCHAR NOT NULL,
        artist_id          VARCHAR NOT NULL,
        session_id         SMALLINT,
        location           VARCHAR,
        user_agent         VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS "user" (
        user_id            SMALLINT NOT NULL PRIMARY KEY,
        first_name         VARCHAR NOT NULL,
        last_name          VARCHAR NOT NULL,
        gender             CHAR(1),
        level              VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id            VARCHAR NOT NULL PRIMARY KEY,
        title              VARCHAR,
        artist_id          VARCHAR NOT NULL,
        year               SMALLINT,
        duration           FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id          VARCHAR NOT NULL,
        name               VARCHAR,
        location           VARCHAR,
        latitude           NUMERIC(8, 5),
        longitude          NUMERIC(8, 5)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time         TIMESTAMP NOT NULL,
        hour               SMALLINT NOT NULL,
        day                SMALLINT NOT NULL,
        week               SMALLINT NOT NULL,
        month              SMALLINT NOT NULL,
        year               SMALLINT NOT NULL,
        weekday            SMALLINT NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM 's3://udacity-dend/log_data'
    IAM_ROLE {}
    JSON 's3://udacity-dend/log_json_path.json';
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM 's3://udacity-dend/song_data'
    IAM_ROLE {}
    JSON 'auto'
""").format(config['IAM_ROLE']['ARN'])

# FINAL TABLES

# Insert songplay taking care of converting the BIGINT ts into a TIMESTAMP
songplay_table_insert = ("""
    INSERT INTO songplay (
        start_time,
        user_id,
        level,
        song_id ,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT 
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
        se.userId AS user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId AS session_id,
        se.location,
        se.userAgent AS user_agent
    FROM staging_events AS se
        JOIN staging_songs AS ss
            ON (se.artist=ss.artist_name) 
            AND (se.song=ss.title)
            AND (se.length=ss.duration)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO "user"
    SELECT DISTINCT 
        userId AS user_id, 
        firstName AS first_name, 
        lastName AS last_name, 
        gender, 
        level
    FROM staging_events
    WHERE user_id IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO song
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artist
    SELECT DISTINCT 
        artist_id, 
        artist_name,
        artist_location, 
        artist_latitude, 
        artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time 
    SELECT DISTINCT
        start_time,
        EXTRACT(HOUR FROM start_time),
        EXTRACT(DAY FROM start_time),
        EXTRACT(WEEK FROM start_time),
        EXTRACT(MONTH FROM start_time),
        EXTRACT(YEAR FROM start_time),
        EXTRACT(DAYOFWEEK FROM start_time)
    FROM
""")

# QUERY LISTS

create_table_queries = [
    ('staging_events', staging_events_table_create), 
    ('staging_songs', staging_songs_table_create), 
    ('songplay', songplay_table_create), 
    ('user', user_table_create), 
    ('song', song_table_create), 
    ('artist', artist_table_create), 
    ('time', time_table_create)
]
drop_table_queries = [
    ('staging_events', staging_events_table_drop), 
    ('staging_songs', staging_songs_table_drop), 
    ('songplay', songplay_table_drop), 
    ('user', user_table_drop), 
    ('song', song_table_drop), 
    ('artist', artist_table_drop), 
    ('time', time_table_drop)
]
copy_table_queries = [
    ('staging_events', staging_events_copy), 
    ('staging_songs', staging_songs_copy)
]
insert_table_queries = [
    ('songplay', songplay_table_insert), 
    ('user', user_table_insert), 
    ('song', song_table_insert), 
    ('artist', artist_table_insert), 
    ('time', time_table_insert)
]
