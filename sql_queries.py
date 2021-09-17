import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id           IDENTITY(0, 1) NOT NULL,
        artist             VARCHAR,
        song               VARCHAR,
        length             NUMERIC NOT NULL,
        userId             SMALLINT NOT NULL,
        firstName          VARCHAR NOT NULL,
        lastName           VARCHAR NOT NULL,
        gender             CHAR(1),
        level              VARCHAR NOT NULL,
        location           VARCHAR,
        itemInSession      SMALLINT NOT NULL,
        sessionId          SMALLINT NOT NULL,
        ts                 TIMESTAMP NOT NULL,
        auth               VARCHAR NOT NULL,
        userAgent          VARCHAR NOT NULL,
        method             VARCHAR NOT NULL,
        page               VARCHAR NOT NULL,
        registration       NUMERIC NOT NULL,
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id            VARCHAR(18) NOT NULL,
        title              VARCHAR NOT NULL,
        duration           NUMERIC NOT NULL,
        year               SMALLINT,
        artist_id          VARCHAR(18) NOT NULL
        artist_name        VARCHAR NOT NULL,
        artist_location    VARCHAR,
        artist_latitude    NUMERIC(8, 5),
        artist_longitude   NUMERIC(8, 5),
        num_songs          BOOL
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
        songplay_id        IDENTITY(0, 1) NOT NULL,
        start_time         TIMESTAMP NOT NULL,
        user_id            SMALLINT NOT NULL,
        level              VARCHAR NOT NULL,
        song_id            VARCHAR(18) NOT NULL,
        artist_id          VARCHAR(18) NOT NULL,
        session_id         SMALLINT NOT NULL,
        location           VARCHAR,
        user_agent         VARCHAR NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user (
        user_id            SMALLINT NOT NULL,
        first_name         VARCHAR NOT NULL,
        last_name          VARCHAR NOT NULL,
        gender             CHAR(1),
        level              VARCHAR NOT NULL
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song (
        song_id            VARCHAR(18) NOT NULL,
        title              VARCHAR NOT NULL,
        artist_id          VARCHAR(18) NOT NULL,
        year               SMALLINT,
        duration           NUMERIC NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist (
        artist_id          VARCHAR(18) NOT NULL,
        name               VARCHAR NOT NULL,
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
        weekday            VARCHAR NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
