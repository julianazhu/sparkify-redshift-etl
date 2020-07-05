"""
SQL Queries for the Redshift cluster DB, following the star-schema:

#####################    FACT TABLE:     ####################
songplays   - records in log data associated with song plays

##################### DIMENSION TABLES: #####################
users       - users in the app
songs       - songs in music database
artists     - artists in music database
times       - timestamps of records in songplays broken down
              into specific units
"""

import json

CFG_FILE = 'dwh_config.json'


# CONFIG
with open(CFG_FILE) as f:
    config = json.load(f)

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS times;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events (
    artist_name	        TEXT,
    auth                VARCHAR(30),
    first_name          TEXT,
    gender              VARCHAR(2),
    item_in_session     INTEGER,
    last_name           TEXT,
    length              FLOAT4,
    level               VARCHAR(15),
    location            TEXT,
    method              VARCHAR(8),
    page                VARCHAR(20),
    registration        FLOAT8,
    session_id          INTEGER,
    song_title          TEXT,
    status              INTEGER,
    ts                  BIGINT,
    user_agent          TEXT,
    user_id             INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id           VARCHAR(30) NOT NULL,
    artist_latitude     FLOAT8,
    artist_location     TEXT,
    artist_longitude    FLOAT8,
    artist_name         TEXT,
    duration            FLOAT4,
    num_songs           INTEGER,
    song_id             TEXT NOT NULL,
    song_title          TEXT,
    year                INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id         INTEGER IDENTITY(0,1)   PRIMARY KEY   SORTKEY DISTKEY,
    start_time          BIGINT,
    user_id             INTEGER,
    level               VARCHAR(15),
    song_id             TEXT,
    artist_id           VARCHAR(30),
    session_id          INTEGER,
    location            TEXT,
    user_agent          TEXT
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id             INTEGER     NOT NULL    SORTKEY,
    first_name          TEXT,
    last_name           TEXT,
    gender              VARCHAR(2),
    level               VARCHAR(15)
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id             TEXT      NOT NULL    SORTKEY,
    title               TEXT, 
    artist_id           VARCHAR(30), 
    year                INTEGER, 
    duration            FLOAT4
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id           VARCHAR(30) NOT NULL    SORTKEY,
    name                TEXT,
    location            TEXT,
    latitude            FLOAT8,
    longitude           FLOAT8
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE times (
    start_time          BIGINT      NOT NULL    SORTKEY,
    hour                INTEGER     NOT NULL,
    day                 INTEGER     NOT NULL,
    week                INTEGER     NOT NULL,
    month               INTEGER     NOT NULL,
    year                INTEGER     NOT NULL,
    weekday             INTEGER     NOT NULL
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}' 
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON '{}'
REGION '{}'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'],
            config['AWS']['REGION'])

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION '{}'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['AWS']['REGION'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    user_id,
    start_time,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT 
    s_events.user_id,
    s_events.ts,
    s_events.level,
    s_songs.song_id,
    s_songs.artist_id,
    s_events.session_id,
    s_events.location,
    s_events.user_agent
FROM staging_songs s_songs
    JOIN staging_events s_events
        ON s_songs.song_title = s_events.song_title
           AND s_songs.artist_name = s_events.artist_name
""")

user_table_insert = ("""
INSERT INTO users(
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT DISTINCT 
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM staging_events
WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title, 
    artist_id, 
    year, 
    duration
)
SELECT DISTINCT
    song_id,
    song_title, 
    artist_id, 
    year, 
    duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude
)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
