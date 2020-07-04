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
    artist     	        text,
    auth                varchar(30),
    first_name          text,
    gender              varchar(2),
    item_in_session     integer,
    last_name           text,
    length              float4,
    level               varchar(15),
    location            text,
    method              varchar(8),
    page                varchar(20),
    registration        float8,
    session_id          integer,
    song                text,
    status              integer,
    ts                  bigint,
    user_agent          text,
    user_id             integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    artist_id           varchar(30) not null,
    artist_latitude     float8,
    artist_location     text,
    artist_longitude    float8,
    artist_name         text,
    duration            float4,
    num_songs           integer,
    song_id             text not null,
    title               text,
    year                integer
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id         text      not null    sortkey distkey,
    start_time          bigint,
    user_id             integer,
    level               varchar(15),
    song_id             text,
    artist_id           integer,
    session_id          integer,
    location            text,
    user_agent          text
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id             integer     not null    sortkey,
    first_name          text,
    last_name           text,
    gender              varchar(2),
    level               varchar(15)
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id             text      not null    sortkey,
    title               text, 
    artist_id           integer, 
    year                integer, 
    duration            float4
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id           varchar(30) not null    sortkey,
    name                text,
    location            text,
    latitude            float8,
    longitude           float8
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE times (
    start_time          bigint      not null    sortkey,
    hour                integer     not null,
    day                 integer     not null,
    week                integer     not null,
    month               integer     not null,
    year                integer     not null,
    weekday             integer     not null
)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events from '{}' 
credentials 'aws_iam_role={}'
format as json '{}'
region '{}'
truncatecolumns blanksasnull emptyasnull;
""").format(config['S3']['LOG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'],
            config['AWS']['REGION'])

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
credentials 'aws_iam_role={}'
format as json 'auto'
region '{}'
truncatecolumns blanksasnull emptyasnull;
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'],
            config['AWS']['REGION'])

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
