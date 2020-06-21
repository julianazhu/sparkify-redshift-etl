import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh_config.json')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS part staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS part staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS part songplays;"
user_table_drop = "DROP TABLE IF EXISTS part users;"
song_table_drop = "DROP TABLE IF EXISTS part songs;"
artist_table_drop = "DROP TABLE IF EXISTS part artists;"
time_table_drop = "DROP TABLE IF EXISTS part times;"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE staging_events (
    user_id             integer not null,
    first_name          string,
    last_name           string,
    level               varchar(15),
    gender              varchar(2),
    auth                varchar(30),
    location            string,
    page                varchar(20),
    method              varchar(8),
    status              integer,
    user_agent          string,
    registration        float8,
    session_id          integer,
    item_in_session     integer,
    artist     	        string,
    song                string,
    length              float4,
    ts                  bigint not null
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs           integer,
    artist_id           integer not null,
    artist_latitude     float8,
    artist_longitude    float8,
    artist_location     string,
    artist_name         string,
    song_id             string not null,
    title               string,
    duration            float4,
    year                integer
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id         string      not null    sortkey distkey,
    start_time          bigint,
    user_id             integer,
    level               varchar(15),
    song_id             string,
    artist_id           integer,
    session_id          integer,
    location            string,
    user_agent          string
);
""")

user_table_create = ("""
CREATE TABLE songplays (
    user_id             integer     not null    sortkey,
    first_name          string,
    last_name           string,
    gender              varchar(2),
    level               varchar(15)
)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id             string      not null    sortkey,
    title               string, 
    artist_id           integer, 
    year                integer, 
    duration            float4,
)
diststyle all;
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id           integer     not null    sortkey,
    name                string,
    location            string,
    latitude            float8,
    longitude           float8
)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE times (
    start_time          bigint      not null    sortkey,
    hour                integer,
    day                 integer,
    week                integer,
    month               integer,
    year                integer,
    weekday             integer
)
diststyle all;
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
