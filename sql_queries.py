import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table if exists staging_events"
staging_songs_table_drop = "DROP table if exists staging_songs"
songplay_table_drop = "DROP table if exists songplay"
user_table_drop = "DROP table if exists user"
song_table_drop = "DROP table if exists song"
artist_table_drop = "DROP table if exists artist"
time_table_drop = "DROP table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
   artist varchar,
   auth varchar,
   firstName varchar,
   gender CHAR,
   itemInSession SMALLINT,
   lastName varchar,
   length REAL,
   level varchar,
   location varchar,
   method varchar,
   page varchar,
   registration REAL,
   sessionId INTEGER,
   song varchar,
   status smallint,
   ts BIGINT,
   userAgent varchar,
   userId INT
   )

""")

staging_songs_table_create = ("""
CREAT TABLE IF NOT EXISTS staging_songs
(
num_songs smallint,
artist_id varchar,
artist_latitude real , 
artist_longitude real,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration real,
year smallint
)
""")

songplay_table_create = ("""
CREATE TABLE if not exists songplays (
songplay_id INTEGER IDENTITY(1,1) ,  
start_time bigint REFERENCES time(start_time), 
user_id int REFERENCES users(user_id), 
level varchar, 
song_id varchar  REFERENCES songs(song_id), 
artist_id varchar REFERENCES artists(artist_id), 
session_id int, 
location varchar, 
user_agent varchar,
PRIMARY KEY (songplay_id ))
""")

user_table_create = ("""
CREATE TABLE    if not exists USERS (
user_id INTEGER , 
first_name varchar NOT NULL, 
last_name varchar, 
gender char, 
level varchar,
PRIMARY KEY(user_id)
)
""")

song_table_create = ("""
CREATE TABLE   if not exists songs (
song_id varchar , 
song_title varchar NOT NULL, 
artist_id varchar NOT NULL, 
year INTEGER, duration real,
PRIMARY KEY ( song_id)
)
""")

artist_table_create = ("""
CREATE TABLE    if not exists artists(
artist_id varchar , 
name varchar NOT NULL, 
location varchar, 
latitude real , 
longitude real,
PRIMARY KEY(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE   if not exists time(
start_time bigint NOT NULL, 
hour int, 
day int, 
week int, 
month int, 
year int, 
weekday varchar ,
PRIMARY KEY ( start_time)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from 's3://udacity-dend/log_data' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';
""")

staging_songs_copy = ("""
copy staging_events from 's3://udacity-dend/song_data' 
credentials 'aws_iam_role=<DWH_ROLE_ARN>'
gzip region 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
INSERT INTO USER
( SELECT 
)
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
