import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')
LOG_JSONPATH = config.get('S3','LOG_JSONPATH')
LOG_DATA = config.get('S3','LOG_DATA')
SONG_DATA = config.get('S3','SONG_DATA')


# DROP SCHEMAS
fact_schema_drop= ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop= ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop= ("DROP SCHEMA IF EXISTS staging_tables CASCADE")


# CREATE SCHEMAS
fact_schema= ("CREATE SCHEMA IF NOT EXISTS fact_tables")
dimension_schema= ("CREATE SCHEMA IF NOT EXISTS dimension_tables")
staging_schema= ("CREATE SCHEMA IF NOT EXISTS staging_tables")


# DROP TABLES
staging_events_table_drop = "DROP table if exists staging_tables.staging_events"
staging_songs_table_drop = "DROP table if exists staging_tables.staging_songs"
songplay_table_drop = "DROP table if exists fact_tables.songplays"
user_table_drop = "DROP table if exists dimension_tables.users"
song_table_drop = "DROP table if exists dimension_tables.songs"
artist_table_drop = "DROP table if exists dimension_tables.artists"
time_table_drop = "DROP table if exists dimension_tables.time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_tables.staging_events
(
   artist varchar ,
   auth varchar ,
   firstName varchar ,
   gender CHAR ,
   itemInSession SMALLINT,
   lastName varchar,
   length double precision,
   level varchar  ,
   location varchar ,
   method varchar,
   page varchar,
   registration REAL,
   sessionId INTEGER ,
   song varchar,
   status SMALLINT ,
   ts BIGINT ,
   user_agent varchar ,
   user_id INTEGER 
   )
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_tables.staging_songs
(
num_songs smallint,
artist_id varchar,
artist_latitude real , 
artist_longitude real,
artist_location varchar,
artist_name varchar ,
song_id varchar,
title varchar(1000),
duration double precision ,
year smallint 
) DISTSTYLE EVEN
""")


user_table_create = ("""
CREATE TABLE    if not exists dimension_tables.users (
user_id INTEGER , 
first_name varchar NOT NULL, 
last_name varchar, 
gender char, 
level varchar NOT NULL,
PRIMARY KEY(user_id)
) DISTSTYLE ALL
""")

song_table_create = ("""
CREATE TABLE   if not exists dimension_tables.songs (
song_id varchar , 
song_title varchar NOT NULL, 
artist_id varchar NOT NULL, 
year INTEGER,
duration double precision ,
PRIMARY KEY ( song_id)
) DISTSTYLE ALL
""")

artist_table_create = ("""
CREATE TABLE    if not exists dimension_tables.artists(
artist_id varchar , 
name varchar NOT NULL, 
location varchar, 
latitude real , 
longitude real,
PRIMARY KEY(artist_id)
) DISTSTYLE ALL
""")


time_table_create = ("""
CREATE TABLE   if not exists dimension_tables.time (
start_time bigint NOT NULL, 
hour int NOT NULL, 
day int NOT NULL, 
week int NOT NULL, 
month int NOT NULL, 
year int NOT NULL, 
weekday varchar NOT NULL,
PRIMARY KEY   ( start_time )
) DISTSTYLE ALL
""")

songplay_table_create = ("""
CREATE TABLE if not exists fact_tables.songplays (
songplay_id INTEGER IDENTITY(1,1) ,  
start_time bigint REFERENCES dimension_tables.time( start_time  ), 
user_id int REFERENCES dimension_tables.users(user_id), 
level varchar, 
song_id varchar  REFERENCES dimension_tables.songs(song_id), 
artist_id varchar REFERENCES dimension_tables.artists(artist_id), 
session_id int, 
location varchar, 
user_agent varchar,
PRIMARY KEY (songplay_id )) DISTSTYLE KEY DISTKEY(artist_id)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_tables.staging_events from {} 
credentials 'aws_iam_role={}'
format as json {}
region 'us-west-2';
""".format(LOG_DATA,DWH_ROLE_ARN ,LOG_JSONPATH ) )


staging_songs_copy = ("""
copy staging_tables.staging_songs from {} 
credentials 'aws_iam_role={}'
format as json 'auto'
 region 'us-west-2';
""".format( SONG_DATA , DWH_ROLE_ARN  ))


# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO fact_tables.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts , e.user_id ,  e.level , s.song_id , s.artist_id , e.sessionid ,e.location ,e.user_agent 
FROM (SELECT ts , user_id , level, song   , sessionid ,location ,user_agent   
        FROM staging_tables.staging_events 
       WHERE page = 'NextSong' ) e 
 LEFT JOIN  dimension_tables.songs s ON ( s.song_title = e.song  )
""")

user_table_insert = ("""
INSERT INTO dimension_tables.users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT s.user_id , firstname , lastname , gender , level 
  FROM staging_tables.staging_events s, 
      ( SELECT  user_id, 
                MAX(ts) OVER (PARTITION BY  user_id  )  latest_rec
          FROM staging_tables.staging_events    WHERE page  = 'NextSong' 
      )  l
 WHERE l.user_id = s.user_id 
   AND l.latest_rec = s.ts
""")

song_table_insert = ("""
INSERT INTO dimension_tables.songs (song_id, song_title, artist_id, year, duration) 
select song_id ,title,artist_id ,year , duration from staging_tables.staging_songs
""")

artist_table_insert = ("""
 INSERT INTO dimension_tables.artists (artist_id, name, location, latitude, longitude) 
 select distinct artist_id , artist_name , artist_location   , artist_latitude  , artist_longitude 
   from  ( select  artist_id , artist_name , artist_location   , artist_latitude  , artist_longitude  , 
                   rank () over ( partition by artist_id order by artist_id  ,artist_location ) rk 
             from staging_tables.staging_songs 
         ) where rk = 1 
""")

time_table_insert = ("""
INSERT INTO dimension_tables.time (start_time, hour, day, week, month, year, weekday)
select ts,  hour, day , week, month, year, weekday  from ( 
select   ts, 
        EXTRACT(hr from date_time) AS hour,
        EXTRACT(d from date_time) AS day,
        EXTRACT(w from date_time) AS week,
        EXTRACT(mon from date_time) AS month,
        EXTRACT(yr from date_time) AS year, 
        EXTRACT(weekday from date_time) AS weekday 
        from ( 
select   DISTINCT ts , ( timestamp 'epoch' + ts /1000 * interval '1 second')  AS date_time   
from staging_tables.staging_events    ) )
""")

# QUERY LISTS
drop_schemas_queries = [fact_schema_drop, dimension_schema_drop, staging_schema_drop]
create_schemas_queries = [fact_schema, dimension_schema, staging_schema]
create_table_queries = [staging_events_table_create, staging_songs_table_create,  time_table_create,user_table_create,song_table_create, artist_table_create ,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,time_table_insert]
