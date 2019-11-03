import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

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
CREATE TABLE IF NOT EXISTS staging_events
(
artist VARCHAR,
auth TEXT,
firstName TEXT,
gender VARCHAR(1),
itemInSession INT,
lastName TEXT,
length FLOAT,
level VARCHAR(4),
location TEXT,
method VARCHAR(3),
page VARCHAR,
registration FLOAT,
sessionId INT,
song VARCHAR,
status int,
ts TIMESTAMP,
user_agent TEXT,
userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
num_songs INT,
artist_id VARCHAR(18),
artist_latitude FLOAT,
artist_longitude FLOAT,
artist_location VARCHAR(MAX),
artist_name VARCHAR,
song_id VARCHAR(18),
title VARCHAR,
duration FLOAT,
year INT)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
songplay_id INT IDENTITY(0, 1) PRIMARY KEY, 
start_time TIMESTAMP SORTKEY DISTKEY NOT NULL, 
user_id INT NOT NULL, 
level VARCHAR(4), 
song_id VARCHAR(18) NOT NULL , 
artist_id VARCHAR(18) NOT NULL , 
session_id INT, 
location TEXT, 
user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id INT SORTKEY PRIMARY KEY,
first_name TEXT, 
last_name TEXT, 
gender VARCHAR(1), 
level VARCHAR(4)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id VARCHAR(18) SORTKEY PRIMARY KEY, 
title TEXT, 
artist_id VARCHAR(18) NOT NULL, 
year INT, 
duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR(18) SORTKEY PRIMARY KEY, 
name TEXT NOT NULL, 
location TEXT, 
latitude FLOAT, 
longitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP SORTKEY DISTKEY PRIMARY KEY, 
hour INT, 
day INT, 
week INT, 
month INT, 
year INT, 
weekday VARCHAR(12)
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON {}
timeformat as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2'
format as JSON 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  e.ts  AS start_time, 
        e.userId        AS user_id, 
        e.level         AS level, 
        s.song_id       AS song_id, 
        s.artist_id     AS artist_id, 
        e.sessionId     AS session_id, 
        e.location      AS location, 
        e.user_agent     AS user_agent
FROM staging_events e
JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
AND e.page  =  'NextSong';
""")

user_table_insert = ("""
DELETE users a FROM staging_events b WHERE a.user_id = b.userId AND a.level != b.level;

INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT  DISTINCT(userId)    AS user_id,
        firstName           AS first_name,
        lastName            AS last_name,
        gender,
        level
FROM staging_events
WHERE user_id IS NOT NULL
AND page  =  'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT  DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT  DISTINCT(artist_id) AS artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL;
""")


time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT(start_time)                AS start_time,
        EXTRACT(hour FROM start_time)       AS hour,
        EXTRACT(day FROM start_time)        AS day,
        EXTRACT(week FROM start_time)       AS week,
        EXTRACT(month FROM start_time)      AS month,
        EXTRACT(year FROM start_time)       AS year,
        EXTRACT(dayofweek FROM start_time)  as weekday
FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
