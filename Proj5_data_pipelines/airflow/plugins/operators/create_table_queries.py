
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



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]