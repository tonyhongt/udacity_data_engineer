# Project: Data Warehouse

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

### Raw Data
- **Song datasets**: all json files are located in *s3://udacity-dend/song_data*. The files are partitioned by the first three letters of each song's track ID. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are located in *s3://udacity-dend/log_data*, and json path file located in *s3://udacity-dend/log_json_path.json*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

## Project Content

### Database
Relational database and Star schema is chosen to store the data. Below are the reason:

- **Defined structure**: As has been shown in the raw data, the input data is structured with predetermined column schema
- **Analytical use case**: Relational database allows flexible queries for different analytical need
- **Ability of using JOIN**: Relational database allows user to apply JOIN between data

#### Tables

##### Staging Tables
**staging_events**: Staging table to host event log data  
- artist VARCHAR  
- auth TEXT  
- firstName TEXT  
- gender VARCHAR(1)  
- itemInSession INT  
- lastName TEXT  
- length FLOAT  
- level VARCHAR(4)  
- location TEXT  
- method VARCHAR(3)  
- page VARCHAR  
- registration FLOAT  
- sessionId INT  
- song VARCHAR  
- status INT  
- ts TIMESTAMP  
- user_agent TEXT  
- userId INT  

**staging_songs**: Staging table to host song data  
- num_songs INT  
- artist_id VARCHAR(18)  
- artist_latitude FLOAT  
- artist_longitude FLOAT  
- artist_location VARCHAR(MAX)  
- artist_name VARCHAR  
- song_id VARCHAR(18)  
- title VARCHAR  
- duration FLOAT  
- year INT  


##### End Tables
**songplays**: Fact table  
- songplay\_id INT IDENTITY(0, 1) PRIMARY KEY: User song play identifier  
- start\_time TIMESTAMP SORTKEY DISTKEY NOT NULL: Song play start time, sort and distribute key to optimize performance  
- user\_id INT NOT NULL: User identifier  
- level VARCHAR(4): User level {free | paid}  
- song\_id VARCHAR(18) NOT NULL: Song identifier  
- artist\_id VARCHAR(18) NOT NULL: Artist identifier  
- session\_id INT: User session identifier  
- location TEXT: User localtion   
- user\_agent TEXT: User access agent  

**users**: Demension table  
- user\_id INT SORTKEY PRIMARY KEY: User identifier, sort to optimize join, no need to distribute since this is a small table  
- first\_name TEXT: User first name  
- last\_name TEXT: User last name  
- gender VARCHAR(1): User gender {M | F}  
- level VARCHAR(4): User level  

**songs**: Demension table  
- song\_id VARCHAR(18) SORTKEY PRIMARY KEY: Song identifier, sort to optimize join, no need to distribute since this is a small table  
- title TEXT: Song name  
- artist_id VARCHAR(18) NOT NULL: Artist identifier  
- year INT: Song released year  
- duration FLOAT: Song duration  

**artists**: Demension table  
- artist\_id VARCHAR(18) SORTKEY PRIMARY KEY: Artist identifier, sort to optimize join, no need to distribute since this is a small table  
- name TEXT NOT NULL: Artist name  
- location TEXT: Artist location  
- latitude FLOAT: Artist location latitude   
- longitude FLOAT: Artist location longitude  

**time**: Demension table  
- start\_time TIMESTAMP SORTYKEY DISTKEY PRIMARY KEY: Song play start time, sort and distribute key to optimize performance   
- hour INT: Song play start hour  
- day INT: Song play start day  
- week INT: Song play start week of year  
- month INT: Song play start month of year  
- year INT: Song play start year  
- weekday VARCHAR(12): Song play start day of week  


### Python Scripts:
#### ETL script: etl.py
Main ETL job script, including below functions:  
**load\_staging\_tables()**: Song data process function
- Interate through different sets of files to be loaded from S3  
- Load song and log data into respective staging tables  

**insert\_tables()**: Long data process function
- Iterate through all fact and dimention tables   
- Insert end table from staging table  


#### DB script: create_tables.py
Python script to create/reset Database environment:

**drop\_tables()**: 
- Drop existing tables  

**create\_tables()**:
- Create new tables

#### Query script: sql_queries.py


