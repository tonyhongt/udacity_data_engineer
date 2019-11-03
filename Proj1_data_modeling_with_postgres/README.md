# Project: Data Modeling with Postgres

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to create a Postgres database with tables designed to optimize queries on song play analysis, and build ETL pipeline for this analysis. 

### Raw Data
- **Song datasets**: all json files are located in subdirectories under */data/song_data*. The files are partitioned by the first three letters of each song's track ID. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are located in subdirectories under */data/log_data*, partitioned by year and month. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

## Project Content

### Database
Relational database and Star schema is chosen to store the data. Below are the reason:

- **Defined structure**: As has been shown in the raw data, the input data is structured with predetermined column schema
- **Amount of data**: The amount of data is small enough to use relational database
- **Analytical use case**: Relational database allows flexible queries for different analytical need
- **Ability of using JOIN**: Relational database allows user to apply JOIN between data

#### Tables
**songplays**: Fact table
- songplay\_id INT PRIMARY KEY: User song play identifier  
- start\_time TIMESTAMP NOT NULL: Song play start time  
- user\_id INT NOT NULL: User identifier  
- level VARCHAR(4): User level {free | paid}  
- song\_id VARCHAR(18) NOT NULL: Song identifier  
- artist\_id VARCHAR(18) NOT NULL: Artist identifier  
- session\_id INT: User session identifier  
- location TEXT: User localtion   
- user\_agent TEXT: User access agent  

**users**: Demension table
- user\_id INT PRIMARY KEY: User identifier  
- first\_name TEXT: User first name  
- last\_name TEXT: User last name  
- gender VARCHAR(1): User gender {M | F}  
- level VARCHAR(4): User level  

**songs**: Demension table
- song\_id VARCHAR(18) PRIMARY KEY: Song identifier  
- title TEXT: Song name  
- artist_id VARCHAR(18) NOT NULL: Artist identifier  
- year INT: Song released year  
- duration FLOAT: Song duration  

**artists**: Demension table
- artist\_id VARCHAR(18) PRIMARY KEY: Artist identifier  
- name TEXT NOT NULL: Artist name  
- location TEXT: Artist location  
- latitude FLOAT: Artist location latitude   
- longitude FLOAT: Artist location longitude  

**time**: Demension table
- start\_time TIMESTAMP PRIMARY KEY: Song play start time  
- hour INT: Song play start hour  
- day INT: Song play start day  
- week INT: Song play start week of year  
- month INT: Song play start month of year  
- year INT: Song play start year  
- weekday VARCHAR(12): Song play start day of week  


### Python Scripts:
#### ETL script: etl.py
Main ETL job script, including below functions:  
**process\_song\_file()**: Song data process function
- Open song file  
- Insert song and artist data into respective tables  

**process\_log\_file()**: Long data process function
- Open log file  
- Insert time, user and song play data into respective tables  

**process\_data()**:
- Iterate through subdirectories and call respective process function

#### DB script: create_tables.py
Python script to create/reset Database environment:
**create\_database()**: 
- Establish server connection  
- Create sparkifydb Database  

**drop\_tables()**: 
- Drop existing tables  

**create\_tables()**:
- Create new tables

#### Query script: sql_queries.py


