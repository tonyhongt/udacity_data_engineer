# Project: Data Lake

## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The task is to build an ETL pipeline that extract data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow analytics team to continue finding insights in what songs users are listening to.

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

### Tables

#### Fact Table
**songplays**: Records in event data associated with song plays i.e. records with page ```NextSong```  
- songplay_id 
- start_time  
- user_id  
- level  
- song_id  
- artist_id  
- session_id  
- location  
- user_agent

####Dimension Tables (Parquet)
**users**: Users in the app  
- user_id  
- first_name  
- last_name  
- gender  
- level 

**songs**: Songs in music database  
- song_id  
- title  
- artist_id  
- year  
- duration

**artists**: Artists in music database  
- artist_id  
- name  
- location  
- lattitude  
- longitude

**time**: timestamps of records in __songplays__ broken down into specific units  
- start_time  
- hour  
- day  
- week  
- month  
- year  
- weekday


### Python Scripts:
#### ETL script: etl.py
Main ETL job script, including below functions:  
**process\_song\_data()**: Song data process function  
- Fetches song_data from S3 into a staging dataframe  
- Extracts the songs and artist tables  
- Exports data back to S3

**process\_log_data()**: Lon data process function  
- Fetches log_data from S3 into a staging dataframe  
- Extracts the time, users and songplays tables  
- Exports data back to S3
