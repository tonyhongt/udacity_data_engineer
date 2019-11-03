import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek
from pyspark.sql.types import StructType as R, StructField as Fld, \
DoubleType as Dbl, StringType as Str, IntegerType as Int, \
DateType as Dat, LongType as Lng, TimestampType
from datetime import datetime

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Description: This function fetches song_data from S3 into a staging dataframe, 
        then extracts the songs and artist tables,
        and eventually exports data back to S3
        
        Parameters:
            spark       : object for Spark Session
            input_data  : location of song_data 
            output_data : location of target S3 bucket
            
    """

    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # define schema
    songdata_schema = R([
    Fld("artist_id",Str()),
    Fld("artist_latitude",Dbl()),
    Fld("artist_location",Str()),
    Fld("artist_longitude",Dbl()),
    Fld("artist_name",Str()),
    Fld("duration",Dbl()),
    Fld("num_songs",Int()),
    Fld("title",Str()),
    Fld("year",Int()),
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema=songdata_schema)

    # extract columns to create songs table
    songs_table = df.select(['artist_name', 'artist_id', 'year', 'duration'])

    songs_table = songs_table.dropDuplicates().withColumn('song_id', monotonically_increasing_id()).\
    select(['song_id', 'artist_name', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs/')

    # extract columns to create artists table
    selection = ['artist_id', 'artist_name as name', \
                 'artist_location as location', 'artist_latitude as latitude', \
                 'artist_longitude as longitude']
    artists_table = df.selectExpr(selection).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
        Description: This function fetches log_data from S3 into a staging dataframe, 
        then extracts the time, users and songplays tables,
        and eventually exports data back to S3
        
        Parameters:
            spark       : object for Spark Session
            input_data  : location of log_data 
            output_data : location of target S3 bucket
            
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # define schema
    logdata_schema = R([
    Fld("artist",Str()),
    Fld("auth",Str()),
    Fld("firstName",Str()),
    Fld("gender", Str()),
    Fld("itemInSession", Lng()),
    Fld("lastName", Str()),
    Fld("length", Str()),
    Fld("level", Str()),
    Fld("location", Str()),
    Fld("method", Str()),
    Fld("page", Str()),
    Fld("registration", Dbl()),
    Fld("sessionId", Int()),
    Fld("song", Str()),
    Fld("status", Lng()),
    Fld("ts", Lng()),
    Fld("user_agent", Str()),
    Fld("userId", Str())
    ])
    
    # read log data file
    df = spark.read.json(log_data, schema=logdata_schema)
    
    # filter by actions for song plays
    df = df.filter(df1['page'] == 'NextSong')

    # extract columns for users table    
    selection = ['userId as user_id', 'firstName as first_name', \
               'lastName as last_name', 'gender as gender', \
               'level as level']

    users_table = df.selectExpr(selection).dropDuplicates()

    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp((x/1000.0)), TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select('start_time').dropDuplicates()

    time_table = time_table.\
        withColumn("hour", hour(time_table.start_time)).\
        withColumn("day", dayofmonth(time_table.start_time)).\
        withColumn("week", weekofyear(time_table.start_time)).\
        withColumn("month", month(time_table.start_time)).\
        withColumn("year", year(time_table.start_time)).\
        withColumn("weekday", dayofweek(time_table.start_time))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs/*/*/*')

    # extract columns from joined song and log datasets to create songplays table 
    selection = ['songplay_id', 'start_time', \
             'userId as user_id', 'level', 'song_id', \
             'artist_id', 'sessionId as session_id', \
             'location', 'user_agent', \
             'year', 'month']
    
    songplays_table = df1.join(songs_table, (df1.song == songs_table.title)).\
        withColumn('songplay_id', monotonically_increasing_id()).\
        withColumn("month", month(songplays_table.start_time)).\
        withColumn("year", year(songplays_table.start_time)).\
        selectExpr(selection)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://REMOVED/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
