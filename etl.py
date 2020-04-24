import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour,  dayofweek, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Descriptil: Create sparksession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):

    """
    Description: User spark to perform ETL operations on the song data, 
    extract fields for the songs and artists tables then save to parquet file on s3

    parameters:
    - spark: spark session
    - input_data: path to input data (from s3)
    - output_data : path to write output parquet data (to s3)
    """

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"
    
    # define schema for song data
    song_schema = StructType([
        StructField('num_songs', IntegerType()),
        StructField('artist_id', StringType()),
        StructField('artist_latitude', DoubleType()),
        StructField('artist_longitude', DoubleType()),
        StructField('artist_location', StringType()),
        StructField('artist_name', StringType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('duration', DoubleType()),
        StructField('year', IntegerType())
    ])

    # read song data file
    df = spark.read.schema(song_schema).json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data,'parquet/songs'), 'overwrite')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as lattitude','artist_longitude as longitude').dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'parquet/artists'), 'overwrite')


def process_log_data(spark, input_data, output_data):

    """
    Description: User spark to perform ETL operations on the log data, 
    extract fields for the users, time and songplays tables and then save to parquet file on s3

    parameters:
    - spark: spark session
    - input_data: path to input data (from s3)
    - output_data : path to write output parquet data (to s3)
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"
    
    # define schema for logs data
    song_schema = StructType([
        StructField('artist', StringType()),
        StructField('auth', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('itemInSession', IntegerType()),
        StructField('lastName', StringType()),
        StructField('length', DoubleType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('method', StringType()),
        StructField('page', StringType()),
        StructField('registration', DoubleType()),
        StructField('sessionId', IntegerType()),
        StructField('song', StringType()),
        StructField('status', IntegerType()),
        StructField('ts', LongType()),
        StructField('userAgent', StringType()),
        StructField('userId', StringType())
    ])
        

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df =  df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table =  df.selectExpr('userId as user_id','firstName as first_name','lastName as last_name', 'gender', 'level').dropDuplicates()

    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'parquet/users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(int(x)/1000))
    df = df.withColumn('timestamp', get_timestamp('ts'), )
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000)))
    df = df.withColumn('datetime', get_datetime('ts'), )
    
    # extract columns to create time table
    time_table = df.selectExpr("datetime as start_time")\
                .withColumn('hour', hour('start_time'))\
                .withColumn('day', dayofmonth('start_time'))\
                .withColumn('week', weekofyear('start_time'))\
                .withColumn('month', month('start_time'))\
                .withColumn('year', year('start_time'))\
                .withColumn('weekday', dayofweek('start_time')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'parquet/time'), 'overwrite')

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    song_df.createOrReplaceTempView("songs_table")
    df.createOrReplaceTempView('logs_table')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_table = spark.sql('''
        SELECT DISTINCT
            l.datetime AS start_time,
            l.userId AS user_id,
            l.level AS level,
            s.song_id AS song_id,
            s.artist_id AS artist_id,
            l.sessionId  AS session_id,
            l.location AS location,
            l.userAgent AS user_agent,
            EXTRACT(month FROM l.datetime)  AS month,
            EXTRACT(year FROM l.datetime) AS year
        FROM  logs_table AS l
        JOIN  songs_table AS s ON s.title=l.song AND s.artist_name=l.artist 
        ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table =  songplays_table.withColumn('songplay_id', monotonically_increasing_id())
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data,'parquet/songplays'), 'overwrite')

def main():
    """
    Description: Entery function, create spark session and pass along side input_data, output_data to 
    process_song_data and process_log_data functions to perform ETL
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://au-sparkify-datalake/"
    
#     input_data = "data/"
#     output_data = "data/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
