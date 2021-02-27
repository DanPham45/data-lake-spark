import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """ Extracts data from S3 into song and artist tables, 
    stores the parsed tables as parquet files back to S3"""
    
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    #song_data = os.path.join(input_data, "song_data/A/A/A/TRAAAAK128F9318786.json")
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id', 'title', 'artist_id', 'year', 'duration'].dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode('overwrite').parquet(os.path.join(output_data, 'songs'))
    
    # extract columns to create artists table
    artists_table = df['artist_id','artist_name', 'artist_location',
                       'artist_latitude', 'artist_longitude'].dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(os.path.join(output_data, 'artists'))


def process_log_data(spark, input_data, output_data):
    """ Extracts data from S3 into users, time and songplays tables, 
    stores the parsed tables as parquet files back to S3"""
    
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*.json")
    #log_data = os.path.join(input_data, "log_data/2018/11/2018-11-12-events.json")
    
    # read log data file
    df = spark.read.json(log_data)

    
    # filter by actions for song plays
    df = df[df['page'] == 'NextSong']

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level'].dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(os.path.join(output_data, 'users'))

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # create datetime column
    df = df.withColumn("start_time", expr("cast(ts/1000.0 as timestamp)"))
    
    
    # extract columns to create time table
    df.createOrReplaceTempView("time_temp")
    time_table = spark.sql("""
        SELECT  DISTINCT start_time,
                         hour(start_time) AS hour,
                         day(start_time)  AS day,
                         weekofyear(start_time) AS week,
                         month(start_time) AS month,
                         year(start_time) AS year,
                         dayofweek(start_time) AS weekday
        FROM time_temp
    """)


    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data, 'time'))

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    #song_data = os.path.join(input_data, "song_data/A/A/A/TRAAAAK128F9318786.json")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("songs")
    
    songplays_table = spark.sql("""
        SELECT  DISTINCT sp.start_time,
            month(sp.start_time) AS month,
            year(sp.start_time) AS year,
            sp.userId as user_id,
            sp.level,
            s.song_id, 
            s.artist_id,
            sp.sessionId as session_id,
            sp.location,
            sp.userAgent as user_agent
        FROM time_temp sp 
        LEFT JOIN songs s 
        ON sp.song = s.title
        AND sp.artist = s.artist_name
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode('overwrite').parquet(os.path.join(output_data, 'songplays'))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacitydan"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
    