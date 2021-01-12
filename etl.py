import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Set up and configures an Apache Spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load the json source files containing the song data from an AWS S3 bucket and extracts the columns for the song and artist tables.
    Saves the resulting dimensional tables as parquet files into the destination AWS S3 bucket.
    
    Parameters:
    -----------    
    spark: session, initialized and configured spark session.
    input_data: path, path to the aws s3 bucket.
    output_data: path, path for the output parquet files.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    
    # read song data file
    df_song = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table=df_song.select("song_id","title","artist_id","year","duration")
    songs_table = songs_table.dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs.parquet'), 'overwrite')

    # extract columns to create artists table
    artists_table=df_song.selectExpr("artist_id","artist_name as name","artist_location as location","artist_latitude as lattitude","artist_longitude as longitude")
    artists_table = artists_table.dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists.parquet'), 'overwrite')
    
    print("song data processing completed")

def process_log_data(spark, input_data, output_data):
    """
    Load the json source files containing the log data from an AWS S3 bucket and extracts the columns for the users and time tables.
    Read the song data and join the two source tables to create and extract the columns for the songplay fact table.
    Saves the resulting fact and dimensional tables as parquet files into the destination AWS S3 bucket.
    
    Parameters:
    -----------
    spark: session, initialized and configured spark session.
    input_data: path, path to the aws s3 bucket.
    output_data: path, path for the output parquet files.
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_filtered = df_log.filter("page=='NextSong'")

    # extract columns for users table    
    users_table=df_filtered.selectExpr("userId as user_id","firstName as first_name","lastName as last_name","gender","level")
    users_table = users_table.dropDuplicates(['user_id'])
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users.parquet'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df_filtered = df_filtered.withColumn('timestamp', get_timestamp(df_filtered.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df_filtered = df_filtered.withColumn("datetime", get_datetime(df_filtered.ts))
    
    # extract columns to create time table
    time_table = df_filtered.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year'))
    
    time_table = time_table.dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time.parquet'), 'overwrite')

    # read in song data to use for songplays table
    song_data = os.path.join(input_data, "song-data/A/A/A/*.json")
    df_song = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    df_filtered = df_filtered.join(df_song, df_song.title == df_filtered.song)
    
    songplays_table = df_filtered.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('sessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        month('datetime').alias('month')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'songplays.parquet'), 'overwrite')
    print("log data processing completed")

def main():
    """
    Initializes the spark session and defines the source and destination AWS S3 buckets.
    Calls the processing functions and starts the ETL process.
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://marc-sparkify/"
    
    #process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
