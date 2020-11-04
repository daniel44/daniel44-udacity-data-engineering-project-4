import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Method that creates a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Method that loads song data from Amazon S3, transforms the data and posts it back to S3
    """
    # get filepath to song data file
    song_data = input_data+'song_data/*/*/*/*.json'
    
    print('Reading ' + song_data)
    
    # read song data file
    df = spark.read.json(song_data)
    
    print('Transforming data into songs table')

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration']).dropDuplicates()
    
    print('Writing data to ' + output_data + 'songs/')
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + 'songs/')
    
    print('Transforming data into artists table')

    # extract columns to create artists table
    artists_table = df.select([df.artist_id, \
                                df.artist_name.alias('name'), \
                                df.artist_location.alias('location'), \
                                df.artist_latitude.alias('latitude'), \
                                df.artist_longitude.alias('longitude')]).dropDuplicates()
    
    print('Writing data to ' + output_data + 'artists/')
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
        Method that loads log data from Amazon S3, transforms the data and posts it back to S3
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    print('Reading' + log_data)
    
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    
    print('Transforming data into users table')

    # extract columns for users table    
    users_table = df.select([df.userId.alias('user_id'), \
                             df.firstName.alias('first_name'), \
                             df.lastName.alias('last_name'), \
                             df.gender, \
                             df.level]).dropDuplicates()
    
    print('Writing data to ' + output_data + 'users/')
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.fromtimestamp(x/1000.0))
    df = df.withColumn('start_time', get_timestamp(df.ts)) 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.fromtimestamp(x/1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('datetime', get_timestamp(df.ts)) 
    
    print('Transforming data into time table')
    
    # extract columns to create time table
    time_table = df.select(df.datetime.alias('start_time'), \
                    hour(df.datetime).alias('hour'),\
                    dayofmonth(df.datetime).alias('day'),\
                    weekofyear(df.datetime).alias('week'),\
                    month(df.datetime).alias('month'),\
                    year(df.datetime).alias('year'),
                    dayofweek(df.datetime).alias('weekday')).dropDuplicates()
    
    print('Writing data to ' + output_data + 'time/')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'time/')
    
    print('Reading' + input_data+'song_data/*/*/*/*.json')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    
    print('Transforming data into songplays table')
    
    #Creating views to use Spark SQL for complex queries
    df.createOrReplaceTempView('log_df')
    song_df.createOrReplaceTempView('song_df')
                        
    songplays_table = spark.sql("""SELECT 
                                        ROW_NUMBER() OVER (ORDER BY l.start_time) AS songplay_id,
                                                l.start_time,
                                                YEAR(l.start_time) as year,
                                                month(l.start_time) as month,
                                                l.userId as user_id,
                                                l.level,
                                                s.song_id,
                                                s.artist_id,
                                                l.sessionId as session_id,
                                                l.location,
                                                l.userAgent as user_agent
                                    FROM        log_df l
                                    JOIN        song_df s 
                                                ON l.artist = s.artist_name 
                                                AND l.song = s.title 
                                                AND l.length = s.duration""")
    
    print('Writing data to ' + output_data + 'songplays/')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + 'songplays/')

def main():
    """
        Extract songs and lof data from Amazon S3, transforms ans posts it back to S3
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://MYAWSBUCKET/"
    
    print('Processing song data')
    process_song_data(spark, input_data, output_data)    
    print('Processing log data')
    process_log_data(spark, input_data, output_data)
    print('Done')


if __name__ == "__main__":
    main()
