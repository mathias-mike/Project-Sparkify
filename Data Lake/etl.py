import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import TimestampType, IntegerType
from pyspark.sql.functions import udf, col, row_number, max
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek

# Reading config file.
# 'dl.cfg' contains AWS KEY and ID for authentication
config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))
# config.read('dl.cfg')

# Setting AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY as environmental variables
# To authenticate to Amazon Web Services, the SDK first checks for credentials in your environment variables.
# The SDK uses the getenv() function to look for the AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN 
# environment variables.
# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_ACCESS_KEY_ID']=config.get('USER', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('USER', 'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    '''
    Get an existing spark session or Create new one is not exist.

    Return
    ---
    spark (SparkSession) - spark session connected to AWS EMR
        cluster
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    '''
    Loads song data, performs ETL on loaded data and write transformed data back to disk

    Parameters
    ----
    spark: SparkSession
        Used to perform operations
    input_data: str
        File path of input data location
    output_data: str
        File path of output data location
    '''
    
    # Read song data into df 
    song_data = '{}song_data/*/*/*/*.json'.format(input_data)
    df = spark.read.json(song_data)

    # Extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # Write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet('{}songs/'.format(output_data), 
                                mode='overwrite', 
                                partitionBy=["year", "artist_id"])

    # Extract columns to create artists table
    artist_window = Window \
                    .partitionBy('artist_id') \
                    .orderBy('song_id') \
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    artists_table = df.withColumn("row_number", row_number().over(artist_window)) \
                        .select("artist_id", col("artist_name").alias("name"), 
                            col("artist_location").alias("location"), 
                            col("artist_latitude").alias("lattitude"), 
                            col("artist_longitude").alias("longitude")) \
                        .filter('row_number == 1')
    
    # Write artists table to parquet files
    artists_table.write \
                .format('parquet') \
                .save('{}artists/'.format(output_data), mode='overwrite')


def process_log_data(spark, input_data, output_data):
    '''
    Loads log data, performs ETL on loaded data and write transformed data back to disk

    Parameters
    ----
    spark: SparkSession
        Used to perform operations
    input_data: str
        File path of input data location
    output_data: str
        File path of output data location
    '''

    # Read log data into df
    log_data = '{}log_data/*/*/*.json'.format(input_data)
    df = spark.read.json(log_data)
    
    # Filter by actions for song plays
    df = df.select('*').filter('page == "NextSong"')

    # Extract columns for users table 
    unique_users = df.groupBy('userid').agg(max('ts').alias('ts'))   
    users_table = df.join(unique_users, ['userid', 'ts'])
    
    # Write users table to parquet files
    users_table.write \
                .format('parquet') \
                .save('{}users/'.format(output_data), mode='overwrite')

    # Create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: ts/1000, IntegerType())
    df = df.withColumn('timestamp', get_timestamp("ts"))
    
    # Create datetime column from timestamp column
    get_datetime = udf(lambda timestamp: datetime.fromtimestamp(timestamp), 
                        TimestampType())
    df = df.withColumn('datetime', get_datetime("timestamp")) 
    
    # Extract columns to create time table
    time_table = df.select(col('datetime').alias('start_time'),
                            hour(col('datetime')).alias('hour'),
                            dayofmonth(col('datetime')).alias('day'),
                            weekofyear(col('datetime')).alias('week'),
                            month(col('datetime')).alias('month'),
                            year(col('datetime')).alias('year'), 
                            dayofweek(col('datetime')).alias('weekday'))
    
    # Write time table to parquet files partitioned by year and month
    time_table.write.parquet('{}time/'.format(output_data), 
                    mode='overwrite', 
                    partitionBy=['year', 'month'])

    # Read in song data to use for songplays table
    song_df = spark.read.json('{}song_data/*/*/*/*.json'.format(input_data))

    # Extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.song == song_df.title) 
                            & (df.artist == song_df.artist_name)
                            & (df.length == song_df.duration)) \
                        .select(df.datetime.alias('start_time'),
                            df.userId.alias('user_id'),
                            df.level,
                            song_df.song_id,
                            song_df.artist_id,
                            df.sessionId.alias('session_id'),
                            df.location,
                            df.userAgent.alias('user_agent'),
                            month(df.datetime).alias('month'),
                            year(df.datetime).alias('year'))

    # Write songplays table to parquet files partitioned by year and month
    songplays_table.write.format('parquet') \
                    .save('{}songplays/'.format(output_data), 
                            mode='overwrite',
                            partitionBy=['year', 'month'])

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3://sparkify-dwh-bucket/data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()


if __name__ == "__main__":
    main()
