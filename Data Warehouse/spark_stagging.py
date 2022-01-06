from pyspark.sql import SparkSession

# The dataset takes a long will to load with Postgresql COPY command.
# This might possibly be caused by the way the file is save (nexted folders).

# In a bid to improve on the perfomance, I fired up a Spark Cluster and 
# ran this script to load the data and save it in a compressed "csv.gzip"
# format on s3. That way I would be easy to read into Redshift. 

# Better would be to directly stage the file from spark to Redshift....
# Working on it


def read_from_s3(spark, path):
    '''
    Reads data from s3 path

    Parameters
    ----
    spark: SparkSession
        Entry point into all the functionality in Spark
    path: str
        S3 path to read data from

    Return
    ----
    Spark DataFrame
        Data loaded into column and row format
    '''
    return spark.read.json(path)


def write_to_s3(dataframe, path):
    '''
    Write data to s3 path

    Parameters
    ----
    dataframe: Spark DataFrame
        Data to load into s3
    path: str
        S3 path to write data to
    '''
    dataframe.write \
        .save(path, format='json')


def main():
    # Instantiating spark object
    spark = SparkSession \
            .builder \
            .config("spark.sql.catalogImplementation","in-memory") \
            .getOrCreate()


    # Loading log-data
    log_read_path = "s3://udacity-dend/log-data/*/*/*.json"
    log_data = read_from_s3(spark, log_read_path)

    # Loading song-data
    song_read_path = "s3://udacity-dend/song-data/*/*/*/*.json"
    song_data = read_from_s3(spark, song_read_path)
    # song_data = spark.read.option("Header", True) \
    #             .csv('s3://sparkify-dwh-bucket/song_data.csv/*.csv')


    # Writing log data in compressed form to s3 path
    log_write_path = 's3://sparkify-dwh-bucket/log-data-json'
    write_to_s3(log_data, log_write_path)

    # Writing song data in compressed form to s3 path
    song_write_path = 's3://sparkify-dwh-bucket/song-data-json'
    write_to_s3(song_data, song_write_path)

if __name__ == "__main__":
    main()

