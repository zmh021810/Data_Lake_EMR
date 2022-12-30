import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id, dayofweek
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    input_song_data=input_data+'song_data/'
    #song_data = "{}*/*/*/*.json".format(input_song_data)
    song_data = input_data + 'song_data/*/*/*/*.json'
    # read song data file
    df = spark.read.json(song_data)
    
    #song_data = "{}*/*/*/*.json".format(input_data)

    # read song data file
    #df = spark.read.json(song_data).dropDuplicates().cache()
    
    
    
    # extract columns to create songs table
    songs_table = df.select(col('song_id'), col('title'), col('artist_id'), col('year'), col('duration'))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet("{}songs_table/songs_table.parquet".format(output_data))

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), col('artist_name'), col('artist_location'), col('artist_latitude'), col('artist_longitude'))
    
    # write artists table to parquet files
    artists_table.write.parquet("{}artists_table/artists_table.parquet".format(output_data))
    
    df.createOrReplaceTempView("song_data_table")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    input_log_data=input_data+"log_data/"
    log_data ="{}*/*/*events.json".format(input_log_data)

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # extract columns for users table    
    users_table = df.select(col("firstName"), col("lastName"), col("gender"), col("level"), col("userId"))
    
    # write users table to parquet files
    users_table.write.parquet("{}users_table/users_table.parquet".format(output_data))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))
    
    # create datetime column from original timestamp column
    format_data = "%d/%m/%y"
    get_datetime = udf(lambda x:datetime.strptime(x, format_data))
    df = df.withColumn("start_time", get_datetime(col("ts")))
    
    # extract columns to create time table
    time_table = df.select('start_time') \
        .withColumn('year', year('timestamp')) \
        .withColumn('month', month('timestamp')) \
        .withColumn('week', weekofyear('timestamp')) \
        .withColumn('weekday', dayofweek('timestamp')) \
        .withColumn('day', dayofyear('timestamp')) \
        .withColumn('hour', hour('timestamp')).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table_path=output_data+"time_table/time_table.parquet"
    #time_table.write.partitionBy("year", "month").parquet("{}time_table/time_table.parquet".format(output_data))
    time_table.to_parquet(time_table_path, partition_cols=['year', 'month'])

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM song_data_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_data = df.join(song_df, df.artist==song_df.artist_name, how="inner")
    songplays_data.drop_duplicates(inplace=True)
    songplays_table = songplays_data.select(col("start_time"), col("userId"), col("level"), col("sessionId"), \
                col("location"), col("userAgent"), col("song_id"), col("artist_id")) \
        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table_path=output_data+"songplays_table/songplays_table.parquet"
    songplays_table.to_parquet(songplays_table_path, partition_cols=['year', 'month'])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data_song = "s3a://udacity-dend/song-data/"
    output_data = "s3://aws-logs-586077415546-us-east-1/elasticmapreduce/"
    
    #process_song_data(spark, input_data, output_data)
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
