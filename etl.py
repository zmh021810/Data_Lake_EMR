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
    """
    create Spark environment
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


# read song data
def process_song_data(spark, input_data, output_data):
    
    """
    input parameters are the spark instance, song data path and the output S3 bucket path
    this function need take data from song and generate songs_table, artists_table
    """
    
    
    # get filepath to song data file
    input_song_data=input_data+'song_data/'
    song_data = "{}*/*/*/*.json".format(input_song_data)
    #song_data = input_data + 'song_data/*/*/*/*.json'
    # read song data file
    df = spark.read.json(song_data)
    
    #song_data = "{}*/*/*/*.json".format(input_data)

    # read song data file
    #df = spark.read.json(song_data).dropDuplicates().cache()
    
    
    
    # extract columns to create songs table
    songs_table = df.select(col('artist_id'), col('year'), col('duration'), col('song_id'), col('title'))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table_path=output_data+"/songs_table.parquet"
    songs_table.write.partitionBy("year", "artist_id").parquet(songs_table_path)

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'), col('artist_name'), col('artist_location'), col('artist_latitude'), col('artist_longitude'))
    
    # write artists table to parquet files
    artists_table_path=output_data+"/artists_table.parquet"
    artists_table.write.parquet(artists_table_path)
    
    # create a temp sql table for the purpose of join in the log function process
    df.createOrReplaceTempView("song_data_table")


def process_log_data(spark, input_data, output_data):
    """
    input parameters are the spark instance, log data path and the output S3 bucket path
    this function need take data from song and generate users_table, time_table and songplays_table 
    """
    
    
    
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
    users_table_path=output_data+"/users_table.parquet"
    users_table.write.parquet(users_table_path)

    # create timestamp column from original timestamp column and create datetime column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn("time_stamp", get_timestamp(col("ts")))
    df = df.withColumn("begintime", get_timestamp(col("ts")))

    
    # extract columns to create time table
    df = df.withColumn("hour", hour("time_stamp"))
    df = df.withColumn("day", dayofmonth("time_stamp"))
    df = df.withColumn("week", weekofyear("time_stamp"))
    
    df = df.withColumn("month", month("time_stamp"))
    df = df.withColumn("year", year("time_stamp"))
    df = df.withColumn("weekday", dayofweek("time_stamp"))
    
    time_table = df.select(col("hour"), col("day"), col("week"), col("month"), col("year"), col("weekday"), col("begintime")).distinct()

    # write time table to parquet files partitioned by year and month
    time_table_path=output_data+"/time_table.parquet"
    time_table.write.partitionBy("year", "month").parquet(time_table_path)

    # read data from the temp table which created in the previous function
    
    artists_table_path=output_data+"/artists_table.parquet"
    
    spark.read.parquet(artists_table_path).createOrReplaceTempView("artists_table")
    
    
    query_song="""
    SELECT song_id, art.artist_id, title,  art.artist_name as artist_name, duration
    FROM song_data_table song, artists_table art
    WHERE song.artist_id = art.artist_id
    """
    
    song_temp_table = spark.sql(query_song)

    # use join the take the data we want and add the song index, using monotonically_increasing_id function
    songplays_table = df.join(song_temp_table, df.artist == song_temp_table.artist_name).distinct().select(col("year"), col("month"), col("userId"), col("level"), col("sessionId"), col("location"), col("userAgent"), col("song_id"), col("artist_id"), col("begintime")).withColumn("song_play_index", monotonically_increasing_id())

    # write songplays data into S3 bucket with parquet data format and partitioned by year and month
    songplays_table_path=output_data+"/songplays_table.parquet"
    songplays_table.write.partitionBy("year", "month").parquet(songplays_table_path)


    # for the purpose of S3 bucket upload test
def test_data(spark, input_data, output_data):
    df = spark.read.csv(input_data)
    df.write.parquet("{}/result.parquet".format(output_data))
  
    
    
    
    
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #input_data_song = "s3a://udacity-dend/song-data/"
    #input_data = "test.csv"
    output_data = "s3a://zmhtest/"
    
    #test_data(spark, input_data, output_data)
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
