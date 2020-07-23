import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import *


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
  """
    Create Spark Session
  """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
  """
    Description: Load song_data from S3 and data by extracting the songs and artist tables and then load data back to S3

    Parameters:
      spark: Spark Session
      input_data: location of song_data json files
      output_data: S3 bucket where dimensional tables in parquet format will be stored

  """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'

    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year", "artist_id])

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude").dropDuplicates()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "artists/"), mode="overwrite")


def process_log_data(spark, input_data, output_data):
  """
  Description: Load log_data from S3 and process it by extracting the songs and artist tables and then load data back to S3.

  Parameters:
  spark: spark session
  input_data: location of log_data json files
  output_data: S3 bucket to store dimensional tables in parquet format
  """
    # get filepath to log data file
    log_data = input_data + "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_fields = ["userId as user_id", "firstName as firts_first_name", "lastName as firts_last", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "users/"), mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))


    # extract columns to create time table
    time_table = df.withColumn("hour", hour("start_time"))\
      .withColumn("day",dayofweek("start_time"))\
      .withColumn("week",weekofyear("start_time"))\
      .withColumn("month",month("start_time"))\
      .withColumn("year",year("start_time"))\
      .withColumn("weekday",dayofweek("start_time"))\
      .select("ts", "start_time", "hour", "day", "week", "month", "year", "weekday").dropDuplicates()

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode="overwrite", partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, "songs"))

    # extract columns from joined song and log datasets to create songplays table
    song_logs = df.join(songs_df, (df.song == song_df.title))

    artist_song_logs = song_logs.join(df.artists, (song_logs.artist == df_artists.name))

    songplays = artist_song_logs.join(timetable, (artist_song_logs.ts == time_table.start_time))

    songplays_table = songsplays.select(
      col("start_time").alias("start_time"),
      col("userId").alias("user_id"),
      col("level").alias("level")
      col("song_id").alias("song_id"),
      col("artist_id").alias("artist_id"),
      col("sessionId").alias("session_id"),
      col("location").alias("location"),
      col("userAgent").alias("user_agent"),
      col("year").alias("year"),
      col("month").alias("month")
    ).repartition("year", "month")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.dropDuplicates().write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year", "month"])


def main():
  """
  Extract songs data from S3, transform data into dimensional tables, and load back to S3 in parquet format
  """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "S3a://udacity-spark-project/output/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
