import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id
import pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function initiates the Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    This function reads the song data from S3, formats the data, then writes the dataframe back to S3 in Parquet format
    """
    
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)
    df = spark.read.json(song_data)
    
    songs_table = df.select("song_id","title","artist_id","year","duration").dropDuplicates()
    songs_table.write.partitionBy("year","artist_id") \
        .parquet(path = "{}/songs/songs.parquet".format(output_data), mode ='overwrite')

    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude") \
        .withColumnRenamed("artist_name", "name") \
        .withColumnRenamed("artist_location", "location") \
        .withColumnRenamed("artist_latitude", "latitude") \
        .withColumnRenamed("artist_longitude", "longitude") \
        .dropDuplicates()
    artists_table.write.parquet(path= "{}/artists/artists.parquet".format(output_data), mode ='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    This function reads the log data from S3, formats the data, then writes the dataframe back to S3 in Parquet format
    """
    
    log_data = "{}/log_data/*.json".format(input_data)
    df = spark.read.json(log_data)
    df = df.filter(df.page == 'NextSong')
    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level") \
        .withColumnRenamed("userId", "user_id") \
        .withColumnRenamed("firstName", "first_name") \
        .withColumnRenamed("lastName", "last_name") \
        .dropDuplicates()

    users_table.write.parquet(path= "{}/users/users.parquet".format(output_data), mode ='overwrite')

    get_timestamp = udf(lambda x: str(int(int(x/1000))))
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x/1000))))
    df = df.withColumn("datetime", get_datetime(df.ts))
    
    time_table = df.select('datetime') \
        .withColumn('start_time', df.datetime) \
        .withColumn('hour', hour(df.datetime)) \
        .withColumn('day', dayofmonth(df.datetime)) \
        .withColumn('week', weekofyear(df.datetime)) \
        .withColumn('month', month(df.datetime)) \
        .withColumn('year', year(df.datetime)) \
        .withColumn('weekday', dayofweek(df.datetime)) \
        .drop('datetime') \
        .dropDuplicates()
    
    time_table.write.partitionBy("year","month") \
        .parquet(path= "{}/time/time.parquet".format(output_data), mode ='overwrite')

    song_df = spark.read.json("{}/song_data/*/*/*/*.json".format(input_data))

    songplays_table = df.join(song_df, df.artist == song_df.artist_name, 'inner') \
        .select('datetime', 'userId', 'level','song_id','artist_id','sessionId','location', 'userAgent') \
        .withColumn('year', year(df.datetime)) \
        .withColumn('month', month(df.datetime)) \
        .withColumnRenamed('userId','user_id') \
        .withColumnRenamed('sessionId','session_id') \
        .withColumnRenamed('userAgent','user_agent') \
        .withColumnRenamed('datetime','start_time')
    
    songplays_table = songplays_table.withColumn('songplay_id', monotonically_increasing_id()) \
        .select('songplay_id', *songplays_table.columns)

    songplays_table.write.partitionBy("year","month") \
        .parquet(path= "{}/songplays/songplays.parquet".format(output_data), mode ='overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://udacity-dend-oliviercar0n"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
