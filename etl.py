import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import databricks.koalas as ks


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    #this method creats a spark Session then returns an instance .
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    #INPUTS : [spark] : an instance of the spark session
    #[input_data] : path to the data set .
    #[output_data] : [path to where to save the processed data]
    
    
    
    # get filepath to song data file
    song_data = input_data +'song_data/*/*/*/*.json'
    
    # read song data file
    df = ks.read_json("s3a://udacity-dend/song_data/*/*/*/*.json")

    # extract columns to create songs table
    songs_table = (ks.sql('''
               SELECT 
               DISTINCT
               row_number() over (ORDER BY year,title,artist_id) id,
               title,
               artist_id,
               year,
               duration
               FROM 
                   {df}''')
              )
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.to_spark().write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = (ks.sql('''
               SELECT 
               DISTINCT
               artist_id,artist_latitude,artist_location,artist_longitude,artist_name
               FROM 
                   {df}''')
              )
    
    # write artists table to parquet files
    artists_table.to_spark().write.mode('overwrite').partitionBy("artist_name").parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    #INPUT :
    #[spark] : an instance of the spark session
    #[input_data] : path to the data set .
    #[output_data] : path to where to save the processed data
    
    # get filepath to log data file
    log_data =input_data + "log_data/*.json"

    # read log data file
    dflogs =  spark.read.json(log_data)
    
    # filter by actions for song plays
    dflogs =  dflogs.filter(logs.page == 'NextSong')
    dflogs.createOrReplaceTempView("logs_view")
    # extract columns for users table    
    users_table = spark.sql('''SELECT DISTINCT L.userId as user_id, 
                            L.firstName as first_name,
                            L.lastName as last_name,
                            L.gender as gender,
                            L.level as level
                            FROM logs_view L''')
    
    # write users table to parquet files
    user_table.write.mode('overwrite').partitionBy("gender").parquet(output_data + 'users/')


    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
   # df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                            X.start_time_sub as start_time,
                            hour(X.start_time_sub) as hour,
                            dayofmonth(X.start_time_sub) as day,
                            weekofyear(X.start_time_sub) as week,
                            month(X.start_time_sub) as month,
                            year(X.start_time_sub) as year,
                            dayofweek(X.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(L.ts/1000) as start_time_sub
                            FROM logs_view L
                            WHERE L.ts IS NOT NULL
                            ) X
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet('time/')

    # read in song data to use for songplays table
    songPath =  input_data +'song_data/*/*/*/*.json'
    song_df = spark.read.json(songPath)
    song_df.createOrReplaceTempView("songs_view")
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(L.ts/1000) as start_time,
                                month(to_timestamp(L.ts/1000)) as month,
                                year(to_timestamp(L.ts/1000)) as year,
                                L.userId as user_id,
                                L.level as level,
                                S.song_id as song_id,
                                S.artist_id as artist_id,
                                L.sessionId as session_id,
                                L.location as location,
                                L.userAgent as user_agent
                                FROM logs_view L
                                JOIN songs_view S on L.artist = S.artist_name and L.song = S.title
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
