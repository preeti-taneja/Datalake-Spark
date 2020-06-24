import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
import  pyspark.sql.functions as F

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Description: Create Spark Session 

    Arguments:
         None

    Returns:
        spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function is used to read the song_data from S3, transforms it to create songs and artists table and writes them to partitioned parquet files in table directories on S3 (output_data).
    
    Arguments:
        spark: the session object. 
        input_data: S3 location where data is stored and needs to be retrieved.
        output_data: S3 location where formatted data needs to be stored.
    Returns:
        None
    """
    
    # get filepath to song data file
    song_data= "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    # read song data file 
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id","title","artist_id","year","duration"]).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    #songs_table 
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data,"songs"), "overwrite")

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude" , "artist_longitude"]).distinct()

    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data,"artists"), "overwrite")
    
def process_log_data(spark, input_data, output_data):
    """
    Description: This function is used to read the log_data from S3(input_data), transforms it to create users ,time and songplays table and writes them to partitioned parquet files in table directories on S3 (output_data).
    
    Arguments:
        spark: the session object. 
        input_data: S3 location where data is stored and needs to be retrieved.
        output_data: S3 location where formatted data needs to be stored.
    Returns:
        None
    """
    
    # get filepath to log data file
    log_data ="s3a://udacity-dend/log_data/*/*/*.json"
   
    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender" , "level"]).distinct()
   
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data,"users"), "overwrite")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("start_time", F.to_timestamp(get_timestamp(df.ts)))
 
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("date_time", get_datetime(df.ts))
  

    # extract columns to create time table
    funcWeekDay =  udf(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%w'))
                       
    df = df.withColumn("month", F.month("start_time")).withColumn("hour", F.hour("start_time")).withColumn("year", F.year("start_time")).withColumn('shortdate',col('start_time').substr(1, 10)).withColumn('weekday', funcWeekDay(col('shortdate'))).drop('shortdate').withColumn("week",F.weekofyear("start_time")).withColumn("day",F.dayofmonth("start_time"))
         
    time_table = df.select(["start_time", "hour", "day", "week" , "month","year","weekday"]).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"times"), "overwrite")
                       
    # read in song data to use for songplays table
    song_data= "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    song_df = spark.read.json(song_data) 
    
    # drop year - duplicate later on while joining dataframes
    song_df = song_df.drop('year')

    # extract columns from joined song and log datasets to create songplays table 
    join_df = df.join(song_df,(df.song==song_df.title)&(df.artist==song_df.artist_name))

    songplays_table = join_df.select(["start_time", "userId", "level", "song_id","artist_id","sessionId","location","userAgent","month","year"])
 
    # write songplays table to parquet files partitioned by year and month
                       
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data,"songplays"), "overwrite")


def main():
    """
    Description: This is the main function that first creats spark session and than it calls the process_song_data and process_log_data function.

    Arguments:
         None

    Returns:
        None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-proj4-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
