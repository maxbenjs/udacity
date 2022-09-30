import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')   

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']




def create_spark_session():
    """Create a Apache Spark session to process the data.
    
    Output:
    * spark -- An Apache Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark





def process_song_data(spark, input_data, output_data):
    """Load JSON input data (song_data), process & extract data
        to create song_table and artist_tables. Data is then stored as parquet files.
    """


    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df['song_id','title','artist_id','year','duration']
    songs_table = songs_table.drop_duplicates()
    print('Created: songs_table')
    
    
    # extract columns to create artists table
    artists_table = df['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']
    artists_table = artists_table.drop_duplicates()
    print('Created: artists_table')
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists'), 'overwrite')

    # write songs table to parquet files partitioned by year and artist
    songs_table.withColumn('_year', df.year).withColumn('_artist_id', df.artist_id).write.partitionBy(['_year', '_artist_id']).parquet(os.path.join(output_data, 'songs'), 'overwrite')





def process_log_data(spark, input_data, output_data):
    """Load JSON input data (log_data), process & extract data
        to create users_table,  time_table & songplay_tables.
        Data is then stored as parquet files.
    """



    # get filepath to log data file
    log_data = log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table    
    users_table = df['userId', 'firstName', 'lastName', 'gender', 'level']
    users_table = users_table.drop_duplicates(subset=['userId'])
    print('Created: users_table')
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, 'users'), 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts // 1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts // 1000), DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(
        F.col('datetime').alias('start_time'),
        F.hour('datetime').alias('hour'),
        F.dayofmonth('datetime').alias('day'),
        F.weekofyear('datetime').alias('week'),
        F.month('datetime').alias('month'),
        F.year('datetime').alias('year'),
        F.date_format('datetime', 'u').alias('weekday')
    )
    time_table = time_table.drop_duplicates()
    print('Created: time_table')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).parquet(os.path.join(output_data, 'time'), 'overwrite')

    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs/_year=*/_artist_id=*/*.parquet'))
    

    # extract columns from joined song and log datasets to create songplays table
    df = df['datetime', 'userId', 'level', 'song', 'artist', 'sessionId', 'location', 'userAgent']

    log_song_df = df.join(song_df, df.song == song_df.title)

    songplays_table = log_song_df.select(
        F.monotonically_increasing_id().alias('songplay_id'),
        F.col('datetime').alias('start_time'),
        F.year('datetime').alias('year'),
        F.month('datetime').alias('month'),
        F.col('userId').alias('user_id'),
        'level',
        'song_id',
        'artist_id',
        F.col('sessionId').alias('session_id'),
        'location',
        F.col('userAgent').alias('user_agent')
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).parquet(os.path.join(output_data, 'songplays'), 'overwrite')





def main():
    """Main function which creates the spark session, loads input data,
        before calling the two functions which process the song_data
        and log_data.
    """

    spark = create_spark_session()
    input_data = config['DATALAKE']['INPUT_DATA']
    output_data = config['DATALAKE']['OUTPUT_DATA']

    os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']

    
    #Processing local data
    #process_song_data(spark, input_data, output_data)    
    #process_log_data(spark, input_data, output_data)

    #Processing S3 Data
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
