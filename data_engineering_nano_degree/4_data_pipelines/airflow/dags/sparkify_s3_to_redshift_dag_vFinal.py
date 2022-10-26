from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from helpers.sql_queries import SqlQueries
from operators import (
      StageToRedshiftOperator
    , LoadFactOperator
    , LoadDimensionOperator
    , DataQualityOperator
)



default_args = {
      'owner': 'maxbs'
    , 'start_date': datetime(2022,10,25)
    , 'depends_on_past': False
    , 'retries': 3
    , 'retry_delay': 300
}


dag = DAG('udac_sparkify_data_pipeline_v2',
            default_args=default_args
          , description='Load and transform data in Redshift with Airflow'
          , schedule_interval='@hourly'
          , catchup=False
        )


start_operator = DummyOperator(task_id='begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
      task_id='stage_events'
    , dag=dag
    , redshift_conn_id='redshift_conn_id'
    , aws_conn_id='aws_credentials'
    , s3_bucket='udacity-dend'
    , s3_prefix='log_data'
    , table='staging_events'
    , copy_options="JSON 's3://udacity-dend/log_json_path.json'"
)


stage_songs_to_redshift = StageToRedshiftOperator(
      task_id='stage_songs'
    , dag=dag
    , redshift_conn_id='redshift_conn_id'
    , aws_conn_id='aws_credentials'
    , s3_bucket='udacity-dend'
    , s3_prefix='song_data'
    , table='staging_songs'
    , copy_options="FORMAT AS JSON 'auto'"
)

s3_to_redshift_operator = DummyOperator(task_id='s3_to_redshift_data_load',  dag=dag)


load_songplays_table = LoadFactOperator(
      task_id='load_songplays_fact_table'
    , dag=dag
    , table='songplays'
    , sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
      task_id='load_user_dim_table'
    , dag=dag
    , table='users'
    , sql=SqlQueries.user_table_insert
    , mode='truncate'
)

load_song_dimension_table = LoadDimensionOperator(
      task_id='load_song_dim_table'
    , dag=dag
    , table='songs'
    , sql=SqlQueries.song_table_insert
    , mode='truncate'
)

load_artist_dimension_table = LoadDimensionOperator(
      task_id='load_artist_dim_table'
    , dag=dag
    , table='artists'
    , sql=SqlQueries.artist_table_insert
    , mode='truncate'
)

load_time_dimension_table = LoadDimensionOperator(
      task_id='load_time_dim_table'
    , dag=dag
    , table='time'
    , sql=SqlQueries.time_table_insert
    , mode='truncate'
)



run_quality_checks = DataQualityOperator(
      task_id='run_data_quality_checks'
    , dag=dag
    , check_stmts=[
        {
              'sql': 'select count(*) from songplays;'
            , 'op': 'gt'
            , 'val': 0
        },
        {
              'sql': 'select count(*) from songplays where songid is null;'
            , 'op': 'eq'
            , 'val': 0
        }
    ]
)


end_operator = DummyOperator(task_id='stop_execution',  dag=dag)



start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> s3_to_redshift_operator

s3_to_redshift_operator >> [load_songplays_table, load_song_dimension_table, load_user_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator