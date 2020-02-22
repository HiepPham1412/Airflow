from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

from helpers import SqlQueries


default_args = {
    'owner': 'hieppham',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('spakify_dag7',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log-data',
    data_format='json',
    format_type='s3://udacity-dend/log_json_path.json',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    data_format='json',
    format_type='auto',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    data_insertion_query=SqlQueries.songplay_table_insert,
    redshift_conn_id='redshift'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    data_selection_query=SqlQueries.user_table_insert,
    table='users',
    truncate_table=True,
    redshift_conn_id='redshift'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    data_selection_query=SqlQueries.song_table_insert,
    table='songs',
    truncate_table=True,
    redshift_conn_id='redshift'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    data_selection_query=SqlQueries.artist_table_insert,
    table='artists',
    truncate_table=True,
    redshift_conn_id='redshift'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    data_selection_query=SqlQueries.time_table_insert,
    table='time',
    truncate_table=True,
    redshift_conn_id='redshift'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=['users', 'songs', 'artists', 'time'],
    redshift_conn_id='redshift'   
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# define the dependency among tasks
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator


