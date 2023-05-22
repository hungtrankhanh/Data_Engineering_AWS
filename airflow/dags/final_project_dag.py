from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries, SqlStatements

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Hung',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'sla': timedelta(hours=1)
}

@dag(
        default_args=default_args,
        description='Load and transform data in Redshift with Airflow',
        schedule_interval='0 * * * *',
        max_active_runs=1
)
def final_project():
    start_operator = DummyOperator(task_id='Begin_execution')

    @task()
    def create_tables():
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run(SqlStatements.create_tables)

    create_table = create_tables()

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        staging_table="staging_events",
        s3_bucket_path="s3://airflow-project-data/log-data/",
        json_format="s3://airflow-project-data/log_json_path.json",
        region="us-east-1",
        clear_table=True
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        staging_table="staging_songs",
        s3_bucket_path="s3://airflow-project-data/song-data/A/A/A/",
        json_format="auto",
        region="us-east-1",
        clear_table=True
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        fact_table="songplays",
        columns="(start_time, userid, level , songid, artistid, sessionid, location, user_agent)",
        query=SqlQueries.songplay_table_insert,
        clear_table=True
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        dimension_table="users",
        columns="(userid, first_name, last_name, gender, level)",
        query=SqlQueries.user_table_insert,
        clear_table=True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        dimension_table="songs",
        columns="(songid, title, artistid, year, duration)",
        query=SqlQueries.song_table_insert,
        clear_table=True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        dimension_table="artists",
        columns="(artistid, name, location, lattitude, longitude)",
        query=SqlQueries.artist_table_insert,
        clear_table=True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        dimension_table="time",
        columns="(start_time, hour, day, week, month, year, weekday)",
        query=SqlQueries.time_table_insert,
        clear_table=True
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=['songplays', 'songs', 'artists', 'users', 'time']
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> create_table

    create_table >> stage_events_to_redshift
    create_table >> stage_songs_to_redshift
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

final_project_dag = final_project()