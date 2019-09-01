from datetime import datetime, timedelta
import os
import pandas as pd
import configparser
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator, SubDagOperator)

from operators.create_dim_fact_tables import CreateTableOperator
from helpers import create_tables
from helpers import insert_queries




config = configparser.ConfigParser()
config.read('dl.cfg')


AWS_KEY =''
AWS_SECRET =''



   
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_retry': False,
}

dag = DAG('udacity_airbnb_final_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval=None, #'0 * * * *',
          catchup=False,
        )




start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_listings_to_redshift = StageToRedshiftOperator(
    task_id='staging_listings',
    dag=dag,
    table_name='staging_listings',
    create_sql = create_tables.CREATE_STAGING_LISTINGS_TABLE_SQL,
    redshift_conn_id='redshift',
    s3_bucket='dend-fwd-bucket',
    s3_key='airbnb_data/listing_detail/',   #{0}-events.csv'.format('{{ ds }}'),
    delimiter=';',
    headers='1',
    quote_char='',
    #json_format='auto',
    jsonPath='auto',
    file_type='CSV',
    aws_credentials={
        'key': AWS_KEY,
        'secret': AWS_SECRET
    }
)

stage_calendar_to_redshift = StageToRedshiftOperator(
    task_id='staging_calendar',
    dag=dag,
    table_name='staging_calendar',
    create_sql = create_tables.CREATE_STAGING_CALENDAR_TABLE_SQL,
    redshift_conn_id='redshift',
    s3_bucket='dend-fwd-bucket',
    s3_key='airbnb_data/calendar_detail/',   
    delimiter=',',
    headers='1',
    #quote_char='',
    #json_format='',
    jsonPath='auto',
    file_type='CSV',
    aws_credentials={
        'key': AWS_KEY,
        'secret': AWS_SECRET
    }
)

stage_review_to_redshift = StageToRedshiftOperator(
    task_id='staging_review',
    dag=dag,
    table_name='staging_review',
    redshift_conn_id='redshift',
    create_sql = create_tables.CREATE_STAGING_REVIEW_TABLE_SQL,
    s3_bucket='dend-fwd-bucket',
    s3_key='airbnb_data/review_detail/',
    delimiter=',',
    headers='1',
    quote_char='"',
    #json_format='',
    jsonPath='s3://dend-fwd-bucket/review_jsonpath.json',
    file_type='JSON',
    aws_credentials={
        'key': AWS_KEY,
        'secret': AWS_SECRET
    }
)

create_all_dimension_table = CreateTableOperator(
        task_id='create_all_dimension_table',    
        dag=dag,
        table_name='dim_review_table',
        redshift_conn_id='redshift',
        sql_statement=create_tables.CREATE_DIM_TABLE_SQL,       
        
 
)

create_all_fact_table = CreateTableOperator(
        task_id='create_all_fact_table',    
        dag=dag,
        table_name='fact_price_table',
        redshift_conn_id='redshift',
        sql_statement=create_tables.CREATE_FACT_TABLE_SQL,       
        
 
)

load_review_dimension_table = LoadDimensionOperator(
        task_id='load_review_dimension_table',    
        dag=dag,
        table_name='dim_review_table',
        redshift_conn_id='redshift',
        sql_statement= insert_queries.dim_review_table_insert
        
 
)

load_property_dimension_table = LoadDimensionOperator(
        task_id='load_property_dimension_table',    
        dag=dag,
        table_name='dim_property_table',
        redshift_conn_id='redshift',
        sql_statement= insert_queries.dim_property_table_insert
        
 
)

load_listing_dimension_table = LoadDimensionOperator(
        task_id='load_listing_dimension_table',    
        dag=dag,
        table_name='dim_lisiting_table',
        redshift_conn_id='redshift',
        sql_statement= insert_queries.dim_listing_table_insert
        
 
)

load_host_dimension_table = LoadDimensionOperator(
        task_id='load_host_dimension_table',    
        dag=dag,
        table_name='dim_host_table',
        redshift_conn_id='redshift',
        sql_statement= insert_queries.dim_host_table_insert
        
 
)

load_calendar_dimension_table = LoadDimensionOperator(
        task_id='load_calendar_dimension_table',    
        dag=dag,
        table_name='dim_calendar_table',
        redshift_conn_id='redshift',
        sql_statement= insert_queries.dim_calendar_table_insert
        
 
)

load_price_fact_table = LoadFactOperator(
        task_id='load_price_fact_table',    
        dag=dag,
        table_name='fact_price_table',
        redshift_conn_id='redshift',
        sql_statement= insert_queries.price_fact_table_insert
        
 
)



run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table_name='fact_price_table',
    column='price',
    redshift_conn_id='redshift'
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >>  [stage_calendar_to_redshift, stage_review_to_redshift, stage_listings_to_redshift] # , stage_listings_to_redshift convert_listing_data_to_parquet >> [stage_listings_to_redshift, stage_calendar_to_redshift,stage_review_to_redshift]

[stage_calendar_to_redshift, stage_review_to_redshift, stage_listings_to_redshift] >>  create_all_dimension_table
[stage_calendar_to_redshift, stage_review_to_redshift, stage_listings_to_redshift] >>  create_all_fact_table
[create_all_dimension_table, create_all_fact_table] >> load_calendar_dimension_table
[create_all_dimension_table, create_all_fact_table] >> load_property_dimension_table
[create_all_dimension_table, create_all_fact_table] >> load_review_dimension_table
[create_all_dimension_table, create_all_fact_table] >> load_listing_dimension_table
[create_all_dimension_table, create_all_fact_table] >> load_host_dimension_table
[load_calendar_dimension_table, load_property_dimension_table, load_review_dimension_table, load_listing_dimension_table, load_host_dimension_table] >> load_price_fact_table
load_price_fact_table >>  run_quality_checks
run_quality_checks >> end_operator
