from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
from snowflake.snowpark import Session
import pandas as pd
from airflow.hooks.base_hook import BaseHook

def load_data_to_snowflake(**kwargs):
    conn = BaseHook.get_connection('Snowflake_Yelp')
    conn_config = {
        "account": conn.extra_dejson.get('account'),
        "user": conn.login,
        "password": conn.password,
        "database": conn.extra_dejson.get('database'),
        "warehouse": conn.extra_dejson.get('warehouse'),
        "role": conn.extra_dejson.get('role'),
        "schema": conn.schema
    }
    session = Session.builder.configs(conn_config).create()

    # Create table in Snowflake without load_datetime field
    session.sql("""CREATE OR REPLACE TABLE PA_review (
        review_id STRING,
        user_id STRING,
        business_id STRING,
        stars INTEGER,
        useful INTEGER,
        funny INTEGER,
        cool INTEGER,
        text STRING,
        date STRING
    )""").collect()

    file_path = '/opt/airflow/CSVFiles/file.csv' 
    batch_size = 10000 
    


    # Read CSV in chunks and directly write each chunk to Snowflake
    for chunk in pd.read_csv(file_path, chunksize=batch_size):
        
        # chunk['date'] = pd.to_datetime(chunk['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        snowpark_df = session.create_dataframe(chunk)
        # snowpark_df['date'] = pd.to_datetime(snowpark_df['date']).dt.strftime('%Y-%m-%d %H:%M:%S')
        snowpark_df.write.mode("append").save_as_table("PA_review")

    session.close()

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'load_review_data_to_snowflake',
    default_args=default_args,
    description='Load data into Snowflake',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define the PythonOperator to load data
load_data_task = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_data_to_snowflake,
    provide_context=True,
    dag=dag,
)

# Set the task in the DAG
load_data_task
