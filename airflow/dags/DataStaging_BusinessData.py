# import os
# from airflow.models import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
# from airflow.models import BaseOperator
# from airflow.utils.decorators import apply_defaults
# from datetime import timedelta
# import pinecone
# import pandas as pd
# from google.cloud import storage
# import pandas as pd
# import json
# import os

# Import required modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import pandas as pd
import json
import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit, current_timestamp
from airflow.hooks.base_hook import BaseHook
from smart_open import open

# These are placeholders for where your actual credentials and file paths would go
# In a production environment, you would want to secure your credentials using Airflow's built-in mechanisms
# and probably use custom operators for GCP and Snowflake

# Set your GCP credentials as an environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/GoogleCloudKey/yelpobot-explorer-b84e171163a9.json"

# Function to download blob from GCS
def download_blob(bucket_name, source_blob_name, destination_file_name, **kwargs):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")

# Function to filter JSON data and save to CSV
def filter_and_save_data_gcs(json_file_path, state, csv_file_path, **kwargs):
    # Create a client to access GCS
    # storage_client = storage.Client()
    # bucket = storage_client.bucket(bucket_name)
    # blob = bucket.blob(source_blob_name)

    # # Construct the URI for the blob
    # blob_uri = f"gs://{bucket_name}/{source_blob_name}"

    # # Stream the JSON file from GCS and filter in memory
    # data = []
    # with open(blob_uri, 'r') as f:
    #     for line in f:
    #         business = json.loads(line)
    #         if business.get("state") == state:
    #             data.append(business)

    # # Create a DataFrame and save to CSV
    # restaurants_df = pd.DataFrame(data)
    # restaurants_df.to_csv(csv_file_path, index=False)
    # print(f"Data for state {state} saved to {csv_file_path}")
    

    data = []
    with open(json_file_path) as f:
        for line in f:
            business = json.loads(line)
            if business.get("state") == state:
                data.append(business)
    restaurants_df = pd.DataFrame(data)
    restaurants_df.to_csv(csv_file_path, index=False)
    print(f"Data for state {state} saved to {csv_file_path}")


# Function to load data to Snowflake
def load_to_snowflake(csv_file_path,csv_review_file_path, **kwargs):
    conn = BaseHook.get_connection('Snowflake_Yelp')
    conn_config = {
        "account": conn.extra_dejson.get('account'),
        "user": conn.login,
        "password": conn.password,
        "database": conn.extra_dejson.get('database'),
        "warehouse": conn.extra_dejson.get('warehouse'),
        "role": conn.extra_dejson.get('role'),
        "schema": conn.schema
        # ... any other configs needed
    }
    session = Session.builder.configs(conn_config).create()
    
    
    
    df = pd.read_csv(csv_file_path)
    snowpark_df = session.create_dataframe(df)
    
    session.sql("""CREATE OR REPLACE TABLE PA_restaurant (
        business_id STRING,
        name STRING,
        address STRING,
        city STRING,
        state STRING,
        postal_code FLOAT,
        latitude FLOAT,
        longitude FLOAT,
        stars FLOAT,
        review_count INTEGER,
        is_open INTEGER,
        attributes STRING,
        categories STRING,
        hours STRING,
        load_datetime TIMESTAMP_LTZ
    )""").collect()
    
    snowpark_df_with_datetime = snowpark_df.with_column("load_datetime", current_timestamp())
    snowpark_df_with_datetime.write.mode("append").save_as_table("PA_restaurant")
    
        #Review
        
    
    df = pd.read_csv(csv_review_file_path)
    snowpark_review_df = session.create_dataframe(df)
    
    # batch_size = 10000 
    session.sql("""CREATE OR REPLACE TABLE PA_review (
        review_id STRING,
        user_id STRING,
        business_id STRING,
        stars INTEGER,
        useful INTEGER,
        funny INTEGER,
        cool INTEGER,
        text STRING,
        date STRING,
        load_datetime TIMESTAMP_LTZ
    )""").collect()
    
    # Iterate through CSV file in chunks
    # for chunk in pd.read_csv(csv_file_path, chunksize=batch_size):
    #     snowpark_df = session.create_dataframe(chunk)
    snowpark_review_df_with_datetime = snowpark_review_df.with_column("load_datetime", current_timestamp())
    snowpark_review_df_with_datetime.write.mode("append").save_as_table("PA_review")
    
    
    
# def load_reviews_to_snowflake(csv_review_file_path, **kwargs):
#     conn = BaseHook.get_connection('Snowflake_Yelp')
#     conn_config = {
#         "account": conn.extra_dejson.get('account'),
#         "user": conn.login,
#         "password": conn.password,
#         "database": conn.extra_dejson.get('database'),
#         "warehouse": conn.extra_dejson.get('warehouse'),
#         "role": conn.extra_dejson.get('role'),
#         "schema": conn.schema
#         # ... any other configs needed
#     }
#     session = Session.builder.configs(conn_config).create()
    
    
    
#     df = pd.read_csv(csv_review_file_path)
#     snowpark_review_df = session.create_dataframe(df)
    
#     # batch_size = 10000 
#     session.sql("""CREATE OR REPLACE TABLE PA_review (
#         review_id STRING,
#         user_id STRING,
#         business_id STRING,
#         stars INTEGER,
#         useful INTEGER,
#         funny INTEGER,
#         cool INTEGER,
#         text STRING,
#         date STRING,
#         load_datetime TIMESTAMP_LTZ
#     )""").collect()
    
#     # Iterate through CSV file in chunks
#     # for chunk in pd.read_csv(csv_file_path, chunksize=batch_size):
#     #     snowpark_df = session.create_dataframe(chunk)
#     snowpark_review_df_with_datetime = snowpark_review_df.with_column("load_datetime", current_timestamp())
#     snowpark_review_df_with_datetime.write.mode("append").save_as_table("PA_review")
    
    

    
    

# Define DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'yelp_reviews_to_snowflake',
    default_args=default_args,
    description='Load Yelp reviews filtered by state into Snowflake',
    schedule_interval=None,
)

# Define the task to download the blob from GCS
t1 = PythonOperator(
    task_id='download_blob',
    python_callable=download_blob,
    op_kwargs={'bucket_name': 'yelp--datasets',
               'source_blob_name': 'yelp_academic_dataset_business.json',
               'destination_file_name': '/tmp/yelp_academic_dataset_business.json'},
    dag=dag,
)

# Define the task to filter JSON data and save to CSV
t2 = PythonOperator(
    task_id='filter_and_save_data',
    python_callable=filter_and_save_data_gcs,
    op_kwargs={ 'json_file_path': '/tmp/yelp_academic_dataset_business.json',
        # 'bucket_name': 'yelp--datasets',
        #        'source_blob_name': 'yelp_academic_dataset_business.json',
               'state': 'PA',  # This should be dynamic based on user input or an Airflow Variable
               'csv_file_path': '/opt/airflow/CSVFiles/Resturant.csv'},
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_to_snowflake,
    op_kwargs={
               'csv_file_path': '/opt/airflow/CSVFiles/Resturant.csv',
               'csv_review_file_path': '/opt/airflow/CSVFiles/file.csv'},
    dag=dag,
)

# t4 = PythonOperator(
#     task_id='load_reviews_data_to_snowflake',
#     python_callable=load_reviews_to_snowflake,
#     op_kwargs={
#                'csv_review_file_path': '/opt/airflow/CSVFiles/file.csv'},
#     dag=dag,
# )

# Define the task

t1 >> t2 >> t3
