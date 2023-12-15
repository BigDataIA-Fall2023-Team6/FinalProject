# Import required modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage
import pandas as pd
import json
import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit, current_timestamp
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
    session= Session.builder.configs({
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }).create()
        
    df = pd.read_csv(csv_file_path)
    
    # Fill missing values with null for the columns you want
    columns_to_fill_null = ["price", "url", "phone", "distance", "image_url", "transactions"]
    df[columns_to_fill_null] = df[columns_to_fill_null].fillna(pd.NA)
    
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
        url STRING,
        price STRING,
        distance STRING,
        image_url STRING,
        transaction STRING,
        load_datetime TIMESTAMP_LTZ
    )""").collect()
    
    snowpark_df_with_datetime = snowpark_df.with_column("load_datetime", current_timestamp())
    snowpark_df_with_datetime.write.mode("append").save_as_table("PA_restaurant")
    
    #     #Review


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
    'yelp_business_to_snowflake',
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

t1 >> t2 >> t3