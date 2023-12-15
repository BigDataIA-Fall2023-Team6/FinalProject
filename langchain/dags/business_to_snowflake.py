from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pandas as pd
import requests
from snowflake.snowpark import Session
from snowflake.snowpark.functions import current_timestamp
from dotenv import load_dotenv
import os
from airflow.utils.dates import days_ago

load_dotenv()

# Task 1
def fetch_yelp_data():
    url = "https://api.yelp.com/v3/businesses/search?location=PA&sort_by=rating&limit=50"
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer T5mJIGewN3PkPbJvZzHUHHFkbLQAuodeiKRZN-hILyp_m5EEGYFAC-CNTQk8nKzZl8n_EhLVlSDOO74KfrxznrpUO0V16vGmZaTPymkcU0oBVpCRjdWHqZsMpEtpZXYx"
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    # Extracting just the businesses data and returning it
    businesses_data = data['businesses']
    # print(businesses_data.head(1))
    return businesses_data

#Function2
def transform_data(**context):
    businesses_data = context['ti'].xcom_pull(task_ids='fetch_yelp_data')
    df = pd.DataFrame(businesses_data)
    df['ADDRESS'] = df['location'].apply(lambda x: x['address1'] if 'address1' in x else None)
    df['CITY'] = df['location'].apply(lambda x: x['city'] if 'city' in x else None)
    df['STATE'] = df['location'].apply(lambda x: x['state'] if 'state' in x else None)
    df['POSTAL_CODE'] = df['location'].apply(lambda x: x['zip_code'] if 'zip_code' in x else None)
    df['LATITUTE'] = df['coordinates'].apply(lambda x: x['latitude'] if 'latitude' in x else None)
    df['LONGITUDE'] = df['coordinates'].apply(lambda x: x['longitude'] if 'longitude' in x else None)

    # Handling the 'categories' field
    df['CATEGORIES'] = df['categories'].apply(lambda x: ', '.join([cat['title'] for cat in x]))

    # Renaming columns to match the Snowflake table schema
    df.rename(columns={'id': 'BUSINESS_ID', 'name': 'NAME', 'rating': 'STARS', 'review_count': 'REVIEW_COUNT'}, inplace=True)

    # Creating the 'IS_OPEN' column (inverting 'is_closed')
    df['IS_OPEN'] = df['is_closed'].apply(lambda x: 0 if x else 1)

    # Dropping unused columns
    df.drop(['alias', 'image_url','categories','price', 'url', 'transactions', 'phone', 'display_phone', 'distance', 'location', 'coordinates', 'is_closed'], axis=1, inplace=True)

    print(df.head(1))

    # Return the transformed DataFrame
    return df

# Function3
def write_to_snowflake(**context):
    df = context['ti'].xcom_pull(task_ids='transform_data')
    conn = BaseHook.get_connection('SnowflakeConnection')
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
    try:
        # Execute a simple query
        result = session.sql("SELECT 1").collect()

        # Check if the result is as expected
        if result and result[0][0] == 1:
            print("Snowflake session is connected and working.")
        else:
            print("Snowflake session is connected, but the test query did not return expected results.")

    except Exception as e:
        print(f"An error occurred: {e}")
    
    # result = session.sql("SELECT UNIQUE BUSINESS ID FROM ").collect()

    # print(f"Count of unique Business IDs before filtering: {df['BUSINESS_ID'].nunique()}")

    table_name = "PA_BUSINESS"
    schema_name = os.getenv("SNOWFLAKE_SCHEMA")
    database_name = os.getenv("SNOWFLAKE_DATABASE")
    full_table_name = f"{database_name}.{schema_name}.{table_name}"  # Fully qualified table name

    # Query the information schema
    table_check_query = f"""
    SELECT COUNT(*) 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = '{schema_name}' 
    AND TABLE_NAME = '{table_name}'
    AND TABLE_CATALOG = '{database_name}';
    """
    table_exists = session.sql(table_check_query).collect()[0][0] > 0

    if not table_exists:
        # Create the table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE {full_table_name} (
            BUSINESS_ID VARCHAR(16777216) PRIMARY KEY,
            NAME VARCHAR(16777216),
            REVIEW_COUNT NUMBER(38,0),
            STARS FLOAT,
            ADDRESS VARCHAR(16777216),
            CITY VARCHAR(16777216),
            STATE VARCHAR(16777216),
            POSTAL_CODE VARCHAR(16777216),
            LATITUTE FLOAT,
            LONGITUDE FLOAT,
            CATEGORIES VARCHAR(16777216),
            IS_OPEN NUMBER(38,0),
            LOAD_DATETIME TIMESTAMP_LTZ(9) NOT NULL
        );
        """
        session.sql(create_table_sql).collect()

    if table_exists:
        # Fetch existing BUSINESS_IDs
        existing_ids_query = f"SELECT DISTINCT BUSINESS_ID FROM {full_table_name}"
        existing_ids_result = session.sql(existing_ids_query).collect()
        existing_ids = set([row[0] for row in existing_ids_result])
        print(f"Count of unique BUSINESS_ID values: {len(existing_ids)}")

        # Filter new data
        df = df[~df['BUSINESS_ID'].isin(existing_ids)]
        print(f"Number of new BUSINESS_IDs to be added: {len(df)}")

    snowpark_df = session.create_dataframe(df)
    snowpark_df_with_datetime = snowpark_df.with_column("LOAD_DATETIME", current_timestamp())

    if not df.empty:
        snowpark_df_with_datetime.write.mode("append").save_as_table(full_table_name)

    session.close()

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# dag = DAG('yelp_to_snowflake',
#           default_args=default_args,
#           description='Load Yelp data to Snowflake',
#           schedule_interval='*/29 * * * *')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'yelp_to_snowflake',
    default_args=default_args,
    description='Load Yelp data to Snowflake',
    # Set to None for manual trigger only
    schedule_interval=None
)


fetch_yelp_data_task = PythonOperator(
    task_id='fetch_yelp_data',
    python_callable=fetch_yelp_data,
    dag=dag,
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

write_to_snowflake_task = PythonOperator(
    task_id='write_to_snowflake',
    python_callable=write_to_snowflake,
    dag=dag,
)

# Set up the task dependencies
fetch_yelp_data_task >> transform_data_task >> write_to_snowflake_task
