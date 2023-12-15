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

# Task 1 Fetch Data for Provided Location and Sort By Value
def fetch_business_yelp_data():
    API_Access_Token = os.getenv("API_Access_Token")
    location ="PA"
    sort_by = "review_count"
    url = f"https://api.yelp.com/v3/businesses/search?location={location}&sort_by={sort_by}&limit=50"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {API_Access_Token}"
    }
    response = requests.get(url, headers=headers)
    data = response.json()
    # Extracting just the businesses data and returning it
    businesses_data = data['businesses']
    # print(businesses_data.head(1))
    return businesses_data

#Function2
def transform_business_data(**context):
    businesses_data = context['ti'].xcom_pull(task_ids='fetch_business_yelp_data')
    df = pd.DataFrame(businesses_data)
    df['ADDRESS'] = df['location'].apply(lambda x: x['address1'] if 'address1' in x else None)
    df['CITY'] = df['location'].apply(lambda x: x['city'] if 'city' in x else None)
    df['STATE'] = df['location'].apply(lambda x: x['state'] if 'state' in x else None)
    df['POSTAL_CODE'] = df['location'].apply(lambda x: x['zip_code'] if 'zip_code' in x else None)
    df['LATITUTE'] = df['coordinates'].apply(lambda x: x['latitude'] if 'latitude' in x else None)
    df['LONGITUDE'] = df['coordinates'].apply(lambda x: x['longitude'] if 'longitude' in x else None)

    df['URL']=df['url']
    df['PRICE']=df['price']
    df['PHONE']=df['phone']
    df['DISTANCE']=df['distance']

    # Handling the 'categories' field
    df['CATEGORIES'] = df['categories'].apply(lambda x: ', '.join([cat['title'] for cat in x]))

    # Renaming columns to match the Snowflake table schema
    df.rename(columns={'id': 'BUSINESS_ID', 'name': 'NAME', 'rating': 'STARS', 'review_count': 'REVIEW_COUNT'}, inplace=True)

    # Creating the 'IS_OPEN' column (inverting 'is_closed')
    df['IS_OPEN'] = df['is_closed'].apply(lambda x: 0 if x else 1)

    # Dropping unused columns
    df.drop(['alias', 'image_url','categories','transactions', 'display_phone', 'location', 'coordinates', 'is_closed'], axis=1, inplace=True)
    # df.drop(['alias', 'image_url','categories', 'transactions', 'display_phone','location', 'coordinates', 'is_closed'], axis=1, inplace=True)

    db_columns = ['BUSINESS_ID', 'NAME', 'REVIEW_COUNT', 'STARS', 'ADDRESS', 'CITY', 'STATE','POSTAL_CODE','LATITUTE','LONGITUDE','CATEGORIES','IS_OPEN','URL','PRICE','PHONE','DISTANCE']
    df = df[db_columns]

    print(df.head(1))

    # Return the transformed DataFrame
    return df

# Function3
def business_to_snowflake(**context):
    df = context['ti'].xcom_pull(task_ids='transform_business_data')
    session= Session.builder.configs({
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }).create()
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

# ########################################################
# #       Review Data Extraction and storage in Snowflake
# ########################################################

def fetch_review_yelp_data(**context):
    API_Access_Token = os.getenv("API_Access_Token")
    # df = context['ti'].xcom_pull(task_ids='transform_business_data')

    session= Session.builder.configs({
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }).create()

    # business_id="t1kc2kz2wsUyEw-5QVFMtg"

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

    if table_exists:
        # Fetch existing BUSINESS_IDs
        existing_ids_query = f"SELECT DISTINCT BUSINESS_ID FROM {full_table_name}"
        existing_ids_result = session.sql(existing_ids_query).collect()
        existing_ids = set([row[0] for row in existing_ids_result])
        print(f"Count of unique BUSINESS_ID values: {len(existing_ids)}")

        # # Filtering only on new data
        # df = df[~df['BUSINESS_ID'].isin(existing_ids)]
        # existing_ids = set([row[0] for row in df])
        # print(f"Number of new BUSINESS_IDs to be added: {len(existing_ids)}")
    #Fetching All Businesses in the PA_Business Table

    headers = {
        "Authorization": f"Bearer {API_Access_Token}",
        "accept": "application/json"
    }

    # Initialize DataFrame to store all reviews
    all_reviews_df = pd.DataFrame()
    for business_id in existing_ids:
        url = f"https://api.yelp.com/v3/businesses/{business_id}/reviews?limit=50&sort_by=newest"
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            review_data = data['reviews']
            df = pd.DataFrame(review_data)
            
            df['REVIEW_ID'] = df['id']
            df['USER_ID'] = df['user'].apply(lambda x: x['id'])
            df['BUSINESS_ID'] = business_id
            df['STARS'] = df['rating']
            df['TEXT'] = df['text']
            df['DATE'] = df['time_created']
            df['USERNAME'] = df['user'].apply(lambda x: x['name'])

            db_columns = ['REVIEW_ID', 'BUSINESS_ID', 'USER_ID', 'USERNAME', 'STARS', 'TEXT', 'DATE']
            df = df[db_columns]

            all_reviews_df = pd.concat([all_reviews_df, df])
    print(all_reviews_df.head(1))
    return all_reviews_df

# Function3
def review_to_snowflake(**context):
    df = context['ti'].xcom_pull(task_ids='fetch_review_yelp_data')
    session= Session.builder.configs({
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }).create()
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
    
    table_name = "PA_REVIEW"
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
            REVIEW_ID VARCHAR(16777216) PRIMARY KEY,
            BUSINESS_ID VARCHAR(16777216),
            USER_ID VARCHAR(16777216),
            USERNAME VARCHAR(16777216),
            STARS NUMBER(38,0),
            TEXT VARCHAR(16777216),
            DATE VARCHAR(16777216),
            LOAD_DATETIME TIMESTAMP_LTZ(9) NOT NULL 
        );
        """ # BUSINESS_ID VARCHAR(16777216),
        session.sql(create_table_sql).collect()

    if table_exists:
        # Fetch existing BUSINESS_IDs
        existing_ids_query = f"SELECT DISTINCT REVIEW_ID FROM {full_table_name}"
        existing_ids_result = session.sql(existing_ids_query).collect()
        existing_ids = set([row[0] for row in existing_ids_result])
        print(f"Count of unique REVIEW_ID values: {len(existing_ids)}")

        # Filter new data
        df = df[~df['REVIEW_ID'].isin(existing_ids)]
        print(f"Number of new REVIEW_ID to be added: {len(df)}")

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
#           schedule_interval='* * 1 * *')

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
    'API_to_snowflake',
    default_args=default_args,
    description='Load Yelp API data to Snowflake',
    # Set to None for manual trigger only
    schedule_interval='* * 1 * *'
)


fetch_yelp_business_data_task = PythonOperator(
    task_id='fetch_business_yelp_data',
    python_callable=fetch_business_yelp_data,
    dag=dag,
)

transform__business_data_task = PythonOperator(
    task_id='transform_business_data',
    python_callable=transform_business_data,
    dag=dag,
)

business_to_snowflake_task = PythonOperator(
    task_id='business_to_snowflake',
    python_callable=business_to_snowflake,
    dag=dag,
)

fetch_yelp_review_data_task = PythonOperator(
    task_id='fetch_review_yelp_data',
    python_callable=fetch_review_yelp_data,
    dag=dag,
)

review_to_snowflake_task = PythonOperator(
    task_id='review_to_snowflake',
    python_callable=review_to_snowflake,
    dag=dag,
)

# Set up the task dependencies
fetch_yelp_business_data_task >> transform__business_data_task >> business_to_snowflake_task >> fetch_yelp_review_data_task >> review_to_snowflake_task
#>> transform_review_data_task