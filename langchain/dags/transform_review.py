from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from snowflake.snowpark import Session
from snowflake.snowpark.functions import current_timestamp
from airflow.utils.dates import days_ago

import requests
import pandas as pd
from dotenv import load_dotenv
import os
from langdetect import detect_langs, LangDetectException
from sklearn.feature_extraction._stop_words import ENGLISH_STOP_WORDS
import snowflake.connector
# import openai

load_dotenv()

conn_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA")
}

# openai.api_key = os.getenv("OPENAI_KEY")

# Task 1 Fetch Data for Provided Location and Sort By Value
def fetch_reviews():
    conn = snowflake.connector.connect(**conn_params)
    cur = conn.cursor()
    
    # Query the data from Snowflake
    cur.execute("SELECT TEXT FROM PA_Review")
    data = cur.fetchall()
    
    # Convert to DataFrame
    df = pd.DataFrame(data, columns=['TEXT'])
    # Close connection
    cur.close()
    conn.close()
    
    print(df.head(1))
    return df

def transform_reviews(**context):
    reviews_text = context['ti'].xcom_pull(task_ids='fetch_reviews')
    df = pd.DataFrame(reviews_text)
    
    my_stop_words = set(ENGLISH_STOP_WORDS)

    def detect_language(text):
        try:
            return detect_langs(text)[0].lang
        except LangDetectException:
            return "unknown"

    # Apply language detection
    df['LANGUAGE'] = df['TEXT'].apply(detect_language)

    # Filter to keep only English texts
    df = df[df['LANGUAGE'] == 'en']
    print("Lang Detect Step completed")
    def process_text(text):
        # Lowercasing and removing stopwords
        words = [word.lower() for word in text.split() if word.lower() not in my_stop_words]
        # Joining words back into a string
        return ' '.join(words)

    # Remove non-alphabetic characters from 'FILTER_TEXT'
    df['FILTER_TEXT'] = df['TEXT'].str.replace('[^a-zA-Z\s]', '', regex=True)

    # Apply text processing
    df['FILTER_TEXT'] = df['FILTER_TEXT'].apply(process_text)
    print("FILTER_TEXT Step completed")
    # Calculate word count
    df['FILTER_WORD_COUNT'] = df['FILTER_TEXT'].apply(lambda x: len(x.split()))

    # Create embeddings in batches
    texts = df['FILTER_TEXT'].tolist()
    df['embeddings'] = [doc.vector for doc in nlp.pipe(texts)]

    print("embeddings Step completed")
    # df.to_csv('airflow/dags/new.csv')
    print(df.head(2))


        









    

    # table_name = "PA_REVIEW"
    # schema_name = os.getenv("SNOWFLAKE_SCHEMA")
    # database_name = os.getenv("SNOWFLAKE_DATABASE")
    # full_table_name = f"{database_name}.{schema_name}.{table_name}"  # Fully qualified table name

    # # Query the information schema
    # table_check_query = f"""
    # SELECT COUNT(*) 
    # FROM INFORMATION_SCHEMA.TABLES 
    # WHERE TABLE_SCHEMA = '{schema_name}' 
    # AND TABLE_NAME = '{table_name}'
    # AND TABLE_CATALOG = '{database_name}';
    # """
    # table_exists = session.sql(table_check_query).collect()[0][0] > 0

    # if not table_exists:
    #     # Create the table if it doesn't exist
    #     create_table_sql = f"""
    #     CREATE TABLE {full_table_name} (
    #         REVIEW_ID VARCHAR(16777216) PRIMARY KEY,
    #         BUSINESS_ID VARCHAR(16777216),
    #         USER_ID VARCHAR(16777216),
    #         USERNAME VARCHAR(16777216),
    #         STARS NUMBER(38,0),
    #         TEXT VARCHAR(16777216),
    #         DATE VARCHAR(16777216),
    #         LOAD_DATETIME TIMESTAMP_LTZ(9) NOT NULL 
    #     );
    #     """ # BUSINESS_ID VARCHAR(16777216),
    #     session.sql(create_table_sql).collect()

    # if table_exists:
    #     # Fetch existing BUSINESS_IDs
    #     existing_ids_query = f"SELECT DISTINCT REVIEW_ID FROM {full_table_name}"
    #     existing_ids_result = session.sql(existing_ids_query).collect()
    #     existing_ids = set([row[0] for row in existing_ids_result])
    #     print(f"Count of unique REVIEW_ID values: {len(existing_ids)}")

    #     # Filter new data
    #     df = df[~df['REVIEW_ID'].isin(existing_ids)]
    #     print(f"Number of new REVIEW_ID to be added: {len(df)}")

    # snowpark_df = session.create_dataframe(df)
    # snowpark_df_with_datetime = snowpark_df.with_column("LOAD_DATETIME", current_timestamp())

    # if not df.empty:
    #     snowpark_df_with_datetime.write.mode("append").save_as_table(full_table_name)
    # session.close()

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
    'Review_Analysis',
    default_args=default_args,
    description='Tranform Review Data to Snowflake',
    # Set to None for manual trigger only
    schedule_interval=None
)


fetch_reviews = PythonOperator(
    task_id='fetch_reviews',
    python_callable=fetch_reviews,
    dag=dag,
)

transform_reviews = PythonOperator(
    task_id='transform_reviews',
    python_callable=transform_reviews,
    dag=dag,
)

# Set up the task dependencies
fetch_reviews >> transform_reviews
# >>