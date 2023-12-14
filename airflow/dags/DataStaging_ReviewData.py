from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import csv
import os
import csv
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit, current_timestamp


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/GoogleCloudKey/yelpobot-explorer-b84e171163a9.json"


def export_query_results_to_local_csv():
    client = bigquery.Client()  
    query_job = client.query('''
        SELECT review_id, user_id, business_id, text, stars, date FROM `Yelp_Dataset.Review_Restaurant_Data` as Review
        WHERE Review.business_id IN (
            SELECT Rest.business_id FROM `Yelp_Dataset.Restaurant_Data` as Rest WHERE Rest.state = "PA"
        )
    ''')
    results = query_job.result()

    local_file_path = '/opt/airflow/CSVFiles/file.csv'  # Update this path
    with open(local_file_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write the header row with the correct column names
        writer.writerow(['review_id', 'user_id', 'business_id', 'text', 'stars', 'date'])
        
        # Iterate over the rows and write them to the CSV file
        for row in results:
            # Handle multi-line text by enclosing it in double quotes and escaping any existing double quotes
            text = row.text.replace('"', '""')
            writer.writerow([row.review_id, row.user_id, row.business_id, text, row.stars, row.date])    
            
            
            
def load_to_snowflake(csv_file_path, **kwargs):
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
    
    batch_size = 10000
    
    session.sql("""CREATE OR REPLACE TABLE PA_review (
            REVIEW_ID VARCHAR(16777216) PRIMARY KEY,
            BUSINESS_ID VARCHAR(16777216),
            USER_ID VARCHAR(16777216),
            STARS NUMBER(38,0),
            TEXT VARCHAR(16777216),
            DATE VARCHAR(16777216),
            LOAD_DATETIME TIMESTAMP_LTZ(9) NOT NULL 
    )""").collect()
    

    
    # Iterate through CSV file in chunks
    for chunk in pd.read_csv(csv_file_path, chunksize=batch_size):
        chunk['date'] = pd.to_datetime(chunk['date'])
        chunk['text'] = chunk['text'].astype(str)
        snowpark_df = session.create_dataframe(chunk)
        snowpark_df_with_datetime = snowpark_df.with_column("load_datetime", current_timestamp())
        snowpark_df_with_datetime.write.mode("append").save_as_table("PA_review")

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
    'yelp_reviews',
    default_args=default_args,
    description='Load Yelp reviews filtered by state into Snowflake',
    schedule_interval=None,
)

t1 = BigQueryExecuteQueryOperator(
    task_id='query_bigquery',
    sql='''
        SELECT * FROM `Yelp_Dataset.Restaurant_Review_Data` as Review
        WHERE Review.business_id IN (
            SELECT Rest.business_id FROM `Yelp_Dataset.Restaurant_Data` as Rest WHERE Rest.state = "PA"
        )
    ''',
    use_legacy_sql=False,
    destination_dataset_table='Yelp_Dataset.temp_table',
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

t2 = PythonOperator(
    task_id='export_query_results_to_local_csv',
    python_callable=export_query_results_to_local_csv,
    dag=dag,
)

t3 = PythonOperator(
    task_id='load_data_to_snowflake',
    python_callable=load_to_snowflake,
    op_kwargs={
               'csv_file_path': '/opt/airflow/CSVFiles/file.csv'},
    dag=dag,
)

# t3

t1 >> t2 >> t3

