from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import csv
import os
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit, current_timestamp
import snowflake.connector


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/GoogleCloudKey/yelpobot-explorer-b84e171163a9.json"


def export_query_results_to_local_csv():
    client = bigquery.Client()
    query_job = client.query('''
    select review_id, user_id, business_id, text, stars, date from `Yelp_Dataset.Review_Restaurant_Data` where date <= '2022-01-19' and date > '2020-01-01' and business_id IN (
            SELECT Rest.business_id FROM `Yelp_Dataset.Restaurant_Data` as Rest WHERE Rest.state = "PA"
        );    ''')
    results = query_job.result()

    local_file_path = '/opt/airflow/CSVFiles/file.csv'  # Update this path
    with open(local_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['review_id', 'user_id', 'business_id', 'text', 'stars', 'date'])  # Correct column order
        for row in results:
            writer.writerow([row['review_id'], row['user_id'], row['business_id'], row['text'], row['stars'], row['date']])

            
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
        # ... any other configs needed
    }    
#     conn = snowflake.connector.connect(
#     user='VickyH',
#     password='Vicky$2800',
#     account='xu61959.us-east4.gcp',
#     warehouse='Yelp_WH',
#     database='Yelp_DB',
#     role='Yelp_ROLE',
#     schema='PA_Restaurants'
# )
    session = Session.builder.configs(conn_config).create()
    # stage_name = "my_stage"
    # cursor = conn.cursor()
    # cursor.execute(f'PUT file://{csv_file_path} @{stage_name}')
    
    # stage_name = "my_stage" 
    
    # target_table = 'REVIEWS'
    # copy_into_sql = f"COPY INTO {target_table} FROM @{stage_name}/file.csv FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1) ON_ERROR = CONTINUE;"
    # cursor.execute(copy_into_sql)

    
    
    
    # cursor.close()
    # conn.close()
    
    # copy_into_sql = f'''
    # COPY INTO Your_Target_Table
    # FROM {csv_file_path}
    # FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    # ON_ERROR = CONTINUE  -- Handle errors gracefully
    # '''
    # session.sql(copy_into_sql).execute()
    batch_size = 10000
    
    # session.sql("""CREATE OR REPLACE TABLE Reviews (
    #     review_id STRING,
    #     user_id STRING,
    #     business_id STRING,
    #     stars INTEGER,
    #     text STRING,
    #     date STRING,
    #     load_datetime TIMESTAMP_LTZ
    # )""").collect()
    
    

    
    # Iterate through CSV file in chunks
    for chunk in pd.read_csv(csv_file_path, chunksize=batch_size):
        chunk['date'] = pd.to_datetime(chunk['date'])
        chunk['text'] = str(chunk['text'])
        snowpark_df = session.create_dataframe(chunk)
        snowpark_df_with_datetime = snowpark_df.with_column("load_datetime", current_timestamp())
        snowpark_df_with_datetime.write.mode("append").save_as_table("Reviews")

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
               'csv_file_path': '/opt/airflow/CSVFiles/Reviews.csv'},
    dag=dag,
)



t1 >> t2 >> t3
