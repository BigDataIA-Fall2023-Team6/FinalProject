from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from snowflake.snowpark import Session
import pandas as pd
import spacy
import pinecone
from langdetect import detect_langs, LangDetectException
import os
from sklearn.feature_extraction._stop_words import ENGLISH_STOP_WORDS

# Initialize spaCy
nlp = spacy.load('en_core_web_md')

# Function to clean and preprocess text
def process_text(text):
    my_stop_words = set(ENGLISH_STOP_WORDS)
    words = [word.lower() for word in text.split() if word.lower() not in my_stop_words]
    return ' '.join(words)

# Function to detect language
def detect_language(text):
    try:
        return detect_langs(text)[0].lang
    except LangDetectException:
        return "unknown"

# Function to batch process reviews

def batch_process_reviews(**kwargs):
    import spacy

    nlp = spacy.load('en_core_web_md')

    # Initialize Pinecone
    Pinecone_API_KEYS = os.getenv('Pinecone_API_KEYS')
    pinecone.init(api_key=Pinecone_API_KEYS, environment="gcp-starter")

    # Create or initialize Pinecone index
    index_name = 'yelpreviewembeddings'
    if index_name not in pinecone.list_indexes():
        pinecone.create_index(name=index_name, dimension=300, metric="cosine")
    index = pinecone.Index(index_name)

    # Create Snowpark session
    session = Session.builder.configs({
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }).create()

    # Batch processing settings
    batch_size = 1000
    total_limit = 20000
    offset = 0
    records_processed = 0

    while records_processed < total_limit:
        records_to_fetch = min(batch_size, total_limit - records_processed)
        batch_query = f"""
            SELECT BUSINESS_ID, DATE, REVIEW_ID, TEXT 
            FROM PA_REVIEW_DATA 
            WHERE \"Embeddings Done\" IS NULL
            LIMIT {records_to_fetch} OFFSET {offset}
        """

        # Execute the batch query
        batch_results = session.sql(batch_query).collect()

        # If there are no more results, break the loop
        if not batch_results:
            break

        # Convert to DataFrame
        df = pd.DataFrame([row.to_dict() for row in batch_results])

        # Process each review
        df['LANGUAGE'] = df['TEXT'].apply(detect_language)
        df = df[df['LANGUAGE'] == 'en']
        df['FILTER_TEXT'] = df['TEXT'].str.replace('[^a-zA-Z\s]', '', regex=True)
        df['FILTER_TEXT'] = df['FILTER_TEXT'].apply(process_text)
        df['embeddings'] = [doc.vector for doc in nlp.pipe(df['FILTER_TEXT'])]

        # Prepare data for Pinecone
        upload_data = [
            (str(row['REVIEW_ID']), row['embeddings'].tolist(), {
                'BUSINESS_ID': row['BUSINESS_ID'],
                'DATE': row['DATE'],
                'REVIEW_ID': row['REVIEW_ID']
            }) for index, row in df.iterrows()
        ]
        index.upsert(vectors=upload_data)

        # Update Snowflake "Embeddings Done" status
        review_ids = df['REVIEW_ID'].tolist()
        update_query = f"""
            UPDATE PA_REVIEW_DATA 
            SET \"Embeddings Done\" = 'Done' 
            WHERE REVIEW_ID IN ({', '.join("'" + str(id) + "'" for id in review_ids)})
        """
        session.sql(update_query).collect()

        # Update the records processed and offset
        records_processed += len(df)
        offset += records_to_fetch
        print(f"Processed batch up to offset {offset}, total records processed: {records_processed}")

    # Close the Pinecone index
    index.close()

    # ...

# Define your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['your_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'batch_process_reviews',
    default_args=default_args,
    description='Batch process reviews and update embeddings',
    schedule_interval='@daily',  # Adjust the schedule as needed
)

# Define the task
process_reviews_task = PythonOperator(
    task_id='batch_process_reviews',
    python_callable=batch_process_reviews,
    dag=dag,
)

# Set the task sequence
process_reviews_task