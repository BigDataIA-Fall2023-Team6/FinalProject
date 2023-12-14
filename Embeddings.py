# %pip install python-dotenv requests snowflake-snowpark-python pandas langdetect spacy scikit-learn pinecone-client

# !python3 -m spacy download en_core_web_md

import snowflake.connector
from snowflake.snowpark import Session
import pandas as pd
import spacy
import pinecone
from langdetect import detect_langs, LangDetectException
from sklearn.feature_extraction._stop_words import ENGLISH_STOP_WORDS



def create_snowpark_session_from_env():
    # Create a Snowpark Session object using environment variables
    return Session.builder.configs({
        "account": "xu61959.us-east4.gcp",
        "user": "VickyH",
        "password": "Vicky$2800",
        "role": "Yelp_ROLE",
        "warehouse": "Yelp_WH",
        "database": "Yelp_DB",
        "schema": "PA_Restaurants"
    }).create()
    
    
# Initialize spaCy
nlp = spacy.load('en_core_web_md')

Pinecone_API_KEYS = "244a8054-da15-4776-a398-50c9a6b668ed"
pinecone.init(api_key=Pinecone_API_KEYS, environment="gcp-starter")

my_stop_words = set(ENGLISH_STOP_WORDS)

index_name = 'yelpreviewembeddings'
if index_name in pinecone.list_indexes():
    pinecone.delete_index(index_name)
pinecone.create_index(name=index_name, dimension = 300, metric="cosine")

index = pinecone.Index(index_name)


################




# Assuming pinecone and other necessary libraries are already installed and imported





# Function to clean and preprocess text
def process_text(text):
    # Use spaCy for tokenization and lemmatization
    # doc = nlp(text)
    # Remove stopwords and non-alphabetic tokens, then lemmatize
    words = [word.lower() for word in text.split() if word.lower() not in my_stop_words]
    return ' '.join(words)

def detect_language(text):
    try:
        return detect_langs(text)[0].lang
    except LangDetectException:
        return "unknown"

# Function to batch process texts

def batch_process(session, query, batch_size=1000, total_limit=800):
    # Add column for tracking embeddings status if it doesn't exist
    session.sql("ALTER TABLE PA_REVIEW_DATA ADD COLUMN IF NOT EXISTS \"Embeddings Done\" STRING").collect()

    # Start at the beginning
    offset = 0
    records_processed = 0
    while records_processed < total_limit:

        records_to_fetch = min(batch_size, total_limit - records_processed)
        # Modify query to fetch the next batch
        batch_query = f"{query} LIMIT {records_to_fetch} OFFSET {offset}"
        # Execute the batch query
        batch_results = session.sql(batch_query).collect()

        # If there are no more results, break the loop
        if not batch_results:
            break

        # Convert to DataFrame
        df = pd.DataFrame(batch_results, columns=['BUSINESS_ID', 'DATE', 'REVIEW_ID', 'TEXT'])

        df['LANGUAGE'] = df['TEXT'].apply(detect_language)
        df = df[df['LANGUAGE'] == 'en']
        print("Lang Detect Step completed")

        df['FILTER_TEXT'] = df['TEXT'].str.replace('[^a-zA-Z\s]', '', regex=True)



        # Assume other processing steps here ...
                # Preprocess texts
        df['FILTER_TEXT'] = df['FILTER_TEXT'].apply(process_text)

        print("FILTER_TEXT Step completed")

        df['FILTER_WORD_COUNT'] = df['FILTER_TEXT'].apply(lambda x: len(x.split()))

        texts = df['FILTER_TEXT'].tolist()

        df['embeddings'] = [doc.vector for doc in nlp.pipe(texts)]

        # Generate embeddings
        # df['embeddings'] = [doc.vector for doc in nlp.pipe(df['FILTER_TEXT'])]

        df['metadata'] = df.apply(
            lambda x: {
                'BUSINESS_ID': x['BUSINESS_ID'],
                'DATE': x['DATE'],  # Already ISO formatted or None
                'REVIEW_ID': x['REVIEW_ID']
            }, axis=1
        )

        # upload_data = [(str(i), emb.tolist(), meta) for i, emb, meta in zip(df.index, df['embeddings'], df['metadata'])]

                # Prepare the data for insertion into Pinecone
        # Prepare the data for insertion into Pinecone
        upload_data = [
            (str(row['REVIEW_ID']), row['embeddings'].tolist(), row['metadata'])
            for index, row in df.iterrows()
        ]


        response = index.upsert(vectors=upload_data)
        print(response)

        # Update the "Embeddings Done" status in Snowflake
        review_ids = df['REVIEW_ID'].tolist()
        update_query = (
            "UPDATE PA_REVIEW_DATA "
            "SET \"Embeddings Done\" = 'Done' "
            "WHERE REVIEW_ID IN ({})".format(", ".join(["'" + str(id) + "'" for id in review_ids]))
        )
        session.sql(update_query).collect()

        # Increment the offset
        # Increment the number of records processed and the offset
        records_processed += len(df)
        offset += records_to_fetch
        print(f"Processed batch up to offset {offset}, total records processed: {records_processed}")


# Call the batch process function with a total limit of 4000 for the test run
session = create_snowpark_session_from_env()
query = "SELECT BUSINESS_ID, DATE, REVIEW_ID, TEXT FROM PA_REVIEW_DATA WHERE \"Embeddings Done\" IS NULL"
batch_process(session, query, total_limit=10000)
