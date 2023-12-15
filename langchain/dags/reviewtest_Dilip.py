import snowflake.connector
import openai
import os
from dotenv import load_dotenv
import pandas as pd
from langdetect import detect_langs
from sklearn.feature_extraction._stop_words import ENGLISH_STOP_WORDS

load_dotenv()

conn_params = {
    "account": "wzgtpox-ua27102",
    "user": "DILIP18",
    "password": "Dilip123",
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "YELP_DATA",
    "schema": "YELP_ENTITIES"
}

openai.api_key = os.getenv("OPENAI_KEY")

conn = snowflake.connector.connect(**conn_params)
cur = conn.cursor()
# Query data from Snowflake
cur.execute("SELECT BUSINESS_ID, DATE, REVIEW_ID, TEXT FROM REVIEWS")
data = cur.fetchall()
# Convert to DataFrame
df = pd.DataFrame(data, columns=['BUSINESS_ID', 'DATE', 'REVIEW_ID', 'TEXT'])

# Close connection
cur.close()
conn.close()
print(df.head(1))
# return df

import spacy
from langdetect import detect_langs, LangDetectException

# Load the SpaCy 'en_core_web_md' model
nlp = spacy.load('en_core_web_md')

# Define your set of stop words
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

import pinecone

Pinecone_API_KEYS = "64388ab1-d4dd-4991-b3f8-80f69bb71996"
pinecone.init(api_key=Pinecone_API_KEYS, environment="gcp-starter")

index_name = 'Yelp_Review_Embeddings'
if index_name in pinecone.list_indexes():
    pinecone.delete_index(index_name)
pinecone.create_index(name=index_name, dimension = 300, metric="cosine")

index = pinecone.Index(index_name)

df['metadata'] = df.apply(lambda x: {'BUSINESS_ID': x['BUSINESS_ID'], 'DATE': x['DATE'], 'REVIEW_ID': x['REVIEW_ID']}, axis=1)

upload_data = [(str(i), emb.tolist(), meta) for i, emb, meta in zip(df.index, df['embeddings'], df['metadata'])]

index.upsert(vectors=upload_data)
print("Upload completed")
# import nltk

# # Download the 'punkt' tokenizer models
# nltk.download('punkt')

# from nltk.tokenize import word_tokenize

# def tokenize_filter(text):
#     # Lowercase and tokenize the input text
#     word_tokens = word_tokenize(text.lower())
#     # Filter out tokens that are not alphabetic or are stopwords
#     filtered_tokens = [word for word in word_tokens if word.isalpha() and word not in my_stop_words]
#     # Join the filtered tokens back into a string
#     filtered_text = ' '.join(filtered_tokens)
#     return filtered_text

# df['TOKENIZED_FILTERED_TEXT'] = df['FILTER_TEXT'].apply(tokenize_filter)


# from nltk.stem import PorterStemmer

# # Initialize the Porter Stemmer
# porter = PorterStemmer()

# def stem_text(text):
#     # Tokenize the text to get a list of words
#     words = text.split()
#     # Stem each word in the list of words
#     stemmed_words = [porter.stem(word) for word in words]
#     # Join the stemmed words back into a string
#     stemmed_text = ' '.join(stemmed_words)
#     return stemmed_text

# # Apply the stem_text function to the 'TOKENIZED_FILTERED_TEXT' column
# df['STEMMED_TEXT'] = df['TOKENIZED_FILTERED_TEXT'].apply(stem_text)

# # Check the first few entries of the stemmed text
# print(df[['TOKENIZED_FILTERED_TEXT', 'STEMMED_TEXT']].head())


# from sklearn.feature_extraction.text import CountVectorizer
# import pandas as pd

# # Initialize CountVectorizer with bigrams
# vect = CountVectorizer(ngram_range=(2, 2))

# # Fit and transform the stemmed text to bigrams
# bigrams = vect.fit_transform(df['TOKENIZED_FILTERED_TEXT'])

# # Convert the matrix to a DataFrame
# bigram_df = pd.DataFrame(bigrams.toarray(), columns=vect.get_feature_names_out())

# bigram_frequency = bigram_df.sum(axis=0).reset_index()
# bigram_frequency.columns = ['bigram', 'frequency']

# # Sort by frequency
# bigram_frequency = bigram_frequency.sort_values(by='frequency', ascending=False)

# def top_n_bigrams(row, n=4):
#     # Get top n bigrams for this row
#     top_bigrams = sorted(zip(vect.get_feature_names_out(), row), key=lambda x: x[1], reverse=True)[:n]
#     # Return only the bigram names
#     return [bigram for bigram, freq in top_bigrams if freq > 0]

# # Apply the function to each row of the bigram DataFrame
# df['TOP_4_BIGRAMS'] = bigram_df.apply(lambda row: top_n_bigrams(row), axis=1)



# # from sklearn.feature_extraction.text import CountVectorizer
# # import pandas as pd

# # # Initialize CountVectorizer with bigrams
# # vect = CountVectorizer(ngram_range=(2, 2))

# # # Fit and transform the stemmed text to bigrams
# # bigrams = vect.fit_transform(df['STEMMED_TEXT'])

# # # Convert the matrix to a DataFrame
# # bigram_df = pd.DataFrame(bigrams.toarray(), columns=vect.get_feature_names_out())

# # # Calculate frequency of each bigram across all documents
# # bigram_frequency = bigram_df.sum().reset_index()
# # bigram_frequency.columns = ['bigram', 'frequency']

# # # Sort by frequency
# # bigram_frequency = bigram_frequency.sort_values(by='frequency', ascending=False)
# # top_4_bigrams = bigram_frequency.head(4)
# # # print(top_4_bigrams)
# # top_4_bigram_list = top_4_bigrams['bigram'].tolist()

# # # Add a new column with the top 4 bigrams
# # df['TOP_4_BIGRAMS'] = [top_4_bigram_list for _ in range(len(df))]
# df.to_csv('airflow/dags/jh.csv')


# # import nltk
# # # nltk.download('stopwords')

# # import re
# # # from nltk.corpus import stopwords
# # from nltk.tokenize import word_tokenize
