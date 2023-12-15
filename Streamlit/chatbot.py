import streamlit as st
import requests
import os
from dotenv import load_dotenv

import json

load_dotenv()
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")

def get_query_embedding(query):
    response = requests.post(f"{BASE_URL}/embeddings/", json={"text": query})
    return response.json()

st.title("Review Query and Analysis")

query = st.text_input("Enter your query for reviews:")
if query:
    response = get_query_embedding(query)
    
    if 'error' in response:
        st.error(f"Error: {response['error']}")
    elif 'matches' in response:
        st.write("Search Results:")
        for match in response['matches']:
            st.write(f"Business ID: {match['metadata']['BUSINESS_ID']}")
            st.write(f"Review ID: {match['metadata']['REVIEW_ID']}")
            st.write(f"Date: {match['metadata']['DATE']}")
            st.write(f"Score: {match['score']}")
            st.write("------")
    else:
        st.write("No matches found or unexpected response format.")