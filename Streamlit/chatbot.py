import streamlit as st
import requests
import os
from dotenv import load_dotenv

import json

load_dotenv()
BASE_URL = os.getenv("BASE_URL", "http://127.0.0.1:8000")

# def show_chatbot_page(token):
import streamlit as st
import requests
import json
import time

# Initialize session state for conversation history
if 'conversation_history' not in st.session_state:
    st.session_state['conversation_history'] = []

# Function to process the query and get response
def get_query_embedding(query, history):
    # Implement logic to modify query based on history or to use history in processing
    # For now, we just send the query as is
    response = requests.post(f"{BASE_URL}/embeddings/", json={"text": query})
    return response.json()

st.title("Review Query Chatbot")

# Chat interface
with st.form("chat_form"):
    user_input = st.text_input("Ask a question about reviews:")
    submit_button = st.form_submit_button("Send")

# Process input when the form is submitted
if submit_button and user_input:
    # Append user query to conversation history
    st.session_state['conversation_history'].append({"role": "user", "content": user_input})

    # Process the query
    response = get_query_embedding(user_input, st.session_state['conversation_history'])

    # Display response and update conversation history
    if 'error' in response:
        st.session_state['conversation_history'].append({"role": "bot", "content": f"Error: {response['error']}"})
    else:
        bot_response = "\n".join([f"Business ID: {match['metadata']['BUSINESS_ID']}" for match in response.get('matches', [])])
        st.session_state['conversation_history'].append({"role": "bot", "content": bot_response})

# Display conversation history
for message in st.session_state['conversation_history']:
    with st.expander(f"{message['role'].title()} says:", expanded=True):
        st.write(message["content"])

        st.write("No matches found or unexpected response format.")