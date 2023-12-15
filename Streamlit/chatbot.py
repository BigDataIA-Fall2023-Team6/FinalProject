# import streamlit as st
# import requests
# import pandas as pd
# import os
# from dotenv import load_dotenv

# load_dotenv()
# BASE_URL = os.getenv("BASE_URL")

# # def show_chatbot(token):
    
# #     # Initialize session state for conversation history
# #     if 'conversation_history' not in st.session_state:
# #         st.session_state['conversation_history'] = []

# #     # Function to process the query and get response
# #     def get_query_embedding(query, history, token):
# #         headers = {"Authorization": f"Bearer {token}"}
# #         response = requests.post(f"{BASE_URL}/embeddings", json={"text": query}, headers=headers)
# #         if response.status_code == 200:
# #             return response.json()
# #         else:
# #             st.error(f"Failed to fetch data: {response.status_code} - {response.text}")
# #             return None

# #     # Function to create DataFrame for display
# #     def create_display_df(business_details):
# #         df = pd.DataFrame({
# #             "Image": [details["image_url"] for details in business_details],
# #             "Name": [details["name"] for details in business_details],
# #             "Rating": [details["rating"] for details in business_details],
# #             "Review Count": [details["review_count"] for details in business_details]
# #         })
# #         return df

# #     st.title("Review Query Chatbot")

# #     # Chat interface
# #     with st.form("chat_form"):
# #         user_input = st.text_input("Ask a question about reviews:")
# #         submit_button = st.form_submit_button("Send")

# #     # Process input when the form is submitted
# #     if submit_button and user_input:
# #         # Insert user query at the beginning of conversation history
# #         st.session_state['conversation_history'].insert(0, {"role": "user", "content": user_input})

# #         # Process the query
# #         response = get_query_embedding(user_input, st.session_state['conversation_history'],token)

# #         # Determine the bot response and update conversation history
# #         if 'error' in response:
# #             bot_response = f"Error: {response['error']}"
# #         elif 'business_details' in response:
# #             business_details = response['business_details']
# #             df = create_display_df(business_details)
            
# #             # Display DataFrame
# #             st.dataframe(df[['Name', 'Rating', 'Review Count']])

# #             # Interactive feature to display more details
# #             for idx, details in enumerate(business_details):
# #                 if st.button(f"More Details", key=idx):
# #                     # Display more details for the selected business
# #                     st.image(details["image_url"], width=300)
# #                     st.write(f"**Name:** {details['name']}")
# #                     st.write(f"**Rating:** {details['rating']} - **Review Count:** {details['review_count']}")
# #                     st.write(f"**Categories:** {', '.join(details['categories'])}")
# #                     st.write(f"**Address:** {details['display_address']}")
# #                     st.write(f"**Price Range:** {details.get('price', 'N/A')}")
# #                     st.write(f"**Phone:** {details['phone']}")
# #                     st.write(f"**URL:** [Yelp Page]({details['url']})")
# #         else:
# #             bot_response = "No matches found or unexpected response format."

# #         # Insert bot response at the beginning of conversation history
# #         st.session_state['conversation_history'].insert(0, {"role": "bot", "content": bot_response})

# #     # Display conversation history with the most recent messages first
# #     for message in st.session_state['conversation_history']:
# #         with st.expander(f"{message['role'].title()} says:", expanded=True):
# #             st.write(message["content"])

# def get_business_details(query, token):
#     headers = {"Authorization": f"Bearer {token}"}
#     response = requests.post(f"{BASE_URL}/embeddings", json={"query": query}, headers=headers)
#     if response.status_code == 200:
#         return response.json().get('business_details', [])
#     else:
#         st.error(f"Failed to fetch business details: {response.status_code} - {response.text}")
#         return []

# def create_display_df(business_details):
#     df = pd.DataFrame(business_details)
#     return df

# def show_chatbot(token):
#     st.title("Business Review Query Chatbot")
    
#     user_input = st.text_input("Enter your query about businesses:")
#     if st.button("Search"):
#         # Get business details based on the query
#         business_details = get_business_details(user_input, token)

#         if business_details:
#             # Display DataFrame
#             df = create_display_df(business_details)
#             st.dataframe(df)
#         else:
#             st.write("No business details found for your query.")


#########Original Code:
import streamlit as st
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("BASE_URL")

import streamlit as st
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("BASE_URL")

def show_chatbot(token):
    # Initialize session state for conversation history
    if 'conversation_history' not in st.session_state:
        st.session_state['conversation_history'] = []

    # Function to process the query and get response
    def get_query_embedding(query, token):
        headers = {"Authorization": f"Bearer {token}"}
        response = requests.post(f"{BASE_URL}/embeddings", json={"text": query}, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to fetch data: {response.status_code} - {response.text}")
            return None

    # Function to create DataFrame for display
    def create_display_df(business_details):
        df = pd.DataFrame({
            "Image": [details["image_url"] for details in business_details],
            "Name": [details["name"] for details in business_details],
            "Rating": [details["rating"] for details in business_details],
            "Review Count": [details["review_count"] for details in business_details]
        })
        return df

    st.title("Review Query Chatbot")

    # Chat interface
    with st.form("chat_form"):
        user_input = st.text_input("Ask a question about reviews:")
        submit_button = st.form_submit_button("Send")

    # Process input when the form is submitted
    if submit_button and user_input:
        # Insert user query at the beginning of conversation history
        st.session_state['conversation_history'].insert(0, {"role": "user", "content": user_input})

        # Process the query
        response = get_query_embedding(user_input, token)

        # Determine the bot response and update conversation history
        if 'error' in response:
            bot_response = f"Error: {response['error']}"
        elif 'business_details' in response:
            business_details = response['business_details']

            df = create_display_df(business_details)
            
            # Display DataFrame
            st.dataframe(df[['Name', 'Rating', 'Review Count']])

            # Interactive feature to display more details
            # for idx, details in enumerate(business_details):
            #     if st.button(f"More Details", key=idx):
            #         # Display more details for the selected business
            #         st.image(details["image_url"], width=300)
            #         st.write(f"**Name:** {details['name']}")
            #         st.write(f"**Rating:** {details['rating']} - **Review Count:** {details['review_count']}")
            #         st.write(f"**Categories:** {', '.join(details['categories'])}")
            #         st.write(f"**Address:** {details['display_address']}")
            #         st.write(f"**Price Range:** {details.get('price', 'N/A')}")
            #         st.write(f"**Phone:** {details['phone']}")
            #         st.write(f"**URL:** [Yelp Page]({details['url']})")

            bot_response = "Business details displayed."

        else:
            bot_response = "No matches found or unexpected response format."

        # Insert bot response at the beginning of conversation history
        st.session_state['conversation_history'].insert(0, {"role": "bot", "content": bot_response})

    # Display conversation history with the most recent messages first
    for message in st.session_state['conversation_history']:
        with st.expander(f"{message['role'].title()} says:", expanded=True):
            st.write(message["content"])
