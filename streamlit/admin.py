import streamlit as st
import requests
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("BASE_URL")

def fetch_data(token):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{BASE_URL}/fetch_data", headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

def show_admin_page(token):
    st.title("Admin Dashboard")
    data = fetch_data(token)
    if data:
        data = pd.DataFrame(data)
        st.sidebar.title("Dashboard Navigation")
        choice = st.sidebar.radio("Choose a page:", ["User Registration Trends", "User Activity Patterns", "User Account Longevity"])

        if choice == "User Registration Trends":
            st.header("User Registration Trends")
            # Specify the format that matches your datetime strings
            data['created_at'] = pd.to_datetime(data['created_at'], format='mixed')
            data['created_at'] = data['created_at'].dt.date
            registration_counts = data['created_at'].value_counts().sort_index()
            st.line_chart(registration_counts)
        elif choice == "User Activity Patterns":
            st.header("User Activity Patterns")
            # Specify the format that matches your datetime strings
            data['last_login'] = pd.to_datetime(data['last_login'], format='mixed')
            data['last_login'] = data['last_login'].dt.date
            last_login_counts = data['last_login'].value_counts().sort_index()
            st.line_chart(last_login_counts)
        elif choice == "User Account Longevity":
            st.header("User Account Longevity")
            # Set the figure size to ensure it fits well within the page
            plt.figure(figsize=(9, 4))  # You can adjust these numbers as needed

            # Parse datetimes without specifying the format, let pandas infer it
            data['created_at'] = pd.to_datetime(data['created_at'], format='mixed')
            data['last_login'] = pd.to_datetime(data['last_login'], format='mixed')
            data['account_longevity'] = (data['last_login'] - data['created_at']).dt.days

            # Plot the histogram
            longevity_distribution = sns.histplot(data['account_longevity'], kde=True)
            plt.title("Distribution of Account Longevity (Days)")
            plt.xlabel("Days")
            plt.ylabel("Number of Users")

            # Adjust layout
            plt.tight_layout()

            # Show the plot in Streamlit
            st.pyplot(plt)
