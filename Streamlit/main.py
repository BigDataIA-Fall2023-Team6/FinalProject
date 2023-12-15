import os
import requests
import streamlit as st
from jose import jwt
import render_analytics_page
import semantic_search
from dotenv import load_dotenv
import admin
import chatbot

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
SECRET_KEY = os.getenv("SECRET_KEY_VALUE")
ADMIN_CODE = os.getenv("ADMIN_CODE_VALUE")
ALGORITHM = "HS256"

def decode_token(token):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except:
        return None

def register_user(username, password, account_type, admin_code=None):
    payload = {
        "username": username, 
        "password": password, 
        "account_type": account_type,
        "admin_code": admin_code
    }
    response = requests.post(f"{BASE_URL}/register", json=payload)
    return response

def login_user(username, password, account_type):
    if account_type == "Admin":
        endpoint = "/admin/login"
    else:
        endpoint = "/user/login"

    response = requests.post(
        f"{BASE_URL}{endpoint}",
        data={"username": username, "password": password}
    )
    return response



def signup():
    st.title("Sign Up")
    account_type = st.radio("Account Type", ["User", "Admin"])
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    confirm_password = st.text_input("Confirm Password", type="password")
    admin_code = st.text_input("Admin Code (only for admins)", type="password") if account_type == "Admin" else None

    if st.button("Sign up"):
        if password != confirm_password:
            st.error("Passwords do not match")
            return

        response = register_user(username, password, account_type, admin_code)
        if response.status_code == 200:
            st.success("Registration successful")
        else:
            st.error(response.json().get("message", "Something went wrong"))

def signin():
    st.title("Sign In")
    account_type = st.radio("Account Type", ["User", "Admin"])
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Sign in"):
        response = login_user(username, password, account_type)
        if response.status_code == 200:
            st.success("Sign in successful")
            token = response.json().get("access_token")
            st.session_state.token = token
            st.session_state.user_role = decode_token(token).get("role")
            st.experimental_rerun()
        else:
            st.error("Invalid credentials or something went wrong")

def main():
    st.set_page_config(page_title="Yelpobot Streamlit Application", layout="wide")
    st.sidebar.title("Welcome to Yelpobot!")

    token = st.session_state.get("token", None)
    user_role = st.session_state.get("user_role", None)
    decoded_token = decode_token(token) if token else None
    user_id = decoded_token.get("sub") if decoded_token else None


    # Define pages based on user role
    if user_id is not None:
        if user_role == "Admin":
            pages = {"Admin Dashboard": admin.show_admin_page}
        else:
            pages = {"Search": semantic_search.search_utility, "Chatbot": chatbot.show_chatbot ,"Analytics": render_analytics_page.render_analytics_dashboard}

        selection = st.sidebar.radio("Go to", list(pages.keys()) + ["Log Out"])
        if selection in pages:
            pages[selection](token)
        elif selection == "Log Out":
            st.session_state.clear()
            st.sidebar.success("Logged out successfully")
            st.experimental_rerun()
    else:
        selection = st.sidebar.radio("Go to", ["SignIn", "SignUp"])
        if selection == "SignIn":
            signin()
        elif selection == "SignUp":
            signup()

if __name__ == "__main__":
    main()