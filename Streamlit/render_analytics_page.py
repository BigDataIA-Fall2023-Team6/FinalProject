import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import os
from geopy.distance import geodesic
import math
import pydeck as pdk
import numpy as np

from dotenv import load_dotenv

load_dotenv()
BASE_URL = os.getenv("BASE_URL")

#########  FETCHING DATA FROM API ENDPOINTS  ############

def fetch_data(endpoint, token):
    headers = {"Authorization": f"Bearer {token}"}
    print("BASE_URL:", BASE_URL)
    response = requests.get(f"{BASE_URL}/{endpoint}", headers=headers)
    print(response)
    if response.status_code == 200:
        return response.json()
    else:
        st.error(f"Failed to fetch data: {response.status_code} - {response.text}")
        return None

#########  SHOWING DENSITY OF BUSINESSES  ############
    
def business_density_page(token):
    data = fetch_data("business_density_data", token)
    if data:
        df = pd.DataFrame(data)
        st.title("Density of Businesses")
        # Using latitude and longitude for plotting
        # Adding hover data to display additional information
        fig = px.scatter_mapbox(df, lat='LATITUDE', lon='LONGITUDE', hover_name='NAME', 
                                hover_data=['ADDRESS', 'CITY', 'POSTAL_CODE', 'STARS', 'CATEGORIES'],
                                color_discrete_sequence=["red"], zoom=10)  # Using a single color for all points
        fig.update_layout(mapbox_style="open-street-map")
        st.plotly_chart(fig)

#########  SHOWING BUSINESS COUNTS OF POPULAR CATEGORIES  ############

def clean_and_process_categories(df):
    # Remove newline characters, brackets, and quotes, then split into a list
    df['CATEGORIES'] = df['CATEGORIES'].str.replace(r'[\n\[\]"]', '', regex=True)
    df['CATEGORIES'] = df['CATEGORIES'].apply(lambda x: x.split(", ") if x else [])
    df_exploded = df.explode('CATEGORIES')
    df_exploded['CATEGORIES'] = df_exploded['CATEGORIES'].str.strip()
    print(df_exploded.head(5))
    return df_exploded

# Function for category popularity analysis with filters
def category_popularity_page(token):

    st.title("Famous Business Categories")

    # User inputs for city and postal code
    city_input = st.text_input("Enter City")
    postal_code_input = st.text_input("Enter Postal Code")

    # Button to apply filters
    filter_button = st.button("Apply Filters")

    data = fetch_data("category_popularity_data", token)
    if data:
        df = pd.DataFrame(data)

        # Clean and process the categories
        df_exploded = clean_and_process_categories(df)


        if filter_button:
            if city_input:
                df_exploded = df_exploded[df_exploded['CITY'].str.lower() == city_input.lower()]
            if postal_code_input:
                df_exploded = df_exploded[df_exploded['POSTAL_CODE'] == postal_code_input]
    
        # Count the occurrences of each category
        category_counts = df_exploded['CATEGORIES'].value_counts().reset_index()
        category_counts.columns = ['CATEGORY', 'BUSINESS_COUNT']
        
        # Plot the top categories
        fig = px.bar(category_counts.head(10), x='CATEGORY', y='BUSINESS_COUNT', title='Top 10 Popular Business Categories')
        st.plotly_chart(fig)

#########  FETCHING RESTAURANTS IN SPECIFIC RADIUS  ############

def calculate_distance(coords_1, coords_2):
    if any(math.isnan(coord) for coord in coords_1) or any(math.isnan(coord) for coord in coords_2):
        return float('nan')  # Return NaN if any of the coordinates are NaN.
    return geodesic(coords_1, coords_2).miles

def perform_search(zip_code, category, radius, token):

    data = fetch_data("fetch_business_data", token)
    if data:
        df = pd.DataFrame(data)

        # Search for the postal code and get the first match's latitude and longitude
        match = df[df['POSTAL_CODE'] == zip_code].head(1)

        if not match.empty:
            latitude = match['LATITUDE'].iloc[0]
            longitude = match['LONGITUDE'].iloc[0]
        else:
            latitude, longitude = None, None

        user_location = (latitude, longitude)

        if None in user_location:
            st.error("Invalid ZIP code or no data available for this ZIP code.")
            return


        # Clean the 'CATEGORIES' column
        df['CATEGORIES_LIST'] = df['CATEGORIES'].str.replace(r'[\n\[\]"\' ]', '', regex=True)
        
        df['CATEGORIES_LIST'] = df['CATEGORIES_LIST'].str.split(',')

        # Explode the list into separate rows
        df_exploded = df.explode('CATEGORIES_LIST')

        print(df_exploded)

        # Step 3: Filter rows where categories contain the specified category
        filtered_df = df_exploded[df_exploded['CATEGORIES_LIST'] == category]

        if filtered_df.empty:
            st.error("No businesses found for the selected category.")
            return

        filtered_df['distance'] = filtered_df.apply(
            lambda row: calculate_distance(user_location, (row['LATITUDE'], row['LONGITUDE'])), axis=1
        )

        nearby_businesses = filtered_df[filtered_df['distance'] <= radius]
        
        if not nearby_businesses.empty:
            best_rated_business = nearby_businesses.sort_values(by='STARS', ascending=False).head(1)
            st.write(f"Best rated business in Selected category:{category}:")
            st.dataframe(best_rated_business[['NAME', 'ADDRESS', 'CITY', 'STATE', 'STARS', 'distance']])
        else:
            st.write("No businesses found within the specified radius.")

        # # Display results on the map
        # st.map(nearby_businesses[['LATITUDE', 'LONGITUDE']])
        
        user_location_data = pd.DataFrame([{'latitude': latitude, 'longitude': longitude}])

        user_location_layer = pdk.Layer(
            'ScatterplotLayer',
            data=user_location_data,
            get_position='[longitude, latitude]',
            get_color='[200, 30, 0, 160]',  # Red color
            get_radius=200,  # Radius in meters
        )
        
        # Create a PyDeck layer for nearby businesses
        nearby_businesses_layer = pdk.Layer(
            'ScatterplotLayer',
            data=nearby_businesses,
            get_position='[LONGITUDE, LATITUDE]',
            get_color='[0, 0, 255, 160]',  # Blue color
            get_radius=100,  # Radius in meters
        )

        # Set the viewport location to center on the user's location
        view_state = pdk.ViewState(
            latitude=latitude,
            longitude=longitude,
            zoom=11,
            pitch=0,
        )

        # Render the map with both layers
        r = pdk.Deck(layers=[user_location_layer, nearby_businesses_layer], initial_view_state=view_state)
        st.pydeck_chart(r)

    else:
        st.error("No businesses found within the specified radius.")

def find_best_restaurants(token):
    st.title("Cuisine Search Analytics")
    # Input fields with default values
    zip_code = st.text_input("Enter your ZIP code:", value='19107')
    radius = st.number_input("Enter radius in miles:", min_value=1, max_value=50, value=5)
    category = st.text_input("Enter a category (e.g., Indian, Italian, Chinese):", value='Indian')

    # Button to find restaurants
    if st.button("Find Restaurants"):
        # Use the input values directly in the search
        perform_search(zip_code, category, radius, token)


#########  SHOWING RESTAURANTS RATINGS SENTIMENT  ############

def show_sentiments(city_input, token):
    data = fetch_data("fetch_business_data", token)
    if data:
        df = pd.DataFrame(data)

        # Filter DataFrame based on selected city
        city_df = df[df['CITY'] == city_input]

        # Group by STARS and count the number of reviews
        review_counts = city_df.groupby('STARS').size().reset_index(name='REVIEW_COUNT')

        # Classify reviews based on star ratings
        review_counts['SENTIMENT'] = pd.cut(review_counts['STARS'], bins=[0, 2, 4, 5], labels=['Negative', 'Neutral', 'Positive'], right=False)

        # Aggregate counts by sentiment
        sentiment_counts = review_counts.groupby('SENTIMENT')['REVIEW_COUNT'].sum().reset_index()

        # Plot sentiment distribution
        fig_sentiment = px.pie(sentiment_counts, values='REVIEW_COUNT', names='SENTIMENT', title=f'Sentiment Distribution in {city_input}')
        st.plotly_chart(fig_sentiment)

        # Aggregate counts by star ratings
        star_counts = review_counts.groupby('STARS')['REVIEW_COUNT'].sum().reset_index()

        # Plot star rating distribution
        fig_star = px.pie(star_counts, values='REVIEW_COUNT', names='STARS', title=f'Star Rating Distribution in {city_input}')
        st.plotly_chart(fig_star)


def sentiment_analysis_page(token):

    st.title("Sentiment Analysis")

    # User inputs for city and postal code
    city_input = st.text_input("Enter City")

    if st.button("Show Charts"):
        # Use the input values directly in the search
        show_sentiments(city_input, token)

        
################  SUGGESTION IMPLEMENTATION LOGIC  ####################
        
def analyze_business(business_df, category, selected_city, num_clusters=5):
    # Clean the 'CATEGORIES' column
    business_df['CATEGORIES_LIST'] = business_df['CATEGORIES'].str.replace(r'[\n\[\]"\' ]', '', regex=True)
    business_df['CATEGORIES_LIST'] = business_df['CATEGORIES_LIST'].str.split(',')

    # Explode the list into separate rows
    df_exploded = business_df.explode('CATEGORIES_LIST')

    # Filter rows where categories contain the specified category
    filtered_df_cat = df_exploded[df_exploded['CATEGORIES_LIST'] == category]

    # Use filtered_df_cat for city filtering
    filtered_df = filtered_df_cat[filtered_df_cat['CITY'] == selected_city]

    if filtered_df.empty:
        return "No businesses found for the selected category in the specified city."

    # Calculate average rating and review count
    filtered_df['WEIGHTED_RATING'] = filtered_df['STARS'] * filtered_df['REVIEW_COUNT']
    city_business_stats = filtered_df.groupby('POSTAL_CODE').agg(
        avg_rating=('WEIGHTED_RATING', lambda x: np.sum(x) / np.sum(filtered_df.loc[x.index, 'REVIEW_COUNT'])),
        total_reviews=('REVIEW_COUNT', 'sum'),
        business_count=('BUSINESS_ID', 'count')
    ).reset_index()

    # Sort by avg_rating and total_reviews to recommend the best location
    recommendations = city_business_stats.sort_values(by=['avg_rating', 'total_reviews'], ascending=[True, False])

    return recommendations

def recommendation_system(token):
    st.title("Yelp Business Recommendation System")

    data_business = fetch_data("fetch_business_data", token)
    if data_business:
        business_df = pd.DataFrame(data_business)

    # User inputs
    selected_city = st.selectbox("Select a city", business_df['CITY'].unique())
    category = st.text_input("Enter a business category (e.g., Indian, Italian, Chinese)", value='Indian')

    if st.button("Recommend Location"):
        if category:
            business_data = analyze_business(business_df, category, selected_city)
            st.write(business_data)
        else:
            st.error("Please enter a business category.")

#######################################################################


def render_analytics_dashboard(token):
    st.sidebar.title("Analytics Dashboard")
    page = st.sidebar.radio("Choose a page:", ["Business Density", "Category Popularity", "Restaurant Specifc Cuisine", "Sentiment Analysis", "Recommendation"])

    if page == "Business Density":
        business_density_page(token)
    elif page == "Category Popularity":
        category_popularity_page(token)
    elif page == "Restaurant Specifc Cuisine":
        find_best_restaurants(token)
    elif page == "Sentiment Analysis":
        sentiment_analysis_page(token)
    elif page == "Recommendation":
        recommendation_system(token)