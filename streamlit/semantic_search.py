import streamlit as st
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv
from st_files_connection import FilesConnection
import boto3
from PIL import Image
import io

# Load environment variables
load_dotenv()


# Function to execute a query in Snowflake and return results and column names
def query_snowflake(sql_query):
    conn_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA")
    }
    conn = snowflake.connector.connect(**conn_params)
    try:
        cursor = conn.cursor()
        print("hi")
        cursor.execute(sql_query)
        result_set = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return result_set, columns
    finally:
        cursor.close()
        conn.close()

query_options = {
    "Something with Cheese": "rev.text LIKE '%cheese%'",
    "Weekend getaway eataway": "rev.text LIKE '%weekend%'",
    "Pizza Time": "rev.text LIKE '%Pizza%'",
    "Aesthetic": "rev.text LIKE '%decor%'",
    "Sushi all the way": "rev.text LIKE '%sushi%'",
    "Date night with your girlfriend?": "(rev.text LIKE '%girlfriend%' or rev.text LIKE '%date%')",
    "Let's hangout with friends": "rev.text LIKE '%friends%'"
}

def run_query(condition):
    sql_query = f"""
        SELECT distinct res.name, res.address
        FROM REVIEWS AS rev
        INNER JOIN BUSINESS AS res
        ON rev.business_id = res.business_id
        WHERE  {condition} and res.stars > 3.5 and res.review_count > 25
        LIMIT 10;
    """
    return get_dataframe_from_query(sql_query)


# Function to convert query results to a Pandas DataFrame
def get_dataframe_from_query(sql_query):
    data, columns = query_snowflake(sql_query)
    df = pd.DataFrame(data, columns=columns)
    return df

# Streamlit app
def search_utility(token):
    # Display the image at the top of the page

    
    image_url = "https://yelp-data-files.s3.us-east-1.amazonaws.com/yelpobotexplorer.png?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEPn%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJHMEUCIEbGYk1NPZ4HkrccoIMT3ByXiMoSmnIW8gbMU1Ld%2BQWxAiEA%2Bc4nb6tOK2krmMbm0LGFwODhxoa8g01CP6sQ6ZRzaQAq%2BwIIchAAGgw2MTQ3MjMzMzMzMDMiDCUbgwl0%2Ft6bHxBe4yrYAmwYdJE%2BQF6StQsLZjWvNsCVdmO4gKkOX6mR%2B%2B%2F0%2BtzjSaECCZABqS1FtAl3KvDdy5HLTuD%2BH6NujEQydCweboVtbIqAXzJPeYRoFi2M5AtZMFeGNvsNJIo%2BgtrsNKMTmFxrZKqTFW2j1Zp7c4ehLVF%2BXdnRFAPe66ZE%2FEbMpyd1NSfwQtSUve5JmMJwYTvJt7zEe363kvenus%2Fk1Pu4j0mhPNVvORZcR4CtGqygRiRMv9kBxpGYW3VphyjYkRA3CPoRs3CnfgjfXedshHdih7Qw5bcDUQrmwvUejCt4dmgPKEJsi%2Fi35eUP9KbwqgIJvlNwF1zYYovOyFM9hRzh9N1SuHXVB0S2XSN%2BcijyvYmn12b%2FPUXSUPvITNxuZllxJAnegzwR%2BMa52jUoUPGzJCkSenbyGUkqXNKeLUy7QjS%2FNi0P%2ByWUoQpLHGHvgS09HL15A9Xt4fUpMJKn8KsGOrMC1wq8T2GrHgv3%2BGhAAdHXJbUprF2u07QHYwQcbYYaRkkUj6q%2Bz8S9MDEQzRMVSffvGYeNGt%2BF90V6Q9htM%2FUCaK4%2FDJZMATge4nbGr0bEsn4zLdWPNCQvmYLfw%2FkG2E5I3m6I%2BNcvTm8DeygrSdr%2FAsg5IwYaizbdSy9zHqiX1MLvGOfDRd27FDJMmUUFgQDT5ba1MrEI16RFBJN9GOCPIChev1d2vvmo0mKZVj4RJZH52mgc6ImjnEszIXYXe%2FhPoTe%2Ba36ccWN570PV9nQBKWLGOyxcu6IAZsjNHSWN72foXjTS40vGDuR7sr32i1PlBnR9qVNt2kOnNZV%2BwohbPTkK2ky5KY1lnfVpHCfGORK4tivO8I4Hv0EkaujlkoOokUtMdmzLX5EkLQHD%2B7Fv0kWW0Q%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20231215T111534Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAY6IC5VS3RU7AKQUQ%2F20231215%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=2c1b9022f0dfefe5d492e96eb476abb5250fe1b19303c637952a4459ba924861"
    caption="Welcome to Yelpobot!"
    # Using Markdown with custom HTML to center the image
    st.markdown(f"<div style='text-align: center'><img src='{image_url}' width='500'><p>{caption}</p></div>", unsafe_allow_html=True)


    st.title("We are currently operational in Pennsylvania State")
    st.title("Restaurant Filter Search")

    # Dropdown for City
    city = st.selectbox('City', ('',
        'Philadelphia', 'Downingtown', 'Newtown', 'Southampton', 'Primos', 'Bridgeport',
        'Wayne', 'Springfield', 'Kimberton', 'West Chester', 'Broomall', 'King of Prussia',
        'Norristown', 'Narberth', 'Warminster', 'Phoenixville', 'Schwenksville', 'Bala Cynwyd',
        'New Hope', 'Richboro', 'Hatfield', 'Media', 'Ardmore', 'Feasterville-Trevose', 'Bensalem'
    ), index=0)

    # Dropdown for Postal Code
    postal_code = st.selectbox('Postal Code', ('',
        '19147', '19152', '19107', '19139', '19335', '19122', '19126', '18940', '19355',
        '19018', '19130', '19125', '19134', '19111', '19405', '19442', '19382', '19176',
        '19301', '19146', '19008', '19380', '19128', '19100', '19403', '18974', '19460',
        '19473', '19104', '19004'
    ), index=0)

    # Dropdown for Open/Closed
    is_open = st.selectbox('Is Open', ('','Yes', 'No'), index=0)
    is_open_value = '1' if is_open == 'Yes' else '0'
    
    # Search button
    if st.button('Search', key='filter_search'):
        conditions = []
        if city:
            conditions.append(f"res.city = '{city}'")
        if postal_code:
            conditions.append(f"res.postal_code = '{postal_code}'")
        if is_open:
            is_open_value = '1' if is_open == 'Yes' else '0'
            conditions.append(f"res.is_open = {is_open_value}")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        query = f"""
            SELECT res.name, res.city, res.postal_code 
            FROM BUSINESS res 
            WHERE res.stars > 4 AND res.review_count > 25 AND {where_clause}
            LIMIT 20;
        """

        # Execute the query
        try:
            df = get_dataframe_from_query(query)
            if not df.empty:
                st.subheader("Search Results:")
                st.dataframe(df)
            else:
                st.info("No results found for the selected criteria.")
        except Exception as e:
            st.error(f"An error occurred: {e}")    
    
    st.title("Confused??......Let us help you out")
    
    
    # Let the user select the query condition
    query_description = st.radio("Choose a theme for your restaurant search:", list(query_options.keys()))
    
    # Fetch the corresponding SQL condition
    selected_condition = query_options[query_description]
    
    if st.button('Show Results', key='themed_search'):
        # Run the query and get the results
        df = run_query(selected_condition)
        # Display the results
        st.dataframe(df)
    
    st.title("PA Restaurants Dashboard")

    tab1, tab2 = st.tabs(["Restaurant Overview", "Category Analysis"])

    with tab1:
        st.subheader("Top Rated Restaurants")
        sql_query = """
            SELECT NAME, CITY, STARS, REVIEW_COUNT, LATITUDE, LONGITUDE
            FROM BUSINESS
            WHERE STATE = 'PA' AND IS_OPEN = 1
            ORDER BY STARS DESC, REVIEW_COUNT DESC
            LIMIT 50;
        """

        df = get_dataframe_from_query(sql_query)
        
        # Create a placeholder for the map
        map_placeholder = st.empty()
        # Initially display the map with all data points
        map_placeholder.map(df[['LATITUDE', 'LONGITUDE']])
        
        # Create a table with selectable rows
        selected_names = st.multiselect("Select restaurants to highlight on map:", options=df.index, format_func=lambda x: df['NAME'][x])
        selected_rows = df.loc[selected_names]
        
        # Highlight the selected restaurants on the map
        if not selected_rows.empty:
            map_placeholder.map(selected_rows[['LATITUDE', 'LONGITUDE']])

        # Plotting top rated restaurants
        
        st.dataframe(df)
    with tab2:
        st.subheader("Restaurant Categories Analysis")
        sql_query = """
            SELECT CATEGORIES, COUNT(*) AS RESTAURANT_COUNT
            FROM BUSINESS
            WHERE STATE = 'PA'
            GROUP BY CATEGORIES
            ORDER BY RESTAURANT_COUNT DESC
            LIMIT 10;
        """

        df = get_dataframe_from_query(sql_query)
        st.dataframe(df)

        # Plotting restaurant categories
        plt.figure(figsize=(12, 6))
        plt.bar(df['CATEGORIES'], df['RESTAURANT_COUNT'], color='tomato')
        plt.xticks(rotation=45, ha='right')
        plt.ylabel('Number of Restaurants')
        plt.title('Most Common Restaurant Categories in PA')
        st.pyplot(plt)