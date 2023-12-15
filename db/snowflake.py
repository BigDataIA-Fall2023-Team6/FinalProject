import snowflake.connector
from snowflake.connector import DictCursor
import pandas as pd
import streamlit as st
from langchain.sql_database import SQLDatabase


def get_snowflake_connection(secrets):
    # Create a connection to Snowflake
    # Create a connection using the passed secrets
    ctx = snowflake.connector.connect(
        user=secrets["user"],
        password=secrets["password"],
        account=secrets["account"],
        warehouse=secrets["warehouse"],
        role=secrets["role"],
        database=secrets["database"],  # Now specifying the database
        schema=secrets["schema"],
    )
    # ctx.cursor().execute(f"USE DATABASE {secrets['database']}")
    return ctx


def get_table_names(secrets):
    # Open connection
    conn = get_snowflake_connection(secrets)
    try:
        with conn.cursor() as cur:
            print("inside this function")
            # Extract the database and schema names from secrets
            database_name = secrets["database"]
            schema_name = secrets["schema"]

            # SQL query that dynamically uses the database and schema names
            query = f"""
                SELECT table_name FROM {database_name}.information_schema.tables 
                WHERE table_schema = '{schema_name}'
            """
            cur.execute(query)

            # Process the results
            items = [
                row[0] for row in cur
            ]  # Using row[0] to access the table/view name

            return items
    finally:
        conn.close()


def get_table_schema(secrets, table_names):
    # Construct the Snowflake URL
    snowflake_url = (
        f"snowflake://{secrets['user']}:{secrets['password']}@"
        f"{secrets['account']}/{secrets['database']}/{secrets['schema']}?"
        f"warehouse={secrets['warehouse']}&role={secrets['role']}"
    )

    table_names = [table.lower() for table in table_names]
    # print(table_names)

    print(snowflake_url)
    # Initialize SQLDatabase instance with Snowflake URL
    db = SQLDatabase.from_uri(
        snowflake_url,
        # schema="PUBLIC",
        sample_rows_in_table_info=0,
        include_tables=table_names,  # Use the provided list of table names
        view_support=True,
    )
    table_info = db.get_table_info()
    # print(table_info)
    # Retrieve table information (DDL) to include in the prompt
    return db.get_table_info()


def execute_query(secrets, sql_query):
    # Open connection
    conn = get_snowflake_connection(secrets)
    try:
        with conn.cursor() as cur:
            # Execute the SQL query
            cur.execute(sql_query)
            print(sql_query)
            # Fetch the results
            rows = cur.fetchall()
            print(rows)
            # Get the column names
            column_names = [col[0] for col in cur.description]

            # Construct a pandas DataFrame from the query result
            df = pd.DataFrame(rows, columns=column_names)

            return df
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        conn.close()