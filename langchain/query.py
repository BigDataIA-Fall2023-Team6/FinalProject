from langchain.sql_database import SQLDatabase
from langchain.llms import OpenAI
from langchain.chains import create_sql_query_chain

FS_TEMPLATE = """ 
-You are an expert SQL developer querying a Snowflake database for flight information. Your job is to write SQL code based on users' questions.
-Only include the SQL, not the thought process. If you don't know the exact column names, use placeholders and mention that in the comments. 
-In case of flights, generally the status can be Delayed, OnTime or Early. Late and Delayed can be used by user interchangeably. Similarly for OnTime and Early, similar words can be used.  
-User will be passing the DDL commands that is the CREATE STATEMENT of the table as well. 
-If a question is ambiguous or lacks detail, ask for clarification. Adhere strictly to the column names provided in the user's DDL commands.
-If you think User's question is not relevant to querying tables using SQL, then just answer "I don't know".

Question: {question}

SQL: ```sql ``` \n
"""


import streamlit as st


def generate_sql_query(question, tables):
    # # Retrieve Snowflake credentials from Streamlit's secrets
    snowflake_credentials = st.secrets["connections"]["snowpark"]
    openai_api_key = st.secrets["openai_api_key"]
    # print(tables)
    # Construct the Snowflake URL
    snowflake_url = (
        f"snowflake://{snowflake_credentials['user']}:{snowflake_credentials['password']}@"
        f"{snowflake_credentials['account']}/{snowflake_credentials['database']}/{snowflake_credentials['schema']}?"
        f"warehouse={snowflake_credentials['warehouse']}&role={snowflake_credentials['role']}"
    )

    table_names = [table.lower() for table in tables]
    # Initialize SQLDatabase instance with Snowflake URL
    db = SQLDatabase.from_uri(
        snowflake_url,
        sample_rows_in_table_info=0,
        include_tables=table_names,  # add more tables if you think query can help
        view_support=True,
    )

    # # Retrieve table information to include in the prompt
    table_info = db.get_table_info()
    # print(table_info)
    # # Format the custom prompt with the question, context, and database tables info

    prompt = FS_TEMPLATE.format(question=question)

    # Initialize the OpenAI instance with the API key from secrets
    llm = OpenAI(
        openai_api_key=openai_api_key,
    )
    print(prompt)
    # Use LangChain to process the natural language query with the custom prompt
    database_chain = create_sql_query_chain(llm, db)
    sql_query = database_chain.invoke({"question": prompt})
    # print(sql_query)
    return sql_query