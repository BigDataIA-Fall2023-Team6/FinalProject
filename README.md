#YelpoBot Explorer
##Aplication Links
[Codelabs](https://codelabs-preview.appspot.com/?file_id=16AosZYTQGxKFyeH_5Vbqt3KUWfzX4Q8RcAOfVA1nRVA)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](http://34.75.198.16:8080/home)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi)](http://34.75.198.16:8000/docs)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit&logoColor=white)](http://34.75.198.16:8090/)

##Abstract

YelpoBot Explorer is a cutting-edge Data-as-a-Service platform designed to empower businesses to leverage vast quantities of data from Yelp for enhanced decision-making. Integrating the Yelp Fusion API, Apache Airflow, Snowflake, OpenAI, Milvus, and Streamlit with FastAPI, the platform orchestrates a seamless flow of data from collection to insightful analytics. It transcends traditional data processing limitations by employing AI-driven insights and real-time processing capabilities, providing a unified solution for data aggregation, analysis, and interactive exploration.

The platform's core advantage lies in its user-centric design, which simplifies the data analytics process, making it accessible to both technical and non-technical users. YelpoBot Explorer offers real-time insights, trend analysis, and predictive analytics that are crucial for strategic planning across marketing, customer experience, product development, and operational efficiency. By harnessing advanced technologies in unison, YelpoBot Explorer serves as a beacon for businesses to navigate the data-rich landscape of the modern digital marketplace.

##Architecture Diagram


![Alt Text](ArchitectureDiagram.png)


## Getting Started

1. Clone the project repository:

   ```bash
   git clone https://github.com/BigDataIA-Fall2023-Team6/FinalProject
2. Navigate to the project directory:
    ```bash
   cd FinalProject
3. Create a .env file and add the following environment variables:
   ```bash
   AIRFLOW_UID = XXXX
   OPENAI_KEY = "XXXX"
   PINECONE_API_KEY = "XXXX"

   OPENAI_KEY=XXX
   SNOWSQL_ACCOUNT=XXX
   SNOWSQL_USER=XXX
   SNOWSQL_PWD=XXX
   SNOWSQL_ROLE=XXX
   SNOWSQL_WAREHOUSE=XXX
   SNOWSQL_DATABASE=XXX
   SNOWSQL_SCHEMA=XXX
4. Run Docker compose for initializing and running the containers for Airflow, FastAPI and Streamlit.
   ```bash
   docker compose up -d 
5. Watch the docker containers for every 2 seconds:
   ```bash
   watch -n 2 docker ps
6. Once all the Airflow containers are healthy then navigate to the port 8080 i.e. 0.0.0.0:8080
7. Login to the Airflow dashboard.
8. Execute the 1st DAG "Yelp_Business_to_Snowflake" to load the datafrom from staging area that is Google Cloud Storage to Snowflake.
9. Execute the 2nd DAG "Yelp_Review_to_Snowflake" to load the data from from staging area that is Google Cloud Storage to Snowflake.
10. Execute the 3rd DAG "Yelp_API" to load the data from from staging area that is Google Cloud Storage to Snowflake.
11. Execute the first DAG "batch_process_reviews.py" for generating the embeddings for the review data stored in snowflake and wait till the completion of this pipeline.
12. Run the  DAG for loading the embeddings and the metadata into the Pinecone vector database and wait till the completion of this pipeline.
13. Head to Pinecone Database and check wheather the embeddings and the metadata are being stored as per the Airflow pipeline execution.
14. Now you can access the main streamlit application which should be running with the help of docker container and you can start using it by registration yourself if you are first time using it and login with your credentials.
15. The login is maintained by session state set to 30mins, hence you will have to login again after 30mins to continue using the application.
16. You can perform semantic search operation or filter through the embeddings and metadata saved on the Pinecone vector database.




![Alt Text](Yelp_Business_to_Snowflake.png)
![Alt Text](YelpAPI.png)

## Application Snippets

![Alt Text](YelpDashBoard.png)
![Alt Text](YelpDashboard_01.png)
![Alt Text](YelpDashboard_03.png)




## Project Structure

```text
Assignment_2
├── 01-snowflake_step/
├── Fast_API/
│   ├── Dockerfile
│   ├── chatbot.py
│   ├── chatbot.py
│   ├── main.py
│   ├── requirements.txt
├── Streamlit/
│   ├── Dockerfile
│   ├── admin.py
│   ├── requirements.txt
│   ├── envexample.env
│   ├── main.py
│   ├── render_analytics_page.py
│   ├── semantic_search.py
├── airflow/
│   ├── .gitignore
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── dags/
│       ├── 01_business_to_snowflake.py
│       ├── 02_review_to_snowflake.py
│       ├── 03_API_to_snowflake.py
│       └── 04_review_embeddings.py
├── streamlit
│   ├── main.py
├── .gitignore
├── README.md
└── docker-compose.yaml
```





> WE ATTEST THAT WE HAVEN’T USED ANY OTHER STUDENTS’ WORK IN OUR ASSIGNMENT AND ABIDE BY THE POLICIES LISTED IN THE STUDENT HANDBOOK.