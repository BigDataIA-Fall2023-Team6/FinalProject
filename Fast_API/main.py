from pydantic import BaseModel
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import jwt
from passlib.context import CryptContext
from datetime import datetime, timedelta
import asyncpg
import os
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

import spacy
import pinecone
from collections import Counter
import requests

load_dotenv()

app = FastAPI()

SECRET_KEY = os.getenv('SECRET_KEY_VAR')
ADMIN_CODE = os.getenv("ADMIN_CODE_VALUE")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

class User(BaseModel):
    username: str
    password: str
    account_type: str = "User"
    admin_code: Optional[str] = None


class Token(BaseModel):
    access_token: str
    token_type: str

async def connect_to_db():
    conn = await asyncpg.connect(
        user=os.getenv('USERNAME'),
        password=os.getenv('PASSWORD'),
        database=os.getenv('DBNAME'),
        host=os.getenv('ENDPOINT'),
        port=5432
    )
    return conn

def load_data_sync(query):
    try:
        with snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            role=os.getenv('SNOWFLAKE_ROLE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                df = pd.DataFrame(cur.fetchall(), columns=[col[0] for col in cur.description])
                return df
    except Exception as e:
        print(f"Error occurred: {e}")
        # Re-raise the exception for the caller to handle
        raise

@app.on_event("startup")
async def startup_db():
    conn = await connect_to_db()
    await create_tables(conn)

async def create_tables(conn):
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            password TEXT NOT NULL,
            account_type VARCHAR(10) NOT NULL DEFAULT 'User',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS admin (
            admin_usr VARCHAR(50) UNIQUE NOT NULL,
            pass TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(oauth2_scheme), conn = Depends(connect_to_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        user = await conn.fetchrow("SELECT * FROM users WHERE username = $1", username)
        if user is None:
            raise credentials_exception
        return User(username=user['username'], password=user['password'], account_type=user['account_type'])
    except jwt.JWTError:
        raise credentials_exception

@app.post("/register")
async def register_user(user: User, conn = Depends(connect_to_db)):
    hashed_password = pwd_context.hash(user.password)

    if user.account_type == "User":
        # Insert user data into the 'users' table for regular users
        user_query = "INSERT INTO users (username, password, account_type) VALUES ($1, $2, $3) RETURNING user_id"
        user_id = await conn.fetchval(user_query, user.username, hashed_password, user.account_type)
        return {"message": "User registration successful"}

    elif user.account_type == "Admin":
        # Handle admin registration
        if user.admin_code != ADMIN_CODE:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid admin code")
        
        # Insert record into 'admin' table
        admin_query = "INSERT INTO admin (admin_usr, pass) VALUES ($1, $2)"
        await conn.execute(admin_query, user.username, hashed_password)
        return {"message": "Admin registration successful"}

    else:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid account type")


class LoginRequest(BaseModel):
    username: str
    password: str
    account_type: str

@app.post("/admin/login", response_model=Token)
async def login_admin(form_data: OAuth2PasswordRequestForm = Depends(), conn = Depends(connect_to_db)):
    username = form_data.username
    password = form_data.password

    # Fetch the admin user from the 'admin' table
    admin_user = await conn.fetchrow("SELECT * FROM admin WHERE admin_usr = $1", username)

    # Verify the admin user's password
    if admin_user is None or not pwd_context.verify(password, admin_user['pass']):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    # Create an access token for the admin
    access_token = create_access_token(data={"sub": username, "role": "Admin"})
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/user/login", response_model=Token)
async def login_user(form_data: OAuth2PasswordRequestForm = Depends(), conn = Depends(connect_to_db)):
    username = form_data.username
    password = form_data.password

    # Fetch the regular user from the 'users' table
    user = await conn.fetchrow("SELECT * FROM users WHERE username = $1", username)

    # Verify the user's password
    if user is None or not pwd_context.verify(password, user['password']):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect username or password")

    # Update last_login timestamp for the user
    await conn.execute("UPDATE users SET last_login = $1 WHERE username = $2", datetime.utcnow(), username)

    # Create an access token for the user
    access_token = create_access_token(data={"sub": username, "role": "User"})
    return {"access_token": access_token, "token_type": "bearer"}


# FastAPI endpoint to fetch data
@app.get("/fetch_data")
async def fetch_data(user: User = Depends(get_current_user), conn = Depends(connect_to_db)):
    query = "SELECT user_id, username, account_type, created_at, last_login FROM users;"
    rows = await conn.fetch(query)
    return [dict(row) for row in rows] 


@app.get("/business_density_data")
def business_density_data(token: str = Depends(oauth2_scheme)):
    try:
        query = """
            SELECT NAME, ADDRESS, CITY, POSTAL_CODE, LATITUDE, LONGITUDE, STARS, CATEGORIES
            FROM BUSINESS
            GROUP BY NAME, ADDRESS, CITY, POSTAL_CODE, LATITUDE, LONGITUDE, STARS, CATEGORIES
        """
        result = load_data_sync(query=query)
        if not result.empty:
            data = result.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
            return data
        else:
            return []  # Return an empty list if there are no results
    except Exception as e:
        print(f"Error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/category_popularity_data")
def category_popularity_data(token: str = Depends(oauth2_scheme)):
    try:
        query = "SELECT CITY, POSTAL_CODE, CATEGORIES FROM BUSINESS"
        result = load_data_sync(query=query)
        if not result.empty:
            data = result.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
            return data
        else:
            return []  # Return an empty list if there are no results
    except Exception as e:
        print(f"Error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/fetch_business_data")
def fetch_business_data(token: str = Depends(oauth2_scheme)):
    try:
        query = "SELECT * FROM BUSINESS"
        result = load_data_sync(query=query)
        if not result.empty:
            data = result.to_dict(orient='records')  # Convert DataFrame to a list of dictionaries
            return data
        else:
            return []  # Return an empty list if there are no results
    except Exception as e:
        print(f"Error occurred: {e}")
        raise HTTPException(status_code=500, detail=str(e))  

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
YELP_API_KEY = os.getenv("YELP_API_KEY")

if not PINECONE_API_KEY:
    raise EnvironmentError("PINECONE_API_KEY not found in environment variables.")
if not YELP_API_KEY:
    raise EnvironmentError("YELP_API_KEY not found in environment variables.")

# Load the SpaCy model
nlp = spacy.load("en_core_web_md")

# Initialize Pinecone
pinecone.init(api_key=PINECONE_API_KEY, environment="gcp-starter")
index = pinecone.Index("yelpreviewembeddings")

# Request model
class Query(BaseModel):
    text: str

# Function to get business details from Yelp API
def get_yelp_business_details(business_id: str):
    url = f"https://api.yelp.com/v3/businesses/{business_id}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {YELP_API_KEY}"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses

        # Extract the necessary data from the Yelp API response
        data = response.json()
        simplified_data = {
            "name": data.get("name"),
            "image_url": data.get("image_url"),
            "is_closed": data.get("is_closed"),
            "url": data.get("url"),
            "phone": data.get("phone"),
            "review_count": data.get("review_count"),
            "categories": [category["title"] for category in data.get("categories", [])],
            "rating": data.get("rating"),
            "display_address": ", ".join(data.get("location", {}).get("display_address", [])),
            "price": data.get("price"),
            "transactions": data.get("transactions", [])
        }
        return simplified_data
    except requests.exceptions.RequestException as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint to create embeddings and query Pinecone
@app.post("/embeddings/")
async def query_reviews(query: Query, token: str = Depends(oauth2_scheme)):
    # Create embedding from the query text
    doc = nlp(query.text)
    embedding = doc.vector.tolist()
    # Query Pinecone with the generated embedding
    try:
        query_result = index.query(embedding, top_k=50, include_metadata=True)

        if 'matches' in query_result:
            matches = query_result['matches']
            # Count occurrences of each BUSINESS_ID
            business_id_counts = Counter(match['metadata']['BUSINESS_ID'] for match in matches)

            # Get top 10 BUSINESS_IDs with the highest count
            top_5_business_ids = business_id_counts.most_common(5)
            print(top_5_business_ids)
            # Get details from Yelp for the most frequent businesses
            yelp_details = []
            for business_id in top_5_business_ids:  # Limit to top 10 results
                try:
                    details = get_yelp_business_details(business_id)
                    yelp_details.append(details)
                except HTTPException as http_err:
                    # Handle error (e.g., log it, return partial data with error info, etc.)
                    continue

            return {"business_details": yelp_details}
        else:
            return {"error": "Unexpected response structure from Pinecone"}

    except Exception as e:
        return {"error": str(e)}
    
class Query(BaseModel):
    text: str

import openai
openai.api_key = 'API_KEY'

@app.post("/chatbot/")
async def chatbot_endpoint(query: Query):
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": f"Answer about Resturants in Pennsylvania, Just Give me 5 output resturants with their review counts and Rating in numeric values, Restrict it to 400 words: {query.text}"}]
        )
        return {"response": response.choices[0].message["content"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))