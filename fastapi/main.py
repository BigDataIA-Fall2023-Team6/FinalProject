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