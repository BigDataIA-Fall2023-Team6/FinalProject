from fastapi.testclient import TestClient
from Fast_API import app  # Replace 'your_application_file' with the name of your FastAPI app file
import pytest

client = TestClient(app)

def test_read_main():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}  # Replace with your expected response

def test_register_user():
    response = client.post("/register", json={"username": "testuser", "password": "testpass"})
    assert response.status_code == 200
    # Add assertions based on expected behavior

def test_login_user():
    # You need to adjust this test to reflect your login logic
    response = client.post("/user/login", data={"username": "testuser", "password": "testpass"})
    assert response.status_code == 200

@pytest.mark.asyncio
async def test_fetch_data():
    response = client.get("/fetch_data")
    assert response.status_code == 200
    # Add more assertions based on your expected response

@pytest.mark.asyncio
async def test_business_density_data():
    response = client.get("/business_density_data")
    assert response.status_code == 200
    # Additional assertions as needed

@pytest.mark.asyncio
async def test_category_popularity_data():
    response = client.get("/category_popularity_data")
    assert response.status_code == 200
    # Additional assertions as needed

@pytest.mark.asyncio
async def test_fetch_business_data():
    response = client.get("/fetch_business_data")
    assert response.status_code == 200
    # Additional assertions as needed

@pytest.mark.asyncio
async def test_embeddings_api():
    # Sample test data
    test_query = {"text": "Example query text"}

    # Send post request to embeddings endpoint
    response = client.post("/embeddings/", json=test_query)
    assert response.status_code == 200