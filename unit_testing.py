from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch
from main import app, User, connect_to_db  # Ensure these are correctly imported

client = TestClient(app)

# Mock the database connection
def override_get_db():
    db = AsyncMock()
    db.fetchrow.side_effect = lambda *args, **kwargs: {"username": "testuser", "hashed_password": "fakehashedpassword", "account_type": "User"} if args[1] == "testuser" else None
    db.fetchval.return_value = 1  # Mock user_id return value
    return db

# Set the override in the dependency_overrides dictionary
app.dependency_overrides[connect_to_db] = override_get_db

# Create the test user outside the test function to avoid scope issues
test_user = User(username="testuser", password="fakehashedpassword", account_type="User")

def test_register_user():
    with patch('main.pwd_context.hash', return_value='fakehashedpassword'), \
         patch('main.pwd_context.verify', return_value=True):
        response = client.post(
            "/register",
            json={"username": "newuser", "password": "newpass", "account_type": "User"}
        )
        assert response.status_code == 200



def test_unauthorized_fetch_data():
    response = client.get("/fetch_data")
    assert response.status_code == 401

# Ensure that the tests can be run with pytest directly
if __name__ == "__main__":
    test_register_user()
    test_unauthorized_fetch_data()
