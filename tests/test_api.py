from fastapi.testclient import TestClient
from scripts.api import app

client = TestClient(app)


def test_home():
    response = client.get("/")

    assert response.status_code == 200
    assert response.json()["message"] == "Urban Analytics API is running"


def test_weather():
    response = client.get("/weather/berlin")

    assert response.status_code == 200

    data = response.json()

    assert data["city"] == "Berlin"
    assert isinstance(data["temperature_c"], (int, float))
    assert isinstance(data["humidity_percent"], int)