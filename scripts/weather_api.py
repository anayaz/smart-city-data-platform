import requests

url = "https://api.open-meteo.com/v1/forecast"

params = {
    "latitude": 52.52,
    "longitude": 13.41,
    "current": "temperature_2m,relative_humidity_2m",
}

response = requests.get(url, params=params, timeout=10)

response.raise_for_status()

data = response.json()

print("Temperature:", data["current"]["temperature_2m"])
print("Humidity:", data["current"]["relative_humidity_2m"])
print("Time:", data["current"]["time"])


import os
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Load database credentials
load_dotenv()

db_url = URL.create(
    drivername="postgresql+psycopg",
    username="urban_user",
    password=os.getenv("POSTGRES_PASSWORD"),
    host="127.0.0.1",
    port=5432,
    database="urban_analytics",
)

engine = create_engine(db_url)

# Prepare weather observation
weather = {
    "city": "Berlin",
    "observed_at": datetime.fromisoformat(data["current"]["time"]),
    "temperature_c": data["current"]["temperature_2m"],
    "humidity_percent": data["current"]["relative_humidity_2m"],
}

# Insert into PostgreSQL
query = text("""
    INSERT INTO weather_observations
        (city, observed_at, temperature_c, humidity_percent)
    VALUES
        (:city, :observed_at, :temperature_c, :humidity_percent)
    ON CONFLICT (city, observed_at)
    DO UPDATE SET
        temperature_c = EXCLUDED.temperature_c,
        humidity_percent = EXCLUDED.humidity_percent
""")

with engine.begin() as connection:
    connection.execute(query, weather)

engine.dispose()

print("Weather data successfully loaded into PostgreSQL!")