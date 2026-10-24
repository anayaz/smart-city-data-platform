from fastapi import FastAPI
import os

from fastapi import FastAPI, HTTPException
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

load_dotenv()

app = FastAPI(title="Urban Analytics API")

db_url = URL.create(
    drivername="postgresql+psycopg",
    username="urban_user",
    password=os.getenv("POSTGRES_PASSWORD"),
    # host="127.0.0.1",
    host=os.getenv("DB_HOST", "127.0.0.1"),
    port=5432,
    database="urban_analytics",
)

engine = create_engine(db_url)


@app.get("/")
def home():
    return {"message": "Urban Analytics API is running"}


@app.get("/weather/{city}")
def get_weather(city: str):
    query = text("""
        SELECT city, observed_at, temperature_c, humidity_percent
        FROM weather_observations
        WHERE LOWER(city) = LOWER(:city)
        ORDER BY observed_at DESC
        LIMIT 1
    """)

    with engine.connect() as connection:
        row = connection.execute(query, {"city": city}).mappings().first()

    if row is None:
        raise HTTPException(status_code=404, detail="City not found")

    return dict(row)
