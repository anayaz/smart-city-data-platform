import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# Load database credentials from .env
load_dotenv()

# Build the database connection URL
db_url = URL.create(
    drivername="postgresql+psycopg",
    username="urban_user",
    password=os.getenv("POSTGRES_PASSWORD"),
    host="127.0.0.1",
    port=5432,
    database="urban_analytics",
)

# Create a database engine
engine = create_engine(db_url)

# Execute a SQL query
with engine.connect() as connection:
    result = connection.execute(
        text("SELECT COUNT(*) FROM campaigns")
    )
    print("Number of campaigns:", result.scalar_one())