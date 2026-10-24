import os
import logging

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from prefect import flow, task

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


@task(retries=2, retry_delay_seconds=5)
def extract(file_path):

    """Extract campaign data from CSV."""
    df = pd.read_csv(file_path)

    logger.info("Extracted %d records", len(df))
    return df



@task
def transform(df):

    """Clean and validate campaign data."""
    df = df.copy()

    df = df.drop_duplicates()

    df["start_date"] = pd.to_datetime(
        df["start_date"], errors="coerce"
    )

    df["location_id"] = pd.to_numeric(
        df["location_id"], errors="coerce"
    )

    df["budget"] = pd.to_numeric(
        df["budget"], errors="coerce"
    )

    df = df.dropna(
        subset=["location_id", "advertiser", "budget", "start_date"]
    )

    df = df[
        (df["budget"] > 0)
        & (df["location_id"] > 0)
        & (df["location_id"] % 1 == 0)
    ].copy()

    df["location_id"] = df["location_id"].astype(int)

    # PostgreSQL DATE column expects dates, not timestamps
    df["start_date"] = df["start_date"].dt.date

    logger.info("Validated %d records", len(df))
    return df

@task(retries=2, retry_delay_seconds=5)
def load(df):
    """Load validated records into PostgreSQL."""
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

    query = text("""
        INSERT INTO campaigns_etl
            (location_id, advertiser, budget, start_date)
        VALUES
            (:location_id, :advertiser, :budget, :start_date)
        ON CONFLICT (location_id, advertiser, start_date)
        DO NOTHING
    """)

    try:
        with engine.begin() as connection:
            result = connection.execute(
                query,
                df.to_dict(orient="records"),
            ) if not df.empty else None

        inserted = result.rowcount if result is not None else 0

        logger.info(
            "Processed %d records; inserted %d new records",
            len(df),
            inserted,
        )

    except Exception:
        logger.exception("Database loading failed")
        raise

    finally:
        engine.dispose()

@flow(name="urban-campaign-etl", log_prints=True)
def main():
    logger.info("Starting ETL pipeline")

    df = extract("data/raw/advertising_campaigns.csv")
    df = transform(df)
    load(df)

    logger.info("ETL pipeline completed")

if __name__ == "__main__":
    main.serve(
        name="campaign-etl-scheduled",
        interval=60,
    )