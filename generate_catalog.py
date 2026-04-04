import os

import boto3
import duckdb
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")
BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
BASE_URL = "https://frc-db.dev"


def upload_catalog(file_path):
    """Uploads the .db file to the root of the R2 bucket."""
    s3_client = boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
    )

    print(f"📤 Uploading {file_path} to R2...")
    s3_client.upload_file(
        file_path,
        BUCKET_NAME,
        file_path,
        # Setting the content-type helps browsers/clients handle it as a binary file
        ExtraArgs={"ContentType": "application/octet-stream"},
    )
    print(f"✨ {file_path} is now live at {BASE_URL}/{file_path}")


def create_pointer_db(output_path="data.db"):
    # 1. Connect and initialize
    con = duckdb.connect(output_path)
    con.execute("INSTALL httpfs; LOAD httpfs;")

    # 2. Build URL lists (1992-2026)
    # We include the early years too—DuckDB will just return empty for
    # years that don't have files yet if you haven't finished the backfill.
    years = range(2005, 2027)
    match_urls = [f"'{BASE_URL}/matches/year={y}/data.parquet'" for y in years]
    event_urls = [f"'{BASE_URL}/events/year={y}/data.parquet'" for y in years]

    print("🛠️  Defining virtual views...")

    # 3. Create Persistent Views
    con.execute(f"""
        CREATE OR REPLACE VIEW matches AS
        SELECT * FROM read_parquet([{", ".join(match_urls)}], union_by_name = true);
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW events AS
        SELECT * FROM read_parquet([{", ".join(event_urls)}], union_by_name = true);
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW teams AS
        SELECT * FROM read_parquet('{BASE_URL}/teams/all_teams.parquet');
    """)

    con.close()
    print(f"✅ Local catalog created: {output_path}")

    # 4. Upload to R2
    upload_catalog(output_path)


if __name__ == "__main__":
    create_pointer_db()
