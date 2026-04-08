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

    print(f"Uploading {file_path} to R2...")
    s3_client.upload_file(
        file_path,
        BUCKET_NAME,
        file_path,
        # Setting the content-type helps browsers/clients handle it as a binary file
        ExtraArgs={"ContentType": "application/octet-stream"},
    )
    print(f"{file_path} is now live at {BASE_URL}/{file_path}")


def create_pointer_db(output_path="data.db"):
    con = duckdb.connect(output_path)
    con.execute("INSTALL httpfs; LOAD httpfs;")

    base_url = "https://frc-db.dev"

    print("Defining virtual views...")

    con.execute(f"CREATE OR REPLACE VIEW matches AS SELECT * FROM read_parquet('{base_url}/matches/data.parquet');")
    con.execute(f"CREATE OR REPLACE VIEW events AS SELECT * FROM read_parquet('{base_url}/events/data.parquet');")
    con.execute(f"CREATE OR REPLACE VIEW teams AS SELECT * FROM read_parquet('{base_url}/teams/all_teams.parquet');")

    con.close()
    print(f"Pointer database created: {output_path}")

    # 4. Upload to R2
    upload_catalog(output_path)


if __name__ == "__main__":
    create_pointer_db()
