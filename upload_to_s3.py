
import logging
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Configuration
BUCKET_NAME = "manjima-imdb-movie-pipeline-2026"
AWS_REGION = "ap-south-1"
LOCAL_FILE = Path("data/processed/movies.parquet")
S3_KEY = "processed/movies.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

def upload_file():
    if not LOCAL_FILE.is_file():
        raise FileNotFoundError(f"File not found: {LOCAL_FILE}")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    try:
        logging.info("Uploading %s to S3...", LOCAL_FILE)
        s3.upload_file(
            str(LOCAL_FILE),
            BUCKET_NAME,
            S3_KEY,
        )
        logging.info(
            "Upload successful: s3://%s/%s",
            BUCKET_NAME,
            S3_KEY,
        )
    except (BotoCoreError, ClientError):
        logging.exception("S3 upload failed.")
        raise

if __name__ == "__main__":
    upload_file()
