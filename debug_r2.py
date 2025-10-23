#!/usr/bin/env python3
"""Debug script to check R2 bucket contents and test file access."""

import os
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# R2 configuration
R2_ACCESS_KEY_ID = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID") or os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY") or os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
R2_BUCKET_NAME = os.getenv("CLOUDFLARE_R2_BUCKET") or os.getenv("R2_BUCKET")
R2_ENDPOINT = (
    os.getenv("CLOUDFLARE_R2_ENDPOINT")
    or os.getenv("R2_ENDPOINT_URL")
    or (f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None)
)

print(f"R2_ENDPOINT: {R2_ENDPOINT}")
print(f"R2_BUCKET_NAME: {R2_BUCKET_NAME}")

# Create R2 client
r2_client = boto3.client(
    "s3",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    endpoint_url=R2_ENDPOINT,
    region_name="auto",
    config=Config(s3={"addressing_style": "path"}),
)

print("\n=== Testing different key patterns ===")

# Test patterns
test_keys = [
    "csv/index.json",
    "flashcard-audio-storage/csv/index.json",
    f"{R2_BUCKET_NAME}/csv/index.json"
]

for key in test_keys:
    try:
        print(f"\nTrying key: {key}")
        obj = r2_client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        print(f"SUCCESS! Content length: {len(data)}")
        print(f"First 100 chars: {data[:100]}")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        print(f"FAILED: {code}")