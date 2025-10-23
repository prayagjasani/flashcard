import boto3
from botocore.config import Config
import os

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("CLOUDFLARE_R2_ENDPOINT"),
    aws_access_key_id=os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY"),
    config=Config(signature_version="s3v4")
)

try:
    resp = s3.list_objects_v2(Bucket=os.getenv("CLOUDFLARE_R2_BUCKET"), MaxKeys=5)
    print(resp)
except Exception as e:
    print("Error:", e)
