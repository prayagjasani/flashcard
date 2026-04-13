import os
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Load env variables
load_dotenv()

R2_ACCESS_KEY_ID = os.getenv("CLOUDFLARE_R2_ACCESS_KEY_ID") or os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("CLOUDFLARE_R2_SECRET_ACCESS_KEY") or os.getenv("R2_SECRET_ACCESS_KEY")
R2_ACCOUNT_ID = os.getenv("CLOUDFLARE_R2_ACCOUNT_ID")
R2_BUCKET_NAME = os.getenv("CLOUDFLARE_R2_BUCKET") or os.getenv("R2_BUCKET")
R2_ENDPOINT = (
    os.getenv("CLOUDFLARE_R2_ENDPOINT")
    or os.getenv("R2_ENDPOINT_URL")
    or (f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com" if R2_ACCOUNT_ID else None)
)

# Connect to R2
r2_client = boto3.client(
    "s3",
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    endpoint_url=R2_ENDPOINT,
    region_name="auto",
    config=Config(s3={"addressing_style": "path", "max_pool_connections": 100}),
)

def process_file(key, prefix):
    """Worker function to process a single file concurrently."""
    try:
        # Extract the part after f"{R2_BUCKET_NAME}/tts/{lang}/"
        path_part = key[len(prefix):]
        
        # If it already contains a slash, it means it's in a subfolder (already migrated)
        if "/" in path_part:
            return "skipped"
            
        safe = path_part[:-4]
        if not safe:
            return "skipped"
            
        # Calculate new path
        safe_hash = hashlib.md5(safe.encode("utf-8")).hexdigest()
        sub_prefix = safe_hash[0:2]
        short_safe = safe[:30]
        
        lang = prefix.split("/")[-2]
        new_key = f"{R2_BUCKET_NAME}/tts/{lang}/{sub_prefix}/{short_safe}_{safe_hash[-8:]}.mp3"
        
        # Copy to new location
        r2_client.copy_object(
            Bucket=R2_BUCKET_NAME,
            CopySource={'Bucket': R2_BUCKET_NAME, 'Key': key},
            Key=new_key
        )
        # Delete old file
        r2_client.delete_object(
            Bucket=R2_BUCKET_NAME,
            Key=key
        )
        return "migrated"
    except Exception as e:
        return f"error: {e}"

def run_migration():
    if not R2_BUCKET_NAME:
        print("Error: R2_BUCKET_NAME environment variable is not set.")
        return

    print(f"Starting High-Speed Audio Migration for Bucket: {R2_BUCKET_NAME}...")
    
    migrated_count = 0
    skipped_count = 0
    error_count = 0
    
    # We will use 50 workers to do 50 operations at the same time
    # This will turn a 5-hour task into a ~5 minute task.
    with ThreadPoolExecutor(max_workers=50) as executor:
        for lang in ["de", "en"]:
            prefix = f"{R2_BUCKET_NAME}/tts/{lang}/"
            continuation = None
            
            print(f"Scanning directory: {prefix}")
            
            while True:
                kwargs = {"Bucket": R2_BUCKET_NAME, "Prefix": prefix}
                if continuation:
                    kwargs["ContinuationToken"] = continuation
                    
                resp = r2_client.list_objects_v2(**kwargs)
                contents = resp.get("Contents", [])
                
                if not contents:
                    break
                    
                futures = []
                for obj in contents:
                    key = obj.get("Key", "")
                    if not key.endswith(".mp3"):
                        continue
                    # Dispatch to worker thread
                    futures.append(executor.submit(process_file, key, prefix))
                
                # Check results as they finish
                for future in as_completed(futures):
                    result = future.result()
                    if result == "migrated":
                        migrated_count += 1
                    elif result == "skipped":
                        skipped_count += 1
                    else:
                        error_count += 1
                        
                print(f"Progress: {migrated_count} migrated, {skipped_count} skipped, {error_count} errors...")
                
                if resp.get("IsTruncated"):
                    continuation = resp.get("NextContinuationToken")
                else:
                    break

    print("\n--- MIGRATION COMPLETE ---")
    print(f"Migrated: {migrated_count}")
    print(f"Already OK (Skipped): {skipped_count}")
    print(f"Errors: {error_count}")

if __name__ == "__main__":
    run_migration()
