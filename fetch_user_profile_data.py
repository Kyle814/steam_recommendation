import os
import glob
import json
import time
import boto3
from steam_collector import SteamDataCollector

def extract_users_from_reviews(bucket_name=None, s3_client=None):
    unique_users = set()
    
    if bucket_name and s3_client:
        # --- THE AWS S3 ROUTE ---
        print(f"☁️ Cloud Mode: Scanning S3 bucket [{bucket_name}] for reviews")
        prefix = "data/raw/app_reviews/"
        
        # We use a paginator because S3 only returns 1,000 files at a time by default
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.json'):
                        try:
                            # Read the file directly from S3 memory
                            file_obj = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                            review_data = json.loads(file_obj['Body'].read().decode('utf-8'))
                            
                            for review in review_data.get("reviews", []):
                                steam_id = review.get("steam_id")
                                if steam_id:
                                    unique_users.add(str(steam_id))
                        except Exception as e:
                            print(f"⚠️ Could not read S3 object {obj['Key']}: {e}")
                            
    else:
        # --- THE LOCAL WINDOWS ROUTE ---
        print("💻 Local Mode: Scanning local disk for reviews")
        review_files_path = "data/raw/app_reviews/*/*.json"
        
        for filepath in glob.glob(review_files_path):
            with open(filepath, "r", encoding="utf-8") as f:
                try:
                    review_data = json.load(f)
                    for review in review_data.get("reviews", []):
                        steam_id = review.get("steam_id")
                        if steam_id:
                            unique_users.add(str(steam_id))
                except Exception as e:
                    print(f"⚠️ Could not read {filepath}: {e}")
                    
    print(f"✅ Found {len(unique_users)} unique users from game reviews.")
    return list(unique_users)


def run_review_based_ingestion(target_user_count=100):
    # Initialize the collector (which already knows if we are in Cloud or Local mode)
    collector = SteamDataCollector()
    
    # Pass the collector's S3 knowledge into our extraction function
    s3_client = getattr(collector, 's3_client', None)
    target_ids = extract_users_from_reviews(collector.bucket_name, s3_client)
    
    if not target_ids:
        print("❌ No users found. Have you run your game ingestion script to download reviews yet?")
        return
        
    successful_profiles = 0
    print(f"\n🚀 Starting User Ingestion (Target: {target_user_count} public profiles)")
    
    for current_id in target_ids:
        if successful_profiles >= target_user_count:
            break
            
        # Attempt to save their library
        is_public = collector.get_user_data(current_id)
        
        if is_public:
            successful_profiles += 1
            print(f"✅ Success! Profile #{successful_profiles} saved.")
        
        # API delay
        time.sleep(1.5)

    print(f"\n🎉 Ingestion Complete!")
    print(f"Total Profiles Staged in Bronze Layer: {successful_profiles}")


if __name__ == "__main__":
    # You can change the target count based on how much data you want
    run_review_based_ingestion(target_user_count=100)