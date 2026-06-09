import os
import glob
import json
import time
from steam_collector import SteamDataCollector
def extract_users_from_reviews():
    
    print("🔍 Scanning local reviews for active User IDs")
    review_files_path = "data/raw/app_reviews/*/*.json"
    unique_users = set()
    
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

def run_review_based_ingestion(target_user_count=5000):
    collector = SteamDataCollector()
    target_ids = extract_users_from_reviews()
    
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
    # You can change the target count based on how much data you want for PySpark
    run_review_based_ingestion(target_user_count=100)