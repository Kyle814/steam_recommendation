import os
import json
import time
import requests
from datetime import datetime
from dotenv import load_dotenv
import glob
import boto3

class SteamDataCollector:
    def __init__(self):
        # load_dotenv() works locally and is safely ignored in AWS
        load_dotenv()
        self.api_key = os.getenv("STEAM_API_KEY")
        
        # The Cloud Switch: Checks if we are in AWS or on your local machine
        self.bucket_name = os.getenv("S3_BUCKET_NAME") 
        self.base_dir = "data/raw"
        self.today = datetime.utcnow().strftime("%Y-%m-%d")
        
        if self.bucket_name:
            print(f"☁️ Cloud Mode Activated: Connected to S3 Bucket [{self.bucket_name}]")
            self.s3_client = boto3.client('s3')
        else:
            print("💻 Local Mode Activated: Saving to local disk.")
            for folder in ["user_profiles", "app_metadata", "app_reviews"]:
                os.makedirs(os.path.join(self.base_dir, folder, self.today), exist_ok=True)

    def save_json(self, data, folder, filename):
        """Saves raw JSON payloads locally or directly to S3 via Boto3."""
        s3_key = f"data/raw/{folder}/{self.today}/{filename}"
        
        if self.bucket_name:
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(data, ensure_ascii=False),
                ContentType='application/json'
            )
            print(f"☁️ Saved to S3: {filename}")
        else:
            # --- THE LOCAL WINDOWS ROUTE ---
            path = os.path.join(self.base_dir, folder, self.today, filename)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            print(f"💾 Saved Locally: {filename}")

    # ==========================================
    # PILLAR 1: User & Playtime Data
    # ==========================================
    def get_user_data(self, steam_id):
        print(f"\nFetching User Data for: {steam_id}")
        url = "https://api.steampowered.com/IPlayerService/GetOwnedGames/v1/"
        params = {
            "key": self.api_key,
            "steamid": steam_id,
            "include_appinfo": False,
            "include_played_free_games": True,
            "format": "json"
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            raw_library = response.json().get("response", {})
            
            if raw_library and raw_library.get("games"):
                clean_games = []
                for game in raw_library["games"]:
                    clean_games.append({
                        "appid": game.get("appid"),
                        "playtime_forever": game.get("playtime_forever", 0)
                    })

                payload = {
                    "ingested_at": datetime.utcnow().isoformat(),
                    "steam_id": steam_id,
                    "game_count": len(clean_games),
                    "games": clean_games
                }
                
                self.save_json(payload, "user_profiles", f"user_{steam_id}.json")
                return True
            else:
                print(f"⚠️ Profile {steam_id} is private or owns 0 games.")
                return False
                
        except Exception as e:
            print(f"❌ Failed to fetch user data for {steam_id}: {e}")
            return False

    def get_friends_list(self, steam_id):
        url = "https://api.steampowered.com/ISteamUser/GetFriendList/v1/"
        params = {"key": self.api_key, "steamid": steam_id, "relationship": "friend"}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            res = response.json()
            friends_data = res.get("friendslist", {}).get("friends", [])
            return [str(friend["steamid"]) for friend in friends_data]
        except Exception:
            return []

    # ==========================================
    # PILLAR 2: Game Metadata (Pre-Filtered)
    # ==========================================
    def get_app_metadata(self, app_id):
        print(f"\nFetching Metadata for App: {app_id}")
        url = "https://store.steampowered.com/api/appdetails"
        params = {"appids": app_id, "l": "english"}

        try:
            res = requests.get(url, params=params).json()
            
            if res and res.get(str(app_id), {}).get("success"):
                raw_data = res[str(app_id)]["data"]
                
                clean_metadata = {
                    "type": raw_data.get("type"),
                    "name": raw_data.get("name"),
                    "steam_appid": raw_data.get("steam_appid"),
                    "required_age": raw_data.get("required_age"),
                    "is_free": raw_data.get("is_free"),
                    "short_description": raw_data.get("short_description"),
                    "detailed_description": raw_data.get("detailed_description"),
                    "developers": raw_data.get("developers", []),
                    "publishers": raw_data.get("publishers", []),
                    "price_overview": raw_data.get("price_overview", {}),
                    "platforms": raw_data.get("platforms", {}),
                    "metacritic": raw_data.get("metacritic", {}),
                    "categories": raw_data.get("categories", []),
                    "genres": raw_data.get("genres", []),
                    "recommendations": raw_data.get("recommendations", {}),
                    "release_date": raw_data.get("release_date", {})
                }

                payload = {
                    "ingested_at": datetime.utcnow().isoformat(),
                    "app_id": app_id,
                    "metadata": clean_metadata
                }
                
                self.save_json(payload, "app_metadata", f"meta_{app_id}.json")
            else:
                print(f"⚠️ App {app_id} metadata not found or restricted.")
                
            time.sleep(2) 

        except Exception as e:
            print(f"❌ Metadata Error ({app_id}): {e}")

    # ==========================================
    # PILLAR 3: Community & Reviews (Pre-Filtered)
    # ==========================================
    def get_app_reviews(self, app_id, limit=100):
        print(f"\nFetching Reviews for App: {app_id}")
        url = f"https://store.steampowered.com/appreviews/{app_id}"
        params = {"json": 1, "filter": "all", "language": "english", "num_per_page": limit}

        try:
            res = requests.get(url, params=params).json()
            
            if res.get("success") and res.get("reviews"):
                clean_reviews = []
                
                for raw_review in res["reviews"]:
                    if raw_review.get("language") != "english":
                        continue
                        
                    try:
                        weighted_score = float(raw_review.get("weighted_vote_score", 0.0))
                    except ValueError:
                        weighted_score = 0.0

                    clean_review = {
                        "review_id": raw_review.get("recommendationid"),
                        "steam_id": raw_review.get("author", {}).get("steamid"),
                        "playtime_minutes": raw_review.get("author", {}).get("playtime_at_review", 0),
                        "voted_up": raw_review.get("voted_up"),
                        "votes_up": raw_review.get("votes_up"),
                        "weighted_score": weighted_score,
                        "steam_purchase": raw_review.get("steam_purchase"),
                        "review_text": raw_review.get("review")
                    }
                    clean_reviews.append(clean_review)

                payload = {
                    "ingested_at": datetime.utcnow().isoformat(),
                    "app_id": app_id,
                    "review_count": len(clean_reviews),
                    "reviews": clean_reviews
                }
                
                self.save_json(payload, "app_reviews", f"reviews_{app_id}.json")
            else:
                print(f"⚠️ No reviews found for {app_id}.")
                
            time.sleep(1.5)

        except Exception as e:
            print(f"❌ Review Error ({app_id}): {e}")

def harvest_discovered_games(collector_instance, limit=5):
    """Reads raw user profiles to extract unique App IDs and fetches metadata/reviews."""
    unique_app_ids = set()
    print("\n🔍 Scanning user profiles for unique games...")
    
    if collector_instance.bucket_name:
        # --- THE AWS S3 ROUTE ---
        prefix = f"data/raw/user_profiles/{collector_instance.today}/"
        response = collector_instance.s3_client.list_objects_v2(
            Bucket=collector_instance.bucket_name, 
            Prefix=prefix
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                file_obj = collector_instance.s3_client.get_object(
                    Bucket=collector_instance.bucket_name, 
                    Key=obj['Key']
                )
                user_data = json.loads(file_obj['Body'].read().decode('utf-8'))
                for game in user_data.get("games", []):
                    unique_app_ids.add(str(game.get("appid")))
    else:
        # --- THE LOCAL WINDOWS ROUTE ---
        user_files_path = f"data/raw/user_profiles/{collector_instance.today}/*.json"
        for filepath in glob.glob(user_files_path):
            with open(filepath, "r", encoding="utf-8") as f:
                user_data = json.load(f)
                for game in user_data.get("games", []):
                    unique_app_ids.add(str(game.get("appid")))

    print(f"Found {len(unique_app_ids)} unique games.")
    app_id_list = list(unique_app_ids)
    apps_to_process = app_id_list[:limit]
    
    print(f"Starting extraction for the first {len(apps_to_process)} games...\n")

    for app_id in apps_to_process:
        collector_instance.get_app_metadata(app_id)
        collector_instance.get_app_reviews(app_id, limit=100)

if __name__ == "__main__":
    collector = SteamDataCollector()
    
    target_users = [os.getenv("TARGET_STEAM_ID"), "76561197960287930"] 
    for steam_id in target_users:
        if steam_id:
            collector.get_user_data(steam_id)
    
    harvest_discovered_games(collector, limit=5)