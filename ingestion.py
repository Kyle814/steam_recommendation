# ingestion.py
import time
from steam_collector import SteamDataCollector
from game_discovery_utils import get_steamspy_top_500, get_steamspy_horror_games

def run_bronze_ingestion():
    # Initialize your core Steam client collector
    collector = SteamDataCollector()
    
    # ---------------------------------------------------------
    # 1. DYNAMICALLY FETCH TARGET LISTS
    # ---------------------------------------------------------
    
    top_500_apps = get_steamspy_top_500()
    
    # Pull the descriptive data for text embeddings (The RAG Niche)
    # Adjust this limit based on how massive you want your vector DB pool to be
    horror_apps = get_steamspy_horror_games(limit=200)
    
    # Merge both lists. Converting to a set automatically eliminates duplicates
    # (e.g., games like 'Dead by Daylight' that belong to both lists)
    combined_targets = list(set(top_500_apps + horror_apps))
    
    print(f"\n🚀 Master target list compiled.")
    print(f" -> Top 500 Global Apps: {len(top_500_apps)}")
    print(f" -> Horror Genre Apps: {len(horror_apps)}")
    print(f" -> Total Unique Apps to Extract: {len(combined_targets)}")

    # ---------------------------------------------------------
    # 2. THE RATE-LIMIT SAFE BATCH ENGINE
    # ---------------------------------------------------------
    # Steam Storefront API limits requests to ~200 every 5 minutes.
    # Each game requires 2 calls (1 metadata, 1 reviews). 
    # Batching by 80 games keeps us at 160 requests per batch, completely safe.
    batch_size = 80 
    
    for i in range(0, len(combined_targets), batch_size):
        batch = combined_targets[i:i + batch_size]
        current_batch_num = (i // batch_size) + 1
        total_batches = (len(combined_targets) + batch_size - 1) // batch_size
        
        print(f"\n--- Processing Batch {current_batch_num}/{total_batches} (Games {i} to {i+len(batch)}) ---")
        
        for app_id in batch:
            try:
                # Extract store description, genres, tags, and pricing
                collector.get_app_metadata(app_id)
                
                # Extract the top 100 text reviews for the vector database
                collector.get_app_reviews(app_id, limit=100)
             
                time.sleep(1.5)
                
            except Exception as e:
                print(f"⚠️ Skipping App {app_id} due to an unexpected error: {e}")
                continue
                
        # Cool down step to clear Steam's 5-minute rolling window
        if i + batch_size < len(combined_targets):
            print("\n⏸️ Batch processing complete. Enforcing a 5-minute cooldown to prevent API rate limits...")
            time.sleep(305) 
            
    print("\n🎉 Bronze Layer Ingestion Complete! All raw JSON files successfully staged.")

if __name__ == "__main__":
    run_bronze_ingestion()