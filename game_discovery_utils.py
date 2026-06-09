import requests

def get_steamspy_top_500():
    print("Fetching Global Top 500 from SteamSpy...")
    url = "https://steamspy.com/api.php?request=all&page=0"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        games_list = list(data.values())
        sorted_games = sorted(games_list, key=lambda x: x.get('ccu', 0), reverse=True)
        top_500_ids = [str(game['appid']) for game in sorted_games[:500]]
        
        print(f"✅ Successfully retrieved {len(top_500_ids)} Top Global App IDs.")
        return top_500_ids
        
    except Exception as e:
        print(f"❌ Failed to reach SteamSpy: {e}")
        return []

def get_steamspy_horror_games(limit=150):
    print(f"\nFetching Top {limit} Horror games from SteamSpy...")
    # Hit the specific tag endpoint
    url = "https://steamspy.com/api.php?request=tag&tag=Horror"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Convert dictionary to list and sort by CCU (active players)
        games_list = list(data.values())
        sorted_games = sorted(games_list, key=lambda x: x.get('ccu', 0), reverse=True)
        
        # Extract IDs up to the limit you set
        horror_ids = [str(game['appid']) for game in sorted_games[:limit]]
        
        print(f"✅ Successfully retrieved {len(horror_ids)} Horror App IDs.")
        return horror_ids
        
    except Exception as e:
        print(f"❌ Failed to reach SteamSpy: {e}")
        return []

if __name__ == "__main__":
    print("--- Testing SteamSpy API Integration ---")
    
    # 1. Test the Global Density Pipeline
    global_games = get_steamspy_top_500()
    
    # 2. Test the Niche RAG Pipeline
    # Let's pull the top 20 horror games to verify the endpoint works
    horror_games = get_steamspy_horror_games(limit=20)
    global_games = get_steamspy_top_500()
    
    if global_games:
        print("\nPreview of the first 10 Global App IDs retrieved:")
        print(global_games[:10])
    if horror_games:
        print("\nPreview of the first 10 Horror App IDs retrieved:")
        print(horror_games[:10])