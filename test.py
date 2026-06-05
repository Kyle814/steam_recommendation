import os
import requests
from dotenv import load_dotenv

def test_steam_connection():
    print("Loading credentials from .env...")
    
    # Load environment variables
    load_dotenv()
    
    api_key = os.getenv("STEAM_API_KEY")
    steam_id = os.getenv("TARGET_STEAM_ID")

    # 1. Validation Check
    if not api_key or not steam_id:
        print("❌ Error: Missing credentials.")
        print("Please ensure your .env file exists and contains STEAM_API_KEY and TARGET_STEAM_ID")
        return

    print("Credentials loaded successfully. Testing connection...")

    # 2. Define the Steam API endpoint
    # GetPlayerSummaries is a great test endpoint because it's lightweight and reliable
    url = "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/"
    params = {
        "key": api_key,
        "steamids": steam_id
    }

    try:
        # 3. Make the Request
        response = requests.get(url, params=params)
        
        # If the API key is invalid, Steam returns a 401 or 403 status code
        response.raise_for_status()
        
        data = response.json()
        players = data.get("response", {}).get("players", [])

        # 4. Handle the Response
        if not players:
            print("❌ Connection successful, but no player found.")
            print(f"Check if {steam_id} is a valid 17-digit SteamID64.")
            return

        # Extract basic info
        player = players[0]
        persona_name = player.get("personaname", "Unknown")
        visibility = player.get("communityvisibilitystate", 1)

        print("\n✅ SUCCESS: Connected to Steam API!")
        print("-" * 30)
        print(f"Steam ID     : {steam_id}")
        print(f"Display Name : {persona_name}")
        
        if visibility == 3:
            print("Profile Vis  : Public (You can fetch full game/playtime data)")
        else:
            print("Profile Vis  : Private/Friends-Only (Game data will be hidden from API)")

    except requests.exceptions.HTTPError as e:
        if response.status_code in [401, 403]:
            print(f"\n❌ Error {response.status_code}: Unauthorized.")
            print("Your API Key is invalid or expired.")
        else:
            print(f"\n❌ HTTP Error occurred: {e}")
            
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    test_steam_connection()