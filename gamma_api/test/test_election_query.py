import requests
import time

# GAMMA API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Define election-related keywords
ELECTION_KEYWORDS = [
    "president", "election", "presidential", "vote", "ballot", 
    "congress", "senate", "governor", "candidate", "primary", "nominee",
    "mayor", "democrat", "republican", "constituency", "parliament",
    "chancellor", "minister", "party", "campaign", "poll"
]

def test_closed_election_markets():
    """
    Test function to query closed markets and filter for election-related ones.
    Only fetches a small sample to verify the approach works.
    """
    print("Testing closed election markets query...")
    
    # Only fetch a small sample (10 markets) for testing
    params = {
        "limit": 20,
        "offset": 0,
        "closed": "true",
        "order": "volume",
        "ascending": "false"
    }
    
    try:
        # Make API request
        print(f"Requesting data from {GAMMA_API_URL}/markets")
        response = requests.get(f"{GAMMA_API_URL}/markets", params=params)
        print(f"Response status code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"Error: {response.text}")
            return
            
        # Parse response
        data = response.json()
        
        # Print raw response structure for debugging
        print(f"\nResponse type: {type(data)}")
        if isinstance(data, dict):
            print(f"Response keys: {list(data.keys())}")
        
        # Extract markets based on response format
        if isinstance(data, list):
            markets = data
        elif isinstance(data, dict) and "markets" in data:
            markets = data.get("markets", [])
        else:
            print(f"Unexpected API response format")
            return
            
        print(f"\nFound {len(markets)} closed markets")
        
        # Find election-related markets
        election_markets = []
        for market in markets:
            question = market.get("question", "").lower()
            
            # Check if any election keyword is in the question
            if any(keyword in question for keyword in ELECTION_KEYWORDS):
                election_markets.append(market)
        
        print(f"\nFound {len(election_markets)} election-related markets")
        
        # Print details of found election markets
        for i, market in enumerate(election_markets):
            print(f"\nElection Market {i+1}:")
            print(f"ID: {market.get('id')}")
            print(f"Question: {market.get('question')}")
            print(f"Volume: {market.get('volume')}")
            print(f"Closed: {market.get('closed')}")
            
    except Exception as e:
        print(f"Error in test: {str(e)}")

if __name__ == "__main__":
    test_closed_election_markets()