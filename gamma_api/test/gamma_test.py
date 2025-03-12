import requests
import json

# GAMMA API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Build query parameters for just one market
params = {
    "limit": 1,          # Just get one market
    "tag": "elections",  # Filter by elections tag
}

print("Querying GAMMA API...")
print(f"Using URL: {GAMMA_API_URL}/markets")
print(f"With parameters: {params}")

# Make API request
response = requests.get(f"{GAMMA_API_URL}/markets", params=params)
print(f"Status code: {response.status_code}")

# Check if request was successful
if response.status_code == 200:
    data = response.json()
    
    # Print the raw response for debugging
    print("\nRaw API Response (first 1000 chars):")
    print(json.dumps(data)[:1000] + "...")
    
    # Check if data is a list (different from expected structure)
    if isinstance(data, list):
        markets = data
    else:
        # Original expected structure
        markets = data.get("markets", [])
    
    print(f"\nFound {len(markets)} markets")
    
    if markets:
        market = markets[0]  # Get the first market
        print("\nExtracted Market Information:")
        print(f"ID: {market.get('id')}")
        print(f"Question: {market.get('question')}")
        print(f"Description: {market.get('description')}")
        print(f"Condition ID: {market.get('conditionId')}")
        print(f"Volume: {market.get('volume', 'N/A')}")
        print(f"Liquidity: {market.get('liquidity')}")
        print(f"End Date: {market.get('endDate')}")
        print(f"Category: {market.get('category')}")
        
        # Check available fields in the first market
        print("\nAvailable fields in market data:")
        print(json.dumps(list(market.keys()), indent=2))
    else:
        print("No markets found.")
else:
    print(f"Request failed: {response.text}")