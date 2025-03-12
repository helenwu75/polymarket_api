import requests
import pandas as pd
import time
import json
import os
from datetime import datetime

# GAMMA API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Define election-related keywords
ELECTION_KEYWORDS = [
    "election", "presidential", "popular vote","VP nominee","presidential nominee"
]

# Define target properties to collect
TARGET_PROPERTIES = [
    "id", "question", "slug","conditionId", "startDate", "endDate", 
     "description", "outcomes", "outcomePrices", "liquidity",
    "volume", "active", "closed", "marketMakerAddress", "createdAt", 
    "updatedAt", "archived", "restricted", "groupItemTitle", 
    "groupItemThreshold", "questionID", "enableOrderBook", 
    "orderPriceMinTickSize", "orderMinSize", "volumeNum", "liquidityNum", 
    "volume24hr", "clobTokenIds", "umaBond", "umaReward", "volume24hrClob", 
    "volumeClob", "liquidityClob", "acceptingOrders", "rewardsMinSize", 
    "rewardsMaxSpread", "spread", "oneDayPriceChange", "lastTradePrice", 
    "bestBid", "bestAsk"
]

# Event properties to collect
EVENT_PROPERTIES = [
    "id", "ticker", "slug", "title", "description", "liquidity", "volume",
    "competitive", "volume24hr", "enableOrderBook", "liquidityClob", "negRisk",
    "negRiskMarketID", "commentCount", "countryName", "electionType", "disqusThread"
]

# Numeric columns that need conversion
NUMERIC_COLUMNS = [
    "volume", "liquidity", "volumeNum", "liquidityNum", "volume24hr", 
    "umaBond", "umaReward", "volume24hrClob", "volumeClob", "liquidityClob", 
    "rewardsMinSize", "rewardsMaxSpread", "spread", "oneDayPriceChange", 
    "lastTradePrice", "bestBid", "bestAsk", "orderPriceMinTickSize", "orderMinSize",
    "event_liquidity", "event_volume", "event_competitive", "event_volume24hr", 
    "event_liquidityClob", "event_commentCount"
]

def get_top_closed_election_markets_by_volume(limit=10):
    """
    Query the GAMMA API for closed markets, filter for election-related ones,
    and return the top ones by volume.
    
    Args:
        limit (int): Maximum number of top markets to retrieve
        
    Returns:
        pd.DataFrame: DataFrame with the top election markets by volume
    """
    print(f"Fetching closed election markets from GAMMA API...")
    
    # Initialize variables for pagination
    all_markets = []
    offset = 0
    page_size = 500  # Fetch 100 markets at a time
    
    # Step 1: Fetch all closed markets
    while True:
        # Build query parameters for closed markets
        params = {
            "limit": page_size,
            "offset": offset,
            "closed": "true"  # Only get closed markets
        }
        
        try:
            # Make API request
            response = requests.get(f"{GAMMA_API_URL}/markets", params=params)
            response.raise_for_status()
            
            # Parse response
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, dict) and "markets" in data:
                markets = data.get("markets", [])
            elif isinstance(data, list):
                markets = data
            else:
                markets = []
            
            # Break if no more markets found
            if not markets:
                break
            
            # Add to our collection
            all_markets.extend(markets)
            
            # Increment offset for next page
            offset += len(markets)
            
            # Print progress
            print(f"Fetched {len(markets)} markets (total: {len(all_markets)})")
            
            # Small delay to avoid API rate limits
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching markets: {e}")
            break
    
    print(f"Fetched a total of {len(all_markets)} closed markets")
    
    # Step 2: Filter for election-related markets
    election_markets = []
    for market in all_markets:
        question = market.get("question", "").lower()
        description = market.get("description", "").lower()
        
        # Check if any election keyword is in the question or description
        if any(keyword in question for keyword in ELECTION_KEYWORDS) or \
           any(keyword in description for keyword in ELECTION_KEYWORDS):
            election_markets.append(market)
    
    print(f"Found {len(election_markets)} election-related markets")
    
    # Step 3: Sort by volume
    def get_volume(market):
        # Try different volume field names
        vol = market.get("volumeNum", market.get("volume", 0))
        if vol is None:
            return 0
        try:
            return float(vol)
        except (ValueError, TypeError):
            return 0
    
    sorted_markets = sorted(election_markets, key=get_volume, reverse=True)
    
    # Step 4: Take the top markets
    result = sorted_markets[:limit]
    print(f"Returning top {len(result)} election markets by volume")
    
    # Convert to DataFrame
    market_df = extract_market_data(result)
    
    return market_df

def extract_market_data(markets):
    """
    Extract relevant information from market data.
    
    Args:
        markets (list): List of market data from the GAMMA API
        
    Returns:
        pd.DataFrame: DataFrame with extracted market information
    """
    # Define the fields we want to extract
    extracted_data = []
    
    for market in markets:
        # Extract basic market information based on TARGET_PROPERTIES
        market_data = {}
        for prop in TARGET_PROPERTIES:
            market_data[prop] = market.get(prop)
            
        # Extract event-related information if available
        if "events" in market and len(market["events"]) > 0:
            event = market["events"][0]  # Take the first event
            for prop in EVENT_PROPERTIES:
                market_data[f"event_{prop}"] = event.get(prop)
        
        # Extract token information if available
        if "tokens" in market and len(market["tokens"]) > 0:
            for i, token in enumerate(market["tokens"]):
                market_data[f"token_{i+1}_id"] = token.get("token_id")
                market_data[f"token_{i+1}_outcome"] = token.get("outcome")
        
        extracted_data.append(market_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(extracted_data)
    
    # Convert numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def save_top_election_markets_data(df, output_dir="election_data"):
    """
    Save the extracted data to CSV and JSON files.
    
    Args:
        df (pd.DataFrame): DataFrame with market data
        output_dir (str): Directory to save the output files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save as CSV
    csv_filename = f"{output_dir}/top_election_markets_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Saved data to {csv_filename}")
    
    # Save as JSON
    json_filename = f"{output_dir}/top_election_markets_{timestamp}.json"
    df.to_json(json_filename, orient="records", indent=2)
    print(f"Saved data to {json_filename}")

def main():
    # Get top election markets by volume
    markets_df = get_top_closed_election_markets_by_volume(limit=100)
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Total Markets: {len(markets_df)}")
    
    # Calculate volume statistics if volume columns exist and are numeric
    volume_col = None
    for col in ['volumeNum', 'volume']:
        if col in markets_df.columns and pd.api.types.is_numeric_dtype(markets_df[col]):
            volume_col = col
            break
    
    if volume_col:
        print(f"Total Volume: {markets_df[volume_col].sum():,.2f}")
        print(f"Average Volume per Market: {markets_df[volume_col].mean():,.2f}")
        print(f"Highest Volume: {markets_df[volume_col].max():,.2f}")
        print(f"Lowest Volume: {markets_df[volume_col].min():,.2f}")
    
    # Save the data
    save_top_election_markets_data(markets_df)

if __name__ == "__main__":
    main()