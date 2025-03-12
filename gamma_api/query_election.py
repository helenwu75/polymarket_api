import requests
import pandas as pd
import time
import json
from datetime import datetime
import os

# GAMMA API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Define election-related keywords
ELECTION_KEYWORDS = [
    "president", "election", "presidential", "vote", "ballot", 
    "congress", "senate", "governor", "candidate", "primary", "nominee",
    "mayor", "democrat", "republican", "constituency", "parliament",
    "chancellor", "minister", "party", "campaign", "poll"
]

# Define target properties to collect
TARGET_PROPERTIES = [
    "id", "question", "conditionId", "slug", "resolutionSource", "endDate", 
    "liquidity", "startDate", "description", "outcomes", "outcomePrices", 
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

def get_closed_election_markets(limit=10):
    """
    Query the GAMMA API for closed markets, then filter for election-related markets by keywords.
    
    Args:
        limit (int): Maximum number of markets to retrieve
        
    Returns:
        list: List of filtered market data dictionaries
    """
    print(f"Fetching closed markets from GAMMA API and filtering for election-related markets...")
    
    # Initialize variables for pagination
    election_markets = []
    offset = 0
    page_size = 500  # Fetch 200 markets at a time
    
    while len(election_markets) < limit:
        # Build query parameters for closed markets sorted by volume
        params = {
            "limit": page_size,
            "offset": offset,
            "closed": "true",
            "order": "volume",
            "ascending": "false"
        }
        
        try:
            # Make API request
            response = requests.get(f"{GAMMA_API_URL}/markets", params=params)
            response.raise_for_status()
            
            # Parse response
            markets = response.json()
            
            # Break if no more markets found
            if not markets:
                print("No more markets found.")
                break
                
            # Filter for election-related markets
            for market in markets:
                question = market.get("question", "").lower()
                
                # Check if any election keyword is in the question
                if any(keyword in question for keyword in ELECTION_KEYWORDS):
                    election_markets.append(market)
                    
                    # Break if we've reached our limit
                    if len(election_markets) >= limit:
                        break
            
            # Increment offset for next page
            offset += len(markets)
            
            # Small delay to avoid API rate limits
            time.sleep(0.5)
            
            # Print progress
            print(f"Processed {len(markets)} markets, found {len(election_markets)} election markets so far")
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching markets: {e}")
            break
    
    # Trim to the requested limit
    result = election_markets[:limit]
    print(f"Retrieved {len(result)} closed election markets")
    
    return result

def extract_market_data(markets):
    """
    Extract relevant information from election market data.
    
    Args:
        markets (list): List of election market data from the GAMMA API
        
    Returns:
        pd.DataFrame: DataFrame with extracted market information and event data
    """
    # Define the fields we want to extract
    extracted_data = []
    
    for market in markets:
        # Extract only target properties
        market_data = {}
        for prop in TARGET_PROPERTIES:
            market_data[prop] = market.get(prop)
        
        # Extract event data if available
        if "events" in market and market["events"] and len(market["events"]) > 0:
            event = market["events"][0]  # Take the first event
            
            # Add event fields with prefix to distinguish from market data
            for prop in EVENT_PROPERTIES:
                if prop in event:
                    market_data[f"event_{prop}"] = event.get(prop)
        
        extracted_data.append(market_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(extracted_data)
    
    # Convert numeric columns to appropriate types
    for col in df.columns:
        # Check if column is in NUMERIC_COLUMNS or starts with 'event_' and the base name is in NUMERIC_COLUMNS
        if col in NUMERIC_COLUMNS or (col.startswith("event_") and col[6:] in NUMERIC_COLUMNS):
            # First, convert to string to handle any None values
            df[col] = df[col].astype(str)
            
            # Replace empty strings with NaN
            df[col] = df[col].replace('None', pd.NA)
            df[col] = df[col].replace('', pd.NA)
            
            # Convert to float, handling errors
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def save_data(df):
    """
    Save the extracted data to CSV and JSON files.
    
    Args:
        df (pd.DataFrame): DataFrame with market data
    """
    # Create output directory
    output_dir = "election_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Get the number of markets
    num_markets = len(df)
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save as CSV
    csv_filename = f"{output_dir}/Top_{num_markets}_election_markets_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Saved data to {csv_filename}")
    
    # Save as JSON
    json_filename = f"{output_dir}/Top_{num_markets}_election_markets_{timestamp}.json"
    df.to_json(json_filename, orient="records", indent=2)
    print(f"Saved data to {json_filename}")

def main():
    # Get closed election markets
    markets = get_closed_election_markets(limit=10)
    
    if not markets:
        print("No closed election markets found. Exiting.")
        return
    
    # Extract and process market data
    market_df = extract_market_data(markets)
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Total Markets: {len(market_df)}")
    
    # Check numeric columns before calculating statistics
    for col in ["volume", "volumeNum"]:
        if col in market_df.columns:
            print(f"\nChecking {col} column:")
            print(f"Data type: {market_df[col].dtype}")
            print(f"Non-numeric values count: {market_df[col].isna().sum()}")
            
            # Calculate statistics only if we have numeric values
            if not market_df[col].isna().all():
                total = market_df[col].sum(skipna=True)
                average = market_df[col].mean(skipna=True)
                print(f"Total {col}: {total:,.2f}")
                print(f"Average {col} per Market: {average:,.2f}")
    
    # Save the data
    save_data(market_df)

if __name__ == "__main__":
    main()