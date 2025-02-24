import requests
import pandas as pd
import time
import json
from datetime import datetime

# GAMMA API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com"

def get_election_markets(limit=500, tag="elections"):
    """
    Query the GAMMA API for election markets, sorted by volume.
    
    Args:
        limit (int): Maximum number of markets to retrieve
        tag (str): Tag to filter markets by (default: "elections")
        
    Returns:
        list: List of market data dictionaries
    """
    print(f"Fetching top {limit} election markets from GAMMA API...")
    
    # Initialize variables for pagination
    all_markets = []
    offset = 0
    page_size = 100  # Fetch 100 markets at a time to avoid large responses
    
    while len(all_markets) < limit:
        # Build query parameters
        params = {
            "limit": page_size,
            "offset": offset,
            "order": "volume_num",  # Sort by volume
            "ascending": "false",   # Descending order (highest volume first)
            "tag": tag              # Filter by elections tag
        }
        
        # Make API request
        try:
            response = requests.get(f"{GAMMA_API_URL}/markets", params=params)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            # Parse response
            data = response.json()
            markets = data.get("markets", [])
            
            # Break if no more markets found
            if not markets:
                break
                
            # Add markets to our collection
            all_markets.extend(markets)
            print(f"Fetched {len(markets)} markets (total: {len(all_markets)})")
            
            # Increment offset for next page
            offset += page_size
            
            # Small delay to avoid API rate limits
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching markets: {e}")
            break
    
    # Trim to the requested limit
    result = all_markets[:limit]
    print(f"Retrieved {len(result)} election markets")
    
    return result

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
        # Extract basic market information
        market_data = {
            "id": market.get("id"),
            "condition_id": market.get("condition_id"),
            "question_id": market.get("question_id"),
            "slug": market.get("slug"),
            "description": market.get("description"),
            "category": market.get("category"),
            "volume": market.get("volume_num"),
            "liquidity": market.get("liquidity_num"),
            "start_date": market.get("start_date"),
            "end_date": market.get("end_date"),
            "active": market.get("active"),
            "closed": market.get("closed")
        }
        
        # Extract token information if available
        if "tokens" in market and len(market["tokens"]) > 0:
            for i, token in enumerate(market["tokens"]):
                market_data[f"token_{i+1}_id"] = token.get("token_id")
                market_data[f"token_{i+1}_outcome"] = token.get("outcome")
        
        extracted_data.append(market_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(extracted_data)
    
    return df

def save_data(df, output_dir="data"):
    """
    Save the extracted data to CSV and JSON files.
    
    Args:
        df (pd.DataFrame): DataFrame with market data
        output_dir (str): Directory to save the output files
    """
    # Create output directory if it doesn't exist
    import os
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate timestamp for filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save as CSV
    csv_filename = f"{output_dir}/election_markets_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Saved data to {csv_filename}")
    
    # Save as JSON
    json_filename = f"{output_dir}/election_markets_{timestamp}.json"
    df.to_json(json_filename, orient="records", indent=2)
    print(f"Saved data to {json_filename}")

def main():
    # Get election markets
    markets = get_election_markets(limit=500, tag="elections")
    
    # Extract and process market data
    market_df = extract_market_data(markets)
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Total Markets: {len(market_df)}")
    print(f"Total Volume: {market_df['volume'].sum():,.2f}")
    print(f"Average Volume per Market: {market_df['volume'].mean():,.2f}")
    print(f"Active Markets: {market_df['active'].sum()}")
    print(f"Closed Markets: {market_df['closed'].sum()}")
    
    # Save the data
    save_data(market_df)

if __name__ == "__main__":
    main()