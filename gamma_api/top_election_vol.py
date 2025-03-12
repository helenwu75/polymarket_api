import requests
import pandas as pd
import time
import json
import os
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# GAMMA API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Define winning and election keywords
WIN_KEYWORDS = ["win", "wins"]
ELECTION_KEYWORDS = ["election", "elected"]

# Pattern to exclude undesired markets
EXCLUDE_PATTERNS = [
    
]

# Precompile regex patterns
COMPILED_EXCLUDE_PATTERNS = [re.compile(pattern, re.IGNORECASE) for pattern in EXCLUDE_PATTERNS]
HAS_NUMBER_PATTERN = re.compile(r'\d+')

# Define target properties to collect
TARGET_PROPERTIES = [
    "id", "question", "slug", "conditionId", "startDate", "endDate", 
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

def fetch_markets_batch(offset, limit=500):
    """Fetch a batch of closed markets from the API with retry logic."""
    # Added closed=true parameter to get only closed markets
    params = {"limit": limit, "offset": offset, "closed": "true"}
    
    # Implement retry logic
    max_retries = 3
    retry_delay = 5  # seconds
    
    for attempt in range(max_retries):
        try:
            response = requests.get(f"{GAMMA_API_URL}/markets", params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, dict) and "markets" in data:
                return data.get("markets", [])
            elif isinstance(data, list):
                return data
            else:
                return []
                
        except Exception as e:
            if attempt < max_retries - 1:
                delay = retry_delay * (2 ** attempt)  # Exponential backoff
                print(f"Error fetching markets at offset {offset}. Retrying in {delay} seconds... ({e})")
                time.sleep(delay)
            else:
                print(f"Failed to fetch markets at offset {offset} after {max_retries} attempts: {e}")
                return []

def is_valid_market(market):
    """Check if market meets all criteria."""
    # Verify the market is closed
    if not market.get("closed", False):
        return False
        
    # Check groupItemTitle first
    group_item_title = market.get("groupItemTitle")
    if not group_item_title or not isinstance(group_item_title, str) or not group_item_title.strip() or HAS_NUMBER_PATTERN.search(group_item_title):
        return False
    
    # Get text content for keyword search
    question = market.get("question", "").lower()
    description = market.get("description", "").lower() if market.get("description") else ""
    text_content = question + " " + description
    
    # Check for excluded patterns
    for pattern in COMPILED_EXCLUDE_PATTERNS:
        if pattern.search(text_content):
            return False
    
    # Check for required keywords
    has_win_keyword = any(win_word in text_content for win_word in WIN_KEYWORDS)
    has_election_keyword = any(election_word in text_content for election_word in ELECTION_KEYWORDS)
    
    return has_win_keyword and has_election_keyword

def fetch_all_markets(max_workers=10):
    """Fetch ALL closed markets exhaustively using pagination."""
    print("Starting exhaustive closed market search...")
    
    all_markets = []
    batch_size = 500
    offset = 0
    has_more = True
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while has_more:
            futures = []
            
            # Create batch of requests
            for i in range(max_workers):
                current_offset = offset + (i * batch_size)
                futures.append((current_offset, executor.submit(fetch_markets_batch, current_offset, batch_size)))
            
            # Process results
            all_empty = True
            for current_offset, future in futures:
                try:
                    markets = future.result()
                    if markets:
                        all_empty = False
                        all_markets.extend(markets)
                        print(f"Fetched {len(markets)} closed markets at offset {current_offset} (total: {len(all_markets)})")
                except Exception as e:
                    print(f"Error processing batch at offset {current_offset}: {e}")
            
            # Update offset for next batch
            offset += batch_size * max_workers
            
            # If all requests returned empty, we've likely reached the end
            if all_empty:
                has_more = False
                print("Reached the end of available closed markets.")
            
            # Brief pause to avoid overwhelming the API
            time.sleep(0.5)
    
    print(f"Completed exhaustive search. Found {len(all_markets)} total closed markets.")
    return all_markets

def extract_market_data(markets):
    """
    Extract relevant information from market data including all target properties
    and event properties, with proper numeric conversion.
    
    Args:
        markets (list): List of market data from the GAMMA API
        
    Returns:
        pd.DataFrame: DataFrame with extracted market information
    """
    # Pre-allocate the list with the correct size for better performance
    extracted_data = []
    
    for market in markets:
        # Extract basic market information based on TARGET_PROPERTIES
        market_data = {}
        for prop in TARGET_PROPERTIES:
            market_data[prop] = market.get(prop)
            
        # Extract event-related information if available
        if "events" in market and market["events"] and len(market["events"]) > 0:
            event = market["events"][0]  # Take the first event
            for prop in EVENT_PROPERTIES:
                market_data[f"event_{prop}"] = event.get(prop)
        
        # Extract token information if available
        if "tokens" in market and market["tokens"] and len(market["tokens"]) > 0:
            for i, token in enumerate(market["tokens"][:2]):  # Process first 2 tokens
                market_data[f"token_{i+1}_id"] = token.get("token_id")
                market_data[f"token_{i+1}_outcome"] = token.get("outcome")
        
        extracted_data.append(market_data)
    
    # Convert to DataFrame
    df = pd.DataFrame(extracted_data)
    
    # Convert numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            # First, convert to string to handle any None values
            df[col] = df[col].astype(str)
            
            # Replace empty strings and 'None' with NaN
            df[col] = df[col].replace('None', pd.NA)
            df[col] = df[col].replace('', pd.NA)
            
            # Convert to numeric, handling errors
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def get_top_markets_by_volume(top_n=500, max_workers=10):
    """Get ALL closed markets, filter them, and return the top N by volume."""
    # Step 1: Fetch ALL closed markets without any filtering
    all_markets = fetch_all_markets(max_workers=max_workers)
    
    # Step 2: Filter markets in parallel
    print("Filtering for closed election win markets...")
    filtered_markets = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Process in chunks to avoid memory issues
        chunk_size = 1000
        for i in range(0, len(all_markets), chunk_size):
            chunk = all_markets[i:i+chunk_size]
            results = list(executor.map(is_valid_market, chunk))
            
            for market, is_valid in zip(chunk, results):
                if is_valid:
                    filtered_markets.append(market)
            
            print(f"Processed {i + len(chunk)}/{len(all_markets)} markets - Found {len(filtered_markets)} matches so far")
    
    print(f"Found {len(filtered_markets)} closed markets matching criteria.")
    
    # Step 3: Sort by volume
    print("Sorting markets by volume...")
    
    def get_volume(market):
        vol = market.get("volumeNum")
        if vol is None:
            vol = market.get("volume", 0)
        try:
            return float(vol) if vol is not None else 0
        except (ValueError, TypeError):
            return 0
    
    filtered_markets.sort(key=get_volume, reverse=True)
    
    # Step 4: Get top N markets
    top_markets = filtered_markets[:top_n]
    print(f"Selected top {len(top_markets)} closed markets by volume out of {len(filtered_markets)} matches.")
    
    return top_markets

def save_market_data(df, output_dir="data"):
    """Save market data to CSV and JSON files."""
    if df.empty:
        print("No markets to save.")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate filenames with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"{output_dir}/top_closed_election_markets_{timestamp}.csv"
    json_filename = f"{output_dir}/top_closed_election_markets_{timestamp}.json"
    
    # Save files
    df.to_csv(csv_filename, index=False)
    df.to_json(json_filename, orient="records", indent=2)
    
    print(f"Saved data to {csv_filename} and {json_filename}")
    
    # Print summary statistics
    if 'volumeNum' in df.columns:
        volume_col = 'volumeNum'
    elif 'volume' in df.columns:
        volume_col = 'volume'
    else:
        volume_col = None
    
    if volume_col and not df.empty:
        total_volume = df[volume_col].sum()
        avg_volume = df[volume_col].mean()
        print(f"\nSummary Statistics:")
        print(f"Total Closed Markets: {len(df)}")
        print(f"Total Volume: {total_volume:,.2f}")
        print(f"Average Volume: {avg_volume:,.2f}")

def main():
    start_time = time.time()
    
    # Get ALL closed markets, filter them, and select top 500 by volume
    markets = get_top_markets_by_volume(
        top_n=500,        # Return top 500 by volume
        max_workers=10    # Use 10 concurrent workers
    )
    
    # Convert markets to dataframe with all properties
    markets_df = extract_market_data(markets)
    
    # Save market data
    save_market_data(markets_df)
    
    # Print execution time
    elapsed_time = time.time() - start_time
    print(f"\nExecution completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()