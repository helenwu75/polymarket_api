import requests
import pandas as pd
import time
import json
import os
from datetime import datetime
import sys

# Create output directory
os.makedirs("polymarket_data", exist_ok=True)

# GAMMA API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Election-related keywords for filtering markets
ELECTION_KEYWORDS = [
    "president", "election", "presidential", "vote", "ballot", 
    "congress", "senate", "governor", "candidate", "primary", "nominee",
    "mayor", "democrat", "republican", "constituency", "parliament",
    "chancellor", "minister", "party", "campaign", "poll"
]

# Properties to include in the CSV file
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

def is_election_related(market):
    """
    Check if a market is election-related based on its text content.
    
    Args:
        market (dict): Market data from the API
        
    Returns:
        bool: True if the market appears to be election-related
    """
    # Extract text content to search
    question = market.get("question", "").lower()
    description = market.get("description", "").lower()
    category = market.get("category", "").lower()
    
    # Check for election keywords in the text
    for keyword in ELECTION_KEYWORDS:
        if keyword.lower() in question or keyword.lower() in description or keyword.lower() in category:
            return True
            
    # Check for special categories
    if "election" in category or "political" in category or "politics" in category:
        return True
        
    # Check for event properties
    events = market.get("events", [])
    if events and isinstance(events, list):
        for event in events:
            event_title = event.get("title", "").lower()
            event_desc = event.get("description", "").lower()
            event_type = event.get("electionType", "").lower()
            
            if event_type or "election" in event_title or any(k.lower() in event_title for k in ELECTION_KEYWORDS) or \
               any(k.lower() in event_desc for k in ELECTION_KEYWORDS):
                return True
    
    return False

def fetch_markets_batched(search_terms, limit_per_term=100, max_retries=3):
    """
    Fetch markets using search terms to find election-related markets.
    
    Args:
        search_terms (list): List of election-related search terms
        limit_per_term (int): Maximum number of markets to retrieve per term
        max_retries (int): Maximum number of retry attempts for API calls
        
    Returns:
        list: Combined list of market data dictionaries
    """
    print(f"Fetching markets using {len(search_terms)} election-related search terms...")
    
    all_markets = []
    seen_ids = set()  # To avoid duplicates
    
    for term in search_terms:
        print(f"\nSearching for markets with term: '{term}'")
        
        # Initialize variables for pagination
        term_markets = []
        offset = 0
        page_size = 20  # Smaller page size to avoid issues
        
        while len(term_markets) < limit_per_term:
            # Build query parameters - simpler query to avoid 422 errors
            params = {
                "limit": page_size,
                "offset": offset,
                "search": term
            }
            
            # Make API request with retries
            for retry in range(max_retries):
                try:
                    print(f"  Requesting page with offset {offset}...")
                    response = requests.get(f"{GAMMA_API_URL}/markets", params=params, timeout=15)
                    response.raise_for_status()
                    
                    # Parse response
                    data = response.json()
                    
                    # Extract markets from response
                    if "markets" in data and isinstance(data["markets"], list):
                        markets = data["markets"]
                    else:
                        print("  Could not find markets data in response.")
                        break
                    
                    # Break if no more markets found
                    if not markets:
                        print("  No more markets found.")
                        break
                    
                    print(f"  Fetched {len(markets)} markets")
                    
                    # Filter for closed markets and add to our collection if not already seen
                    for market in markets:
                        market_id = market.get("id")
                        if market_id and market_id not in seen_ids and market.get("closed") == True:
                            term_markets.append(market)
                            seen_ids.add(market_id)
                    
                    print(f"  Running total: {len(term_markets)} closed markets for term '{term}'")
                    
                    # Increment offset for next page
                    offset += page_size
                    
                    # Small delay to avoid API rate limits
                    time.sleep(1)
                    
                    # Success, exit retry loop
                    break
                    
                except requests.exceptions.RequestException as e:
                    print(f"  Error fetching markets: {e}")
                    if retry < max_retries - 1:
                        wait_time = 2 ** retry  # Exponential backoff
                        print(f"  Retrying in {wait_time} seconds... ({retry+1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        print(f"  Max retries reached. Moving on.")
                        break
            
            # If we didn't get any new markets in this batch, break the loop
            if len(markets) == 0 or len(term_markets) >= limit_per_term:
                break
        
        print(f"  Found total of {len(term_markets)} closed markets for term '{term}'")
        all_markets.extend(term_markets)
    
    # Remove duplicates (though we should have handled this above)
    unique_markets = {market.get("id"): market for market in all_markets if market.get("id")}.values()
    unique_markets_list = list(unique_markets)
    
    print(f"\nTotal unique markets found across all search terms: {len(unique_markets_list)}")
    
    # Save raw data before filtering
    raw_data_file = "polymarket_data/raw_markets_data.json"
    with open(raw_data_file, "w") as f:
        json.dump(unique_markets_list, f, indent=2)
    print(f"Saved raw market data to {raw_data_file}")
    
    return unique_markets_list

def get_election_markets(limit=300):
    """
    Get election-related markets using search terms.
    
    Args:
        limit (int): Maximum number of markets to return
        
    Returns:
        list: Sorted list of election-related market data
    """
    # Search terms relevant to elections
    search_terms = [
        "election", "president", "vote", "ballot", "candidate",
        "congress", "senate", "governor", "mayor", "parliament",
        "chancellor", "minister", "party", "campaign"
    ]
    
    # Fetch markets using search terms
    all_markets = fetch_markets_batched(search_terms, limit_per_term=50)
    
    # Additional filtering for election-related markets
    print("Further filtering for election-related markets...")
    election_markets = []
    
    for market in all_markets:
        if is_election_related(market):
            # Make sure volume field exists and is numeric
            if "volumeNum" in market:
                try:
                    market["volumeNum"] = float(market["volumeNum"])
                except (ValueError, TypeError):
                    market["volumeNum"] = 0
            elif "volume" in market:
                try:
                    market["volumeNum"] = float(market["volume"])
                except (ValueError, TypeError):
                    market["volumeNum"] = 0
            else:
                market["volumeNum"] = 0
                
            election_markets.append(market)
    
    print(f"Found {len(election_markets)} election-related markets out of {len(all_markets)} total markets")
    
    # Sort by volume (descending)
    sorted_markets = sorted(election_markets, key=lambda x: x.get("volumeNum", 0), reverse=True)
    
    # Save raw election market data
    election_data_file = "polymarket_data/raw_election_markets_data.json"
    with open(election_data_file, "w") as f:
        json.dump(sorted_markets, f, indent=2)
    print(f"Saved raw election market data to {election_data_file}")
    
    # Trim to requested limit
    result = sorted_markets[:limit]
    print(f"Returning top {len(result)} election markets by volume")
    
    return result

def extract_events_data(events):
    """
    Extract the specified event properties from a list of events.
    
    Args:
        events (list): List of event objects
        
    Returns:
        dict: Dictionary with extracted event properties
    """
    if not events or not isinstance(events, list):
        return {}
    
    events_data = {}
    
    # Properties to extract from the first event
    event_properties = [
        "id", "slug", "liquidity", "description", "category", 
        "volume", "volume24hr", "liquidityClob", "commentCount",
        "countryName", "electionType"
    ]
    
    # Take data from the first event (most relevant)
    if events:
        first_event = events[0]
        for prop in event_properties:
            if prop in first_event:
                events_data[f"event_{prop}"] = first_event.get(prop)
    
    return events_data

def process_market_data(market):
    """
    Process a market dictionary to extract and format all required properties.
    
    Args:
        market (dict): Raw market data from API
        
    Returns:
        dict: Processed market data with extracted properties
    """
    processed = {}
    
    # Handle events separately
    events_data = {}
    if "events" in market and isinstance(market["events"], list):
        events_data = extract_events_data(market["events"])
    
    # Extract specified target properties
    for prop in TARGET_PROPERTIES:
        if prop in market:
            # Handle special cases like lists and dictionaries
            if isinstance(market[prop], (list, dict)):
                processed[prop] = json.dumps(market[prop])
            else:
                processed[prop] = market[prop]
        else:
            processed[prop] = None
    
    # Add event data
    processed.update(events_data)
    
    return processed

def create_market_dataframe(markets):
    """
    Convert market data to a pandas DataFrame with specified properties.
    
    Args:
        markets (list): List of market data dictionaries
        
    Returns:
        pd.DataFrame: DataFrame with market properties
    """
    # If no markets found, return empty DataFrame
    if not markets:
        print("No markets found matching criteria.")
        return pd.DataFrame()
    
    # Process markets to extract required properties
    processed_markets = [process_market_data(market) for market in markets]
    
    # Create DataFrame
    df = pd.DataFrame(processed_markets)
    
    # Prioritize key columns in ordering
    key_columns = [
        'id', 'question', 'conditionId', 'slug', 'volume', 'volumeNum',
        'liquidity', 'liquidityNum', 'active', 'closed',
        'startDate', 'endDate', 'closedTime', 'outcomePrices'
    ]
    
    # Identify columns that are in the DataFrame
    available_columns = [col for col in key_columns if col in df.columns]
    
    # Get remaining columns
    remaining_columns = [col for col in df.columns if col not in available_columns]
    
    # Combine column lists to reorder columns
    all_columns = available_columns + remaining_columns
    
    # Reorder columns
    df = df[all_columns]
    
    # Convert numeric columns that are stored as strings
    numeric_columns = ['volume', 'volumeNum', 'liquidity', 'liquidityNum', 
                      'volume24hr', 'volumeClob', 'liquidityClob', 
                      'orderMinSize', 'orderPriceMinTickSize', 'umaBond', 
                      'umaReward', 'spread', 'oneDayPriceChange', 
                      'lastTradePrice', 'bestBid', 'bestAsk']
    
    for col in numeric_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                pass  # Keep as is if conversion fails
    
    print(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
    return df

def main():
    """
    Main function to query and export election market data.
    """
    print("Starting Polymarket election markets data collection...")
    
    # Get top election markets
    markets = get_election_markets(limit=300)
    
    # If no markets found, exit
    if not markets:
        print("No election markets found. Exiting.")
        return
    
    # Convert to DataFrame
    df = create_market_dataframe(markets)
    
    # If DataFrame is empty, exit
    if df.empty:
        print("No data to export after processing. Exiting.")
        return
    
    # Generate timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save to CSV
    filename = f"polymarket_data/closed_election_markets_{timestamp}.csv"
    
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Successfully saved data to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        
        # Try again with a simpler approach
        try:
            # Select non-problematic columns
            problematic_types = [list, dict, set]
            safe_columns = []
            for col in df.columns:
                try:
                    if not any(isinstance(x, tuple(problematic_types)) for x in df[col] if x is not None):
                        safe_columns.append(col)
                except:
                    pass  # Skip this column if checking causes an error
            
            df_safe = df[safe_columns]
            safe_filename = f"polymarket_data/closed_election_markets_safe_{timestamp}.csv"
            df_safe.to_csv(safe_filename, index=False, encoding='utf-8')
            print(f"Saved data with safe columns to {safe_filename}")
        except Exception as e2:
            print(f"Could not save CSV files: {e2}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Total Markets: {len(df)}")
    
    # Calculate volume statistics
    volume_col = None
    for col in ['volumeNum', 'volume']:
        if col in df.columns and df[col].notna().any():
            volume_col = col
            break
    
    if volume_col:
        try:
            df[volume_col] = pd.to_numeric(df[volume_col], errors='coerce')
            df_valid = df[df[volume_col].notna()]
            
            if not df_valid.empty:
                print(f"\nVolume Statistics (using {volume_col}):")
                print(f"Total Volume: {df_valid[volume_col].sum():,.2f}")
                print(f"Average Volume per Market: {df_valid[volume_col].mean():,.2f}")
                print(f"Max Volume: {df_valid[volume_col].max():,.2f}")
                print(f"Min Volume: {df_valid[volume_col].min():,.2f}")
                
                # Print the top 5 markets by volume
                print("\nTop 5 Markets by Volume:")
                top_markets = df_valid.sort_values(by=volume_col, ascending=False).head(5)
                for _, market in top_markets.iterrows():
                    print(f"- {market.get('question', 'Unnamed')} | Volume: {market[volume_col]:,.2f} | ID: {market.get('id', 'Unknown')}")
        except Exception as e:
            print(f"Could not calculate volume statistics: {e}")
    
    print("\nQuery complete! Data has been saved to CSV file.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"Error in main execution: {e}")
        print("\nDetailed error information:")
        traceback.print_exc()
        print("\nPlease check the log output for more details.")