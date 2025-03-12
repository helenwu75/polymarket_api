import requests
import pandas as pd
import time
import json
import os
from datetime import datetime

# GAMMA API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com"

def get_election_markets(limit=300, tag="elections", closed=True, neg_risk=True, max_retries=3):
    """
    Query the GAMMA API for election markets with specified properties.
    
    Args:
        limit (int): Maximum number of markets to retrieve
        tag (str): Tag to filter markets by (default: "elections")
        closed (bool): Whether to filter for closed markets
        neg_risk (bool): Whether to filter for markets with negRisk=true
        max_retries (int): Maximum number of retry attempts for API calls
        
    Returns:
        list: List of market data dictionaries
    """
    print(f"Fetching top {limit} closed election markets with negRisk=true...")
    
    # Initialize variables for pagination
    all_markets = []
    offset = 0
    page_size = 100  # Fetch 100 markets at a time
    
    while len(all_markets) < limit:
        # Build query parameters
        params = {
            "limit": page_size,
            "offset": offset,
            "order": "volume_num",  # Sort by volume
            "ascending": "false",   # Descending order (highest volume first)
            "tag": tag,             # Filter by elections tag
            "closed": str(closed).lower(),  # Filter by closed status
        }
        
        # Make API request with retries
        for retry in range(max_retries):
            try:
                print(f"Requesting page with offset {offset}...")
                response = requests.get(f"{GAMMA_API_URL}/markets", params=params, timeout=15)
                response.raise_for_status()
                
                # Parse response
                data = response.json()
                
                # Try different response formats (API might vary)
                if isinstance(data, list):
                    # Direct list of markets
                    markets = data
                elif isinstance(data, dict) and "markets" in data:
                    # Object with markets array
                    markets = data["markets"]
                else:
                    print(f"Unexpected API response structure. First 500 chars: {str(data)[:500]}...")
                    # Try to find any list in the response that might contain markets
                    markets = []
                    for key, value in data.items():
                        if isinstance(value, list) and len(value) > 0 and isinstance(value[0], dict):
                            if any(field in value[0] for field in ["conditionId", "question", "volume"]):
                                markets = value
                                print(f"Found potential markets list under key: {key}")
                                break
                    
                    if not markets:
                        print("Could not find markets data in response.")
                        if retry < max_retries - 1:
                            print(f"Retrying ({retry+1}/{max_retries})...")
                            time.sleep(2)
                            continue
                        else:
                            break
                
                # Break if no more markets found
                if not markets:
                    print("No more markets found.")
                    break
                
                # Filter for markets with negRisk=true
                filtered_markets = [market for market in markets if market.get("negRisk") == True]
                print(f"Fetched {len(markets)} markets, {len(filtered_markets)} with negRisk=true")
                
                # Add filtered markets to our collection
                all_markets.extend(filtered_markets)
                print(f"Total markets collected: {len(all_markets)}")
                
                # Increment offset for next page
                offset += page_size
                
                # Small delay to avoid API rate limits
                time.sleep(1)
                
                # Success, exit retry loop
                break
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching markets: {e}")
                if retry < max_retries - 1:
                    wait_time = 2 ** retry  # Exponential backoff
                    print(f"Retrying in {wait_time} seconds... ({retry+1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    print(f"Max retries reached. Moving on.")
                    break
        
        # If we didn't get any markets in this batch, break the loop
        if len(all_markets) == 0 or (len(all_markets) > 0 and len(filtered_markets) == 0):
            break
    
    # Save raw data for debugging if needed
    with open("raw_election_markets_data.json", "w") as f:
        json.dump(all_markets, f, indent=2)
    
    # Trim to the requested limit
    result = all_markets[:limit]
    print(f"Retrieved {len(result)} election markets matching criteria")
    
    return result

def create_market_dataframe(markets):
    """
    Convert market data to a pandas DataFrame with all available properties.
    Handles nested structures and normalizes data.
    
    Args:
        markets (list): List of market data dictionaries
        
    Returns:
        pd.DataFrame: DataFrame with all market properties
    """
    # If no markets found, return empty DataFrame
    if not markets:
        print("No markets found matching criteria.")
        return pd.DataFrame()
    
    # Process the markets to handle nested structures
    processed_markets = []
    
    for market in markets:
        market_copy = market.copy()
        
        # Handle potentially nested structures by flattening or converting to string
        for key, value in market.items():
            # Convert lists and dictionaries to strings
            if isinstance(value, (list, dict)):
                market_copy[key] = json.dumps(value)
                
            # For fields like 'events' that might contain nested markets
            if key == 'events' and isinstance(value, list):
                # Extract only essential event information
                events_summary = []
                for event in value:
                    event_summary = {
                        'event_id': event.get('id'),
                        'event_title': event.get('title'),
                        'event_slug': event.get('slug'),
                        'event_volume': event.get('volume')
                    }
                    events_summary.append(event_summary)
                market_copy['events_summary'] = json.dumps(events_summary)
        
        processed_markets.append(market_copy)
    
    # Create DataFrame
    df = pd.DataFrame(processed_markets)
    
    # Determine the most important columns for priority ordering
    key_columns = [
        'id', 'question', 'conditionId', 'slug', 'volume', 'volumeNum',
        'liquidity', 'liquidityNum', 'active', 'closed', 'negRisk',
        'startDate', 'endDate', 'closedTime', 'outcomePrices'
    ]
    
    # Reorder columns to put key columns first
    all_columns = list(df.columns)
    for col in reversed(key_columns):
        if col in all_columns:
            all_columns.remove(col)
            all_columns.insert(0, col)
    
    # Apply the column reordering
    df = df[all_columns]
    
    # Try to convert numeric columns that might be stored as strings
    numeric_columns = ['volume', 'volumeNum', 'liquidity', 'liquidityNum', 
                     'volume24hr', 'volumeClob', 'liquidityClob']
    
    for col in numeric_columns:
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                pass  # Keep as is if conversion fails
    
    print(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
    return df

def main():
    # Create output directory if it doesn't exist
    output_dir = "polymarket_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Get election markets matching criteria
    markets = get_election_markets(limit=300, tag="elections", closed=True, neg_risk=True)
    
    # Convert to DataFrame
    df = create_market_dataframe(markets)
    
    # If no data found, exit
    if df.empty:
        print("No data to export.")
        return
    
    # Generate timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save to CSV
    filename = f"{output_dir}/closed_election_markets_negrisk_{timestamp}.csv"
    
    try:
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Successfully saved data to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")
        # Try again with a simpler approach
        try:
            # Select only the most important columns to avoid encoding issues
            essential_columns = [col for col in df.columns if isinstance(df[col].iloc[0], (str, int, float, bool, type(None)))]
            df_essential = df[essential_columns]
            df_essential.to_csv(f"{output_dir}/closed_election_markets_essential_{timestamp}.csv", index=False)
            print(f"Saved essential data to {output_dir}/closed_election_markets_essential_{timestamp}.csv")
        except Exception as e2:
            print(f"Could not save CSV files: {e2}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print(f"Total Markets: {len(df)}")
    
    # Calculate volume statistics if available
    volume_col = None
    for col in ['volumeNum', 'volume']:
        if col in df.columns and df[col].notna().any():
            volume_col = col
            break
    
    if volume_col:
        # Ensure numeric
        try:
            df[volume_col] = pd.to_numeric(df[volume_col], errors='coerce')
            df_valid = df[df[volume_col].notna()]
            
            if not df_valid.empty:
                print(f"\nVolume Statistics (using {volume_col}):")
                print(f"Total Volume: {df_valid[volume_col].sum():,.2f}")
                print(f"Average Volume per Market: {df_valid[volume_col].mean():,.2f}")
                print(f"Max Volume: {df_valid[volume_col].max():,.2f}")
                print(f"Min Volume: {df_valid[volume_col].min():,.2f}")
        except Exception as e:
            print(f"Could not calculate volume statistics: {e}")
    
    # Print the top 5 markets by volume
    if volume_col and not df_valid.empty:
        print("\nTop 5 Markets by Volume:")
        top_markets = df_valid.sort_values(by=volume_col, ascending=False).head(5)
        for _, market in top_markets.iterrows():
            print(f"- {market.get('question', 'Unnamed')} | Volume: {market[volume_col]:,.2f} | ID: {market.get('id', 'Unknown')}")
            
    print("\nQuery complete! Data has been saved to CSV file.")

def alternative_query():
    """
    Alternative approach to query markets in case tag-based query fails.
    This function tries to find election-related markets by keywords in questions.
    """
    print("Attempting alternative query method based on keywords...")
    
    # Keywords related to elections
    election_keywords = [
        "president", "election", "presidential", "vote", "ballot", 
        "congress", "senate", "governor", "candidate", "primary",
        "mayor", "democrat", "republican", "constituency", "parliament"
    ]
    
    # Query without tags, then filter by keywords
    all_markets = []
    offset = 0
    page_size = 100
    
    while len(all_markets) < 500:  # Get more to filter down later
        params = {
            "limit": page_size,
            "offset": offset,
            "order": "volume_num",
            "ascending": "false",
            "closed": "true"
        }
        
        try:
            response = requests.get(f"{GAMMA_API_URL}/markets", params=params, timeout=15)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, list):
                markets = data
            elif isinstance(data, dict) and "markets" in data:
                markets = data["markets"]
            else:
                print("Could not parse response.")
                break
                
            if not markets:
                break
                
            # Filter for election-related markets with negRisk=true
            for market in markets:
                question = market.get("question", "").lower()
                description = market.get("description", "").lower()
                
                # Check if any election keyword appears in question or description
                if any(keyword in question or keyword in description for keyword in election_keywords):
                    if market.get("negRisk") == True:
                        all_markets.append(market)
            
            print(f"Found {len(all_markets)} election-related markets so far")
            offset += page_size
            time.sleep(1)
            
        except Exception as e:
            print(f"Error in alternative query: {e}")
            break
    
    # Return top markets by volume
    return sorted(all_markets, key=lambda x: float(x.get("volumeNum", 0) or 0), reverse=True)[:300]

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error in main execution: {e}")
        print("Trying alternative approach...")
        
        try:
            # Try alternative approach if main fails
            markets = alternative_query()
            if markets:
                df = create_market_dataframe(markets)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_dir = "polymarket_data"
                os.makedirs(output_dir, exist_ok=True)
                df.to_csv(f"{output_dir}/election_markets_keywords_{timestamp}.csv", index=False)
                print(f"Saved data using alternative approach to election_markets_keywords_{timestamp}.csv")
            else:
                print("Alternative approach found no markets.")
        except Exception as e2:
            print(f"Alternative approach also failed: {e2}")
            print("Please check API access and try again.")