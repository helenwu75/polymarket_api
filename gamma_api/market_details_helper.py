import requests
import json
import pandas as pd
import os
import re
from datetime import datetime

# GAMMA API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com"

def get_market_details(identifier, id_type="id"):
    """
    Get detailed information about a specific Polymarket market.
    
    Args:
        identifier (str): The identifier for the market (id, slug, or condition_id)
        id_type (str): Type of identifier. Options:
                       - "id": Numeric ID of the market
                       - "slug": Market slug (URL-friendly name)
                       - "condition_id": The market's condition ID
    
    Returns:
        dict: Complete market data with all available fields
    """
    print(f"Fetching market details for {id_type}: {identifier}")
    
    try:
        # Different query approaches based on identifier type
        if id_type == "id":
            # Direct fetch by ID
            response = requests.get(f"{GAMMA_API_URL}/markets/{identifier}")
        elif id_type == "slug":
            # Query by slug
            params = {"slug": identifier}
            response = requests.get(f"{GAMMA_API_URL}/markets", params=params)
        elif id_type == "condition_id":
            # Query by condition ID
            params = {"condition_ids": identifier}
            response = requests.get(f"{GAMMA_API_URL}/markets", params=params)
        else:
            raise ValueError(f"Invalid id_type: {id_type}. Must be 'id', 'slug', or 'condition_id'.")
        
        response.raise_for_status()
        data = response.json()
        
        # Handle different response formats
        if isinstance(data, dict) and "markets" in data:
            # Response is an object with markets array
            markets = data["markets"]
        elif isinstance(data, list):
            # Response is a list of markets
            markets = data
        elif isinstance(data, dict) and "market" in data:
            # Response is an object with a single market
            return data["market"]
        elif isinstance(data, dict):
            # Response is a single market object
            return data
        else:
            raise ValueError("Unexpected API response format")
        
        # Check if we found any markets
        if not markets:
            print(f"No market found with {id_type}: {identifier}")
            return None
            
        # If we get multiple markets, return the first one
        if len(markets) > 1:
            print(f"Found {len(markets)} markets. Returning the first one.")
            
        return markets[0]
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching market details: {e}")
        return None

def print_market_details(market_data):
    """
    Print detailed information about a market in a readable format.
    
    Args:
        market_data (dict): Market data from the GAMMA API
    """
    if not market_data:
        print("No market data available.")
        return
        
    # Print summary information
    print("\n===== MARKET SUMMARY =====")
    print(f"ID: {market_data.get('id')}")
    print(f"Question: {market_data.get('question')}")
    print(f"Condition ID: {market_data.get('conditionId')}")
    print(f"Category: {market_data.get('category')}")
    print(f"Volume: {market_data.get('volume')}")
    print(f"End Date: {market_data.get('endDate')}")
    print(f"Status: {'Active' if market_data.get('active') else 'Inactive'}, " +
          f"{'Closed' if market_data.get('closed') else 'Open'}")
    
    # Print all available fields
    print("\n===== ALL AVAILABLE FIELDS =====")
    for key, value in sorted(market_data.items()):
        # Format value for better readability
        if isinstance(value, (dict, list)):
            formatted_value = f"[{type(value).__name__} with {len(value)} items]"
        elif value is None:
            formatted_value = "None"
        elif isinstance(value, str) and len(value) > 50:
            formatted_value = value[:50] + "..."
        else:
            formatted_value = value
            
        print(f"{key}: {formatted_value}")
    
    # Handle specific fields with special formatting
    
    # For outcomes field
    if "outcomes" in market_data:
        print("\n===== OUTCOMES =====")
        outcomes = market_data["outcomes"]
        
        # Check if it's already a proper list
        if isinstance(outcomes, list):
            for i, outcome in enumerate(outcomes):
                print(f"Outcome {i+1}: {outcome}")
        # Check if it's a string representation of a list
        elif isinstance(outcomes, str):
            try:
                import json
                parsed_outcomes = json.loads(outcomes.replace("'", "\""))
                for i, outcome in enumerate(parsed_outcomes):
                    print(f"Outcome {i+1}: {outcome}")
            except json.JSONDecodeError:
                # If JSON parsing fails, look for alternatives
                print("Outcomes: " + outcomes)
    
    # For outcomePrices field
    if "outcomePrices" in market_data:
        print("\n===== OUTCOME PRICES =====")
        prices = market_data["outcomePrices"]
        
        # Check if it's already a proper list
        if isinstance(prices, list):
            for i, price in enumerate(prices):
                print(f"Price {i+1}: {price}")
        # Check if it's a string representation of a list
        elif isinstance(prices, str):
            try:
                import json
                parsed_prices = json.loads(prices.replace("'", "\""))
                for i, price in enumerate(parsed_prices):
                    print(f"Price {i+1}: {price}")
            except json.JSONDecodeError:
                # If JSON parsing fails, look for alternatives
                print("Prices: " + prices)
    
    # For clobTokenIds field
    if "clobTokenIds" in market_data:
        print("\n===== CLOB TOKEN IDs =====")
        token_ids = market_data["clobTokenIds"]
        
        # Check if it's already a proper list
        if isinstance(token_ids, list):
            for i, token_id in enumerate(token_ids):
                print(f"Token ID {i+1}: {token_id}")
        # Check if it's a string representation of a list
        elif isinstance(token_ids, str):
            try:
                import json
                # Try to parse it as JSON
                parsed_tokens = json.loads(token_ids.replace("'", "\""))
                # If there are only two token IDs, display them nicely
                if len(parsed_tokens) <= 5:
                    for i, token_id in enumerate(parsed_tokens):
                        print(f"Token ID {i+1}: {token_id}")
                # If there are many token IDs, just show the first few
                else:
                    for i, token_id in enumerate(parsed_tokens[:3]):
                        print(f"Token ID {i+1}: {token_id}")
                    print(f"... and {len(parsed_tokens) - 3} more token IDs")
            except json.JSONDecodeError:
                # If it's a truncated string (looks like in your example)
                if "..." in token_ids:
                    print(f"CLOB Token IDs: {token_ids}")
                # Otherwise, just display the string as is
                else:
                    print(f"CLOB Token IDs: {token_ids}")

def export_market_details(market_data, format="all"):
    """
    Export market data to various formats in a 'market_data' folder.
    
    Args:
        market_data (dict): Market data from the GAMMA API
        format (str): Export format - "csv", "json", or "all"
        
    Returns:
        dict: Dictionary with file paths of the exports
    """
    if not market_data:
        print("No market data to export.")
        return {}
    
    # Create market_data directory if it doesn't exist
    os.makedirs("market_data", exist_ok=True)
    
    result = {}
    
    # Sanitize market slug to remove any characters that might cause issues in filenames
    def sanitize_filename(slug):
        # Remove or replace characters that are problematic in filenames
        return re.sub(r'[^\w\-_\.]', '_', str(slug).lower())
    
    # Get market slug, use a fallback if not available
    market_slug = sanitize_filename(market_data.get("slug", market_data.get("id", "unknown_market")))
    
    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Export as JSON
    if format in ["json", "all"]:
        json_filename = os.path.join("market_data", f"{market_slug}_{timestamp}.json")
        with open(json_filename, "w") as f:
            json.dump(market_data, f, indent=2)
        print(f"Exported JSON to {json_filename}")
        result["json"] = json_filename
    
    # Export as CSV (flattening nested structures)
    if format in ["csv", "all"]:
        # Create a flattened version of the market data
        flat_data = {}
        for key, value in market_data.items():
            if isinstance(value, (list, dict)):
                flat_data[key] = json.dumps(value)
            else:
                flat_data[key] = value
                
        df = pd.DataFrame([flat_data])
        csv_filename = os.path.join("market_data", f"{market_slug}_{timestamp}.csv")
        df.to_csv(csv_filename, index=False)
        print(f"Exported CSV to {csv_filename}")
        result["csv"] = csv_filename
        
    return result

# Example usage
# from market_details_helper import export_market_details, get_market_details, print_market_details
# if __name__ == "__main__":
# Example 1: Get market by ID
# market = get_market_details("12", id_type="id")
# print_market_details(market)

# Example 2: Get market by slug
# market = get_market_details("will-donald-trump-win-the-2024-us-presidential-election", id_type="slug")
# print_market_details(market)
# export_market_details(market)