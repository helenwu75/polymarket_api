import requests
import json
import pandas as pd

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
    
    # Print any nested objects if they exist
    if "outcomes" in market_data and market_data["outcomes"]:
        print("\n===== OUTCOMES =====")
        for i, outcome in enumerate(market_data["outcomes"]):
            print(f"Outcome {i+1}: {outcome}")
    
    if "outcomePrices" in market_data and market_data["outcomePrices"]:
        print("\n===== OUTCOME PRICES =====")
        for i, price in enumerate(market_data["outcomePrices"]):
            print(f"Price {i+1}: {price}")
    
    if "clobTokenIds" in market_data and market_data["clobTokenIds"]:
        print("\n===== CLOB TOKEN IDs =====")
        for i, token_id in enumerate(market_data["clobTokenIds"]):
            print(f"Token ID {i+1}: {token_id}")

def export_market_details(market_data, format="all"):
    """
    Export market data to various formats.
    
    Args:
        market_data (dict): Market data from the GAMMA API
        format (str): Export format - "csv", "json", or "all"
        
    Returns:
        dict: Dictionary with file paths of the exports
    """
    if not market_data:
        print("No market data to export.")
        return {}
        
    result = {}
    market_id = market_data.get("id", "unknown")
    
    # Export as JSON
    if format in ["json", "all"]:
        json_path = f"market_{market_id}_details.json"
        with open(json_path, "w") as f:
            json.dump(market_data, f, indent=2)
        print(f"Exported JSON to {json_path}")
        result["json"] = json_path
    
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
        csv_path = f"market_{market_id}_details.csv"
        df.to_csv(csv_path, index=False)
        print(f"Exported CSV to {csv_path}")
        result["csv"] = csv_path
        
    return result

# Example usage
# if __name__ == "__main__":
    # Example 1: Get market by ID
    # market = get_market_details("12", id_type="id")
    # print_market_details(market)
    
    # Example 2: Get market by slug
    # market = get_market_details("will-joe-biden-get-coronavirus-before-the-election", id_type="slug")
    # print_market_details(market)
    # export_market_details(market)