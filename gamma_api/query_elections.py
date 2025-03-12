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