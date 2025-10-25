#!/usr/bin/env python3
"""
Simple script to fetch the top 10 trending bets from Polymarket API.
Uses only Python standard library - NO external dependencies required!
"""

import urllib.request
import urllib.parse
import json


def get_trending_markets(limit=10):
    """
    Fetch trending markets from Polymarket API.

    Args:
        limit (int): Number of markets to retrieve (default: 10)

    Returns:
        list: List of market dictionaries
    """
    base_url = "https://gamma-api.polymarket.com/markets"

    # Parameters to get trending markets (ordered by 24h volume)
    params = {
        'closed': 'false',           # Only active markets
        'order': 'volume24hr',       # Order by 24-hour trading volume
        'ascending': 'false',        # Descending order (highest volume first)
        'limit': str(limit)          # Top N markets
    }

    # Build URL with query parameters
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    try:
        # Create request with User-Agent header
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

        with urllib.request.urlopen(req, timeout=10) as response:
            data = response.read()
            return json.loads(data)
    except Exception as e:
        print(f"Error fetching data from Polymarket API: {e}")
        return []


def display_market(market, index):
    """
    Display market information in a readable format.

    Args:
        market (dict): Market data
        index (int): Market ranking
    """
    question = market.get('question', 'N/A')
    description = market.get('description', 'No description')
    volume_24h = market.get('volume24hr', 0)
    liquidity = market.get('liquidity', 0)

    # Get outcome prices (probabilities)
    outcomes = market.get('outcomePrices', [])

    print(f"\n{'='*80}")
    print(f"#{index} - {question}")
    print(f"{'='*80}")
    print(f"Description: {description[:150]}..." if len(description) > 150 else f"Description: {description}")
    print(f"\n24h Volume: ${float(volume_24h):,.2f}")
    print(f"Liquidity: ${float(liquidity):,.2f}")

    if outcomes:
        # Convert prices to percentages (Polymarket uses 0-1 scale)
        print(f"\nCurrent Odds:")
        tokens = market.get('tokens', [])
        for i, (price, token) in enumerate(zip(outcomes, tokens)):
            outcome_name = token.get('outcome', f'Outcome {i+1}')
            probability = float(price) * 100
            print(f"  {outcome_name}: {probability:.1f}%")

    # Market URL
    market_slug = market.get('slug', '')
    if market_slug:
        print(f"\nView market: https://polymarket.com/event/{market_slug}")


def main():
    print("Fetching top 10 trending bets from Polymarket...")
    print("(Ordered by 24-hour trading volume)\n")

    markets = get_trending_markets(limit=10)

    if not markets:
        print("No markets found or error occurred.")
        return

    for index, market in enumerate(markets, start=1):
        display_market(market, index)

    print(f"\n{'='*80}")
    print(f"Total markets retrieved: {len(markets)}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
