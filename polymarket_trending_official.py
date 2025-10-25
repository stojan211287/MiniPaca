#!/usr/bin/env python3
"""
Fetch top trending bets from Polymarket using the official py-clob-client.

Installation: pip install py-clob-client
"""

from py_clob_client.client import ClobClient


def get_trending_markets(limit=10):
    """
    Fetch markets from Polymarket and sort by volume.

    Args:
        limit (int): Number of markets to display

    Returns:
        list: List of market dictionaries sorted by volume
    """
    client = ClobClient("https://clob.polymarket.com")

    try:
        # Get all markets
        response = client.get_simplified_markets()
        markets = response.get("data", [])

        # Filter for active markets only
        active_markets = [m for m in markets if not m.get("closed", True)]

        # Sort by volume (descending)
        sorted_markets = sorted(
            active_markets,
            key=lambda x: float(x.get("volume", 0) or 0),
            reverse=True
        )

        return sorted_markets[:limit]

    except Exception as e:
        print(f"Error fetching markets: {e}")
        return []


def display_market(market, index):
    """
    Display market information in a readable format.

    Args:
        market (dict): Market data
        index (int): Market ranking
    """
    question = market.get("question", "N/A")
    description = market.get("description", "No description")
    volume = market.get("volume", 0)
    active = not market.get("closed", True)

    print(f"\n{'='*80}")
    print(f"#{index} - {question}")
    print(f"{'='*80}")

    if description and description != question:
        desc_preview = description[:200] + "..." if len(description) > 200 else description
        print(f"Description: {desc_preview}")

    print(f"\nTotal Volume: ${float(volume):,.2f}")
    print(f"Status: {'Active' if active else 'Closed'}")

    # Display outcome tokens and their probabilities
    tokens = market.get("tokens", [])
    if tokens:
        print(f"\nCurrent Odds:")
        for token in tokens:
            outcome = token.get("outcome", "Unknown")
            price = token.get("price", 0)
            if price:
                probability = float(price) * 100
                print(f"  {outcome}: {probability:.1f}%")

    # Construct market URL
    condition_id = market.get("condition_id", "")
    if condition_id:
        print(f"\nView market: https://polymarket.com/event/{condition_id}")


def main():
    print("Fetching top 10 trending bets from Polymarket...")
    print("(Ordered by total trading volume)\n")

    markets = get_trending_markets(limit=10)

    if not markets:
        print("No markets found or error occurred.")
        print("\nMake sure you have installed: pip install py-clob-client")
        return

    for index, market in enumerate(markets, start=1):
        display_market(market, index)

    print(f"\n{'='*80}")
    print(f"Total markets retrieved: {len(markets)}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
