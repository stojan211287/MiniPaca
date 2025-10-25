#!/usr/bin/env python3
"""
Polymarket Trending Bets Fetcher with Enhanced Debugging

This script attempts multiple methods to fetch Polymarket data:
1. Official CLOB API client
2. Direct HTTP requests with various headers
3. Helpful debugging information

Requires: pip install py-clob-client requests
"""

import sys


def try_official_client():
    """Attempt to fetch data using official py-clob-client"""
    print("\n[METHOD 1] Trying official py-clob-client...")

    try:
        from py_clob_client.client import ClobClient

        client = ClobClient("https://clob.polymarket.com")

        # Test basic connectivity first
        print("  Testing basic connectivity...")
        ok = client.get_ok()
        print(f"  API OK status: {ok}")

        server_time = client.get_server_time()
        print(f"  Server time: {server_time}")

        # Try to get markets
        print("  Fetching markets...")
        response = client.get_simplified_markets()
        markets = response.get("data", [])

        print(f"  ✓ Success! Found {len(markets)} markets")
        return markets

    except ImportError:
        print("  ✗ py-clob-client not installed")
        print("    Install with: pip install py-clob-client")
        return None
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None


def try_direct_request():
    """Attempt direct HTTP request with various strategies"""
    print("\n[METHOD 2] Trying direct HTTP requests...")

    try:
        import requests
    except ImportError:
        print("  ✗ requests library not installed")
        print("    Install with: pip install requests")
        return None

    endpoints = [
        "https://clob.polymarket.com/markets",
        "https://gamma-api.polymarket.com/markets",
        "https://strapi-matic.poly.market/markets",
    ]

    headers_variants = [
        {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
            "Origin": "https://polymarket.com",
            "Referer": "https://polymarket.com/",
        },
        {
            "User-Agent": "py-clob-client/1.0",
            "Accept": "application/json",
        },
    ]

    for endpoint in endpoints:
        for i, headers in enumerate(headers_variants):
            print(f"  Trying {endpoint} (header variant {i+1})...")
            try:
                response = requests.get(
                    endpoint,
                    headers=headers,
                    params={"limit": 10, "closed": "false"},
                    timeout=10
                )

                if response.status_code == 200:
                    data = response.json()
                    print(f"  ✓ Success! Status code: {response.status_code}")
                    return data if isinstance(data, list) else data.get("data", data.get("markets", []))
                else:
                    print(f"  ✗ Status code: {response.status_code}")
                    if response.text:
                        print(f"    Response: {response.text[:100]}")

            except Exception as e:
                print(f"  ✗ Error: {e}")

    return None


def display_markets(markets):
    """Display market information"""
    if not markets:
        print("\n❌ No markets retrieved")
        return

    # Sort by volume
    sorted_markets = sorted(
        markets,
        key=lambda x: float(x.get("volume", 0) or x.get("volume24hr", 0) or 0),
        reverse=True
    )[:10]

    print(f"\n{'='*80}")
    print("TOP 10 TRENDING MARKETS")
    print(f"{'='*80}\n")

    for i, market in enumerate(sorted_markets, start=1):
        question = market.get("question", market.get("title", "N/A"))
        volume = market.get("volume", market.get("volume24hr", 0))

        print(f"#{i}. {question}")
        print(f"   Volume: ${float(volume):,.2f}")

        # Try to get probabilities
        tokens = market.get("tokens", [])
        outcomes = market.get("outcomePrices", [])

        if tokens:
            print("   Odds:", end="")
            for token in tokens[:2]:  # Show first 2 outcomes
                outcome = token.get("outcome", "?")
                price = token.get("price", 0)
                prob = float(price) * 100 if price else 0
                print(f" {outcome}: {prob:.1f}%", end=" |")
            print()
        elif outcomes:
            print("   Odds:", end="")
            for j, price in enumerate(outcomes[:2]):
                prob = float(price) * 100
                print(f" Outcome {j+1}: {prob:.1f}%", end=" |")
            print()

        print()


def print_system_info():
    """Print system and environment information"""
    print("SYSTEM INFORMATION")
    print("=" * 80)
    print(f"Python version: {sys.version}")

    try:
        import requests
        print(f"requests version: {requests.__version__}")
    except ImportError:
        print("requests: NOT INSTALLED")

    try:
        import py_clob_client
        print("py-clob-client: INSTALLED")
    except ImportError:
        print("py-clob-client: NOT INSTALLED")

    print("=" * 80)


def main():
    print_system_info()

    # Try official client first
    markets = try_official_client()

    # If that fails, try direct requests
    if not markets:
        markets = try_direct_request()

    # Display results
    display_markets(markets)

    # Provide guidance
    if not markets:
        print("\n" + "=" * 80)
        print("TROUBLESHOOTING")
        print("=" * 80)
        print("""
The Polymarket API appears to be blocking requests. This could be due to:

1. Cloudflare protection blocking automated requests
2. Geographic restrictions (API may be region-locked)
3. IP-based rate limiting
4. Network/firewall restrictions

SOLUTIONS TO TRY:

1. Install dependencies:
   pip install py-clob-client requests

2. Use a VPN or proxy:
   export HTTPS_PROXY="http://your-proxy:port"

3. Run from a different network/location

4. Check Polymarket's documentation for API changes:
   https://docs.polymarket.com/

5. Use the official web interface:
   https://polymarket.com/markets
        """)


if __name__ == "__main__":
    main()
