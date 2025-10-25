# Polymarket Trending Bets Scripts

Scripts to fetch the top 10 trending bets from Polymarket.

## Problem: API Access

The Polymarket API (`clob.polymarket.com` and `gamma-api.polymarket.com`) appears to be protected by Cloudflare or similar anti-bot measures. This means direct API access may be blocked depending on:
- Your IP address
- Geographic location
- Request headers
- Rate limiting

## Solutions Provided

### 1. Official Client (Recommended)
**File:** `polymarket_trending_official.py`

Uses the official `py-clob-client` library which may handle some protections internally.

```bash
# Install
pip install py-clob-client

# Run
python3 polymarket_trending_official.py
```

### 2. Standard Library Version
**File:** `polymarket_trending_stdlib.py`

Uses only Python standard library (no dependencies) but makes direct HTTP requests.

```bash
python3 polymarket_trending_stdlib.py
```

### 3. Requests Library Version
**File:** `polymarket_trending.py`

Uses the `requests` library for cleaner HTTP handling.

```bash
pip install requests
python3 polymarket_trending.py
```

## Troubleshooting 403 Errors

If you get "Access denied" or 403 errors, try these solutions:

### Option A: Use a Proxy or VPN
The API might be geo-restricted. Try:
```bash
# With environment proxy
export HTTP_PROXY="http://your-proxy:port"
export HTTPS_PROXY="http://your-proxy:port"
python3 polymarket_trending_official.py
```

### Option B: Browser-Based Approach
If direct API access fails, you can:

1. Visit https://polymarket.com directly
2. Open browser DevTools (F12)
3. Go to Network tab
4. Browse markets on the site
5. Look for API calls to extract data

### Option C: Alternative Data Sources

#### Using GitHub Projects
Check these open-source projects that may have working implementations:
- https://github.com/Polymarket/agents
- https://github.com/Polymarket/py-clob-client

#### Unofficial APIs
Some third-party services aggregate Polymarket data:
- Apify Polymarket Scraper
- FinFeedAPI (paid service)

## Expected Output Format

When working, the scripts will display:

```
================================================================================
#1 - Will Trump win the 2024 election?
================================================================================
Description: Market resolves to YES if Donald Trump wins...

Total Volume: $45,234,567.89
Status: Active

Current Odds:
  Yes: 52.3%
  No: 47.7%

View market: https://polymarket.com/event/...
================================================================================
```

## API Endpoints Reference

### Gamma Markets API (Read-Only)
- Base: `https://gamma-api.polymarket.com`
- Markets: `/markets?closed=false&order=volume24hr&limit=10`

### CLOB API
- Base: `https://clob.polymarket.com`
- Markets: `/markets`
- Simplified markets: Client method `get_simplified_markets()`

## Parameters for Filtering

When querying the API, you can use:

- `closed` (boolean): Filter by market status
- `order`: Sort field (`volume24hr`, `liquidity`, `id`)
- `ascending` (boolean): Sort direction
- `limit`: Number of results
- `offset`: Pagination offset
- `active`: Only active markets

## Alternative: Manual Data Collection

If automation doesn't work, you can manually check trending markets at:
https://polymarket.com/markets

The homepage shows trending markets with:
- Current odds
- Trading volume
- Number of traders
- Market description

## Contributing

If you find a working solution for API access, please update these scripts!
