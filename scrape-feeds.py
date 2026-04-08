#!/usr/bin/env python3
import json
import time
from urllib.request import urlopen, Request

CONTRACTS = [
    {"commodity": "Cocoa", "symbol": "CCK26.NYB"},
    {"commodity": "Coffee", "symbol": "KCK26.NYB"},
    {"commodity": "Copper", "symbol": "HGK26.CMX"},
    {"commodity": "Corn", "symbol": "ZCN26.CBT"},
    {"commodity": "Corn", "symbol": "ZCZ26.CBT"},
    {"commodity": "Cotton", "symbol": "CTK26.NYB"},
    {"commodity": "Crude Oil WTI", "symbol": "CLM26.NYM"},
    {"commodity": "Feeder Cattle", "symbol": "GFK26.CME"},
    {"commodity": "Gold", "symbol": "GCJ26.CMX"},
    {"commodity": "Hard Red Wheat", "symbol": "KEN26.CBT"},
    {"commodity": "Lean Hogs", "symbol": "HEM26.CME"},
    {"commodity": "Live Cattle", "symbol": "LEJ26.CME"},
    {"commodity": "Nasdaq 100 E-Mini", "symbol": "NQM26.CME"},
    {"commodity": "Natural Gas", "symbol": "NGM26.NYM"},
    {"commodity": "Rice", "symbol": "ZRN26.CBT"},
    {"commodity": "S&P 500 E-Mini", "symbol": "ESM26.CME"},
    {"commodity": "Silver", "symbol": "SIM26.CMX"},
    {"commodity": "Soybean Meal", "symbol": "ZMN26.CBT"},
    {"commodity": "Soybean Oil", "symbol": "ZLN26.CBT"},
    {"commodity": "Soybeans", "symbol": "ZSN26.CBT"},
    {"commodity": "Soybeans", "symbol": "ZSX26.CBT"},
    {"commodity": "US Dollar", "symbol": "DXM26.NYB"},
    {"commodity": "Wheat", "symbol": "ZWN26.CBT"},
]

TICK_SIZES = {
    "Cocoa": 1,
    "Coffee": 0.05,
    "Copper": 0.0005,
    "Corn": 0.25,
    "Cotton": 0.01,
    "Crude Oil WTI": 0.01,
    "Feeder Cattle": 0.025,
    "Gold": 0.1,
    "Hard Red Wheat": 0.25,
    "Lean Hogs": 0.025,
    "Live Cattle": 0.025,
    "Nasdaq 100 E-Mini": 0.25,
    "Natural Gas": 0.001,
    "Rice": 0.5,
    "S&P 500 E-Mini": 0.25,
    "Silver": 0.005,
    "Soybean Meal": 0.1,
    "Soybean Oil": 0.01,
    "Soybeans": 0.25,
    "US Dollar": 0.005,
    "Wheat": 0.25,
}

def round_to_tick(value: float, tick: float) -> float:
    return round(round(value / tick) * tick, 10)

def format_tick(value: float, tick: float) -> str:
    decimals = len(str(tick).split(".")[1]) if "." in str(tick) else 0
    out = f"{round(value, decimals):.{decimals}f}" if decimals else str(int(round(value)))
    return out.rstrip("0").rstrip(".") if "." in out else out

def fetch_yahoo_history(symbol: str):
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        "?range=3mo&interval=1d&includePrePost=false&events=div%2Csplits"
    )
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        data = json.load(resp)

    result = data["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]

    highs = quote.get("high", [])
    lows = quote.get("low", [])
    timestamps = result.get("timestamp", [])

    rows = []
    for ts, high, low in zip(timestamps, highs, lows):
        if high is None or low is None:
            continue
        rows.append({
            "timestamp": ts,
            "high": float(high),
            "low": float(low),
        })

    if not rows:
        raise RuntimeError(f"No historical rows for {symbol}")

    rows.sort(key=lambda x: x["timestamp"], reverse=True)
    return rows

def parse_rows(rows, commodity: str):
    tick = TICK_SIZES[commodity]

    if len(rows) < 4:
        raise RuntimeError("Not enough rows")

    latest_day = rows[0]

    previous_daily_ranges = [
        format_tick(round_to_tick(r["high"] - r["low"], tick), tick)
        for r in rows[1:4]
    ]

    latest_five = rows[:5]
    weekly_high = ""
    weekly_low = ""
    if len(latest_five) == 5:
        weekly_high = format_tick(max(r["high"] for r in latest_five), tick)
        weekly_low = format_tick(min(r["low"] for r in latest_five), tick)

    previous_weekly_ranges = []
    for start in (5, 10, 15):
        block = rows[start:start + 5]
        if len(block) < 5:
            continue
        block_high = max(r["high"] for r in block)
        block_low = min(r["low"] for r in block)
        previous_weekly_ranges.append(
            format_tick(round_to_tick(block_high - block_low, tick), tick)
        )

    return {
        "dailyHigh": format_tick(round_to_tick(latest_day["high"], tick), tick),
        "dailyLow": format_tick(round_to_tick(latest_day["low"], tick), tick),
        "weeklyHigh": weekly_high,
        "weeklyLow": weekly_low,
        "previousDailyRanges": previous_daily_ranges,
        "previousWeeklyRanges": previous_weekly_ranges,
    }

def main():
    daily_feed = []
    weekly_feed = []
    previous_ranges_feed = []
    errors = []

    for contract in CONTRACTS:
        symbol = contract["symbol"]
        commodity = contract["commodity"]

        try:
            rows = fetch_yahoo_history(symbol)
            parsed = parse_rows(rows, commodity)

            daily_feed.append({
                "symbol": symbol.replace(".CBT", "").replace(".CMX", "").replace(".NYB", "").replace(".NYM", "").replace(".CME", ""),
                "dailyHigh": parsed["dailyHigh"],
                "dailyLow": parsed["dailyLow"],
            })

            weekly_feed.append({
                "symbol": symbol.replace(".CBT", "").replace(".CMX", "").replace(".NYB", "").replace(".NYM", "").replace(".CME", ""),
                "weeklyHigh": parsed["weeklyHigh"],
                "weeklyLow": parsed["weeklyLow"],
            })

            previous_ranges_feed.append({
                "symbol": symbol.replace(".CBT", "").replace(".CMX", "").replace(".NYB", "").replace(".NYM", "").replace(".CME", ""),
                "previousDailyRanges": parsed["previousDailyRanges"],
                "previousWeeklyRanges": parsed["previousWeeklyRanges"],
            })

            print("OK", symbol)
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)})
            print("ERR", symbol, exc)

        time.sleep(0.5)

    with open("daily-feed-full.json", "w", encoding="utf-8") as f:
        json.dump(daily_feed, f, indent=2)

    with open("weekly-feed-full.json", "w", encoding="utf-8") as f:
        json.dump(weekly_feed, f, indent=2)

    with open("previous-ranges-feed-full.json", "w", encoding="utf-8") as f:
        json.dump(previous_ranges_feed, f, indent=2)

    with open("errors.json", "w", encoding="utf-8") as f:
        json.dump(errors, f, indent=2)

if __name__ == "__main__":
    main()
