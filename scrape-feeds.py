#!/usr/bin/env python3
import csv
import io
import json
import re
import time
from typing import Dict, List, Tuple

from playwright.sync_api import sync_playwright

CONTRACTS = [
    {"commodity": "Cocoa", "symbol": "CCK26", "month": "May"},
    {"commodity": "Coffee", "symbol": "KCK26", "month": "May"},
    {"commodity": "Copper", "symbol": "HGK26", "month": "May"},
    {"commodity": "Corn", "symbol": "ZCN26", "month": "Jul"},
    {"commodity": "Corn", "symbol": "ZCZ26", "month": "Dec"},
    {"commodity": "Cotton", "symbol": "CTK26", "month": "May"},
    {"commodity": "Crude Oil WTI", "symbol": "CLM26", "month": "Jun"},
    {"commodity": "Feeder Cattle", "symbol": "GFK26", "month": "May"},
    {"commodity": "Gold", "symbol": "GCJ26", "month": "Apr"},
    {"commodity": "Hard Red Wheat", "symbol": "KEN26", "month": "Jul"},
    {"commodity": "Lean Hogs", "symbol": "HEM26", "month": "Jun"},
    {"commodity": "Live Cattle", "symbol": "LEJ26", "month": "Apr"},
    {"commodity": "Nasdaq 100 E-Mini", "symbol": "NQM26", "month": "Jun"},
    {"commodity": "Natural Gas", "symbol": "NGM26", "month": "Jun"},
    {"commodity": "Rice", "symbol": "ZRN26", "month": "Jul"},
    {"commodity": "S&P 500 E-Mini", "symbol": "ESM26", "month": "Jun"},
    {"commodity": "Silver", "symbol": "SIM26", "month": "Jun"},
    {"commodity": "Soybean Meal", "symbol": "ZMN26", "month": "Jul"},
    {"commodity": "Soybean Oil", "symbol": "ZLN26", "month": "Jul"},
    {"commodity": "Soybeans", "symbol": "ZSN26", "month": "Jul"},
    {"commodity": "Soybeans", "symbol": "ZSX26", "month": "Nov"},
    {"commodity": "US Dollar", "symbol": "DXM26", "month": "Jun"},
    {"commodity": "Wheat", "symbol": "ZWN26", "month": "Jul"},
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

GRAIN_PREFIXES = ("ZC", "ZS", "ZW", "KE", "ZR")


def round_to_tick(value: float, tick: float) -> float:
    return round(round(value / tick) * tick, 10)


def format_tick(value: float, tick: float) -> str:
    decimals = len(str(tick).split(".")[1]) if "." in str(tick) else 0
    out = f"{round(value, decimals):.{decimals}f}" if decimals else str(int(round(value)))
    return out.rstrip("0").rstrip(".") if "." in out else out


def barchart_price_to_decimal(symbol: str, raw: str) -> float:
    s = raw.strip().lower().replace("s", "").replace(",", "")
    if re.fullmatch(r"\d+-\d+", s):
        left, right = s.split("-")
        whole = int(left)
        frac = int(right)
        if symbol.startswith(GRAIN_PREFIXES):
            return whole / 100 + frac * 0.0025
        return whole + frac / 8.0
    return float(s)


def accept_cookies(page) -> None:
    for selector in [
        "button:has-text('Accept')",
        "button:has-text('I Accept')",
        "button:has-text('Agree')",
    ]:
        try:
            page.locator(selector).first.click(timeout=1200)
            page.wait_for_timeout(500)
            return
        except Exception:
            pass


def parse_day_high_low(text: str, symbol: str) -> Tuple[float, float]:
    patterns = [
        r"Day High\s+([0-9.,\-s]+)\s+Day Low\s+([0-9.,\-s]+)",
        r"Day High / Low\s+([0-9.,\-s]+)\s*/\s*([0-9.,\-s]+)",
        r"High\s+([0-9.,\-s]+)\s+Low\s+([0-9.,\-s]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return (
                barchart_price_to_decimal(symbol, m.group(1)),
                barchart_price_to_decimal(symbol, m.group(2)),
            )
    raise RuntimeError(f"Could not parse day high/low for {symbol}")


def fetch_historical_csv(page, symbol: str) -> List[Dict[str, str]]:
    url = f"https://www.barchart.com/futures/quotes/{symbol}/historical-download"
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3000)
    accept_cookies(page)

    href = None
    for selector in [
        "a[href*='historical.csv']",
        "a[href*='download']",
        "a:has-text('Download')",
    ]:
        try:
            href = page.locator(selector).first.get_attribute("href", timeout=1500)
            if href:
                break
        except Exception:
            pass

    if not href:
        raise RuntimeError(f"Could not find historical CSV link for {symbol}")

    if href.startswith("/"):
        href = "https://www.barchart.com" + href

    response = page.context.request.get(href, headers={"Referer": url})
    if not response.ok:
        raise RuntimeError(f"Historical CSV request failed for {symbol}: {response.status}")

    text = response.text()
    reader = csv.DictReader(io.StringIO(text))
    rows = [row for row in reader if row]
    if not rows:
        raise RuntimeError(f"Historical CSV empty for {symbol}")
    return rows


def parse_historical(rows: List[Dict[str, str]], contract: Dict[str, str]) -> Dict[str, object]:
    symbol = contract["symbol"]
    tick = TICK_SIZES[contract["commodity"]]

    cleaned = []
    for row in rows:
        high_raw = row.get("High") or row.get("high") or ""
        low_raw = row.get("Low") or row.get("low") or ""
        if not high_raw or not low_raw:
            continue
        try:
            cleaned.append({
                "date": row.get("Trading Day") or row.get("Date") or "",
                "high": barchart_price_to_decimal(symbol, str(high_raw)),
                "low": barchart_price_to_decimal(symbol, str(low_raw)),
            })
        except Exception:
            continue

    if len(cleaned) < 4:
        raise RuntimeError(f"Not enough historical rows for {symbol}")

    previous_daily_ranges = [
        format_tick(round_to_tick(r["high"] - r["low"], tick), tick)
        for r in cleaned[1:4]
    ]

    latest_five = cleaned[:5]
    weekly_high = ""
    weekly_low = ""
    if len(latest_five) == 5:
        weekly_high = format_tick(max(r["high"] for r in latest_five), tick)
        weekly_low = format_tick(min(r["low"] for r in latest_five), tick)

    previous_weekly_ranges = []
    for start in (5, 10, 15):
        block = cleaned[start:start + 5]
        if len(block) < 5:
            continue
        block_high = max(r["high"] for r in block)
        block_low = min(r["low"] for r in block)
        previous_weekly_ranges.append(format_tick(round_to_tick(block_high - block_low, tick), tick))

    return {
        "weeklyHigh": weekly_high,
        "weeklyLow": weekly_low,
        "previousDailyRanges": previous_daily_ranges,
        "previousWeeklyRanges": previous_weekly_ranges,
    }


def scrape_contract(page, contract: Dict[str, str]):
    symbol = contract["symbol"]
    tick = TICK_SIZES[contract["commodity"]]

    overview_url = f"https://www.barchart.com/futures/quotes/{symbol}/overview"
    page.goto(overview_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(2500)
    accept_cookies(page)

    overview_text = page.locator("body").inner_text()
    daily_high, daily_low = parse_day_high_low(overview_text, symbol)

    historical_rows = fetch_historical_csv(page, symbol)
    historical = parse_historical(historical_rows, contract)

    return (
        {
            "symbol": symbol,
            "dailyHigh": format_tick(round_to_tick(daily_high, tick), tick),
            "dailyLow": format_tick(round_to_tick(daily_low, tick), tick),
        },
        {
            "symbol": symbol,
            "weeklyHigh": historical["weeklyHigh"],
            "weeklyLow": historical["weeklyLow"],
        },
        {
            "symbol": symbol,
            "previousDailyRanges": historical["previousDailyRanges"],
            "previousWeeklyRanges": historical["previousWeeklyRanges"],
        },
    )


def main() -> None:
    daily_feed = []
    weekly_feed = []
    previous_ranges_feed = []
    errors = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        for contract in CONTRACTS:
            symbol = contract["symbol"]
            try:
                daily_row, weekly_row, previous_row = scrape_contract(page, contract)
                daily_feed.append(daily_row)
                weekly_feed.append(weekly_row)
                previous_ranges_feed.append(previous_row)
                print(f"OK {symbol}")
            except Exception as exc:
                errors.append({"symbol": symbol, "error": str(exc)})
                print(f"ERR {symbol}: {exc}")
            time.sleep(1.2)

        browser.close()

    with open("daily-feed-full.json", "w", encoding="utf-8") as f:
        json.dump(daily_feed, f, indent=2)

    with open("weekly-feed-full.json", "w", encoding="utf-8") as f:
        json.dump(weekly_feed, f, indent=2)

    with open("previous-ranges-feed-full.json", "w", encoding="utf-8") as f:
        json.dump(previous_ranges_feed, f, indent=2)

    with open("errors.json", "w", encoding="utf-8") as f:
        json.dump(errors, f, indent=2)

    print("Done.")


if __name__ == "__main__":
    main()
