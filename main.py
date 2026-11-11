import os
import time
import requests
from datetime import datetime, timezone

# -----------------------------
# SETTINGS
# -----------------------------

KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
SERIES = "KXBTC15M"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

CHECK_EVERY = 15  # seconds

last_alert = None
last_market = None


# -----------------------------
# TELEGRAM
# -----------------------------

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram variables missing.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"

    try:
        response = requests.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message
            },
            timeout=10
        )

        response.raise_for_status()

    except Exception as e:
        print("Telegram error:", e)


# -----------------------------
# KALSHI
# -----------------------------

def get_live_btc_market():

    url = f"{KALSHI_BASE}/markets"

    params = {
        "series_ticker": SERIES,
        "status": "open",
        "limit": 100
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    markets = response.json().get("markets", [])

    if not markets:
        return None

    now = datetime.now(timezone.utc)

    active = []

    for market in markets:

        try:
            close_time = datetime.fromisoformat(
                market["close_time"].replace("Z", "+00:00")
            )

            if close_time > now:
                active.append((close_time, market))

        except Exception:
            continue

    if not active:
        return None

    active.sort(key=lambda x: x[0])

    return active[0][1]


# -----------------------------
# HELPERS
# -----------------------------

def money(value):

    try:
        return float(value)
    except:
        return None


def seconds_left(market):

    close_time = datetime.fromisoformat(
        market["close_time"].replace("Z", "+00:00")
    )

    now = datetime.now(timezone.utc)

    return max(0, int((close_time - now).total_seconds()))


# -----------------------------
# ANALYSIS
# -----------------------------

def analyze_market(market):

    yes_bid = money(market.get("yes_bid_dollars"))
    yes_ask = money(market.get("yes_ask_dollars"))

    no_bid = money(market.get("no_bid_dollars"))
    no_ask = money(market.get("no_ask_dollars"))

    last_price = money(market.get("last_price_dollars"))

    remaining = seconds_left(market)

    # Approximate market probability using midpoint
    if yes_bid is not None and yes_ask is not None:
        yes_probability = (yes_bid + yes_ask) / 2

    elif last_price is not None:
        yes_probability = last_price

    else:
        return None

    up_pct = round(yes_probability * 100, 1)
    down_pct = round((1 - yes_probability) * 100, 1)

    # -------------------------
    # SIGNAL LOGIC
    # -------------------------

    signal = "WAIT"
    strength = "NO EDGE"

    # Strong directional pricing
    if up_pct >= 70:
        signal = "UP WATCH"
        strength = "STRONG"

    elif down_pct >= 70:
        signal = "DOWN WATCH"
        strength = "STRONG"

    # Moderate directional pricing
    elif up_pct >= 60:
        signal = "UP WATCH"
        strength = "MODERATE"

    elif down_pct >= 60:
        signal = "DOWN WATCH"
        strength = "MODERATE"

    # Avoid chasing extremely expensive contracts
    if up_pct >= 90:
        strength = "LATE / EXPENSIVE"

    if down_pct >= 90:
        strength = "LATE / EXPENSIVE"

    return {
        "signal": signal,
        "strength": strength,
        "up_pct": up_pct,
        "down_pct": down_pct,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,
        "seconds_left": remaining
    }


# -----------------------------
# FORMAT ALERT
# -----------------------------

def build_message(market, analysis):

    seconds = analysis["seconds_left"]

    minutes = seconds // 60
    secs = seconds % 60

    ticker = market.get("ticker", "Unknown")

    message = f"""
🚨 BTC 15-MIN MONITOR

{analysis['signal']}
Strength: {analysis['strength']}

🟢 UP: {analysis['up_pct']}%
🔴 DOWN: {analysis['down_pct']}%

⏱ Time left: {minutes}:{secs:02d}

YES bid/ask:
{analysis['yes_bid']} / {analysis['yes_ask']}

NO bid/ask:
{analysis['no_bid']} / {analysis['no_ask']}

Contract:
{ticker}

⚠️ Alert only — no automatic trade.
"""

    return message.strip()


# -----------------------------
# MAIN LOOP
# -----------------------------

def main():

    global last_alert
    global last_market

    print("BTC Kalshi Monitor starting...")

    send_telegram(
        "🟢 BTC Kalshi monitor is ONLINE."
    )

    while True:

        try:

            market = get_live_btc_market()

            if not market:
                print("No open BTC 15-minute market.")
                time.sleep(CHECK_EVERY)
                continue

            ticker = market.get("ticker")

            analysis = analyze_market(market)

            if not analysis:
                time.sleep(CHECK_EVERY)
                continue

            # Tell us when Kalshi rolls into a new 15m contract
            if ticker != last_market:

                last_market = ticker
                last_alert = None

                send_telegram(
                    f"🆕 NEW BTC 15-MIN CONTRACT\n\n{ticker}"
                )

            signal_key = (
                ticker,
                analysis["signal"],
                analysis["strength"]
            )

            # Only alert when the signal changes
            if signal_key != last_alert:

                last_alert = signal_key

                message = build_message(
                    market,
                    analysis
                )

                send_telegram(message)

                print(message)

            time.sleep(CHECK_EVERY)

        except Exception as e:

            print("Monitor error:", e)

            time.sleep(30)


if __name__ == "__main__":
    main()
