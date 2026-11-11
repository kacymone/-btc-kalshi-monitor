import os
import time
import requests
from datetime import datetime, timezone

# =========================================================
# SETTINGS
# =========================================================

KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
SERIES = "KXBTC15M"

# Kraken BTC/USD candle feed
KRAKEN_OHLC = "https://api.kraken.com/0/public/OHLC"
KRAKEN_PAIR = "XBTUSD"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

CHECK_EVERY = 15

# ---- Strategy settings ----

# Number of completed 15m candles used for recent structure
STRUCTURE_CANDLES = 8

# BTC can be this close to support/resistance and count as "testing"
LEVEL_TOLERANCE_PCT = 0.15

# Minimum distance through a level to help confirm a breakout
BREAKOUT_BUFFER_PCT = 0.05

# Don't chase Kalshi contracts above this ask price
MAX_ENTRY_PRICE = 0.88

# We want enough time to actually act
MIN_SECONDS_FOR_ENTRY = 120

# Avoid calling something an entry immediately after a contract opens
MAX_SECONDS_FOR_ENTRY = 13 * 60

last_alert = None
last_market = None


# =========================================================
# TELEGRAM
# =========================================================

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


# =========================================================
# KALSHI
# =========================================================

def get_live_btc_market():

    url = f"{KALSHI_BASE}/markets"

    params = {
        "series_ticker": SERIES,
        "status": "open",
        "limit": 100
    }

    response = requests.get(
        url,
        params=params,
        timeout=10
    )

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


# =========================================================
# BTC PRICE DATA
# =========================================================

def get_kraken_candles(interval):

    params = {
        "pair": KRAKEN_PAIR,
        "interval": interval
    }

    response = requests.get(
        KRAKEN_OHLC,
        params=params,
        timeout=10
    )

    response.raise_for_status()

    data = response.json()

    if data.get("error"):
        raise Exception(
            f"Kraken error: {data['error']}"
        )

    result = data.get("result", {})

    candle_key = None

    for key in result:

        if key != "last":
            candle_key = key
            break

    if not candle_key:
        return []

    raw_candles = result[candle_key]

    candles = []

    for candle in raw_candles:

        try:

            candles.append({
                "time": int(float(candle[0])),
                "open": float(candle[1]),
                "high": float(candle[2]),
                "low": float(candle[3]),
                "close": float(candle[4]),
                "volume": float(candle[6])
            })

        except Exception:
            continue

    return candles


# =========================================================
# BUILD 3-MIN CANDLES FROM 1-MIN DATA
# =========================================================

def build_3m_candles(one_minute):

    grouped = {}

    for candle in one_minute:

        bucket = candle["time"] - (
            candle["time"] % 180
        )

        if bucket not in grouped:

            grouped[bucket] = {
                "time": bucket,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle["volume"]
            }

        else:

            grouped[bucket]["high"] = max(
                grouped[bucket]["high"],
                candle["high"]
            )

            grouped[bucket]["low"] = min(
                grouped[bucket]["low"],
                candle["low"]
            )

            grouped[bucket]["close"] = candle["close"]

            grouped[bucket]["volume"] += candle["volume"]

    candles = [
        grouped[key]
        for key in sorted(grouped.keys())
    ]

    # Remove currently-forming 3m candle
    now = int(time.time())

    completed = [
        c for c in candles
        if c["time"] + 180 <= now
    ]

    return completed


# =========================================================
# HELPERS
# =========================================================

def money(value):

    try:
        return float(value)

    except Exception:
        return None


def seconds_left(market):

    close_time = datetime.fromisoformat(
        market["close_time"].replace(
            "Z",
            "+00:00"
        )
    )

    now = datetime.now(timezone.utc)

    return max(
        0,
        int(
            (close_time - now).total_seconds()
        )
    )


def pct_distance(price, level):

    if not level:
        return 999

    return abs(
        (price - level) / level
    ) * 100


# =========================================================
# 15-MIN STRUCTURE
# =========================================================

def get_structure():

    candles = get_kraken_candles(15)

    if len(candles) < STRUCTURE_CANDLES + 2:
        return None

    # Last Kraken candle may still be forming
    completed = candles[:-1]

    recent = completed[
        -STRUCTURE_CANDLES:
    ]

    support = min(
        candle["low"]
        for candle in recent
    )

    resistance = max(
        candle["high"]
        for candle in recent
    )

    closes = [
        candle["close"]
        for candle in recent
    ]

    # Simple directional context
    first_half = closes[:4]
    second_half = closes[4:]

    first_avg = sum(first_half) / len(first_half)
    second_avg = sum(second_half) / len(second_half)

    if second_avg > first_avg * 1.001:
        trend = "BULLISH"

    elif second_avg < first_avg * 0.999:
        trend = "BEARISH"

    else:
        trend = "RANGE"

    return {
        "support": support,
        "resistance": resistance,
        "trend": trend
    }


# =========================================================
# 3-MIN PRICE ACTION
# =========================================================

def analyze_price_action(
    support,
    resistance
):

    one_minute = get_kraken_candles(1)

    three_minute = build_3m_candles(
        one_minute
    )

    if len(three_minute) < 3:
        return None

    previous = three_minute[-2]
    current = three_minute[-1]

    price = current["close"]

    near_support = (
        pct_distance(
            price,
            support
        )
        <= LEVEL_TOLERANCE_PCT
    )

    near_resistance = (
        pct_distance(
            price,
            resistance
        )
        <= LEVEL_TOLERANCE_PCT
    )

    current_green = (
        current["close"] >
        current["open"]
    )

    current_red = (
        current["close"] <
        current["open"]
    )

    # -----------------------------------------
    # SUPPORT HOLD / REJECTION
    # -----------------------------------------

    support_rejection = (
        current["low"]
        <= support * (
            1 + LEVEL_TOLERANCE_PCT / 100
        )
        and
        current["close"] > support
        and
        current_green
    )

    # -----------------------------------------
    # RESISTANCE REJECTION
    # -----------------------------------------

    resistance_rejection = (
        current["high"]
        >= resistance * (
            1 - LEVEL_TOLERANCE_PCT / 100
        )
        and
        current["close"] < resistance
        and
        current_red
    )

    # -----------------------------------------
    # BREAKOUT ABOVE RESISTANCE
    # -----------------------------------------

    breakout_up = (
        previous["close"] <= resistance
        and
        current["close"]
        >
        resistance * (
            1 + BREAKOUT_BUFFER_PCT / 100
        )
    )

    # -----------------------------------------
    # BREAKDOWN BELOW SUPPORT
    # -----------------------------------------

    breakout_down = (
        previous["close"] >= support
        and
        current["close"]
        <
        support * (
            1 - BREAKOUT_BUFFER_PCT / 100
        )
    )

    # -----------------------------------------
    # RETEST AFTER BREAK ABOVE
    # -----------------------------------------

    retest_up = (
        previous["close"] > resistance
        and
        current["low"] <=
        resistance * (
            1 + LEVEL_TOLERANCE_PCT / 100
        )
        and
        current["close"] > resistance
        and
        current_green
    )

    # -----------------------------------------
    # RETEST AFTER BREAK BELOW
    # -----------------------------------------

    retest_down = (
        previous["close"] < support
        and
        current["high"] >=
        support * (
            1 - LEVEL_TOLERANCE_PCT / 100
        )
        and
        current["close"] < support
        and
        current_red
    )

    return {
        "price": price,

        "near_support": near_support,
        "near_resistance": near_resistance,

        "support_rejection":
            support_rejection,

        "resistance_rejection":
            resistance_rejection,

        "breakout_up":
            breakout_up,

        "breakout_down":
            breakout_down,

        "retest_up":
            retest_up,

        "retest_down":
            retest_down
    }


# =========================================================
# FULL ANALYSIS
# =========================================================

def analyze_market(market):

    yes_bid = money(
        market.get("yes_bid_dollars")
    )

    yes_ask = money(
        market.get("yes_ask_dollars")
    )

    no_bid = money(
        market.get("no_bid_dollars")
    )

    no_ask = money(
        market.get("no_ask_dollars")
    )

    remaining = seconds_left(market)

    structure = get_structure()

    if not structure:
        return None

    price_action = analyze_price_action(
        structure["support"],
        structure["resistance"]
    )

    if not price_action:
        return None

    signal = "WAIT"
    strength = "NO EDGE"
    reason = "BTC is between key levels."

    direction = None

    # -----------------------------------------
    # CONFIRMED UP SETUPS
    # -----------------------------------------

    if price_action["retest_up"]:

        signal = "🟢 BUY UP"
        strength = "CONFIRMED"
        reason = (
            "Resistance broke and the "
            "3m retest held above it."
        )
        direction = "UP"

    elif price_action["support_rejection"]:

        signal = "🟢 BUY UP"
        strength = "CONFIRMED"
        reason = (
            "BTC tested 15m support "
            "and the 3m candle rejected it."
        )
        direction = "UP"

    elif price_action["breakout_up"]:

        signal = "👀 UP BREAKOUT"
        strength = "WATCH RETEST"
        reason = (
            "BTC broke above 15m resistance. "
            "Watching for the retest to hold."
        )
        direction = "UP"

    # -----------------------------------------
    # CONFIRMED DOWN SETUPS
    # -----------------------------------------

    elif price_action["retest_down"]:

        signal = "🔴 BUY DOWN"
        strength = "CONFIRMED"
        reason = (
            "Support broke and the "
            "3m retest failed below it."
        )
        direction = "DOWN"

    elif price_action["resistance_rejection"]:

        signal = "🔴 BUY DOWN"
        strength = "CONFIRMED"
        reason = (
            "BTC tested 15m resistance "
            "and the 3m candle rejected it."
        )
        direction = "DOWN"

    elif price_action["breakout_down"]:

        signal = "👀 DOWN BREAK"
        strength = "WATCH RETEST"
        reason = (
            "BTC broke below 15m support. "
            "Watching for the retest to fail."
        )
        direction = "DOWN"

    # -----------------------------------------
    # APPROACHING LEVELS
    # -----------------------------------------

    elif price_action["near_support"]:

        signal = "👀 WATCHING SUPPORT"
        strength = "SETUP FORMING"
        reason = (
            "BTC is testing 15m support. "
            "Waiting for 3m confirmation."
        )

    elif price_action["near_resistance"]:

        signal = "👀 WATCHING RESISTANCE"
        strength = "SETUP FORMING"
        reason = (
            "BTC is testing 15m resistance. "
            "Waiting for 3m confirmation."
        )

    # -----------------------------------------
    # KALSHI ENTRY FILTER
    # -----------------------------------------

    entry_price = None

    if direction == "UP":
        entry_price = yes_ask

    elif direction == "DOWN":
        entry_price = no_ask

    if direction and entry_price is not None:

        if entry_price > MAX_ENTRY_PRICE:

            strength = "LATE / EXPENSIVE"

            reason += (
                f" Kalshi {direction} is already "
                f"${entry_price:.2f}."
            )

        elif remaining < MIN_SECONDS_FOR_ENTRY:

            strength = "TOO LATE"

            reason += (
                " Not enough time remains "
                "for a fresh entry."
            )

        elif remaining > MAX_SECONDS_FOR_ENTRY:

            strength = "EARLY"

            reason += (
                " Contract is still early."
            )

    return {
        "signal": signal,
        "strength": strength,
        "reason": reason,

        "direction": direction,

        "btc_price":
            price_action["price"],

        "support":
            structure["support"],

        "resistance":
            structure["resistance"],

        "trend":
            structure["trend"],

        "yes_bid": yes_bid,
        "yes_ask": yes_ask,

        "no_bid": no_bid,
        "no_ask": no_ask,

        "entry_price": entry_price,

        "seconds_left": remaining
    }


# =========================================================
# MESSAGE
# =========================================================

def build_message(
    market,
    analysis
):

    seconds = analysis["seconds_left"]

    minutes = seconds // 60
    secs = seconds % 60

    ticker = market.get(
        "ticker",
        "Unknown"
    )

    message = f"""
🚨 KATE'S BTC WATCHER

{analysis['signal']}
Strength: {analysis['strength']}

BTC: ${analysis['btc_price']:,.2f}

15M STRUCTURE
Support: ${analysis['support']:,.2f}
Resistance: ${analysis['resistance']:,.2f}
Trend: {analysis['trend']}

3M READ
{analysis['reason']}

KALSHI
🟢 UP ask: {analysis['yes_ask']}
🔴 DOWN ask: {analysis['no_ask']}

⏱ Time left: {minutes}:{secs:02d}

Contract:
{ticker}

⚠️ Signal only — no automatic trade.
"""

    return message.strip()


# =========================================================
# MAIN LOOP
# =========================================================

def main():

    global last_alert
    global last_market

    print(
        "Kate's BTC Watcher V2 starting..."
    )

    send_telegram(
        "🟢 KATE'S BTC WATCHER V2 IS ONLINE.\n\n"
        "15M structure + 3M confirmation enabled."
    )

    while True:

        try:

            market = get_live_btc_market()

            if not market:

                print(
                    "No open BTC 15-minute market."
                )

                time.sleep(CHECK_EVERY)

                continue

            ticker = market.get("ticker")

            analysis = analyze_market(
                market
            )

            if not analysis:

                time.sleep(CHECK_EVERY)

                continue

            # ---------------------------------
            # NEW CONTRACT
            # ---------------------------------

            if ticker != last_market:

                last_market = ticker
                last_alert = None

                send_telegram(
                    "🆕 NEW BTC 15-MIN CONTRACT\n\n"
                    f"{ticker}"
                )

            # ---------------------------------
            # PREVENT DUPLICATE ALERTS
            # ---------------------------------

            signal_key = (
                ticker,
                analysis["signal"],
                analysis["strength"]
            )

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

            print(
                "Monitor error:",
                e
            )

            time.sleep(30)


if __name__ == "__main__":
    main()
