import os
import time
import requests
from datetime import datetime, timezone

# =========================================================
# SETTINGS
# =========================================================

KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
SERIES = "KXBTC15M"

KRAKEN_OHLC = "https://api.kraken.com/0/public/OHLC"
KRAKEN_PAIR = "XBTUSD"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

CHECK_EVERY = 15

STRUCTURE_CANDLES = 8
LEVEL_TOLERANCE_PCT = 0.15
BREAKOUT_BUFFER_PCT = 0.05

# Entry classifications
SCALP_PRICE = 0.88
HARD_SKIP_PRICE = 0.97

# Timing
MIN_SECONDS_FOR_ENTRY = 60
MAX_SECONDS_FOR_ENTRY = 13 * 60

# Retest can remain armed this many completed 3m candles
RETEST_MAX_CANDLES = 4

# =========================================================
# STATE
# =========================================================

last_alert = None
last_market = None
last_analysis = None

watcher_enabled = True
telegram_offset = None

retest_state = {
    "direction": None,
    "level": None,
    "armed_at": None
}


# =========================================================
# TELEGRAM
# =========================================================

def telegram_url(method):
    return (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/{method}"
    )


def send_telegram(message):

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram variables missing.")
        return

    try:

        response = requests.post(
            telegram_url("sendMessage"),
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message
            },
            timeout=10
        )

        response.raise_for_status()

    except Exception as e:
        print("Telegram send error:", e)


def initialize_telegram_offset():
    """
    Ignore old Telegram commands that were sent before
    this deployment started.
    """

    global telegram_offset

    if not TELEGRAM_TOKEN:
        return

    try:

        response = requests.get(
            telegram_url("getUpdates"),
            params={
                "timeout": 0,
                "limit": 100
            },
            timeout=10
        )

        response.raise_for_status()

        updates = response.json().get(
            "result",
            []
        )

        if updates:
            telegram_offset = (
                updates[-1]["update_id"] + 1
            )

    except Exception as e:
        print(
            "Telegram initialization error:",
            e
        )


def check_telegram_commands():

    global telegram_offset
    global watcher_enabled
    global last_alert

    if not TELEGRAM_TOKEN:
        return

    try:

        params = {
            "timeout": 0,
            "limit": 20
        }

        if telegram_offset is not None:
            params["offset"] = telegram_offset

        response = requests.get(
            telegram_url("getUpdates"),
            params=params,
            timeout=10
        )

        response.raise_for_status()

        updates = response.json().get(
            "result",
            []
        )

        for update in updates:

            telegram_offset = (
                update["update_id"] + 1
            )

            message = update.get(
                "message",
                {}
            )

            chat_id = str(
                message.get(
                    "chat",
                    {}
                ).get("id", "")
            )

            # Only YOU control this bot
            if chat_id != str(
                TELEGRAM_CHAT_ID
            ):
                continue

            text = (
                message.get("text", "")
                .strip()
                .lower()
            )

            if text.startswith("/stop"):

                watcher_enabled = False

                send_telegram(
                    "💤 KATE'S BTC WATCHER IS OFF.\n\n"
                    "No trading alerts until you send /start."
                )

            elif text.startswith("/start"):

                watcher_enabled = True
                last_alert = None

                send_telegram(
                    "🟢 KATE'S BTC WATCHER IS ON.\n\n"
                    "I'm watching BTC again 👀"
                )

            elif text.startswith("/status"):

                send_status()

    except Exception as e:
        print(
            "Telegram command error:",
            e
        )


def send_status():

    if not watcher_enabled:

        send_telegram(
            "💤 WATCHER STATUS: OFF\n\n"
            "Send /start when you're ready."
        )

        return

    if not last_analysis:

        send_telegram(
            "🟢 WATCHER STATUS: ON\n\n"
            "I'm monitoring BTC...no current "
            "analysis available yet."
        )

        return

    analysis = last_analysis

    seconds = analysis["seconds_left"]

    minutes = seconds // 60
    secs = seconds % 60

    send_telegram(
        "🟢 WATCHER STATUS: ON\n\n"
        f"{analysis['action']}\n\n"
        f"BTC: ${analysis['btc_price']:,.2f}\n"
        f"Trend: {analysis['trend']}\n"
        f"UP ask: {analysis['yes_ask']}\n"
        f"DOWN ask: {analysis['no_ask']}\n"
        f"Time left: {minutes}:{secs:02d}\n\n"
        f"{analysis['reason']}"
    )


# =========================================================
# KALSHI
# =========================================================

def get_live_btc_market():

    response = requests.get(
        f"{KALSHI_BASE}/markets",
        params={
            "series_ticker": SERIES,
            "status": "open",
            "limit": 100
        },
        timeout=10
    )

    response.raise_for_status()

    markets = response.json().get(
        "markets",
        []
    )

    if not markets:
        return None

    now = datetime.now(timezone.utc)
    active = []

    for market in markets:

        try:

            close_time = datetime.fromisoformat(
                market["close_time"].replace(
                    "Z",
                    "+00:00"
                )
            )

            if close_time > now:
                active.append(
                    (close_time, market)
                )

        except Exception:
            continue

    if not active:
        return None

    active.sort(
        key=lambda item: item[0]
    )

    return active[0][1]


# =========================================================
# BTC DATA
# =========================================================

def get_kraken_candles(interval):

    response = requests.get(
        KRAKEN_OHLC,
        params={
            "pair": KRAKEN_PAIR,
            "interval": interval
        },
        timeout=10
    )

    response.raise_for_status()

    data = response.json()

    if data.get("error"):
        raise Exception(
            f"Kraken error: {data['error']}"
        )

    result = data.get(
        "result",
        {}
    )

    candle_key = None

    for key in result:

        if key != "last":
            candle_key = key
            break

    if not candle_key:
        return []

    candles = []

    for candle in result[candle_key]:

        try:

            candles.append({
                "time": int(
                    float(candle[0])
                ),
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
# BUILD COMPLETED 3M CANDLES
# =========================================================

def build_3m_candles(one_minute):

    grouped = {}

    for candle in one_minute:

        bucket = (
            candle["time"]
            - candle["time"] % 180
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

            grouped[bucket]["close"] = (
                candle["close"]
            )

            grouped[bucket]["volume"] += (
                candle["volume"]
            )

    candles = [
        grouped[key]
        for key in sorted(grouped)
    ]

    now = int(time.time())

    return [
        candle
        for candle in candles
        if candle["time"] + 180 <= now
    ]


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

    return max(
        0,
        int(
            (
                close_time
                - datetime.now(
                    timezone.utc
                )
            ).total_seconds()
        )
    )


def pct_distance(price, level):

    if not level:
        return 999

    return (
        abs(price - level)
        / level
        * 100
    )


def clear_retest():

    retest_state["direction"] = None
    retest_state["level"] = None
    retest_state["armed_at"] = None


def arm_retest(
    direction,
    level,
    candle_time
):

    retest_state["direction"] = direction
    retest_state["level"] = level
    retest_state["armed_at"] = candle_time


# =========================================================
# 15M STRUCTURE
# =========================================================

def get_structure():

    candles = get_kraken_candles(15)

    if len(candles) < (
        STRUCTURE_CANDLES + 2
    ):
        return None

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

    midpoint = len(closes) // 2

    first_avg = (
        sum(closes[:midpoint])
        / len(closes[:midpoint])
    )

    second_avg = (
        sum(closes[midpoint:])
        / len(closes[midpoint:])
    )

    if second_avg > (
        first_avg * 1.001
    ):

        trend = "BULLISH"

    elif second_avg < (
        first_avg * 0.999
    ):

        trend = "BEARISH"

    else:
        trend = "RANGE"

    return {
        "support": support,
        "resistance": resistance,
        "trend": trend
    }


# =========================================================
# 3M PRICE ACTION + STATEFUL RETEST
# =========================================================

def analyze_price_action(
    support,
    resistance
):

    one_minute = get_kraken_candles(1)

    candles = build_3m_candles(
        one_minute
    )

    if len(candles) < 3:
        return None

    previous = candles[-2]
    current = candles[-1]

    price = current["close"]

    green = (
        current["close"]
        > current["open"]
    )

    red = (
        current["close"]
        < current["open"]
    )

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

    support_rejection = (
        current["low"]
        <= support
        * (
            1
            + LEVEL_TOLERANCE_PCT
            / 100
        )
        and current["close"] > support
        and green
    )

    resistance_rejection = (
        current["high"]
        >= resistance
        * (
            1
            - LEVEL_TOLERANCE_PCT
            / 100
        )
        and current["close"] < resistance
        and red
    )

    breakout_up = (
        previous["close"]
        <= resistance
        and current["close"]
        >
        resistance
        * (
            1
            + BREAKOUT_BUFFER_PCT
            / 100
        )
    )

    breakout_down = (
        previous["close"]
        >= support
        and current["close"]
        <
        support
        * (
            1
            - BREAKOUT_BUFFER_PCT
            / 100
        )
    )

    # Arm a persistent retest
    if breakout_up:

        arm_retest(
            "UP",
            resistance,
            current["time"]
        )

    elif breakout_down:

        arm_retest(
            "DOWN",
            support,
            current["time"]
        )

    retest_up = False
    retest_down = False

    armed_direction = (
        retest_state["direction"]
    )

    armed_level = (
        retest_state["level"]
    )

    armed_at = (
        retest_state["armed_at"]
    )

    if (
        armed_direction
        and armed_level
        and armed_at
    ):

        candles_since = int(
            (
                current["time"]
                - armed_at
            )
            / 180
        )

        if (
            candles_since
            > RETEST_MAX_CANDLES
        ):

            clear_retest()

        elif (
            candles_since >= 1
            and armed_direction == "UP"
        ):

            touched = (
                current["low"]
                <= armed_level
                * (
                    1
                    + LEVEL_TOLERANCE_PCT
                    / 100
                )
            )

            held = (
                current["close"]
                > armed_level
                and green
            )

            if touched and held:

                retest_up = True
                clear_retest()

            # Breakout completely failed
            elif (
                current["close"]
                <
                armed_level
                * (
                    1
                    - BREAKOUT_BUFFER_PCT
                    / 100
                )
            ):

                clear_retest()

        elif (
            candles_since >= 1
            and armed_direction == "DOWN"
        ):

            touched = (
                current["high"]
                >= armed_level
                * (
                    1
                    - LEVEL_TOLERANCE_PCT
                    / 100
                )
            )

            failed = (
                current["close"]
                < armed_level
                and red
            )

            if touched and failed:

                retest_down = True
                clear_retest()

            # Breakdown completely failed
            elif (
                current["close"]
                >
                armed_level
                * (
                    1
                    + BREAKOUT_BUFFER_PCT
                    / 100
                )
            ):

                clear_retest()

    return {
        "price": price,
        "candle_time": current["time"],

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
            retest_down,

        "armed_direction":
            retest_state["direction"]
    }


# =========================================================
# FINAL ACTION DECISION
# =========================================================

def analyze_market(market):

    yes_bid = money(
        market.get(
            "yes_bid_dollars"
        )
    )

    yes_ask = money(
        market.get(
            "yes_ask_dollars"
        )
    )

    no_bid = money(
        market.get(
            "no_bid_dollars"
        )
    )

    no_ask = money(
        market.get(
            "no_ask_dollars"
        )
    )

    remaining = seconds_left(
        market
    )

    structure = get_structure()

    if not structure:
        return None

    pa = analyze_price_action(
        structure["support"],
        structure["resistance"]
    )

    if not pa:
        return None

    action = "🗣️ WAIT 🟡"

    setup = "NONE"

    reason = (
        "BTC is between key levels."
    )

    direction = None
    confirmed = False

    # -----------------------------------------
    # STRONG CONFIRMATIONS
    # -----------------------------------------

    if pa["retest_up"]:

        direction = "UP"
        confirmed = True
        setup = "RETEST HOLD"

        reason = (
            "Resistance broke...BTC came "
            "back to retest it and the 3m "
            "candle held above the level."
        )

    elif pa["retest_down"]:

        direction = "DOWN"
        confirmed = True
        setup = "RETEST REJECTION"

        reason = (
            "Support broke...BTC came "
            "back to retest it and the 3m "
            "candle rejected the old support."
        )

    elif pa["support_rejection"]:

        direction = "UP"
        confirmed = True
        setup = "SUPPORT REJECTION"

        reason = (
            "BTC tested 15m support and "
            "the completed 3m candle "
            "rejected the level upward."
        )

    elif pa["resistance_rejection"]:

        direction = "DOWN"
        confirmed = True
        setup = "RESISTANCE REJECTION"

        reason = (
            "BTC tested 15m resistance and "
            "the completed 3m candle "
            "rejected the level downward."
        )

    # -----------------------------------------
    # DEVELOPING SETUPS
    # -----------------------------------------

    elif pa["breakout_up"]:

        setup = "UP BREAKOUT"

        reason = (
            "BTC broke resistance...waiting "
            "for a 3m retest before entering."
        )

    elif pa["breakout_down"]:

        setup = "DOWN BREAK"

        reason = (
            "BTC broke support...waiting "
            "for a 3m retest before entering."
        )

    elif (
        pa["armed_direction"]
        == "UP"
    ):

        setup = "UP RETEST ARMED"

        reason = (
            "The resistance breakout is "
            "still armed...waiting for BTC "
            "to retest and hold the level."
        )

    elif (
        pa["armed_direction"]
        == "DOWN"
    ):

        setup = "DOWN RETEST ARMED"

        reason = (
            "The support breakdown is "
            "still armed...waiting for BTC "
            "to retest and reject the level."
        )

    elif pa["near_support"]:

        setup = "WATCH SUPPORT"

        reason = (
            "BTC is near 15m support..."
            "waiting for the completed 3m "
            "candle to show its hand."
        )

    elif pa["near_resistance"]:

        setup = "WATCH RESISTANCE"

        reason = (
            "BTC is near 15m resistance..."
            "waiting for the completed 3m "
            "candle to show its hand."
        )

    # -----------------------------------------
    # KALSHI ENTRY DECISION
    # -----------------------------------------

    entry_price = None
    scalp = False

    if direction == "UP":
        entry_price = yes_ask

    elif direction == "DOWN":
        entry_price = no_ask

    if confirmed:

        if entry_price is None:

            action = (
                "🗣️ DON'T TOUCH THIS SHIT 🚫"
            )

            reason += (
                " Kalshi doesn't currently "
                "have a usable ask price."
            )

        elif remaining < (
            MIN_SECONDS_FOR_ENTRY
        ):

            action = (
                "🗣️ DON'T TOUCH THIS SHIT 🚫"
            )

            reason += (
                " There's less than a minute "
                "left...too late for a fresh "
                "entry signal."
            )

        elif entry_price >= (
            HARD_SKIP_PRICE
        ):

            action = (
                "🗣️ DON'T TOUCH THIS SHIT 🚫"
            )

            reason += (
                f" {direction} is already "
                f"${entry_price:.2f}...almost "
                "all the contract value is "
                "already priced in."
            )

        else:

            if direction == "UP":
                action = (
                    "🗣️ ENTER UP 🟢"
                )

            else:
                action = (
                    "🗣️ ENTER DOWN 🔴"
                )

            if entry_price >= (
                SCALP_PRICE
            ):

                scalp = True

                reason += (
                    f" 💰 SCALP: {direction} "
                    f"is ${entry_price:.2f}, "
                    "so the remaining room "
                    "is smaller."
                )

            elif remaining > (
                MAX_SECONDS_FOR_ENTRY
            ):

                reason += (
                    " Contract is still early..."
                    "expect more movement."
                )

    # -----------------------------------------
    # RANGE / CHOP
    # -----------------------------------------

    if (
        not confirmed
        and setup == "NONE"
        and structure["trend"] == "RANGE"
    ):

        action = (
            "🗣️ DON'T TOUCH THIS SHIT 🚫"
        )

        reason = (
            "BTC is ranging in the middle "
            "of the structure with no clean "
            "3m setup."
        )

    return {
        "action": action,
        "setup": setup,
        "reason": reason,

        "direction": direction,
        "confirmed": confirmed,
        "scalp": scalp,

        "btc_price":
            pa["price"],

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

        "entry_price":
            entry_price,

        "seconds_left":
            remaining,

        "candle_time":
            pa["candle_time"]
    }


# =========================================================
# MESSAGE
# =========================================================

def build_message(
    market,
    analysis
):

    seconds = (
        analysis["seconds_left"]
    )

    minutes = seconds // 60
    secs = seconds % 60

    ticker = market.get(
        "ticker",
        "Unknown"
    )

    scalp_line = ""

    if analysis["scalp"]:

        scalp_line = (
            "\n💰 SCALP SETUP — "
            "smaller profit window"
        )

    entry_line = ""

    if (
        analysis["direction"]
        and analysis["entry_price"]
        is not None
    ):

        entry_line = (
            f"\nSuggested side: "
            f"{analysis['direction']}"
            f"\nCurrent ask: "
            f"${analysis['entry_price']:.2f}"
        )

    return f"""
{analysis['action']}

Setup: {analysis['setup']}
{analysis['reason']}
{scalp_line}

BTC: ${analysis['btc_price']:,.2f}

📊 15M STRUCTURE
Support: ${analysis['support']:,.2f}
Resistance: ${analysis['resistance']:,.2f}
Trend: {analysis['trend']}

🎯 KALSHI
🟢 UP ask: {analysis['yes_ask']}
🔴 DOWN ask: {analysis['no_ask']}
{entry_line}

⏱ Time left: {minutes}:{secs:02d}

Contract:
{ticker}

⚠️ Signal only — no automatic trade.
""".strip()


# =========================================================
# MAIN LOOP
# =========================================================

def main():

    global last_alert
    global last_market
    global last_analysis

    print(
        "Kate's BTC Watcher V3 starting..."
    )

    initialize_telegram_offset()

    send_telegram(
        "🔥 KATE'S BTC WATCHER V3 IS ONLINE.\n\n"
        "Commands:\n"
        "/start — turn alerts ON\n"
        "/stop — turn alerts OFF\n"
        "/status — current read\n\n"
        "I'm ready 👀"
    )

    while True:

        try:

            # Always check commands,
            # even while watcher is OFF.
            check_telegram_commands()

            if not watcher_enabled:

                time.sleep(CHECK_EVERY)
                continue

            market = (
                get_live_btc_market()
            )

            if not market:

                print(
                    "No open BTC "
                    "15-minute market."
                )

                time.sleep(
                    CHECK_EVERY
                )

                continue

            ticker = market.get(
                "ticker"
            )

            analysis = (
                analyze_market(
                    market
                )
            )

            if not analysis:

                time.sleep(
                    CHECK_EVERY
                )

                continue

            last_analysis = analysis

            # ---------------------------------
            # NEW CONTRACT
            # ---------------------------------

            if ticker != last_market:

                last_market = ticker
                last_alert = None

                # Don't carry a retest setup
                # into another Kalshi contract.
                clear_retest()

                send_telegram(
                    "🆕 NEW BTC 15-MIN CONTRACT\n\n"
                    f"{ticker}\n\n"
                    "I'm watching 👀"
                )

            # ---------------------------------
            # ALERT WHEN THE MEANINGFUL
            # DECISION STATE CHANGES
            # ---------------------------------

            signal_key = (
                ticker,
                analysis["action"],
                analysis["setup"],
                analysis["scalp"],
                analysis["candle_time"]
                if analysis["confirmed"]
                else None
            )

            if (
                signal_key
                != last_alert
            ):

                last_alert = (
                    signal_key
                )

                message = (
                    build_message(
                        market,
                        analysis
                    )
                )

                send_telegram(
                    message
                )

                print(message)

            time.sleep(
                CHECK_EVERY
            )

        except Exception as e:

            print(
                "Monitor error:",
                e
            )

            # Still periodically wake up
            # so Telegram commands aren't
            # ignored forever after an error.
            time.sleep(15)


if __name__ == "__main__":
    main()
