import os
import time
import sqlite3
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

SCALP_PRICE = 0.88
HARD_SKIP_PRICE = 0.97

MIN_SECONDS_FOR_ENTRY = 60
MAX_SECONDS_FOR_ENTRY = 13 * 60

LEVEL_TOLERANCE_PCT = 0.15
BREAKOUT_BUFFER_PCT = 0.05
RETEST_MAX_CANDLES = 4

DATA_DIR = os.environ.get("DATA_DIR", "/data")

last_alert = None
last_market = None
last_analysis = None

watcher_enabled = True
telegram_offset = None

logged_signal_keys = set()

retest_state = {
    "direction": None,
    "level": None,
    "armed_candle_time": None
}


# =========================================================
# DATABASE
# =========================================================

def get_db_path():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)

        test_path = os.path.join(DATA_DIR, ".write_test")

        with open(test_path, "w") as f:
            f.write("ok")

        os.remove(test_path)

        return os.path.join(
            DATA_DIR,
            "kate_btc_watcher.db"
        )

    except Exception:
        return "kate_btc_watcher.db"


DB_PATH = get_db_path()


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_connect() as conn:

        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT UNIQUE,
                ticker TEXT,
                side TEXT,
                setup TEXT,
                trend TEXT,
                entry_price REAL,
                btc_price REAL,
                support REAL,
                resistance REAL,
                seconds_left INTEGER,
                candle_time INTEGER,
                created_at INTEGER,
                best_price REAL,
                worst_price REAL,
                price_1m REAL,
                price_3m REAL,
                price_5m REAL,
                hit_3c INTEGER DEFAULT 0,
                hit_5c INTEGER DEFAULT 0,
                hit_10c INTEGER DEFAULT 0,
                last_price REAL,
                last_updated INTEGER,
                finalized INTEGER DEFAULT 0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at INTEGER NOT NULL,
                ticker TEXT NOT NULL,
                seconds_left INTEGER,
                btc_price REAL,
                contract_target REAL,
                target_distance REAL,
                target_distance_pct REAL,
                target_position TEXT,

                yes_bid REAL,
                yes_ask REAL,
                no_bid REAL,
                no_ask REAL,

                yes_spread REAL,
                no_spread REAL,

                support REAL,
                resistance REAL,
                trend TEXT,

                distance_to_support REAL,
                distance_to_resistance REAL,

                candle_time INTEGER,
                candle_color TEXT,
                candle_open REAL,
                candle_high REAL,
                candle_low REAL,
                candle_close REAL,

                near_support INTEGER,
                near_resistance INTEGER,

                support_rejection INTEGER,
                resistance_rejection INTEGER,

                breakout_up INTEGER,
                breakout_down INTEGER,

                retest_up INTEGER,
                retest_down INTEGER,
                armed_direction TEXT,

                target_cross TEXT,
                target_velocity REAL,

                action TEXT,
                setup TEXT,
                direction TEXT,
                confirmed INTEGER,
                scalp INTEGER
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_snapshots_ticker_time
            ON market_snapshots(ticker, captured_at)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_ticker
            ON signals(ticker)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_created
            ON signals(created_at)
        """)

        conn.commit()


def load_logged_signal_keys():
    try:
        with db_connect() as conn:
            rows = conn.execute("""
                SELECT signal_key
                FROM signals
            """).fetchall()

            for row in rows:
                logged_signal_keys.add(
                    row["signal_key"]
                )

    except Exception as e:
        print("Load signal keys error:", e)


# =========================================================
# BASIC HELPERS
# =========================================================

def money(value):
    if value is None:
        return None

    try:
        return float(value)
    except Exception:
        return None


def format_money(value):
    if value is None:
        return "N/A"

    return f"${value:.2f}"


def utc_now():
    return datetime.now(timezone.utc)


def seconds_left(market):
    close_time = market.get("close_time")

    if not close_time:
        return None

    try:
        close_dt = datetime.fromisoformat(
            close_time.replace("Z", "+00:00")
        )

        remaining = (
            close_dt - utc_now()
        ).total_seconds()

        return max(0, int(remaining))

    except Exception:
        return None


# =========================================================
# TELEGRAM
# =========================================================

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram not configured.")
        return

    try:
        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_TOKEN}/sendMessage"
        )

        requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message
            },
            timeout=10
        )

    except Exception as e:
        print("Telegram send error:", e)


def get_telegram_updates():
    global telegram_offset

    if not TELEGRAM_TOKEN:
        return []

    try:
        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_TOKEN}/getUpdates"
        )

        params = {
            "timeout": 0
        }

        if telegram_offset is not None:
            params["offset"] = telegram_offset

        response = requests.get(
            url,
            params=params,
            timeout=10
        )

        data = response.json()

        if not data.get("ok"):
            return []

        return data.get("result", [])

    except Exception as e:
        print("Telegram updates error:", e)
        return []


def initialize_telegram_offset():
    global telegram_offset

    updates = get_telegram_updates()

    if updates:
        telegram_offset = (
            updates[-1]["update_id"] + 1
        )


def check_telegram_commands():
    global telegram_offset
    global watcher_enabled

    updates = get_telegram_updates()

    for update in updates:
        telegram_offset = (
            update["update_id"] + 1
        )

        message = update.get("message")

        if not message:
            continue

        chat = message.get("chat", {})
        chat_id = str(chat.get("id"))

        if chat_id != str(TELEGRAM_CHAT_ID):
            continue

        text = (
            message.get("text") or ""
        ).strip().lower()

        if text == "/start":
            watcher_enabled = True

            send_telegram(
                "🟢 BTC watcher is ON."
            )

        elif text == "/stop":
            watcher_enabled = False

            send_telegram(
                "🔴 BTC watcher is OFF."
            )

        elif text == "/status":
            send_status()

        elif text == "/last":
            send_last_signal()

        elif text == "/stats":
            send_stats()


# =========================================================
# KALSHI
# =========================================================

def get_open_markets():
    try:
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

        return response.json().get(
            "markets",
            []
        )

    except Exception as e:
        print("Kalshi markets error:", e)
        return []


def get_live_btc_market():
    markets = get_open_markets()

    if not markets:
        return None

    now = utc_now()

    candidates = []

    for market in markets:
        close_time = market.get(
            "close_time"
        )

        if not close_time:
            continue

        try:
            close_dt = datetime.fromisoformat(
                close_time.replace(
                    "Z",
                    "+00:00"
                )
            )

            remaining = (
                close_dt - now
            ).total_seconds()

            if remaining > 0:
                candidates.append(
                    (remaining, market)
                )

        except Exception:
            continue

    if not candidates:
        return None

    candidates.sort(
        key=lambda item: item[0]
    )

    return candidates[0][1]


# =========================================================
# CONTRACT TARGET
# =========================================================

def get_contract_target(market):
    """
    Read the BTC target directly from Kalshi market data.

    Different Kalshi market structures can expose the strike
    under slightly different fields, so we check the known
    strike fields instead of trying to guess from the ticker.
    """

    possible_fields = [
        "functional_strike",
        "floor_strike",
        "strike",
        "strike_value"
    ]

    for field in possible_fields:
        value = money(
            market.get(field)
        )

        if value is not None:
            return value

    return None


def get_target_context(
    btc_price,
    target
):
    if (
        btc_price is None
        or target is None
    ):
        return {
            "target": target,
            "distance": None,
            "distance_pct": None,
            "position": "UNKNOWN"
        }

    distance = (
        btc_price - target
    )

    if target != 0:
        distance_pct = (
            distance / target
        ) * 100
    else:
        distance_pct = None

    if distance > 0:
        position = "ABOVE"
    elif distance < 0:
        position = "BELOW"
    else:
        position = "AT TARGET"

    return {
        "target": target,
        "distance": distance,
        "distance_pct": distance_pct,
        "position": position
    }


# =========================================================
# KRAKEN CANDLES
# =========================================================

def get_kraken_ohlc(interval):
    try:
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
            print(
                "Kraken error:",
                data["error"]
            )
            return []

        result = data.get(
            "result",
            {}
        )

        key = None

        for item in result.keys():
            if item != "last":
                key = item
                break

        if not key:
            return []

        raw = result[key]

        candles = []

        for candle in raw:
            candles.append({
                "time": int(candle[0]),
                "open": float(candle[1]),
                "high": float(candle[2]),
                "low": float(candle[3]),
                "close": float(candle[4]),
                "vwap": float(candle[5]),
                "volume": float(candle[6]),
                "count": int(candle[7])
            })

        return candles

    except Exception as e:
        print("Kraken OHLC error:", e)
        return []


def completed_candles(
    candles,
    interval_minutes
):
    if not candles:
        return []

    now_ts = int(time.time())

    interval_seconds = (
        interval_minutes * 60
    )

    result = []

    for candle in candles:
        candle_end = (
            candle["time"]
            + interval_seconds
        )

        if candle_end <= now_ts:
            result.append(candle)

    return result


# =========================================================
# BUILD COMPLETED 3-MINUTE CANDLES FROM 1M
# =========================================================

def build_3m_candles():
    one_minute = get_kraken_ohlc(1)

    completed_1m = completed_candles(
        one_minute,
        1
    )

    if len(completed_1m) < 3:
        return []

    grouped = {}

    for candle in completed_1m:

        bucket = (
            candle["time"] // 180
        ) * 180

        if bucket not in grouped:
            grouped[bucket] = []

        grouped[bucket].append(candle)

    candles_3m = []

    for bucket in sorted(
        grouped.keys()
    ):
        group = grouped[bucket]

        if len(group) != 3:
            continue

        expected_times = [
            bucket,
            bucket + 60,
            bucket + 120
        ]

        actual_times = [
            c["time"]
            for c in group
        ]

        if actual_times != expected_times:
            continue

        candles_3m.append({
            "time": bucket,
            "open": group[0]["open"],
            "high": max(
                c["high"]
                for c in group
            ),
            "low": min(
                c["low"]
                for c in group
            ),
            "close": group[-1]["close"]
        })

    return candles_3m


# =========================================================
# 15-MINUTE STRUCTURE
# =========================================================

def get_structure():
    candles = get_kraken_ohlc(15)

    completed = completed_candles(
        candles,
        15
    )

    if len(completed) < 8:
        return None

    recent = completed[-8:]

    support = min(
        candle["low"]
        for candle in recent
    )

    resistance = max(
        candle["high"]
        for candle in recent
    )

    half = len(recent) // 2

    first_avg = sum(
        candle["close"]
        for candle in recent[:half]
    ) / half

    second_avg = sum(
        candle["close"]
        for candle in recent[half:]
    ) / (
        len(recent) - half
    )

    if second_avg > first_avg:
        trend = "BULLISH"
    elif second_avg < first_avg:
        trend = "BEARISH"
    else:
        trend = "FLAT"

    return {
        "support": support,
        "resistance": resistance,
        "trend": trend
    }


# =========================================================
# RETEST MEMORY
# =========================================================

def clear_retest():
    retest_state["direction"] = None
    retest_state["level"] = None
    retest_state[
        "armed_candle_time"
    ] = None


def arm_retest(
    direction,
    level,
    candle_time
):
    retest_state["direction"] = (
        direction
    )

    retest_state["level"] = level

    retest_state[
        "armed_candle_time"
    ] = candle_time


def candles_since_armed(
    current_candle_time
):
    armed = retest_state.get(
        "armed_candle_time"
    )

    if armed is None:
        return None

    return int(
        (
            current_candle_time
            - armed
        ) / 180
    )


# =========================================================
# PRICE ACTION
# =========================================================

def analyze_price_action(
    support,
    resistance
):
    candles = build_3m_candles()

    if len(candles) < 2:
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
        abs(
            price - support
        ) / support * 100
        <= LEVEL_TOLERANCE_PCT
    )

    near_resistance = (
        abs(
            price - resistance
        ) / resistance * 100
        <= LEVEL_TOLERANCE_PCT
    )

    above_support = (
        price >= support
    )

    below_support = (
        price < support
    )

    below_resistance = (
        price <= resistance
    )

    above_resistance = (
        price > resistance
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
        and current["close"]
        < resistance
        and red
    )

    breakout_up = (
        previous["close"]
        <= resistance
        and current["close"]
        > resistance
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
        < support
        * (
            1
            - BREAKOUT_BUFFER_PCT
            / 100
        )
    )

    retest_up = False
    retest_down = False

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

    armed_direction = (
        retest_state["direction"]
    )

    armed_level = (
        retest_state["level"]
    )

    if (
        armed_direction
        and armed_level is not None
    ):
        candles_since = (
            candles_since_armed(
                current["time"]
            )
        )

        if (
            candles_since is not None
            and candles_since
            > RETEST_MAX_CANDLES
        ):
            clear_retest()

        elif (
            candles_since is not None
            and candles_since >= 1
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

            elif (
                current["close"]
                < armed_level
                * (
                    1
                    - BREAKOUT_BUFFER_PCT
                    / 100
                )
            ):
                clear_retest()

        elif (
            candles_since is not None
            and candles_since >= 1
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

            elif (
                current["close"]
                > armed_level
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

        "candle_open": current["open"],
        "candle_high": current["high"],
        "candle_low": current["low"],
        "candle_close": current["close"],

        "candle_color": (
            "GREEN"
            if green
            else "RED"
            if red
            else "FLAT"
        ),

        "near_support": near_support,
        "near_resistance": near_resistance,

        "above_support": above_support,
        "below_support": below_support,

        "below_resistance": (
            below_resistance
        ),

        "above_resistance": (
            above_resistance
        ),

        "support_rejection": (
            support_rejection
        ),

        "resistance_rejection": (
            resistance_rejection
        ),

        "breakout_up": breakout_up,
        "breakout_down": breakout_down,

        "retest_up": retest_up,
        "retest_down": retest_down,

        "armed_direction": (
            retest_state["direction"]
        )
    }

# =========================================================
# FINAL ACTION DECISION
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

    pa = analyze_price_action(
        structure["support"],
        structure["resistance"]
    )

    if not pa:
        return None

    btc_price = pa["price"]

    contract_target = (
        get_contract_target(market)
    )

    target_context = get_target_context(
        btc_price,
        contract_target
    )

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
            "Resistance broke...BTC came back "
            "to retest it and the 3m candle "
            "held above the level."
        )

    elif pa["retest_down"]:
        direction = "DOWN"
        confirmed = True
        setup = "RETEST REJECTION"

        reason = (
            "Support broke...BTC came back "
            "to retest it and the 3m candle "
            "rejected the old support."
        )

    elif pa["support_rejection"]:
        direction = "UP"
        confirmed = True
        setup = "SUPPORT REJECTION"

        reason = (
            "BTC tested 15m support and the "
            "completed 3m candle bounced "
            "back up."
        )

    elif pa["resistance_rejection"]:
        direction = "DOWN"
        confirmed = True
        setup = "RESISTANCE REJECTION"

        reason = (
            "BTC tested 15m resistance and "
            "the completed 3m candle turned "
            "back down."
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
        pa["armed_direction"] == "UP"
    ):
        setup = "UP RETEST ARMED"

        reason = (
            "The resistance breakout is still "
            "armed...waiting for BTC to retest "
            "and stay above the level."
        )

    elif (
        pa["armed_direction"] == "DOWN"
    ):
        setup = "DOWN RETEST ARMED"

        reason = (
            "The support break is still "
            "armed...waiting for BTC to retest "
            "and turn back down."
        )

    elif (
        pa["near_support"]
        and pa["below_support"]
    ):
        setup = "SUPPORT BREAKING"

        reason = (
            "BTC is below 15m support, but "
            "the break is not confirmed enough "
            "for an entry yet...WAIT."
        )

    elif (
        pa["near_support"]
        and pa["above_support"]
    ):
        setup = "WATCH SUPPORT"

        reason = (
            "BTC is near 15m support but has "
            "not completed a strong enough "
            "3m rejection yet."
        )

    elif (
        pa["near_resistance"]
        and pa["above_resistance"]
    ):
        setup = "RESISTANCE BREAKING"

        reason = (
            "BTC is above 15m resistance, but "
            "the breakout is not confirmed "
            "enough for an entry yet...WAIT."
        )

    elif (
        pa["near_resistance"]
        and pa["below_resistance"]
    ):
        setup = "WATCH RESISTANCE"

        reason = (
            "BTC is near 15m resistance but "
            "has not completed a strong enough "
            "3m rejection yet."
        )

    # -----------------------------------------
    # ENTRY PRICE
    # -----------------------------------------

    entry_price = None

    if direction == "UP":
        entry_price = yes_ask

    elif direction == "DOWN":
        entry_price = no_ask

    scalp = False

    # -----------------------------------------
    # ENTRY RULES
    # -----------------------------------------

    if confirmed and direction:

        if remaining is None:
            action = "🗣️ WAIT 🟡"

            reason = (
                "Setup confirmed, but I cannot "
                "read the contract clock."
            )

        elif remaining < MIN_SECONDS_FOR_ENTRY:
            action = (
                "🗣️ DON'T TOUCH THIS SHIT 🚫"
            )

            reason = (
                "The setup confirmed too late. "
                "There is less than 1 minute "
                "left in this contract."
            )

        elif remaining > MAX_SECONDS_FOR_ENTRY:
            action = "🗣️ WAIT 🟡"

            reason = (
                "The setup is confirmed, but "
                "this contract is still too "
                "early for an entry."
            )

        elif entry_price is None:
            action = "🗣️ WAIT 🟡"

            reason = (
                "Setup confirmed, but Kalshi "
                "is not giving me a usable "
                "ask price."
            )

        elif entry_price >= HARD_SKIP_PRICE:
            action = (
                "🗣️ DON'T TOUCH THIS SHIT 🚫"
            )

            reason = (
                f"{direction} is already "
                f"${entry_price:.2f}. There is "
                "not enough room left for the "
                "kind of scalp we want."
            )

        else:
            action = (
                f"🗣️ ENTER {direction} "
                + (
                    "🟢"
                    if direction == "UP"
                    else "🔴"
                )
            )

            if entry_price >= SCALP_PRICE:
                scalp = True

                reason += (
                    f"\n\n💰 SCALP ONLY: "
                    f"{direction} is already "
                    f"${entry_price:.2f}. "
                    "Take the little move if "
                    "the contract gives it."
                )

    return {
        "action": action,
        "setup": setup,
        "reason": reason,

        "direction": direction,
        "confirmed": confirmed,
        "scalp": scalp,

        "btc_price": btc_price,

        "support": (
            structure["support"]
        ),

        "resistance": (
            structure["resistance"]
        ),

        "trend": structure["trend"],

        "yes_bid": yes_bid,
        "yes_ask": yes_ask,

        "no_bid": no_bid,
        "no_ask": no_ask,

        "entry_price": entry_price,

        "seconds_left": remaining,

        "contract_target": (
            target_context["target"]
        ),

        "target_distance": (
            target_context["distance"]
        ),

        "target_distance_pct": (
            target_context["distance_pct"]
        ),

        "target_position": (
            target_context["position"]
        ),

        "candle_time": (
            pa["candle_time"]
        ),

        "candle_open": (
            pa["candle_open"]
        ),

        "candle_high": (
            pa["candle_high"]
        ),

        "candle_low": (
            pa["candle_low"]
        ),

        "candle_close": (
            pa["candle_close"]
        ),

        "candle_color": (
            pa["candle_color"]
        ),

        "near_support": (
            pa["near_support"]
        ),

        "near_resistance": (
            pa["near_resistance"]
        ),

        "support_rejection": (
            pa["support_rejection"]
        ),

        "resistance_rejection": (
            pa["resistance_rejection"]
        ),

        "breakout_up": (
            pa["breakout_up"]
        ),

        "breakout_down": (
            pa["breakout_down"]
        ),

        "retest_up": (
            pa["retest_up"]
        ),

        "retest_down": (
            pa["retest_down"]
        ),

        "armed_direction": (
            pa["armed_direction"]
        )
    }


# =========================================================
# V4.2 BLACK-BOX SNAPSHOT RECORDER
# =========================================================

def record_market_snapshot(
    market,
    analysis
):
    """
    Save what the bot can see every monitor loop.

    IMPORTANT:
    This records WAIT states too.

    It does NOT change the official ENTER rules.
    """

    ticker = market.get("ticker")

    if not ticker or not analysis:
        return

    now = int(time.time())

    yes_bid = analysis.get("yes_bid")
    yes_ask = analysis.get("yes_ask")

    no_bid = analysis.get("no_bid")
    no_ask = analysis.get("no_ask")

    if (
        yes_bid is not None
        and yes_ask is not None
    ):
        yes_spread = (
            yes_ask - yes_bid
        )
    else:
        yes_spread = None

    if (
        no_bid is not None
        and no_ask is not None
    ):
        no_spread = (
            no_ask - no_bid
        )
    else:
        no_spread = None

    btc = analysis.get("btc_price")

    support = analysis.get(
        "support"
    )

    resistance = analysis.get(
        "resistance"
    )

    try:
        with db_connect() as conn:

            previous = conn.execute(
                """
                SELECT
                    target_distance,
                    captured_at
                FROM market_snapshots
                WHERE ticker = ?
                ORDER BY
                    captured_at DESC,
                    id DESC
                LIMIT 1
                """,
                (ticker,)
            ).fetchone()

            target_cross = None
            target_velocity = None

            distance = analysis.get(
                "target_distance"
            )

            if (
                previous
                and distance is not None
                and previous[
                    "target_distance"
                ] is not None
            ):
                old_distance = float(
                    previous[
                        "target_distance"
                    ]
                )

                elapsed = max(
                    1,
                    now
                    - int(
                        previous[
                            "captured_at"
                        ]
                    )
                )

                target_velocity = (
                    float(distance)
                    - old_distance
                ) / elapsed

                if (
                    old_distance <= 0
                    and distance > 0
                ):
                    target_cross = "UP"

                elif (
                    old_distance >= 0
                    and distance < 0
                ):
                    target_cross = "DOWN"

            if (
                btc is not None
                and support is not None
            ):
                distance_to_support = (
                    btc - support
                )
            else:
                distance_to_support = None

            if (
                btc is not None
                and resistance is not None
            ):
                distance_to_resistance = (
                    resistance - btc
                )
            else:
                distance_to_resistance = None

            conn.execute(
                """
                INSERT INTO market_snapshots (
                    captured_at,
                    ticker,
                    seconds_left,
                    btc_price,
                    contract_target,
                    target_distance,
                    target_distance_pct,
                    target_position,

                    yes_bid,
                    yes_ask,
                    no_bid,
                    no_ask,

                    yes_spread,
                    no_spread,

                    support,
                    resistance,
                    trend,

                    distance_to_support,
                    distance_to_resistance,

                    candle_time,
                    candle_color,
                    candle_open,
                    candle_high,
                    candle_low,
                    candle_close,

                    near_support,
                    near_resistance,

                    support_rejection,
                    resistance_rejection,

                    breakout_up,
                    breakout_down,

                    retest_up,
                    retest_down,
                    armed_direction,

                    target_cross,
                    target_velocity,

                    action,
                    setup,
                    direction,
                    confirmed,
                    scalp
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    ?, ?,
                    ?, ?, ?, ?, ?
                )
                """,
                (
                    now,
                    ticker,
                    analysis.get(
                        "seconds_left"
                    ),
                    btc,
                    analysis.get(
                        "contract_target"
                    ),
                    distance,
                    analysis.get(
                        "target_distance_pct"
                    ),
                    analysis.get(
                        "target_position"
                    ),

                    yes_bid,
                    yes_ask,
                    no_bid,
                    no_ask,

                    yes_spread,
                    no_spread,

                    support,
                    resistance,
                    analysis.get("trend"),

                    distance_to_support,
                    distance_to_resistance,

                    analysis.get(
                        "candle_time"
                    ),
                    analysis.get(
                        "candle_color"
                    ),
                    analysis.get(
                        "candle_open"
                    ),
                    analysis.get(
                        "candle_high"
                    ),
                    analysis.get(
                        "candle_low"
                    ),
                    analysis.get(
                        "candle_close"
                    ),

                    int(
                        bool(
                            analysis.get(
                                "near_support"
                            )
                        )
                    ),

                    int(
                        bool(
                            analysis.get(
                                "near_resistance"
                            )
                        )
                    ),

                    int(
                        bool(
                            analysis.get(
                                "support_rejection"
                            )
                        )
                    ),

                    int(
                        bool(
                            analysis.get(
                                "resistance_rejection"
                            )
                        )
                    ),

                    int(
                        bool(
                            analysis.get(
                                "breakout_up"
                            )
                        )
                    ),

                    int(
                        bool(
                            analysis.get(
                                "breakout_down"
                            )
                        )
                    ),

                    int(
                        bool(
                            analysis.get(
                                "retest_up"
                            )
                        )
                    ),

                    int(
                        bool(
                            analysis.get(
                                "retest_down"
                            )
                        )
                    ),

                    analysis.get(
                        "armed_direction"
                    ),

                    target_cross,
                    target_velocity,

                    analysis.get("action"),
                    analysis.get("setup"),
                    analysis.get(
                        "direction"
                    ),

                    int(
                        bool(
                            analysis.get(
                                "confirmed"
                            )
                        )
                    ),

                    int(
                        bool(
                            analysis.get(
                                "scalp"
                            )
                        )
                    )
                )
            )

            conn.commit()

    except Exception as e:
        print(
            "Snapshot recorder error:",
            e
        )


# =========================================================
# MESSAGE
# =========================================================

def build_message(
    market,
    analysis
):
    ticker = market.get(
        "ticker",
        "UNKNOWN"
    )

    remaining = analysis.get(
        "seconds_left"
    )

    if remaining is None:
        minutes = 0
        secs = 0
    else:
        minutes = (
            remaining // 60
        )

        secs = (
            remaining % 60
        )

    scalp_line = ""

    if analysis["scalp"]:
        scalp_line = (
            "\n💰 SCALP MODE"
        )

    entry_line = ""

    if (
        analysis["direction"]
        and analysis[
            "entry_price"
        ] is not None
    ):
        entry_line = (
            f"\nSuggested side: "
            f"{analysis['direction']}"
            f"\nCurrent ask: "
            f"${analysis['entry_price']:.2f}"
        )

    target = analysis.get(
        "contract_target"
    )

    target_distance = analysis.get(
        "target_distance"
    )

    target_position = analysis.get(
        "target_position"
    )

    if target is None:
        target_block = (
            "Target: unavailable"
        )

    else:
        target_block = (
            f"Target: ${target:,.2f}"
        )

        if target_distance is not None:

            if target_distance > 0:
                target_block += (
                    f"\nBTC is "
                    f"${abs(target_distance):,.2f} "
                    "ABOVE target"
                )

            elif target_distance < 0:
                target_block += (
                    f"\nBTC is "
                    f"${abs(target_distance):,.2f} "
                    "BELOW target"
                )

            else:
                target_block += (
                    "\nBTC is AT target"
                )

        if target_position == "ABOVE":
            target_block += (
                "\n🟢 UP is currently "
                "on the winning side."
            )

        elif target_position == "BELOW":
            target_block += (
                "\n🔴 DOWN is currently "
                "on the winning side."
            )

    yes_ask_text = (
        f"${analysis['yes_ask']:.3f}"
        if analysis["yes_ask"]
        is not None
        else "N/A"
    )

    no_ask_text = (
        f"${analysis['no_ask']:.3f}"
        if analysis["no_ask"]
        is not None
        else "N/A"
    )

    yes_bid_text = (
        f"${analysis['yes_bid']:.3f}"
        if analysis["yes_bid"]
        is not None
        else "N/A"
    )

    no_bid_text = (
        f"${analysis['no_bid']:.3f}"
        if analysis["no_bid"]
        is not None
        else "N/A"
    )

    return f"""
{analysis['action']}

Setup: {analysis['setup']}
{analysis['reason']}
{scalp_line}

BTC: ${analysis['btc_price']:,.2f}

🎯 CONTRACT TARGET
{target_block}

📊 15M STRUCTURE
Support: ${analysis['support']:,.2f}
Resistance: ${analysis['resistance']:,.2f}
Trend: {analysis['trend']}

🎯 KALSHI
🟢 UP bid: {yes_bid_text}
🟢 UP ask: {yes_ask_text}
🔴 DOWN bid: {no_bid_text}
🔴 DOWN ask: {no_ask_text}
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
        "Kate's BTC Watcher V4.2 starting..."
    )

    init_db()
    load_logged_signal_keys()
    initialize_telegram_offset()

    send_telegram(
        "🔥 KATE'S BTC WATCHER V4.2 IS ONLINE.\n\n"
        "📊 ENTER calls are being tracked.\n"
        "🎯 Contract target is being read.\n"
        "🧠 Black-box snapshots are recording every loop.\n"
        "💵 Scalp exits are measured from the sellable bid."
    )

    while True:
        try:
            check_telegram_commands()

            if not watcher_enabled:
                time.sleep(CHECK_EVERY)
                continue

            market = get_live_btc_market()

            if not market:
                print(
                    "No live BTC market found."
                )

                time.sleep(CHECK_EVERY)
                continue

            ticker = market.get("ticker")

            # ---------------------------------
            # NEW CONTRACT
            # ---------------------------------

            if (
                last_market is not None
                and ticker != last_market
            ):
                clear_retest()

                finalize_old_signals(
                    ticker
                )

                last_alert = None

            last_market = ticker

            # ---------------------------------
            # ANALYZE
            # ---------------------------------

            analysis = analyze_market(
                market
            )

            if not analysis:
                print(
                    "Not enough market data "
                    "to analyze yet."
                )

                time.sleep(CHECK_EVERY)
                continue

            last_analysis = analysis

            # ---------------------------------
            # BLACK-BOX RECORDING
            # ---------------------------------

            record_market_snapshot(
                market,
                analysis
            )

            # ---------------------------------
            # UPDATE EXISTING ENTER STATS
            # ---------------------------------

            update_open_signals(
                market
            )

            # ---------------------------------
            # BUILD CURRENT MESSAGE
            # ---------------------------------

            message = build_message(
                market,
                analysis
            )

            action = analysis.get(
                "action",
                ""
            )

            setup = analysis.get(
                "setup",
                "NONE"
            )

            direction = analysis.get(
                "direction"
            )

            candle_time = analysis.get(
                "candle_time"
            )

            # ---------------------------------
            # SIGNAL KEY
            # ---------------------------------

            signal_key = (
                f"{ticker}|"
                f"{direction}|"
                f"{setup}|"
                f"{candle_time}"
            )

            # ---------------------------------
            # RECORD REAL ENTER CALL
            # ---------------------------------

            if action.startswith(
                "🗣️ ENTER"
            ):
                record_enter_signal(
                    ticker,
                    analysis
                )

            # ---------------------------------
            # TELEGRAM ALERT LOGIC
            # ---------------------------------

            alert_key = (
                ticker,
                action,
                setup,
                direction,
                candle_time
            )

            if (
                action.startswith(
                    "🗣️ ENTER"
                )
            ):
                if alert_key != last_alert:
                    send_telegram(
                        message
                    )

                    last_alert = alert_key

            elif (
                action.startswith(
                    "🗣️ DON'T TOUCH"
                )
            ):
                if alert_key != last_alert:
                    send_telegram(
                        message
                    )

                    last_alert = alert_key

            else:
                # WAIT alerts only when the
                # state/setup actually changes.
                wait_key = (
                    ticker,
                    action,
                    setup,
                    direction
                )

                if wait_key != last_alert:
                    send_telegram(
                        message
                    )

                    last_alert = wait_key

            # ---------------------------------
            # CONSOLE
            # ---------------------------------

            print(
                datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "|",
                ticker,
                "|",
                action,
                "|",
                setup,
                "| BTC",
                analysis.get(
                    "btc_price"
                ),
                "| Target",
                analysis.get(
                    "contract_target"
                )
            )

        except KeyboardInterrupt:
            print(
                "Watcher stopped manually."
            )
            break

        except Exception as e:
            print(
                "Main loop error:",
                e
            )

        time.sleep(CHECK_EVERY)


# =========================================================
# START
# =========================================================

if __name__ == "__main__":
    main()
