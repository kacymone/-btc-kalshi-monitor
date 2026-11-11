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

STRUCTURE_CANDLES = 8
LEVEL_TOLERANCE_PCT = 0.15
BREAKOUT_BUFFER_PCT = 0.05

SCALP_PRICE = 0.88
HARD_SKIP_PRICE = 0.97

MIN_SECONDS_FOR_ENTRY = 60
MAX_SECONDS_FOR_ENTRY = 13 * 60

RETEST_MAX_CANDLES = 4

# Railway Volume:
# Mount a Railway Volume at /data and the bot will automatically
# keep its stats across restarts/redeploys.
DATA_DIR = os.environ.get("DATA_DIR", "/data")
FALLBACK_DATA_DIR = "."
DB_FILENAME = "kate_btc_watcher.db"

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

# Prevent the same completed 3m confirmation from being logged twice.
logged_signal_keys = set()

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
        return os.path.join(DATA_DIR, DB_FILENAME)
    except Exception:
        return os.path.join(FALLBACK_DATA_DIR, DB_FILENAME)


DB_PATH = get_db_path()


def db_connect():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db_connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_key TEXT UNIQUE NOT NULL,
                ticker TEXT NOT NULL,
                side TEXT NOT NULL,
                setup TEXT NOT NULL,
                trend TEXT,
                entry_price REAL NOT NULL,
                btc_price REAL,
                support REAL,
                resistance REAL,
                seconds_left INTEGER,
                candle_time INTEGER,
                created_at INTEGER NOT NULL,

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
            CREATE INDEX IF NOT EXISTS idx_signals_ticker
            ON signals(ticker)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_signals_created
            ON signals(created_at)
        """)
        conn.commit()


def load_logged_signal_keys():
    global logged_signal_keys
    try:
        with db_connect() as conn:
            rows = conn.execute(
                "SELECT signal_key FROM signals"
            ).fetchall()
        logged_signal_keys = {
            row["signal_key"] for row in rows
        }
    except Exception as e:
        print("Load signal keys error:", e)


def make_signal_key(ticker, analysis):
    return (
        f"{ticker}|"
        f"{analysis['direction']}|"
        f"{analysis['setup']}|"
        f"{analysis['candle_time']}"
    )


def record_enter_signal(ticker, analysis):
    if not analysis["confirmed"]:
        return

    if not analysis["action"].startswith("🗣️ ENTER"):
        return

    if analysis["entry_price"] is None:
        return

    signal_key = make_signal_key(ticker, analysis)

    if signal_key in logged_signal_keys:
        return

    now = int(time.time())
    entry = float(analysis["entry_price"])

    try:
        with db_connect() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO signals (
                    signal_key,
                    ticker,
                    side,
                    setup,
                    trend,
                    entry_price,
                    btc_price,
                    support,
                    resistance,
                    seconds_left,
                    candle_time,
                    created_at,
                    best_price,
                    worst_price,
                    last_price,
                    last_updated
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                signal_key,
                ticker,
                analysis["direction"],
                analysis["setup"],
                analysis["trend"],
                entry,
                analysis["btc_price"],
                analysis["support"],
                analysis["resistance"],
                analysis["seconds_left"],
                analysis["candle_time"],
                now,
                entry,
                entry,
                entry,
                now
            ))
            conn.commit()

        logged_signal_keys.add(signal_key)
        print("Logged ENTER signal:", signal_key)

    except Exception as e:
        print("Record signal error:", e)


def side_ask_for_signal(side, market):
    if side == "UP":
        return money(market.get("yes_ask_dollars"))
    return money(market.get("no_ask_dollars"))


def update_open_signals(market):
    ticker = market.get("ticker")
    if not ticker:
        return

    now = int(time.time())

    try:
        with db_connect() as conn:
            rows = conn.execute("""
                SELECT *
                FROM signals
                WHERE ticker = ?
                  AND finalized = 0
            """, (ticker,)).fetchall()

            for row in rows:
                current_price = side_ask_for_signal(
                    row["side"],
                    market
                )

                if current_price is None:
                    continue

                entry = float(row["entry_price"])
                age = now - int(row["created_at"])

                best = row["best_price"]
                worst = row["worst_price"]

                best = current_price if best is None else max(
                    float(best), current_price
                )
                worst = current_price if worst is None else min(
                    float(worst), current_price
                )

                price_1m = row["price_1m"]
                price_3m = row["price_3m"]
                price_5m = row["price_5m"]

                if price_1m is None and age >= 60:
                    price_1m = current_price

                if price_3m is None and age >= 180:
                    price_3m = current_price

                if price_5m is None and age >= 300:
                    price_5m = current_price

                hit_3c = 1 if best >= entry + 0.03 else row["hit_3c"]
                hit_5c = 1 if best >= entry + 0.05 else row["hit_5c"]
                hit_10c = 1 if best >= entry + 0.10 else row["hit_10c"]

                conn.execute("""
                    UPDATE signals
                    SET best_price = ?,
                        worst_price = ?,
                        price_1m = ?,
                        price_3m = ?,
                        price_5m = ?,
                        hit_3c = ?,
                        hit_5c = ?,
                        hit_10c = ?,
                        last_price = ?,
                        last_updated = ?
                    WHERE id = ?
                """, (
                    best,
                    worst,
                    price_1m,
                    price_3m,
                    price_5m,
                    hit_3c,
                    hit_5c,
                    hit_10c,
                    current_price,
                    now,
                    row["id"]
                ))

            conn.commit()

    except Exception as e:
        print("Update signal tracking error:", e)


def finalize_old_signals(current_ticker):
    try:
        with db_connect() as conn:
            conn.execute("""
                UPDATE signals
                SET finalized = 1
                WHERE ticker != ?
                  AND finalized = 0
            """, (current_ticker,))
            conn.commit()
    except Exception as e:
        print("Finalize signals error:", e)


def cents(value):
    if value is None:
        return "—"
    return f"{float(value) * 100:.0f}¢"


def signed_cents(delta):
    if delta is None:
        return "—"
    return f"{delta * 100:+.0f}¢"


def pct_text(count, total):
    if not total:
        return "—"
    return f"{count / total * 100:.0f}%"


def price_bucket(price):
    cents_value = price * 100

    if cents_value < 50:
        return "<50¢"
    if cents_value < 70:
        return "50–69¢"
    if cents_value < 80:
        return "70–79¢"
    if cents_value < 90:
        return "80–89¢"
    return "90¢+"


def time_bucket(seconds):
    if seconds >= 600:
        return "10+ min"
    if seconds >= 300:
        return "5–10 min"
    if seconds >= 120:
        return "2–5 min"
    return "<2 min"


def get_last_signal():
    try:
        with db_connect() as conn:
            return conn.execute("""
                SELECT *
                FROM signals
                ORDER BY id DESC
                LIMIT 1
            """).fetchone()
    except Exception as e:
        print("Last signal error:", e)
        return None


def send_last():
    row = get_last_signal()

    if not row:
        send_telegram(
            "📭 NO ENTER CALLS LOGGED YET.\n\n"
            "I'm waiting on my first V4 ENTER."
        )
        return

    entry = float(row["entry_price"])
    best = row["best_price"]
    worst = row["worst_price"]

    best_move = (
        float(best) - entry
        if best is not None
        else None
    )

    worst_move = (
        float(worst) - entry
        if worst is not None
        else None
    )

    status = (
        "FINAL"
        if row["finalized"]
        else "STILL TRACKING"
    )

    send_telegram(
        "🧾 LAST ENTER CALL\n\n"
        f"{row['side']} @ {cents(entry)}\n"
        f"Setup: {row['setup']}\n"
        f"Trend: {row['trend']}\n"
        f"Status: {status}\n\n"
        f"Best price: {cents(best)} "
        f"({signed_cents(best_move)})\n"
        f"Worst price: {cents(worst)} "
        f"({signed_cents(worst_move)})\n\n"
        f"After 1m: {cents(row['price_1m'])}\n"
        f"After 3m: {cents(row['price_3m'])}\n"
        f"After 5m: {cents(row['price_5m'])}\n\n"
        f"+3¢ available: {'✅' if row['hit_3c'] else '❌'}\n"
        f"+5¢ available: {'✅' if row['hit_5c'] else '❌'}\n"
        f"+10¢ available: {'✅' if row['hit_10c'] else '❌'}"
    )


def send_stats():
    try:
        with db_connect() as conn:
            rows = conn.execute("""
                SELECT *
                FROM signals
                ORDER BY id ASC
            """).fetchall()
    except Exception as e:
        print("Stats error:", e)
        send_telegram("Stats database error.")
        return

    total = len(rows)

    if total == 0:
        send_telegram(
            "📊 WATCHER STATS\n\n"
            "No ENTER calls logged yet.\n"
            "WAIT calls do not count."
        )
        return

    hit3 = sum(int(row["hit_3c"] or 0) for row in rows)
    hit5 = sum(int(row["hit_5c"] or 0) for row in rows)
    hit10 = sum(int(row["hit_10c"] or 0) for row in rows)

    up_rows = [r for r in rows if r["side"] == "UP"]
    down_rows = [r for r in rows if r["side"] == "DOWN"]

    def hit5_rate(group):
        if not group:
            return "—"
        hits = sum(int(r["hit_5c"] or 0) for r in group)
        return pct_text(hits, len(group))

    setup_groups = {}
    for row in rows:
        setup_groups.setdefault(row["setup"], []).append(row)

    setup_lines = []
    for setup, group in sorted(
        setup_groups.items(),
        key=lambda item: len(item[1]),
        reverse=True
    ):
        hits = sum(int(r["hit_5c"] or 0) for r in group)
        setup_lines.append(
            f"• {setup}: {hits}/{len(group)} "
            f"hit +5¢ ({pct_text(hits, len(group))})"
        )

    price_groups = {}
    for row in rows:
        bucket = price_bucket(float(row["entry_price"]))
        price_groups.setdefault(bucket, []).append(row)

    price_order = ["<50¢", "50–69¢", "70–79¢", "80–89¢", "90¢+"]
    price_lines = []

    for bucket in price_order:
        group = price_groups.get(bucket, [])
        if not group:
            continue
        hits = sum(int(r["hit_5c"] or 0) for r in group)
        price_lines.append(
            f"• {bucket}: {hits}/{len(group)} "
            f"hit +5¢ ({pct_text(hits, len(group))})"
        )

    time_groups = {}
    for row in rows:
        bucket = time_bucket(int(row["seconds_left"] or 0))
        time_groups.setdefault(bucket, []).append(row)

    time_order = ["10+ min", "5–10 min", "2–5 min", "<2 min"]
    time_lines = []

    for bucket in time_order:
        group = time_groups.get(bucket, [])
        if not group:
            continue
        hits = sum(int(r["hit_5c"] or 0) for r in group)
        time_lines.append(
            f"• {bucket}: {hits}/{len(group)} "
            f"hit +5¢ ({pct_text(hits, len(group))})"
        )

    completed_best_moves = []
    completed_worst_moves = []

    for row in rows:
        entry = float(row["entry_price"])

        if row["best_price"] is not None:
            completed_best_moves.append(
                float(row["best_price"]) - entry
            )

        if row["worst_price"] is not None:
            completed_worst_moves.append(
                float(row["worst_price"]) - entry
            )

    avg_best = (
        sum(completed_best_moves) / len(completed_best_moves)
        if completed_best_moves
        else None
    )

    avg_worst = (
        sum(completed_worst_moves) / len(completed_worst_moves)
        if completed_worst_moves
        else None
    )

    message = (
        "📊 KATE'S WATCHER STATS\n\n"
        f"ENTER calls: {total}\n"
        f"+3¢ available: {hit3}/{total} ({pct_text(hit3, total)})\n"
        f"+5¢ available: {hit5}/{total} ({pct_text(hit5, total)})\n"
        f"+10¢ available: {hit10}/{total} ({pct_text(hit10, total)})\n\n"
        f"Average best move: {signed_cents(avg_best)}\n"
        f"Average worst move: {signed_cents(avg_worst)}\n\n"
        f"UP calls: {len(up_rows)} | +5¢: {hit5_rate(up_rows)}\n"
        f"DOWN calls: {len(down_rows)} | +5¢: {hit5_rate(down_rows)}\n\n"
        "🧠 BY SETUP (+5¢)\n"
        + ("\n".join(setup_lines) if setup_lines else "—")
        + "\n\n💵 BY ENTRY PRICE (+5¢)\n"
        + ("\n".join(price_lines) if price_lines else "—")
        + "\n\n⏱ BY TIME LEFT (+5¢)\n"
        + ("\n".join(time_lines) if time_lines else "—")
        + "\n\n"
        "More calls = more useful these numbers become."
    )

    send_telegram(message)


# =========================================================
# TELEGRAM
# =========================================================

def telegram_url(method):
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


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

        updates = response.json().get("result", [])

        if updates:
            telegram_offset = updates[-1]["update_id"] + 1

    except Exception as e:
        print("Telegram initialization error:", e)


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

        updates = response.json().get("result", [])

        for update in updates:
            telegram_offset = update["update_id"] + 1

            message = update.get("message", {})

            chat_id = str(
                message.get("chat", {}).get("id", "")
            )

            if chat_id != str(TELEGRAM_CHAT_ID):
                continue

            text = message.get("text", "").strip().lower()

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

            elif text.startswith("/stats"):
                send_stats()

            elif text.startswith("/last"):
                send_last()

    except Exception as e:
        print("Telegram command error:", e)


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
            "I'm monitoring BTC...no current analysis available yet."
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

    active.sort(key=lambda item: item[0])

    return active[0][1]



def get_contract_target(market):
    """Return Kalshi's BTC threshold/target for this market.

    Prefer structured strike fields from the API. Fall back to parsing
    human-readable market text only if Kalshi does not populate them.
    """
    candidates = [
        market.get("floor_strike"),
        market.get("functional_strike"),
    ]

    for value in candidates:
        if value is None:
            continue
        try:
            return float(str(value).replace(",", "").replace("$", "").strip())
        except (TypeError, ValueError):
            pass

    # Defensive fallback for markets where the threshold is only present
    # in the title/subtitle. We do NOT infer it from the ticker.
    import re
    for field in ("title", "subtitle", "yes_sub_title", "no_sub_title"):
        text = str(market.get(field) or "")
        matches = re.findall(r"\$?([0-9]{2,3}(?:,[0-9]{3})+(?:\.[0-9]+)?|[0-9]{4,6}(?:\.[0-9]+)?)", text)
        for match in matches:
            try:
                value = float(match.replace(",", ""))
                if 10000 <= value <= 1000000:
                    return value
            except ValueError:
                continue

    return None


def target_context(btc_price, target, direction=None):
    if target is None or btc_price is None:
        return {
            "target": target,
            "distance": None,
            "distance_pct": None,
            "position": "UNKNOWN",
            "side_status": "UNKNOWN"
        }

    distance = float(btc_price) - float(target)
    distance_pct = (distance / float(target)) * 100 if target else None
    position = "ABOVE" if distance > 0 else "BELOW" if distance < 0 else "AT"

    side_status = "UNKNOWN"
    if direction == "UP":
        side_status = "AHEAD" if distance >= 0 else "BEHIND"
    elif direction == "DOWN":
        side_status = "AHEAD" if distance < 0 else "BEHIND"

    return {
        "target": float(target),
        "distance": distance,
        "distance_pct": distance_pct,
        "position": position,
        "side_status": side_status
    }

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
        raise Exception(f"Kraken error: {data['error']}")

    result = data.get("result", {})

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
# BUILD COMPLETED 3M CANDLES
# =========================================================

def build_3m_candles(one_minute):
    grouped = {}

    for candle in one_minute:
        bucket = candle["time"] - candle["time"] % 180

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
        market["close_time"].replace("Z", "+00:00")
    )

    return max(
        0,
        int(
            (
                close_time
                - datetime.now(timezone.utc)
            ).total_seconds()
        )
    )


def pct_distance(price, level):
    if not level:
        return 999

    return abs(price - level) / level * 100


def clear_retest():
    retest_state["direction"] = None
    retest_state["level"] = None
    retest_state["armed_at"] = None


def arm_retest(direction, level, candle_time):
    retest_state["direction"] = direction
    retest_state["level"] = level
    retest_state["armed_at"] = candle_time


# =========================================================
# 15M STRUCTURE
# =========================================================

def get_structure():
    candles = get_kraken_candles(15)

    if len(candles) < STRUCTURE_CANDLES + 2:
        return None

    completed = candles[:-1]
    recent = completed[-STRUCTURE_CANDLES:]

    support = min(candle["low"] for candle in recent)
    resistance = max(candle["high"] for candle in recent)

    closes = [candle["close"] for candle in recent]

    midpoint = len(closes) // 2

    first_avg = (
        sum(closes[:midpoint])
        / len(closes[:midpoint])
    )

    second_avg = (
        sum(closes[midpoint:])
        / len(closes[midpoint:])
    )

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
# 3M PRICE ACTION + STATEFUL RETEST
# =========================================================

def analyze_price_action(support, resistance):
    one_minute = get_kraken_candles(1)

    candles = build_3m_candles(one_minute)

    if len(candles) < 3:
        return None

    previous = candles[-2]
    current = candles[-1]

    price = current["close"]

    green = current["close"] > current["open"]
    red = current["close"] < current["open"]

    near_support = (
        pct_distance(price, support)
        <= LEVEL_TOLERANCE_PCT
    )

    near_resistance = (
        pct_distance(price, resistance)
        <= LEVEL_TOLERANCE_PCT
    )

    # V4 wording/state helpers.
    above_support = price >= support
    below_support = price < support
    below_resistance = price <= resistance
    above_resistance = price > resistance

    support_rejection = (
        current["low"]
        <= support * (1 + LEVEL_TOLERANCE_PCT / 100)
        and current["close"] > support
        and green
    )

    resistance_rejection = (
        current["high"]
        >= resistance * (1 - LEVEL_TOLERANCE_PCT / 100)
        and current["close"] < resistance
        and red
    )

    breakout_up = (
        previous["close"] <= resistance
        and current["close"]
        > resistance * (1 + BREAKOUT_BUFFER_PCT / 100)
    )

    breakout_down = (
        previous["close"] >= support
        and current["close"]
        < support * (1 - BREAKOUT_BUFFER_PCT / 100)
    )

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

    armed_direction = retest_state["direction"]
    armed_level = retest_state["level"]
    armed_at = retest_state["armed_at"]

    if armed_direction and armed_level and armed_at:
        candles_since = int(
            (current["time"] - armed_at) / 180
        )

        if candles_since > RETEST_MAX_CANDLES:
            clear_retest()

        elif candles_since >= 1 and armed_direction == "UP":
            touched = (
                current["low"]
                <= armed_level
                * (1 + LEVEL_TOLERANCE_PCT / 100)
            )

            held = (
                current["close"] > armed_level
                and green
            )

            if touched and held:
                retest_up = True
                clear_retest()

            elif (
                current["close"]
                < armed_level
                * (1 - BREAKOUT_BUFFER_PCT / 100)
            ):
                clear_retest()

        elif candles_since >= 1 and armed_direction == "DOWN":
            touched = (
                current["high"]
                >= armed_level
                * (1 - LEVEL_TOLERANCE_PCT / 100)
            )

            failed = (
                current["close"] < armed_level
                and red
            )

            if touched and failed:
                retest_down = True
                clear_retest()

            elif (
                current["close"]
                > armed_level
                * (1 + BREAKOUT_BUFFER_PCT / 100)
            ):
                clear_retest()

    return {
        "price": price,
        "candle_time": current["time"],

        "near_support": near_support,
        "near_resistance": near_resistance,

        "above_support": above_support,
        "below_support": below_support,
        "below_resistance": below_resistance,
        "above_resistance": above_resistance,

        "support_rejection": support_rejection,
        "resistance_rejection": resistance_rejection,

        "breakout_up": breakout_up,
        "breakout_down": breakout_down,

        "retest_up": retest_up,
        "retest_down": retest_down,

        "armed_direction": retest_state["direction"]
    }


# =========================================================
# FINAL ACTION DECISION
# =========================================================

def analyze_market(market):
    yes_bid = money(market.get("yes_bid_dollars"))
    yes_ask = money(market.get("yes_ask_dollars"))
    no_bid = money(market.get("no_bid_dollars"))
    no_ask = money(market.get("no_ask_dollars"))

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

    contract_target = get_contract_target(market)

    action = "🗣️ WAIT 🟡"
    setup = "NONE"
    reason = "BTC is between key levels."

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
            "completed 3m candle bounced back up."
        )

    elif pa["resistance_rejection"]:
        direction = "DOWN"
        confirmed = True
        setup = "RESISTANCE REJECTION"

        reason = (
            "BTC tested 15m resistance and the "
            "completed 3m candle turned back down."
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

    elif pa["armed_direction"] == "UP":
        setup = "UP RETEST ARMED"

        reason = (
            "The resistance breakout is still "
            "armed...waiting for BTC to retest "
            "and stay above the level."
        )

    elif pa["armed_direction"] == "DOWN":
        setup = "DOWN RETEST ARMED"

        reason = (
            "The support break is still armed..."
            "waiting for BTC to retest and turn "
            "back down."
        )

    # V4 FIX:
    # Don't call BTC "near support" as if it is still above
    # support when the completed 3m close is already below it.
    elif pa["near_support"] and pa["below_support"]:
        setup = "SUPPORT BREAKING"

        reason = (
            "BTC is below 15m support, but the "
            "break is not confirmed enough for "
            "an entry yet...WAIT."
        )

    elif pa["near_support"] and pa["above_support"]:
        setup = "WATCH SUPPORT"

        reason = (
            "BTC is just above 15m support..."
            "waiting to see if it bounces or breaks."
        )

    elif pa["near_resistance"] and pa["above_resistance"]:
        setup = "RESISTANCE BREAKING"

        reason = (
            "BTC is above 15m resistance, but the "
            "break is not confirmed enough for "
            "an entry yet...WAIT."
        )

    elif pa["near_resistance"] and pa["below_resistance"]:
        setup = "WATCH RESISTANCE"

        reason = (
            "BTC is just below 15m resistance..."
            "waiting to see if it breaks through "
            "or turns back down."
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

    target_read = target_context(pa["price"], contract_target, direction)

    # The target is core contract context. A confirmed BTC setup can still
    # be useful for a scalp on either side, so V4.1 does not invent an
    # arbitrary distance cutoff. Instead it explicitly tells us whether
    # the suggested side is currently ahead or still has to cross the target.
    if confirmed and contract_target is not None:
        gap = abs(target_read["distance"])
        if target_read["side_status"] == "AHEAD":
            reason += (
                f" 🎯 {direction} is currently AHEAD of the "
                f"${contract_target:,.2f} target by ${gap:,.2f}."
            )
        else:
            reason += (
                f" 🎯 {direction} is currently BEHIND the "
                f"${contract_target:,.2f} target by ${gap:,.2f}; "
                "BTC still needs to move toward/cross the target."
            )

    if confirmed:
        if entry_price is None:
            action = "🗣️ DON'T TOUCH THIS SHIT 🚫"

            reason += (
                " Kalshi doesn't currently have "
                "a usable ask price."
            )

        elif remaining < MIN_SECONDS_FOR_ENTRY:
            action = "🗣️ DON'T TOUCH THIS SHIT 🚫"

            reason += (
                " There's less than a minute left..."
                "too late for a fresh entry signal."
            )

        elif entry_price >= HARD_SKIP_PRICE:
            action = "🗣️ DON'T TOUCH THIS SHIT 🚫"

            reason += (
                f" {direction} is already "
                f"${entry_price:.2f}...almost all "
                "the contract value is priced in."
            )

        else:
            if direction == "UP":
                action = "🗣️ ENTER UP 🟢"
            else:
                action = "🗣️ ENTER DOWN 🔴"

            if entry_price >= SCALP_PRICE:
                scalp = True

                reason += (
                    f" 💰 SCALP: {direction} is "
                    f"${entry_price:.2f}, so the "
                    "remaining room is smaller."
                )

            elif remaining > MAX_SECONDS_FOR_ENTRY:
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
        action = "🗣️ DON'T TOUCH THIS SHIT 🚫"

        reason = (
            "BTC is ranging in the middle of the "
            "structure with no clean 3m setup."
        )

    return {
        "action": action,
        "setup": setup,
        "reason": reason,

        "direction": direction,
        "confirmed": confirmed,
        "scalp": scalp,

        "btc_price": pa["price"],
        "contract_target": contract_target,
        "target_distance": target_read["distance"],
        "target_distance_pct": target_read["distance_pct"],
        "target_position": target_read["position"],
        "target_side_status": target_read["side_status"],

        "support": structure["support"],
        "resistance": structure["resistance"],
        "trend": structure["trend"],

        "yes_bid": yes_bid,
        "yes_ask": yes_ask,

        "no_bid": no_bid,
        "no_ask": no_ask,

        "entry_price": entry_price,
        "seconds_left": remaining,
        "candle_time": pa["candle_time"]
    }


# =========================================================
# MESSAGE
# =========================================================

def target_line(analysis):
    target = analysis.get("contract_target")
    distance = analysis.get("target_distance")
    position = analysis.get("target_position", "UNKNOWN")
    side_status = analysis.get("target_side_status", "UNKNOWN")

    if target is None or distance is None:
        return "Target: unavailable from Kalshi API ⚠️"

    signed = f"{distance:+,.2f}"
    status = ""
    if analysis.get("direction"):
        status = f" | {analysis['direction']} is {side_status}"

    return (
        f"Target: ${target:,.2f}\n"
        f"BTC is {position} target by ${abs(distance):,.2f} "
        f"({signed} vs target){status}"
    )


def build_message(market, analysis):
    seconds = analysis["seconds_left"]

    minutes = seconds // 60
    secs = seconds % 60

    ticker = market.get("ticker", "Unknown")

    scalp_line = ""

    if analysis["scalp"]:
        scalp_line = (
            "\n💰 SCALP SETUP — smaller profit window"
        )

    entry_line = ""

    if (
        analysis["direction"]
        and analysis["entry_price"] is not None
    ):
        entry_line = (
            f"\nSuggested side: {analysis['direction']}"
            f"\nCurrent ask: ${analysis['entry_price']:.2f}"
        )

    return f"""
{analysis['action']}

Setup: {analysis['setup']}
{analysis['reason']}
{scalp_line}

BTC: ${analysis['btc_price']:,.2f}

🎯 CONTRACT TARGET
{target_line(analysis)}

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

    print("Kate's BTC Watcher V4.1 starting...")
    print("Stats database:", DB_PATH)

    init_db()
    load_logged_signal_keys()
    initialize_telegram_offset()

    send_telegram(
        "🔥 KATE'S BTC WATCHER V4.1 IS ONLINE.\n\n"
        "Commands:\n"
        "/start — turn alerts ON\n"
        "/stop — turn alerts OFF\n"
        "/status — current read\n"
        "/last — last ENTER call\n"
        "/stats — performance stats\n\n"
        "📊 ENTER calls are being tracked.\n🎯 Contract target is now being read.\n"
        "I'm ready 👀"
    )

    while True:
        try:
            # Commands always work, even if trading alerts are OFF.
            check_telegram_commands()

            if not watcher_enabled:
                time.sleep(CHECK_EVERY)
                continue

            market = get_live_btc_market()

            if not market:
                print("No open BTC 15-minute market.")
                time.sleep(CHECK_EVERY)
                continue

            ticker = market.get("ticker")

            # Keep already-recorded ENTER calls updated every loop,
            # even if there is no new Telegram alert.
            update_open_signals(market)
            finalize_old_signals(ticker)

            analysis = analyze_market(market)

            if not analysis:
                time.sleep(CHECK_EVERY)
                continue

            last_analysis = analysis

            # ---------------------------------
            # NEW CONTRACT
            # ---------------------------------

            if ticker != last_market:
                last_market = ticker
                last_alert = None

                # Don't carry a retest setup into
                # another Kalshi contract.
                clear_retest()

                send_telegram(
                    "🆕 NEW BTC 15-MIN CONTRACT\n\n"
                    f"{ticker}\n\n"
                    "I'm watching 👀"
                )

            # ---------------------------------
            # RECORD EVERY REAL ENTER CALL
            # ---------------------------------

            record_enter_signal(
                ticker,
                analysis
            )

            # ---------------------------------
            # ALERT WHEN MEANINGFUL STATE CHANGES
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
            time.sleep(15)


if __name__ == "__main__":
    main()
