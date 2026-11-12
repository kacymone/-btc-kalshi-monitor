import os
import re
import time
import sqlite3
import requests
from datetime import datetime, timezone

# -----------------------------
# SETTINGS
# -----------------------------

KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
SERIES = "KXBTC15M"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

CHECK_EVERY = 15

LEVEL_LOOKBACK = 8
LEVEL_TOLERANCE_PCT = 0.15
BREAK_BUFFER_PCT = 0.03

SCALP_PRICE = 0.88
HARD_SKIP_PRICE = 0.97

MIN_SECONDS_FOR_ENTRY = 60
MAX_SECONDS_FOR_ENTRY = 13 * 60

RETEST_MAX_CANDLES = 4

DATA_DIR = os.environ.get("DATA_DIR", "/data")


# -----------------------------
# STATE
# -----------------------------

last_alert = None
last_market = None
last_analysis = None

watcher_enabled = True
telegram_offset = 0

retest_state = {
    "direction": None,
    "level": None,
    "armed_at": None,
}

logged_signal_keys = set()


# -----------------------------
# DATABASE
# -----------------------------

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
        return os.path.join(
            os.getcwd(),
            "kate_btc_watcher.db"
        )


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

        conn.commit()


# -----------------------------
# ENTER SIGNAL TRACKING
# -----------------------------

def make_signal_key(ticker, analysis):
    return (
        f"{ticker}|"
        f"{analysis.get('direction')}|"
        f"{analysis.get('setup')}|"
        f"{analysis.get('candle_time')}"
    )


def record_enter_signal(ticker, analysis):
    if not analysis["action"].startswith("🗣️ ENTER"):
        return

    signal_key = make_signal_key(
        ticker,
        analysis
    )

    if signal_key in logged_signal_keys:
        return

    side = analysis["direction"]
    entry_price = analysis["entry_price"]

    if side == "UP":
        exit_bid = analysis.get("yes_bid")
    else:
        exit_bid = analysis.get("no_bid")

    if entry_price is None:
        return

    now = int(time.time())

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
                    last_updated,
                    finalized
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, 0
                )
            """, (
                signal_key,
                ticker,
                side,
                analysis["setup"],
                analysis["trend"],
                entry_price,
                analysis["btc_price"],
                analysis["support"],
                analysis["resistance"],
                analysis["seconds_left"],
                analysis["candle_time"],
                now,
                exit_bid,
                exit_bid,
                exit_bid,
                now
            ))

            conn.commit()

        logged_signal_keys.add(signal_key)

    except Exception as e:
        print(
            "Record ENTER error:",
            e
        )


def side_bid_for_signal(
    market,
    side
):
    if side == "UP":
        return market.get(
            "yes_bid_dollars"
        )

    return market.get(
        "no_bid_dollars"
    )


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
                current_price = (
                    side_bid_for_signal(
                        market,
                        row["side"]
                    )
                )

                if current_price is None:
                    continue

                current_price = float(
                    current_price
                )

                entry_price = float(
                    row["entry_price"]
                )

                best_price = (
                    float(row["best_price"])
                    if row["best_price"]
                    is not None
                    else current_price
                )

                worst_price = (
                    float(row["worst_price"])
                    if row["worst_price"]
                    is not None
                    else current_price
                )

                best_price = max(
                    best_price,
                    current_price
                )

                worst_price = min(
                    worst_price,
                    current_price
                )

                age = (
                    now
                    - int(row["created_at"])
                )

                price_1m = row["price_1m"]
                price_3m = row["price_3m"]
                price_5m = row["price_5m"]

                if (
                    age >= 60
                    and price_1m is None
                ):
                    price_1m = current_price

                if (
                    age >= 180
                    and price_3m is None
                ):
                    price_3m = current_price

                if (
                    age >= 300
                    and price_5m is None
                ):
                    price_5m = current_price

                hit_3c = int(
                    row["hit_3c"] or 0
                )

                hit_5c = int(
                    row["hit_5c"] or 0
                )

                hit_10c = int(
                    row["hit_10c"] or 0
                )

                if (
                    best_price
                    >= entry_price + 0.03
                ):
                    hit_3c = 1

                if (
                    best_price
                    >= entry_price + 0.05
                ):
                    hit_5c = 1

                if (
                    best_price
                    >= entry_price + 0.10
                ):
                    hit_10c = 1

                conn.execute("""
                    UPDATE signals
                    SET
                        best_price = ?,
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
                    best_price,
                    worst_price,
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
        print(
            "Update signals error:",
            e
        )


def finalize_old_signals(
    current_ticker
):
    try:
        with db_connect() as conn:
            conn.execute("""
                UPDATE signals
                SET finalized = 1
                WHERE finalized = 0
                  AND ticker != ?
            """, (current_ticker,))

            conn.commit()

    except Exception as e:
        print(
            "Finalize signals error:",
            e
        )


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
        print(
            "Load signal keys error:",
            e
        )


# -----------------------------
# STATS HELPERS
# -----------------------------

def cents(value):
    if value is None:
        return "—"

    return f"{float(value) * 100:.0f}¢"


def signed_cents(value):
    if value is None:
        return "—"

    value = float(value)

    sign = "+" if value >= 0 else ""

    return (
        f"{sign}{value * 100:.1f}¢"
    )


def pct_text(
    numerator,
    denominator
):
    if not denominator:
        return "—"

    return (
        f"{numerator / denominator * 100:.0f}%"
    )


def price_bucket(price):
    if price < 0.50:
        return "<50¢"

    if price < 0.70:
        return "50–69¢"

    if price < 0.80:
        return "70–79¢"

    if price < 0.90:
        return "80–89¢"

    return "90¢+"


def time_bucket(seconds_left):
    if seconds_left is None:
        return "Unknown"

    if seconds_left < 180:
        return "<3m"

    if seconds_left < 360:
        return "3–5m"

    if seconds_left < 540:
        return "6–8m"

    if seconds_left < 720:
        return "9–11m"

    return "12m+"


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
        print(
            "Last signal error:",
            e
        )

        return None


def send_last():
    row = get_last_signal()

    if not row:
        send_telegram(
            "📭 NO ENTER CALLS LOGGED YET.\n\n"
            "I'm waiting on my first V4 ENTER."
        )
        return

    entry = float(
        row["entry_price"]
    )

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

    send_telegram(
        "🧾 LAST ENTER CALL\n\n"
        f"Side: {row['side']}\n"
        f"Setup: {row['setup']}\n"
        f"Trend: {row['trend']}\n\n"
        f"Entry ask: {cents(entry)}\n"
        f"Best sellable bid: {cents(best)}\n"
        f"Best move: {signed_cents(best_move)}\n"
        f"Worst sellable bid: {cents(worst)}\n"
        f"Worst move: {signed_cents(worst_move)}\n\n"
        f"After ~1m: {cents(row['price_1m'])}\n"
        f"After ~3m: {cents(row['price_3m'])}\n"
        f"After ~5m: {cents(row['price_5m'])}\n\n"
        f"+3¢ available: {'YES' if row['hit_3c'] else 'NO'}\n"
        f"+5¢ available: {'YES' if row['hit_5c'] else 'NO'}\n"
        f"+10¢ available: {'YES' if row['hit_10c'] else 'NO'}"
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
        print(
            "Stats error:",
            e
        )

        send_telegram(
            "⚠️ I couldn't read the stats database."
        )
        return

    total = len(rows)

    if total == 0:
        send_telegram(
            "📊 V4.2 STATS\n\n"
            "No ENTER calls logged yet.\n\n"
            "🧠 Black-box WAIT data is still "
            "being recorded in the background."
        )
        return

    hit_3 = sum(
        int(row["hit_3c"] or 0)
        for row in rows
    )

    hit_5 = sum(
        int(row["hit_5c"] or 0)
        for row in rows
    )

    hit_10 = sum(
        int(row["hit_10c"] or 0)
        for row in rows
    )

    up_rows = [
        row
        for row in rows
        if row["side"] == "UP"
    ]

    down_rows = [
        row
        for row in rows
        if row["side"] == "DOWN"
    ]

    up_hit_3 = sum(
        int(row["hit_3c"] or 0)
        for row in up_rows
    )

    down_hit_3 = sum(
        int(row["hit_3c"] or 0)
        for row in down_rows
    )

    lines = [
        "📊 V4.2 ENTER STATS",
        "",
        f"Total ENTER calls: {total}",
        "",
        "💰 SCALP OPPORTUNITY",
        f"+3¢ available: {hit_3}/{total} "
        f"({pct_text(hit_3, total)})",
        f"+5¢ available: {hit_5}/{total} "
        f"({pct_text(hit_5, total)})",
        f"+10¢ available: {hit_10}/{total} "
        f"({pct_text(hit_10, total)})",
        "",
        "⬆️⬇️ BY SIDE",
        f"UP: {len(up_rows)} calls | "
        f"+3¢: {up_hit_3}/{len(up_rows)} "
        f"({pct_text(up_hit_3, len(up_rows))})",
        f"DOWN: {len(down_rows)} calls | "
        f"+3¢: {down_hit_3}/{len(down_rows)} "
        f"({pct_text(down_hit_3, len(down_rows))})",
    ]

    setup_groups = {}

    price_groups = {}

    time_groups = {}

    for row in rows:
        setup = (
            row["setup"]
            or "UNKNOWN"
        )

        if setup not in setup_groups:
            setup_groups[setup] = {
                "total": 0,
                "hit3": 0
            }

        setup_groups[setup]["total"] += 1

        setup_groups[setup]["hit3"] += int(
            row["hit_3c"] or 0
        )

        entry = float(
            row["entry_price"]
        )

        p_bucket = price_bucket(
            entry
        )

        if p_bucket not in price_groups:
            price_groups[p_bucket] = {
                "total": 0,
                "hit3": 0
            }

        price_groups[p_bucket][
            "total"
        ] += 1

        price_groups[p_bucket][
            "hit3"
        ] += int(
            row["hit_3c"] or 0
        )

        t_bucket = time_bucket(
            row["seconds_left"]
        )

        if t_bucket not in time_groups:
            time_groups[t_bucket] = {
                "total": 0,
                "hit3": 0
            }

        time_groups[t_bucket][
            "total"
        ] += 1

        time_groups[t_bucket][
            "hit3"
        ] += int(
            row["hit_3c"] or 0
        )

    lines.extend([
        "",
        "🧩 BY SETUP"
    ])

    for setup, data in sorted(
        setup_groups.items()
    ):
        lines.append(
            f"{setup}: "
            f"{data['hit3']}/"
            f"{data['total']} hit +3¢ "
            f"({pct_text(data['hit3'], data['total'])})"
        )

    lines.extend([
        "",
        "💵 BY ENTRY PRICE"
    ])

    price_order = [
        "<50¢",
        "50–69¢",
        "70–79¢",
        "80–89¢",
        "90¢+"
    ]

    for bucket in price_order:
        if bucket not in price_groups:
            continue

        data = price_groups[bucket]

        lines.append(
            f"{bucket}: "
            f"{data['hit3']}/"
            f"{data['total']} hit +3¢ "
            f"({pct_text(data['hit3'], data['total'])})"
        )

    lines.extend([
        "",
        "⏱ BY TIME LEFT"
    ])

    time_order = [
        "<3m",
        "3–5m",
        "6–8m",
        "9–11m",
        "12m+",
        "Unknown"
    ]

    for bucket in time_order:
        if bucket not in time_groups:
            continue

        data = time_groups[bucket]

        lines.append(
            f"{bucket}: "
            f"{data['hit3']}/"
            f"{data['total']} hit +3¢ "
            f"({pct_text(data['hit3'], data['total'])})"
        )

    lines.extend([
        "",
        "🧠 WAIT states + target movement "
        "are also being saved for later analysis."
    ])

    send_telegram(
        "\n".join(lines)
    )


# -----------------------------
# TELEGRAM
# -----------------------------

def send_telegram(text):
    if (
        not TELEGRAM_TOKEN
        or not TELEGRAM_CHAT_ID
    ):
        print(
            "Telegram is not configured."
        )
        return

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/sendMessage"
    )

    try:
        requests.post(
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text
            },
            timeout=10
        )

    except Exception as e:
        print(
            "Telegram send error:",
            e
        )


def send_status():
    if not watcher_enabled:
        send_telegram(
            "🔴 WATCHER STATUS: OFF\n\n"
            "Send /start when you want me "
            "watching again."
        )
        return

    if (
        last_market is None
        or last_analysis is None
    ):
        send_telegram(
            "🟡 WATCHER STATUS: ON\n\n"
            "I'm running, but I don't have "
            "a complete market read yet."
        )
        return

    remaining = last_analysis.get(
        "seconds_left"
    )

    if remaining is None:
        time_text = "N/A"
    else:
        mins = remaining // 60
        secs = remaining % 60

        time_text = (
            f"{mins}:{secs:02d}"
        )

    target = last_analysis.get(
        "contract_target"
    )

    target_text = (
        f"${target:,.2f}"
        if target is not None
        else "N/A"
    )

    distance = last_analysis.get(
        "target_distance"
    )

    if distance is None:
        distance_text = "N/A"

    elif distance >= 0:
        distance_text = (
            f"${abs(distance):,.2f} ABOVE"
        )

    else:
        distance_text = (
            f"${abs(distance):,.2f} BELOW"
        )

    send_telegram(
        "🟢 WATCHER STATUS: ON\n\n"
        f"{last_analysis['action']}\n"
        f"Setup: {last_analysis['setup']}\n\n"
        f"BTC: ${last_analysis['btc_price']:,.2f}\n"
        f"Target: {target_text}\n"
        f"BTC vs target: {distance_text}\n\n"
        f"UP ask: {cents(last_analysis['yes_ask'])}\n"
        f"DOWN ask: {cents(last_analysis['no_ask'])}\n"
        f"Time left: {time_text}\n\n"
        f"Contract:\n{last_market}"
    )


def get_updates():
    global telegram_offset

    if not TELEGRAM_TOKEN:
        return []

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/getUpdates"
    )

    params = {
        "timeout": 0,
        "offset": telegram_offset
    }

    try:
        response = requests.get(
            url,
            params=params,
            timeout=10
        )

        data = response.json()

        if not data.get("ok"):
            return []

        return data.get(
            "result",
            []
        )

    except Exception as e:
        print(
            "Telegram update error:",
            e
        )

        return []


def check_commands():
    global telegram_offset
    global watcher_enabled

    updates = get_updates()

    for update in updates:
        update_id = update.get(
            "update_id"
        )

        if update_id is not None:
            telegram_offset = (
                update_id + 1
            )

        message = update.get(
            "message",
            {}
        )

        chat = message.get(
            "chat",
            {}
        )

        chat_id = str(
            chat.get("id", "")
        )

        if (
            TELEGRAM_CHAT_ID
            and chat_id
            != str(TELEGRAM_CHAT_ID)
        ):
            continue

        text = (
            message.get("text", "")
            .strip()
            .lower()
        )

        if text == "/start":
            watcher_enabled = True

            send_telegram(
                "🟢 WATCHER ON.\n\n"
                "I'm watching BTC again."
            )

        elif text == "/stop":
            watcher_enabled = False

            send_telegram(
                "🔴 WATCHER OFF.\n\n"
                "No market alerts until "
                "you send /start."
            )

        elif text == "/status":
            send_status()

        elif text == "/last":
            send_last()

        elif text == "/stats":
            send_stats()


# -----------------------------
# KALSHI
# -----------------------------

def get_live_market():
    url = (
        f"{KALSHI_BASE}/markets"
    )

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

    markets = response.json().get(
        "markets",
        []
    )

    if not markets:
        return None

    now = datetime.now(
        timezone.utc
    )

    candidates = []

    for market in markets:
        close_time = market.get(
            "close_time"
        )

        if not close_time:
            continue

        try:
            close_dt = (
                datetime.fromisoformat(
                    close_time.replace(
                        "Z",
                        "+00:00"
                    )
                )
            )

        except Exception:
            continue

        seconds = (
            close_dt - now
        ).total_seconds()

        if seconds > 0:
            candidates.append(
                (
                    seconds,
                    market
                )
            )

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: x[0]
    )

    return candidates[0][1]


def get_seconds_left(market):
    close_time = market.get(
        "close_time"
    )

    if not close_time:
        return None

    try:
        close_dt = (
            datetime.fromisoformat(
                close_time.replace(
                    "Z",
                    "+00:00"
                )
            )
        )

        seconds = int(
            (
                close_dt
                - datetime.now(
                    timezone.utc
                )
            ).total_seconds()
        )

        return max(
            0,
            seconds
        )

    except Exception:
        return None


def get_contract_target(market):
    candidate_fields = [
        "functional_strike",
        "floor_strike",
        "strike",
        "cap_strike"
    ]

    for field in candidate_fields:
        value = market.get(field)

        if value is None:
            continue

        try:
            value = float(value)

            if value > 1000:
                return value

        except Exception:
            pass

    text_fields = [
        "subtitle",
        "title",
        "yes_sub_title",
        "rules_primary"
    ]

    for field in text_fields:
        text = market.get(
            field
        )

        if not text:
            continue

        matches = re.findall(
            r"\$?\s*([0-9]{2,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)",
            str(text)
        )

        for match in matches:
            try:
                value = float(
                    match.replace(
                        ",",
                        ""
                    )
                )

                if value > 1000:
                    return value

            except Exception:
                continue

    return None


# -----------------------------
# KRAKEN
# -----------------------------

def get_kraken_candles(
    interval=1
):
    url = (
        "https://api.kraken.com/"
        "0/public/OHLC"
    )

    params = {
        "pair": "XBTUSD",
        "interval": interval
    }

    response = requests.get(
        url,
        params=params,
        timeout=10
    )

    response.raise_for_status()

    payload = response.json()

    if payload.get("error"):
        raise RuntimeError(
            str(payload["error"])
        )

    result = payload.get(
        "result",
        {}
    )

    pair_key = next(
        (
            key
            for key in result
            if key != "last"
        ),
        None
    )

    if pair_key is None:
        return []

    candles = []

    for row in result[pair_key]:
        candles.append({
            "time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[6])
        })

    return candles


def completed_1m_candles():
    candles = get_kraken_candles(
        interval=1
    )

    if len(candles) <= 1:
        return []

    return candles[:-1]


def build_completed_3m_candles(
    one_minute
):
    grouped = {}

    for candle in one_minute:
        bucket = (
            candle["time"]
            - candle["time"] % 180
        )

        grouped.setdefault(
            bucket,
            []
        ).append(candle)

    result = []

    for bucket in sorted(
        grouped.keys()
    ):
        rows = sorted(
            grouped[bucket],
            key=lambda x: x["time"]
        )

        if len(rows) != 3:
            continue

        result.append({
            "time": bucket,
            "open": rows[0]["open"],
            "high": max(
                row["high"]
                for row in rows
            ),
            "low": min(
                row["low"]
                for row in rows
            ),
            "close": rows[-1]["close"],
            "volume": sum(
                row["volume"]
                for row in rows
            )
        })

    return result


def completed_15m_candles():
    candles = get_kraken_candles(
        interval=15
    )

    if len(candles) <= 1:
        return []

    return candles[:-1]
    # -----------------------------
# 15M STRUCTURE
# -----------------------------

def get_structure():
    candles = completed_15m_candles()

    if len(candles) < LEVEL_LOOKBACK:
        return None

    recent = candles[
        -LEVEL_LOOKBACK:
    ]

    support = min(
        candle["low"]
        for candle in recent
    )

    resistance = max(
        candle["high"]
        for candle in recent
    )

    midpoint = (
        len(recent) // 2
    )

    first_half = recent[
        :midpoint
    ]

    second_half = recent[
        midpoint:
    ]

    first_avg = sum(
        candle["close"]
        for candle in first_half
    ) / len(first_half)

    second_avg = sum(
        candle["close"]
        for candle in second_half
    ) / len(second_half)

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


# -----------------------------
# RETEST MEMORY
# -----------------------------

def clear_retest():
    retest_state["direction"] = None
    retest_state["level"] = None
    retest_state["armed_at"] = None


def arm_retest(
    direction,
    level,
    candle_time
):
    retest_state["direction"] = (
        direction
    )

    retest_state["level"] = (
        level
    )

    retest_state["armed_at"] = (
        candle_time
    )


# -----------------------------
# 3M PRICE ACTION
# -----------------------------

def analyze_price_action(
    support,
    resistance
):
    one_minute = (
        completed_1m_candles()
    )

    candles = (
        build_completed_3m_candles(
            one_minute
        )
    )

    if len(candles) < 2:
        return None

    previous = candles[-2]
    current = candles[-1]

    price = current["close"]

    support_tolerance = (
        support
        * LEVEL_TOLERANCE_PCT
        / 100
    )

    resistance_tolerance = (
        resistance
        * LEVEL_TOLERANCE_PCT
        / 100
    )

    break_support = (
        support
        * BREAK_BUFFER_PCT
        / 100
    )

    break_resistance = (
        resistance
        * BREAK_BUFFER_PCT
        / 100
    )

    near_support = (
        abs(
            price - support
        )
        <= support_tolerance
    )

    near_resistance = (
        abs(
            price - resistance
        )
        <= resistance_tolerance
    )

    green = (
        current["close"]
        > current["open"]
    )

    red = (
        current["close"]
        < current["open"]
    )

    support_rejection = (
        current["low"]
        <= support + support_tolerance
        and current["close"] > support
        and green
    )

    resistance_rejection = (
        current["high"]
        >= (
            resistance
            - resistance_tolerance
        )
        and current["close"]
        < resistance
        and red
    )

    breakout_up = (
        current["close"]
        > (
            resistance
            + break_resistance
        )
        and previous["close"]
        <= (
            resistance
            + break_resistance
        )
    )

    breakout_down = (
        current["close"]
        < (
            support
            - break_support
        )
        and previous["close"]
        >= (
            support
            - break_support
        )
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
        and armed_level is not None
        and armed_at is not None
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
                <= (
                    armed_level
                    + (
                        armed_level
                        * LEVEL_TOLERANCE_PCT
                        / 100
                    )
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
                < (
                    armed_level
                    - (
                        armed_level
                        * BREAK_BUFFER_PCT
                        / 100
                    )
                )
            ):
                clear_retest()

        elif (
            candles_since >= 1
            and armed_direction == "DOWN"
        ):
            touched = (
                current["high"]
                >= (
                    armed_level
                    - (
                        armed_level
                        * LEVEL_TOLERANCE_PCT
                        / 100
                    )
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
                > (
                    armed_level
                    + (
                        armed_level
                        * BREAK_BUFFER_PCT
                        / 100
                    )
                )
            ):
                clear_retest()

    return {
        "price": price,

        "candle_time": (
            current["time"]
        ),

        "candle_open": (
            current["open"]
        ),

        "candle_high": (
            current["high"]
        ),

        "candle_low": (
            current["low"]
        ),

        "candle_close": (
            current["close"]
        ),

        "candle_color": (
            "GREEN"
            if green
            else (
                "RED"
                if red
                else "FLAT"
            )
        ),

        "near_support": (
            near_support
        ),

        "near_resistance": (
            near_resistance
        ),

        "support_rejection": (
            support_rejection
        ),

        "resistance_rejection": (
            resistance_rejection
        ),

        "breakout_up": (
            breakout_up
        ),

        "breakout_down": (
            breakout_down
        ),

        "retest_up": (
            retest_up
        ),

        "retest_down": (
            retest_down
        ),

        "armed_direction": (
            retest_state["direction"]
        )
    }


# -----------------------------
# MARKET ANALYSIS
# -----------------------------

def analyze_market(market):
    structure = get_structure()

    if not structure:
        return None

    support = (
        structure["support"]
    )

    resistance = (
        structure["resistance"]
    )

    trend = (
        structure["trend"]
    )

    pa = analyze_price_action(
        support,
        resistance
    )

    if not pa:
        return None

    btc_price = pa["price"]

    yes_bid = market.get(
        "yes_bid_dollars"
    )

    yes_ask = market.get(
        "yes_ask_dollars"
    )

    no_bid = market.get(
        "no_bid_dollars"
    )

    no_ask = market.get(
        "no_ask_dollars"
    )

    for name in [
        "yes_bid",
        "yes_ask",
        "no_bid",
        "no_ask"
    ]:
        value = locals()[name]

        if value is not None:
            try:
                if name == "yes_bid":
                    yes_bid = float(value)

                elif name == "yes_ask":
                    yes_ask = float(value)

                elif name == "no_bid":
                    no_bid = float(value)

                elif name == "no_ask":
                    no_ask = float(value)

            except Exception:
                pass

    seconds_left = (
        get_seconds_left(
            market
        )
    )

    target = (
        get_contract_target(
            market
        )
    )

    target_distance = None
    target_distance_pct = None
    target_position = "UNKNOWN"

    if (
        target is not None
        and btc_price is not None
    ):
        target_distance = (
            btc_price - target
        )

        if target != 0:
            target_distance_pct = (
                target_distance
                / target
                * 100
            )

        if target_distance > 0:
            target_position = "ABOVE"

        elif target_distance < 0:
            target_position = "BELOW"

        else:
            target_position = "AT"

    action = "🗣️ WAIT 🟡"
    setup = "NONE"
    direction = None
    confirmed = False

    reason = (
        "BTC is between the key "
        "support and resistance levels."
    )

    if pa["retest_up"]:
        direction = "UP"
        confirmed = True
        setup = "RETEST HOLD"

        reason = (
            "BTC broke resistance, "
            "came back to test it, "
            "and the completed 3m candle "
            "held above it."
        )

    elif pa["retest_down"]:
        direction = "DOWN"
        confirmed = True
        setup = "RETEST REJECTION"

        reason = (
            "BTC broke support, "
            "came back to test it, "
            "and the completed 3m candle "
            "rejected the old support."
        )

    elif pa["support_rejection"]:
        direction = "UP"
        confirmed = True
        setup = "SUPPORT REJECTION"

        reason = (
            "BTC tested support and "
            "the completed 3m candle "
            "bounced back above it."
        )

    elif pa[
        "resistance_rejection"
    ]:
        direction = "DOWN"
        confirmed = True
        setup = (
            "RESISTANCE REJECTION"
        )

        reason = (
            "BTC tested resistance and "
            "the completed 3m candle "
            "turned back down."
        )

    elif pa["breakout_up"]:
        setup = "UP BREAKOUT"

        reason = (
            "BTC broke resistance. "
            "The bot is waiting for "
            "the retest confirmation."
        )

    elif pa["breakout_down"]:
        setup = "DOWN BREAK"

        reason = (
            "BTC broke support. "
            "The bot is waiting for "
            "the retest confirmation."
        )

    elif (
        pa["armed_direction"]
        == "UP"
    ):
        setup = "UP RETEST ARMED"

        reason = (
            "The resistance breakout "
            "is armed. Waiting for BTC "
            "to retest and hold."
        )

    elif (
        pa["armed_direction"]
        == "DOWN"
    ):
        setup = (
            "DOWN RETEST ARMED"
        )

        reason = (
            "The support break is armed. "
            "Waiting for BTC to retest "
            "and reject."
        )

    elif (
        pa["near_support"]
        and btc_price < support
    ):
        setup = "SUPPORT BREAKING"

        reason = (
            "BTC is below support, "
            "but the break is not "
            "confirmed yet."
        )

    elif (
        pa["near_support"]
        and btc_price >= support
    ):
        setup = "WATCH SUPPORT"

        reason = (
            "BTC is near support, "
            "but there is not a completed "
            "3m rejection yet."
        )

    elif (
        pa["near_resistance"]
        and btc_price > resistance
    ):
        setup = (
            "RESISTANCE BREAKING"
        )

        reason = (
            "BTC is above resistance, "
            "but the breakout is not "
            "confirmed yet."
        )

    elif (
        pa["near_resistance"]
        and btc_price <= resistance
    ):
        setup = (
            "WATCH RESISTANCE"
        )

        reason = (
            "BTC is near resistance, "
            "but there is not a completed "
            "3m rejection yet."
        )

    entry_price = None

    if direction == "UP":
        entry_price = yes_ask

    elif direction == "DOWN":
        entry_price = no_ask

    scalp = False

    if confirmed and direction:
        if seconds_left is None:
            action = "🗣️ WAIT 🟡"

            reason = (
                "The setup confirmed, "
                "but I cannot read the "
                "contract clock."
            )

        elif (
            seconds_left
            < MIN_SECONDS_FOR_ENTRY
        ):
            action = (
                "🗣️ DON'T TOUCH "
                "THIS SHIT 🚫"
            )

            reason = (
                "The setup confirmed "
                "with less than one minute "
                "left. Too late."
            )

        elif (
            seconds_left
            > MAX_SECONDS_FOR_ENTRY
        ):
            action = "🗣️ WAIT 🟡"

            reason = (
                "The setup confirmed, "
                "but this contract is "
                "still too early."
            )

        elif entry_price is None:
            action = "🗣️ WAIT 🟡"

            reason = (
                "The setup confirmed, "
                "but Kalshi is not giving "
                "a usable ask price."
            )

        elif (
            entry_price
            >= HARD_SKIP_PRICE
        ):
            action = (
                "🗣️ DON'T TOUCH "
                "THIS SHIT 🚫"
            )

            reason = (
                f"{direction} is already "
                f"{cents(entry_price)}. "
                "There is not enough room "
                "left for our scalp."
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

            if (
                entry_price
                >= SCALP_PRICE
            ):
                scalp = True

                reason += (
                    "\n\n💰 SCALP ONLY — "
                    "the contract is expensive, "
                    "so we're looking for "
                    "the little move."
                )

    return {
        "action": action,
        "setup": setup,
        "direction": direction,
        "confirmed": confirmed,
        "scalp": scalp,
        "reason": reason,

        "btc_price": btc_price,
        "support": support,
        "resistance": resistance,
        "trend": trend,

        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "no_bid": no_bid,
        "no_ask": no_ask,

        "entry_price": (
            entry_price
        ),

        "seconds_left": (
            seconds_left
        ),

        "contract_target": (
            target
        ),

        "target_distance": (
            target_distance
        ),

        "target_distance_pct": (
            target_distance_pct
        ),

        "target_position": (
            target_position
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
            pa[
                "resistance_rejection"
            ]
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


# -----------------------------
# BLACK-BOX SNAPSHOTS
# -----------------------------

def record_market_snapshot(
    market,
    analysis
):
    ticker = market.get(
        "ticker"
    )

    if (
        not ticker
        or not analysis
    ):
        return

    now = int(time.time())

    yes_bid = analysis.get(
        "yes_bid"
    )

    yes_ask = analysis.get(
        "yes_ask"
    )

    no_bid = analysis.get(
        "no_bid"
    )

    no_ask = analysis.get(
        "no_ask"
    )

    yes_spread = None
    no_spread = None

    if (
        yes_bid is not None
        and yes_ask is not None
    ):
        yes_spread = (
            yes_ask - yes_bid
        )

    if (
        no_bid is not None
        and no_ask is not None
    ):
        no_spread = (
            no_ask - no_bid
        )

    btc_price = analysis.get(
        "btc_price"
    )

    support = analysis.get(
        "support"
    )

    resistance = analysis.get(
        "resistance"
    )

    distance_to_support = None
    distance_to_resistance = None

    if (
        btc_price is not None
        and support is not None
    ):
        distance_to_support = (
            btc_price - support
        )

    if (
        btc_price is not None
        and resistance is not None
    ):
        distance_to_resistance = (
            resistance - btc_price
        )

    target_cross = None
    target_velocity = None

    try:
        with db_connect() as conn:
            previous = conn.execute("""
                SELECT
                    captured_at,
                    target_distance
                FROM market_snapshots
                WHERE ticker = ?
                ORDER BY
                    captured_at DESC,
                    id DESC
                LIMIT 1
            """, (
                ticker,
            )).fetchone()

            current_distance = (
                analysis.get(
                    "target_distance"
                )
            )

            if (
                previous
                and current_distance
                is not None
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
                    current_distance
                    - old_distance
                ) / elapsed

                if (
                    old_distance <= 0
                    and current_distance > 0
                ):
                    target_cross = "UP"

                elif (
                    old_distance >= 0
                    and current_distance < 0
                ):
                    target_cross = "DOWN"

            conn.execute("""
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
            """, (
                now,
                ticker,
                analysis.get(
                    "seconds_left"
                ),
                btc_price,
                analysis.get(
                    "contract_target"
                ),
                current_distance,
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
                analysis.get(
                    "trend"
                ),

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

                analysis.get(
                    "action"
                ),
                analysis.get(
                    "setup"
                ),
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
            ))

            conn.commit()

    except Exception as e:
        print(
            "Snapshot recorder error:",
            e
        )
        # -----------------------------
# TELEGRAM MESSAGE
# -----------------------------

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
        time_text = "N/A"

    else:
        minutes = (
            remaining // 60
        )

        seconds = (
            remaining % 60
        )

        time_text = (
            f"{minutes}:{seconds:02d}"
        )

    target = analysis.get(
        "contract_target"
    )

    distance = analysis.get(
        "target_distance"
    )

    position = analysis.get(
        "target_position"
    )

    if target is None:
        target_block = (
            "Target: N/A"
        )

    else:
        target_block = (
            f"Target: ${target:,.2f}"
        )

        if distance is not None:
            if distance > 0:
                target_block += (
                    f"\nBTC is "
                    f"${abs(distance):,.2f} "
                    "ABOVE target"
                )

            elif distance < 0:
                target_block += (
                    f"\nBTC is "
                    f"${abs(distance):,.2f} "
                    "BELOW target"
                )

            else:
                target_block += (
                    "\nBTC is AT target"
                )

        if position == "ABOVE":
            target_block += (
                "\n🟢 UP is currently AHEAD"
            )

        elif position == "BELOW":
            target_block += (
                "\n🔴 DOWN is currently AHEAD"
            )

    yes_bid_text = cents(
        analysis.get(
            "yes_bid"
        )
    )

    yes_ask_text = cents(
        analysis.get(
            "yes_ask"
        )
    )

    no_bid_text = cents(
        analysis.get(
            "no_bid"
        )
    )

    no_ask_text = cents(
        analysis.get(
            "no_ask"
        )
    )

    scalp_text = ""

    if analysis.get("scalp"):
        scalp_text = (
            "\n💰 SCALP MODE"
        )

    return (
        f"{analysis['action']}\n\n"
        f"Setup: {analysis['setup']}\n"
        f"{analysis['reason']}"
        f"{scalp_text}\n\n"

        f"BTC: "
        f"${analysis['btc_price']:,.2f}\n\n"

        f"🎯 CONTRACT TARGET\n"
        f"{target_block}\n\n"

        f"📊 15M STRUCTURE\n"
        f"Support: "
        f"${analysis['support']:,.2f}\n"
        f"Resistance: "
        f"${analysis['resistance']:,.2f}\n"
        f"Trend: "
        f"{analysis['trend']}\n\n"

        f"🎯 KALSHI\n"
        f"🟢 UP bid: "
        f"{yes_bid_text}\n"
        f"🟢 UP ask: "
        f"{yes_ask_text}\n"
        f"🔴 DOWN bid: "
        f"{no_bid_text}\n"
        f"🔴 DOWN ask: "
        f"{no_ask_text}\n\n"

        f"⏱ Time left: "
        f"{time_text}\n\n"

        f"Contract:\n"
        f"{ticker}\n\n"

        f"⚠️ Signal only — "
        f"no automatic trade."
    )


# -----------------------------
# ALERT KEY
# -----------------------------

def make_alert_key(
    ticker,
    analysis
):
    action = analysis.get(
        "action"
    )

    setup = analysis.get(
        "setup"
    )

    direction = analysis.get(
        "direction"
    )

    candle_time = analysis.get(
        "candle_time"
    )

    if (
        action
        and action.startswith(
            "🗣️ ENTER"
        )
    ):
        return (
            ticker,
            action,
            setup,
            direction,
            candle_time
        )

    return (
        ticker,
        action,
        setup,
        direction
    )


# -----------------------------
# MAIN
# -----------------------------

def main():
    global last_alert
    global last_market
    global last_analysis

    print(
        "Kate's BTC Watcher "
        "V4.2 starting..."
    )

    init_db()
    load_logged_signal_keys()

    send_telegram(
        "🔥 KATE'S BTC WATCHER "
        "V4.2 IS ONLINE.\n\n"
        "📊 ENTER calls are being tracked.\n"
        "🎯 Contract target is being read.\n"
        "🧠 Black-box snapshots are "
        "recording every loop.\n"
        "💵 Scalp exits are measured "
        "from the sellable bid."
    )

    while True:
        loop_started = time.time()

        try:
            # Always listen for Telegram
            # commands first.
            check_commands()

            if not watcher_enabled:
                elapsed = (
                    time.time()
                    - loop_started
                )

                sleep_for = max(
                    1,
                    CHECK_EVERY - elapsed
                )

                time.sleep(
                    sleep_for
                )

                continue

            market = get_live_market()

            if not market:
                print(
                    "No open BTC "
                    "15m market found."
                )

                elapsed = (
                    time.time()
                    - loop_started
                )

                sleep_for = max(
                    1,
                    CHECK_EVERY - elapsed
                )

                time.sleep(
                    sleep_for
                )

                continue

            ticker = market.get(
                "ticker"
            )

            if not ticker:
                print(
                    "Market has no ticker."
                )

                elapsed = (
                    time.time()
                    - loop_started
                )

                sleep_for = max(
                    1,
                    CHECK_EVERY - elapsed
                )

                time.sleep(
                    sleep_for
                )

                continue

            # -------------------------
            # NEW CONTRACT
            # -------------------------

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

            # -------------------------
            # ANALYZE MARKET
            # -------------------------

            analysis = (
                analyze_market(
                    market
                )
            )

            if not analysis:
                print(
                    "Waiting for enough "
                    "BTC candle data..."
                )

                elapsed = (
                    time.time()
                    - loop_started
                )

                sleep_for = max(
                    1,
                    CHECK_EVERY - elapsed
                )

                time.sleep(
                    sleep_for
                )

                continue

            last_analysis = analysis

            # -------------------------
            # BLACK BOX
            # -------------------------

            record_market_snapshot(
                market,
                analysis
            )

            # -------------------------
            # TRACK OPEN ENTER CALLS
            # -------------------------

            update_open_signals(
                market
            )

            # -------------------------
            # RECORD NEW ENTER
            # -------------------------

            if (
                analysis["action"]
                .startswith(
                    "🗣️ ENTER"
                )
            ):
                record_enter_signal(
                    ticker,
                    analysis
                )

            # -------------------------
            # ALERT
            # -------------------------

            alert_key = (
                make_alert_key(
                    ticker,
                    analysis
                )
            )

            if (
                alert_key
                != last_alert
            ):
                send_telegram(
                    build_message(
                        market,
                        analysis
                    )
                )

                last_alert = alert_key

            # -------------------------
            # CONSOLE
            # -------------------------

            print(
                datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                "|",
                ticker,
                "|",
                analysis["action"],
                "|",
                analysis["setup"],
                "| BTC",
                analysis["btc_price"],
                "| TARGET",
                analysis.get(
                    "contract_target"
                )
            )

        except Exception as e:
            print(
                "Main loop error:",
                repr(e)
            )

        # Keep the full loop close
        # to CHECK_EVERY seconds.
        elapsed = (
            time.time()
            - loop_started
        )

        sleep_for = max(
            1,
            CHECK_EVERY - elapsed
        )

        time.sleep(
            sleep_for
        )


# -----------------------------
# RUN
# -----------------------------

if __name__ == "__main__":
    main()
