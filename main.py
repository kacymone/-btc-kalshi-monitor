import os
import time
import sqlite3
import requests
from datetime import datetime, timezone

# =========================================================
# KATE'S BTC WATCHER V4.3
# PRICE-ACTION SHADOW MODE
#
# OFFICIAL ENTER LOGIC:
#   Still uses the existing V4.2 confirmed rules.
#
# SHADOW LOGIC:
#   Watches developing price action and can issue:
#   👀 GET READY UP
#   👀 GET READY DOWN
#
# GET READY IS NOT AN ENTER SIGNAL.
# =========================================================


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

# ---------------------------------------------------------
# V4.3 SHADOW / PRICE-ACTION SETTINGS
# ---------------------------------------------------------

# GET READY is deliberately separate from ENTER.
# It is an early observation layer used to collect data.

SHADOW_MIN_SCORE = 4

# Don't bother yelling GET READY with almost no time left.
SHADOW_MIN_SECONDS = 75

# Prevent repeated GET READY alerts for the same side/setup
# from blowing up Telegram.
SHADOW_ALERT_COOLDOWN = 45

# How many recent completed 1m candles to use for
# short-term price-action recognition.
PRICE_ACTION_1M_LOOKBACK = 4

# A 1m candle whose body is at least this percentage of
# its full range is considered a relatively decisive body.
STRONG_BODY_RATIO = 0.60

# Railway Volume:
# Mount a Railway Volume at /data and the bot will
# automatically keep stats across restarts/redeploys.
DATA_DIR = os.environ.get("DATA_DIR", "/data")
FALLBACK_DATA_DIR = "."
DB_FILENAME = "kate_btc_watcher.db"


# =========================================================
# STATE
# =========================================================

last_alert = None
last_shadow_alert = None
last_shadow_alert_time = 0

last_market = None
last_analysis = None

watcher_enabled = True
telegram_offset = None

retest_state = {
    "direction": None,
    "level": None,
    "armed_at": None
}

# Prevent the same completed 3m confirmation from
# being logged twice.
logged_signal_keys = set()

# Prevent duplicate shadow formations from being inserted
# repeatedly while the same formation remains active.
active_shadow = {
    "ticker": None,
    "direction": None,
    "shadow_id": None
}


# =========================================================
# DATABASE
# =========================================================

def get_db_path():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)

        test_path = os.path.join(
            DATA_DIR,
            ".write_test"
        )

        with open(test_path, "w") as f:
            f.write("ok")

        os.remove(test_path)

        return os.path.join(
            DATA_DIR,
            DB_FILENAME
        )

    except Exception:
        return os.path.join(
            FALLBACK_DATA_DIR,
            DB_FILENAME
        )


DB_PATH = get_db_path()


def db_connect():
    conn = sqlite3.connect(
        DB_PATH,
        timeout=10
    )

    conn.row_factory = sqlite3.Row

    return conn


def init_db():
    with db_connect() as conn:

        # -------------------------------------------------
        # EXISTING V4.2 ENTER SIGNAL TABLE
        # -------------------------------------------------

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

        # -------------------------------------------------
        # EXISTING V4.2 BLACK-BOX TABLE
        # -------------------------------------------------

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

        # -------------------------------------------------
        # V4.3:
        # SAVE COMPLETED 1-MINUTE CANDLES
        #
        # V4.2 used 1m candles to build 3m candles,
        # but did not preserve the individual 1m sequence.
        # -------------------------------------------------

        conn.execute("""
            CREATE TABLE IF NOT EXISTS one_minute_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                ticker TEXT NOT NULL,
                candle_time INTEGER NOT NULL,

                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,

                candle_color TEXT,
                body_size REAL,
                candle_range REAL,
                body_ratio REAL,

                captured_at INTEGER NOT NULL,

                UNIQUE(ticker, candle_time)
            )
        """)

        # -------------------------------------------------
        # V4.3:
        # SHADOW / GET READY FORMATIONS
        #
        # These DO NOT count as official ENTER calls.
        # -------------------------------------------------

        conn.execute("""
            CREATE TABLE IF NOT EXISTS shadow_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                ticker TEXT NOT NULL,
                direction TEXT NOT NULL,

                created_at INTEGER NOT NULL,
                seconds_left INTEGER,

                btc_price REAL,

                contract_target REAL,
                target_distance REAL,
                target_velocity REAL,

                support REAL,
                resistance REAL,
                trend TEXT,

                entry_ask REAL,
                initial_bid REAL,

                score INTEGER,
                evidence TEXT,

                best_bid REAL,
                worst_bid REAL,

                hit_3c INTEGER DEFAULT 0,
                hit_5c INTEGER DEFAULT 0,
                hit_10c INTEGER DEFAULT 0,

                official_enter INTEGER DEFAULT 0,
                official_enter_price REAL,
                official_enter_at INTEGER,

                failed INTEGER DEFAULT 0,
                finalized INTEGER DEFAULT 0,

                last_bid REAL,
                last_updated INTEGER
            )
        """)

        # -------------------------------------------------
        # INDEXES
        # -------------------------------------------------

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_snapshots_ticker_time
            ON market_snapshots(
                ticker,
                captured_at
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_signals_ticker
            ON signals(ticker)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_signals_created
            ON signals(created_at)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_one_minute_ticker_time
            ON one_minute_snapshots(
                ticker,
                candle_time
            )
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_shadow_ticker
            ON shadow_signals(ticker)
        """)

        conn.execute("""
            CREATE INDEX IF NOT EXISTS
            idx_shadow_created
            ON shadow_signals(created_at)
        """)

        conn.commit()


def load_logged_signal_keys():
    global logged_signal_keys

    try:
        with db_connect() as conn:
            rows = conn.execute(
                """
                SELECT signal_key
                FROM signals
                """
            ).fetchall()

        logged_signal_keys = {
            row["signal_key"]
            for row in rows
        }

    except Exception as e:
        print(
            "Load signal keys error:",
            e
        )


# =========================================================
# BASIC FORMAT HELPERS
# =========================================================

def money(value):
    try:
        return float(value)
    except Exception:
        return None


def cents(value):
    if value is None:
        return "—"

    return (
        f"{float(value) * 100:.0f}¢"
    )


def signed_cents(delta):
    if delta is None:
        return "—"

    return (
        f"{delta * 100:+.0f}¢"
    )


def pct_text(count, total):
    if not total:
        return "—"

    return (
        f"{count / total * 100:.0f}%"
    )


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


# =========================================================
# EXISTING V4.2 ENTER SIGNAL TRACKING
# =========================================================

def make_signal_key(
    ticker,
    analysis
):
    return (
        f"{ticker}|"
        f"{analysis['direction']}|"
        f"{analysis['setup']}|"
        f"{analysis['candle_time']}"
    )


def record_enter_signal(
    ticker,
    analysis
):
    if not analysis["confirmed"]:
        return

    if not analysis["action"].startswith(
        "🗣️ ENTER"
    ):
        return

    if analysis["entry_price"] is None:
        return

    signal_key = make_signal_key(
        ticker,
        analysis
    )

    if signal_key in logged_signal_keys:
        return

    now = int(time.time())

    entry = float(
        analysis["entry_price"]
    )

    # Entry cost is ASK.
    # Immediate achievable exit is BID.
    if analysis["direction"] == "UP":
        initial_exit_bid = analysis.get(
            "yes_bid"
        )

    else:
        initial_exit_bid = analysis.get(
            "no_bid"
        )

    if initial_exit_bid is not None:
        initial_exit_bid = float(
            initial_exit_bid
        )

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
                VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?
                )
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
                initial_exit_bid,
                initial_exit_bid,
                initial_exit_bid,
                now
            ))

            conn.commit()

        logged_signal_keys.add(
            signal_key
        )

        print(
            "Logged ENTER signal:",
            signal_key
        )

    except Exception as e:
        print(
            "Record signal error:",
            e
        )


def side_bid_for_signal(
    side,
    market
):
    if side == "UP":
        return money(
            market.get(
                "yes_bid_dollars"
            )
        )

    return money(
        market.get(
            "no_bid_dollars"
        )
    )


def update_open_signals(
    market
):
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
            """, (
                ticker,
            )).fetchall()

            for row in rows:

                current_price = (
                    side_bid_for_signal(
                        row["side"],
                        market
                    )
                )

                if current_price is None:
                    continue

                entry = float(
                    row["entry_price"]
                )

                age = (
                    now
                    - int(
                        row["created_at"]
                    )
                )

                best = row["best_price"]
                worst = row["worst_price"]

                if best is None:
                    best = current_price
                else:
                    best = max(
                        float(best),
                        current_price
                    )

                if worst is None:
                    worst = current_price
                else:
                    worst = min(
                        float(worst),
                        current_price
                    )

                price_1m = row["price_1m"]
                price_3m = row["price_3m"]
                price_5m = row["price_5m"]

                if (
                    price_1m is None
                    and age >= 60
                ):
                    price_1m = current_price

                if (
                    price_3m is None
                    and age >= 180
                ):
                    price_3m = current_price

                if (
                    price_5m is None
                    and age >= 300
                ):
                    price_5m = current_price

                hit_3c = (
                    1
                    if best >= entry + 0.03
                    else row["hit_3c"]
                )

                hit_5c = (
                    1
                    if best >= entry + 0.05
                    else row["hit_5c"]
                )

                hit_10c = (
                    1
                    if best >= entry + 0.10
                    else row["hit_10c"]
                )

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
        print(
            "Update signal tracking error:",
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
                WHERE ticker != ?
                  AND finalized = 0
            """, (
                current_ticker,
            ))

            conn.execute("""
                UPDATE shadow_signals
                SET finalized = 1
                WHERE ticker != ?
                  AND finalized = 0
            """, (
                current_ticker,
            ))

            conn.commit()

    except Exception as e:
        print(
            "Finalize signals error:",
            e
        )
        # =========================================================
# LAST ENTER + STATS
# =========================================================

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
            "I'm waiting on my first V4.3 ENTER."
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
        f"Best sellable bid: {cents(best)} "
        f"({signed_cents(best_move)})\n"
        f"Worst sellable bid: {cents(worst)} "
        f"({signed_cents(worst_move)})\n\n"
        f"After 1m: {cents(row['price_1m'])}\n"
        f"After 3m: {cents(row['price_3m'])}\n"
        f"After 5m: {cents(row['price_5m'])}\n\n"
        f"+3¢ available: "
        f"{'✅' if row['hit_3c'] else '❌'}\n"
        f"+5¢ available: "
        f"{'✅' if row['hit_5c'] else '❌'}\n"
        f"+10¢ available: "
        f"{'✅' if row['hit_10c'] else '❌'}"
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
        send_telegram(
            "Stats database error."
        )
        return

    total = len(rows)

    if total == 0:
        send_telegram(
            "📊 WATCHER STATS\n\n"
            "No ENTER calls logged yet.\n"
            "GET READY and WAIT calls do not "
            "count as official ENTER calls."
        )
        return

    hit3 = sum(
        int(row["hit_3c"] or 0)
        for row in rows
    )

    hit5 = sum(
        int(row["hit_5c"] or 0)
        for row in rows
    )

    hit10 = sum(
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

    def hit5_rate(group):
        if not group:
            return "—"

        hits = sum(
            int(row["hit_5c"] or 0)
            for row in group
        )

        return pct_text(
            hits,
            len(group)
        )

    setup_groups = {}

    for row in rows:
        setup_groups.setdefault(
            row["setup"],
            []
        ).append(row)

    setup_lines = []

    for setup, group in sorted(
        setup_groups.items(),
        key=lambda item: len(item[1]),
        reverse=True
    ):
        hits = sum(
            int(row["hit_5c"] or 0)
            for row in group
        )

        setup_lines.append(
            f"• {setup}: "
            f"{hits}/{len(group)} "
            f"hit +5¢ "
            f"({pct_text(hits, len(group))})"
        )

    price_groups = {}

    for row in rows:
        bucket = price_bucket(
            float(row["entry_price"])
        )

        price_groups.setdefault(
            bucket,
            []
        ).append(row)

    price_order = [
        "<50¢",
        "50–69¢",
        "70–79¢",
        "80–89¢",
        "90¢+"
    ]

    price_lines = []

    for bucket in price_order:
        group = price_groups.get(
            bucket,
            []
        )

        if not group:
            continue

        hits = sum(
            int(row["hit_5c"] or 0)
            for row in group
        )

        price_lines.append(
            f"• {bucket}: "
            f"{hits}/{len(group)} "
            f"hit +5¢ "
            f"({pct_text(hits, len(group))})"
        )

    time_groups = {}

    for row in rows:
        bucket = time_bucket(
            int(
                row["seconds_left"]
                or 0
            )
        )

        time_groups.setdefault(
            bucket,
            []
        ).append(row)

    time_order = [
        "10+ min",
        "5–10 min",
        "2–5 min",
        "<2 min"
    ]

    time_lines = []

    for bucket in time_order:
        group = time_groups.get(
            bucket,
            []
        )

        if not group:
            continue

        hits = sum(
            int(row["hit_5c"] or 0)
            for row in group
        )

        time_lines.append(
            f"• {bucket}: "
            f"{hits}/{len(group)} "
            f"hit +5¢ "
            f"({pct_text(hits, len(group))})"
        )

    best_moves = []
    worst_moves = []

    for row in rows:
        entry = float(
            row["entry_price"]
        )

        if row["best_price"] is not None:
            best_moves.append(
                float(
                    row["best_price"]
                ) - entry
            )

        if row["worst_price"] is not None:
            worst_moves.append(
                float(
                    row["worst_price"]
                ) - entry
            )

    avg_best = (
        sum(best_moves) / len(best_moves)
        if best_moves
        else None
    )

    avg_worst = (
        sum(worst_moves) / len(worst_moves)
        if worst_moves
        else None
    )

    message = (
        "📊 KATE'S WATCHER STATS\n\n"
        f"ENTER calls: {total}\n"
        f"+3¢ available: "
        f"{hit3}/{total} "
        f"({pct_text(hit3, total)})\n"
        f"+5¢ available: "
        f"{hit5}/{total} "
        f"({pct_text(hit5, total)})\n"
        f"+10¢ available: "
        f"{hit10}/{total} "
        f"({pct_text(hit10, total)})\n\n"

        f"Average best move: "
        f"{signed_cents(avg_best)}\n"

        f"Average worst move: "
        f"{signed_cents(avg_worst)}\n\n"

        f"UP calls: {len(up_rows)} "
        f"| +5¢: {hit5_rate(up_rows)}\n"

        f"DOWN calls: {len(down_rows)} "
        f"| +5¢: {hit5_rate(down_rows)}\n\n"

        "🧠 BY SETUP (+5¢)\n"
        + (
            "\n".join(setup_lines)
            if setup_lines
            else "—"
        )

        + "\n\n💵 BY ENTRY PRICE (+5¢)\n"
        + (
            "\n".join(price_lines)
            if price_lines
            else "—"
        )

        + "\n\n⏱ BY TIME LEFT (+5¢)\n"
        + (
            "\n".join(time_lines)
            if time_lines
            else "—"
        )

        + "\n\n"
        "GET READY formations are tracked "
        "separately in shadow mode."
    )

    send_telegram(message)


# =========================================================
# V4.3 SHADOW STATS
# =========================================================

def send_shadow_stats():
    try:
        with db_connect() as conn:
            rows = conn.execute("""
                SELECT *
                FROM shadow_signals
                ORDER BY id ASC
            """).fetchall()

    except Exception as e:
        print(
            "Shadow stats error:",
            e
        )

        send_telegram(
            "Shadow stats database error."
        )

        return

    total = len(rows)

    if total == 0:
        send_telegram(
            "👀 SHADOW STATS\n\n"
            "No GET READY formations "
            "logged yet."
        )

        return

    up_rows = [
        row
        for row in rows
        if row["direction"] == "UP"
    ]

    down_rows = [
        row
        for row in rows
        if row["direction"] == "DOWN"
    ]

    hit3 = sum(
        int(row["hit_3c"] or 0)
        for row in rows
    )

    hit5 = sum(
        int(row["hit_5c"] or 0)
        for row in rows
    )

    hit10 = sum(
        int(row["hit_10c"] or 0)
        for row in rows
    )

    converted = sum(
        int(
            row["official_enter"]
            or 0
        )
        for row in rows
    )

    failed = sum(
        int(row["failed"] or 0)
        for row in rows
    )

    send_telegram(
        "👀 PRICE-ACTION SHADOW STATS\n\n"
        f"GET READY formations: {total}\n"
        f"UP: {len(up_rows)}\n"
        f"DOWN: {len(down_rows)}\n\n"

        f"+3¢ later available: "
        f"{hit3}/{total} "
        f"({pct_text(hit3, total)})\n"

        f"+5¢ later available: "
        f"{hit5}/{total} "
        f"({pct_text(hit5, total)})\n"

        f"+10¢ later available: "
        f"{hit10}/{total} "
        f"({pct_text(hit10, total)})\n\n"

        f"Became official ENTER: "
        f"{converted}/{total} "
        f"({pct_text(converted, total)})\n"

        f"Failed/reversed: "
        f"{failed}/{total} "
        f"({pct_text(failed, total)})\n\n"

        "👁️ Shadow calls are research only."
    )


# =========================================================
# TELEGRAM
# =========================================================

def telegram_url(method):
    return (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/{method}"
    )


def send_telegram(message):
    if (
        not TELEGRAM_TOKEN
        or not TELEGRAM_CHAT_ID
    ):
        print(
            "Telegram variables missing."
        )
        return

    try:
        response = requests.post(
            telegram_url(
                "sendMessage"
            ),
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message
            },
            timeout=10
        )

        response.raise_for_status()

    except Exception as e:
        print(
            "Telegram send error:",
            e
        )


def initialize_telegram_offset():
    global telegram_offset

    if not TELEGRAM_TOKEN:
        return

    try:
        response = requests.get(
            telegram_url(
                "getUpdates"
            ),
            params={
                "timeout": 0,
                "limit": 100
            },
            timeout=10
        )

        response.raise_for_status()

        updates = (
            response.json()
            .get(
                "result",
                []
            )
        )

        if updates:
            telegram_offset = (
                updates[-1][
                    "update_id"
                ]
                + 1
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
    global last_shadow_alert

    if not TELEGRAM_TOKEN:
        return

    try:
        params = {
            "timeout": 0,
            "limit": 20
        }

        if telegram_offset is not None:
            params[
                "offset"
            ] = telegram_offset

        response = requests.get(
            telegram_url(
                "getUpdates"
            ),
            params=params,
            timeout=10
        )

        response.raise_for_status()

        updates = (
            response.json()
            .get(
                "result",
                []
            )
        )

        for update in updates:
            telegram_offset = (
                update["update_id"]
                + 1
            )

            message = update.get(
                "message",
                {}
            )

            chat_id = str(
                message.get(
                    "chat",
                    {}
                ).get(
                    "id",
                    ""
                )
            )

            if chat_id != str(
                TELEGRAM_CHAT_ID
            ):
                continue

            text = (
                message.get(
                    "text",
                    ""
                )
                .strip()
                .lower()
            )

            if text.startswith(
                "/stop"
            ):
                watcher_enabled = False

                send_telegram(
                    "💤 KATE'S BTC WATCHER "
                    "IS OFF.\n\n"
                    "No trading alerts until "
                    "you send /start."
                )

            elif text.startswith(
                "/start"
            ):
                watcher_enabled = True
                last_alert = None
                last_shadow_alert = None

                send_telegram(
                    "🟢 KATE'S BTC WATCHER "
                    "IS ON.\n\n"
                    "I'm watching BTC again 👀"
                )

            elif text.startswith(
                "/status"
            ):
                send_status()

            elif text.startswith(
                "/shadowstats"
            ):
                send_shadow_stats()

            elif text.startswith(
                "/stats"
            ):
                send_stats()

            elif text.startswith(
                "/last"
            ):
                send_last()

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
            "I'm monitoring BTC..."
            "no current analysis "
            "available yet."
        )
        return

    analysis = last_analysis

    seconds = analysis[
        "seconds_left"
    ]

    minutes = seconds // 60
    secs = seconds % 60

    shadow = analysis.get(
        "shadow"
    )

    shadow_line = ""

    if (
        shadow
        and shadow.get(
            "direction"
        )
    ):
        shadow_line = (
            "\n\n👀 SHADOW READ: "
            f"GET READY "
            f"{shadow['direction']} "
            f"(score "
            f"{shadow['score']})"
        )

    send_telegram(
        "🟢 WATCHER STATUS: ON\n\n"
        f"{analysis['action']}\n\n"
        f"BTC: "
        f"${analysis['btc_price']:,.2f}\n"
        f"Trend: "
        f"{analysis['trend']}\n"
        f"UP ask: "
        f"{analysis['yes_ask']}\n"
        f"DOWN ask: "
        f"{analysis['no_ask']}\n"
        f"Time left: "
        f"{minutes}:{secs:02d}\n\n"
        f"{analysis['reason']}"
        f"{shadow_line}"
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

    markets = (
        response.json()
        .get(
            "markets",
            []
        )
    )

    if not markets:
        return None

    now = datetime.now(
        timezone.utc
    )

    active = []

    for market in markets:
        try:
            close_time = (
                datetime.fromisoformat(
                    market[
                        "close_time"
                    ].replace(
                        "Z",
                        "+00:00"
                    )
                )
            )

            if close_time > now:
                active.append(
                    (
                        close_time,
                        market
                    )
                )

        except Exception:
            continue

    if not active:
        return None

    active.sort(
        key=lambda item: item[0]
    )

    return active[0][1]


def get_contract_target(market):
    candidates = [
        market.get(
            "floor_strike"
        ),
        market.get(
            "functional_strike"
        )
    ]

    for value in candidates:
        if value is None:
            continue

        try:
            return float(
                str(value)
                .replace(",", "")
                .replace("$", "")
                .strip()
            )

        except (
            TypeError,
            ValueError
        ):
            pass

    import re

    for field in (
        "title",
        "subtitle",
        "yes_sub_title",
        "no_sub_title"
    ):
        text = str(
            market.get(field)
            or ""
        )

        matches = re.findall(
            (
                r"\$?("
                r"[0-9]{2,3}"
                r"(?:,[0-9]{3})+"
                r"(?:\.[0-9]+)?"
                r"|"
                r"[0-9]{4,6}"
                r"(?:\.[0-9]+)?"
                r")"
            ),
            text
        )

        for match in matches:
            try:
                value = float(
                    match.replace(
                        ",",
                        ""
                    )
                )

                if (
                    10000
                    <= value
                    <= 1000000
                ):
                    return value

            except ValueError:
                continue

    return None


def target_context(
    btc_price,
    target,
    direction=None
):
    if (
        target is None
        or btc_price is None
    ):
        return {
            "target": target,
            "distance": None,
            "distance_pct": None,
            "position": "UNKNOWN",
            "side_status": "UNKNOWN"
        }

    distance = (
        float(btc_price)
        - float(target)
    )

    distance_pct = (
        (
            distance
            / float(target)
        )
        * 100
        if target
        else None
    )

    if distance > 0:
        position = "ABOVE"

    elif distance < 0:
        position = "BELOW"

    else:
        position = "AT"

    side_status = "UNKNOWN"

    if direction == "UP":
        side_status = (
            "AHEAD"
            if distance >= 0
            else "BEHIND"
        )

    elif direction == "DOWN":
        side_status = (
            "AHEAD"
            if distance < 0
            else "BEHIND"
        )

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
        raise Exception(
            f"Kraken error: "
            f"{data['error']}"
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

    for candle in result[
        candle_key
    ]:
        try:
            candles.append({
                "time": int(
                    float(candle[0])
                ),
                "open": float(
                    candle[1]
                ),
                "high": float(
                    candle[2]
                ),
                "low": float(
                    candle[3]
                ),
                "close": float(
                    candle[4]
                ),
                "volume": float(
                    candle[6]
                )
            })

        except Exception:
            continue

    return candles


# =========================================================
# COMPLETED 1M CANDLES
# =========================================================

def get_completed_1m_candles():
    candles = get_kraken_candles(
        1
    )

    if not candles:
        return []

    now = int(time.time())

    return [
        candle
        for candle in candles
        if (
            candle["time"] + 60
            <= now
        )
    ]


def save_one_minute_candles(
    ticker,
    candles
):
    if not ticker or not candles:
        return

    now = int(time.time())

    try:
        with db_connect() as conn:

            # Only the recent sequence needs to be
            # attempted each loop. UNIQUE prevents
            # duplicates.
            for candle in candles[-10:]:

                candle_range = (
                    candle["high"]
                    - candle["low"]
                )

                body_size = abs(
                    candle["close"]
                    - candle["open"]
                )

                body_ratio = (
                    body_size
                    / candle_range
                    if candle_range > 0
                    else 0
                )

                if (
                    candle["close"]
                    > candle["open"]
                ):
                    color = "GREEN"

                elif (
                    candle["close"]
                    < candle["open"]
                ):
                    color = "RED"

                else:
                    color = "FLAT"

                conn.execute("""
                    INSERT OR IGNORE INTO
                    one_minute_snapshots (
                        ticker,
                        candle_time,
                        open,
                        high,
                        low,
                        close,
                        volume,
                        candle_color,
                        body_size,
                        candle_range,
                        body_ratio,
                        captured_at
                    )
                    VALUES (
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?
                    )
                """, (
                    ticker,
                    candle["time"],
                    candle["open"],
                    candle["high"],
                    candle["low"],
                    candle["close"],
                    candle["volume"],
                    color,
                    body_size,
                    candle_range,
                    body_ratio,
                    now
                ))

            conn.commit()

    except Exception as e:
        print(
            "1m recorder error:",
            e
        )


# =========================================================
# BUILD COMPLETED 3M CANDLES
# =========================================================

def build_3m_candles(
    one_minute
):
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
            grouped[bucket][
                "high"
            ] = max(
                grouped[bucket][
                    "high"
                ],
                candle["high"]
            )

            grouped[bucket][
                "low"
            ] = min(
                grouped[bucket][
                    "low"
                ],
                candle["low"]
            )

            grouped[bucket][
                "close"
            ] = candle["close"]

            grouped[bucket][
                "volume"
            ] += candle["volume"]

    candles = [
        grouped[key]
        for key in sorted(
            grouped
        )
    ]

    now = int(time.time())

    return [
        candle
        for candle in candles
        if (
            candle["time"] + 180
            <= now
        )
    ]


# =========================================================
# GENERAL MARKET HELPERS
# =========================================================

def seconds_left(market):
    close_time = (
        datetime.fromisoformat(
            market[
                "close_time"
            ].replace(
                "Z",
                "+00:00"
            )
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


def pct_distance(
    price,
    level
):
    if not level:
        return 999

    return (
        abs(
            price - level
        )
        / level
        * 100
    )


def clear_retest():
    retest_state[
        "direction"
    ] = None

    retest_state[
        "level"
    ] = None

    retest_state[
        "armed_at"
    ] = None


def arm_retest(
    direction,
    level,
    candle_time
):
    retest_state[
        "direction"
    ] = direction

    retest_state[
        "level"
    ] = level

    retest_state[
        "armed_at"
    ] = candle_time


# =========================================================
# 15M STRUCTURE
# =========================================================

def get_structure():
    candles = get_kraken_candles(
        15
    )

    if (
        len(candles)
        < STRUCTURE_CANDLES + 2
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

    midpoint = (
        len(closes) // 2
    )

    first_avg = (
        sum(
            closes[:midpoint]
        )
        / len(
            closes[:midpoint]
        )
    )

    second_avg = (
        sum(
            closes[midpoint:]
        )
        / len(
            closes[midpoint:]
        )
    )

    if (
        second_avg
        > first_avg * 1.001
    ):
        trend = "BULLISH"

    elif (
        second_avg
        < first_avg * 0.999
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
# V4.3 1M PRICE-ACTION RECOGNITION
# =========================================================

def analyze_1m_price_action(candles):
    if (
        not candles
        or len(candles)
        < PRICE_ACTION_1M_LOOKBACK
    ):
        return None

    recent = candles[
        -PRICE_ACTION_1M_LOOKBACK:
    ]

    latest = recent[-1]

    closes = [
        candle["close"]
        for candle in recent
    ]

    highs = [
        candle["high"]
        for candle in recent
    ]

    lows = [
        candle["low"]
        for candle in recent
    ]

    green_count = sum(
        1
        for candle in recent
        if candle["close"] > candle["open"]
    )

    red_count = sum(
        1
        for candle in recent
        if candle["close"] < candle["open"]
    )

    rising_closes = all(
        closes[index]
        > closes[index - 1]
        for index in range(
            1,
            len(closes)
        )
    )

    falling_closes = all(
        closes[index]
        < closes[index - 1]
        for index in range(
            1,
            len(closes)
        )
    )

    higher_lows = all(
        lows[index]
        >= lows[index - 1]
        for index in range(
            1,
            len(lows)
        )
    )

    lower_highs = all(
        highs[index]
        <= highs[index - 1]
        for index in range(
            1,
            len(highs)
        )
    )

    latest_range = (
        latest["high"]
        - latest["low"]
    )

    latest_body = abs(
        latest["close"]
        - latest["open"]
    )

    latest_body_ratio = (
        latest_body / latest_range
        if latest_range > 0
        else 0
    )

    strong_green = (
        latest["close"] > latest["open"]
        and latest_body_ratio
        >= STRONG_BODY_RATIO
    )

    strong_red = (
        latest["close"] < latest["open"]
        and latest_body_ratio
        >= STRONG_BODY_RATIO
    )

    upper_wick = (
        latest["high"]
        - max(
            latest["open"],
            latest["close"]
        )
    )

    lower_wick = (
        min(
            latest["open"],
            latest["close"]
        )
        - latest["low"]
    )

    bullish_rejection = (
        latest_range > 0
        and lower_wick
        > latest_body
        and latest["close"]
        >= latest["open"]
    )

    bearish_rejection = (
        latest_range > 0
        and upper_wick
        > latest_body
        and latest["close"]
        <= latest["open"]
    )

    bullish_pressure = (
        green_count >= 3
    )

    bearish_pressure = (
        red_count >= 3
    )

    net_move = (
        closes[-1]
        - closes[0]
    )

    return {
        "latest_time": latest["time"],
        "latest_open": latest["open"],
        "latest_high": latest["high"],
        "latest_low": latest["low"],
        "latest_close": latest["close"],

        "green_count": green_count,
        "red_count": red_count,

        "rising_closes": rising_closes,
        "falling_closes": falling_closes,

        "higher_lows": higher_lows,
        "lower_highs": lower_highs,

        "strong_green": strong_green,
        "strong_red": strong_red,

        "bullish_rejection": bullish_rejection,
        "bearish_rejection": bearish_rejection,

        "bullish_pressure": bullish_pressure,
        "bearish_pressure": bearish_pressure,

        "body_ratio": latest_body_ratio,
        "net_move": net_move
    }


# =========================================================
# V4.3 TARGET VELOCITY LOOKUP
# =========================================================

def get_recent_target_motion(ticker):
    try:
        with db_connect() as conn:
            rows = conn.execute("""
                SELECT
                    captured_at,
                    target_distance
                FROM market_snapshots
                WHERE ticker = ?
                  AND target_distance
                      IS NOT NULL
                ORDER BY
                    captured_at DESC,
                    id DESC
                LIMIT 3
            """, (
                ticker,
            )).fetchall()

        if len(rows) < 2:
            return {
                "velocity": None,
                "cross": None
            }

        newest = rows[0]
        oldest = rows[-1]

        newest_distance = float(
            newest["target_distance"]
        )

        oldest_distance = float(
            oldest["target_distance"]
        )

        dt = max(
            1,
            int(newest["captured_at"])
            - int(oldest["captured_at"])
        )

        velocity = (
            newest_distance
            - oldest_distance
        ) / dt

        cross = None

        # Check the most recent pair for
        # an actual target crossing.
        previous = rows[1]

        previous_distance = float(
            previous["target_distance"]
        )

        if (
            previous_distance <= 0
            and newest_distance > 0
        ):
            cross = "UP"

        elif (
            previous_distance >= 0
            and newest_distance < 0
        ):
            cross = "DOWN"

        return {
            "velocity": velocity,
            "cross": cross
        }

    except Exception as e:
        print(
            "Target motion lookup error:",
            e
        )

        return {
            "velocity": None,
            "cross": None
        }


# =========================================================
# V4.3 KALSHI REPRICING LOOKUP
# =========================================================

def get_recent_kalshi_motion(ticker):
    try:
        with db_connect() as conn:
            rows = conn.execute("""
                SELECT
                    captured_at,
                    yes_bid,
                    yes_ask,
                    no_bid,
                    no_ask
                FROM market_snapshots
                WHERE ticker = ?
                ORDER BY
                    captured_at DESC,
                    id DESC
                LIMIT 3
            """, (
                ticker,
            )).fetchall()

        if len(rows) < 2:
            return {
                "yes_bid_change": None,
                "no_bid_change": None,
                "yes_ask_change": None,
                "no_ask_change": None
            }

        newest = rows[0]
        oldest = rows[-1]

        def change(field):
            if (
                newest[field] is None
                or oldest[field] is None
            ):
                return None

            return (
                float(newest[field])
                - float(oldest[field])
            )

        return {
            "yes_bid_change": change(
                "yes_bid"
            ),
            "no_bid_change": change(
                "no_bid"
            ),
            "yes_ask_change": change(
                "yes_ask"
            ),
            "no_ask_change": change(
                "no_ask"
            )
        }

    except Exception as e:
        print(
            "Kalshi motion lookup error:",
            e
        )

        return {
            "yes_bid_change": None,
            "no_bid_change": None,
            "yes_ask_change": None,
            "no_ask_change": None
        }


# =========================================================
# V4.3 GET READY SHADOW BRAIN
# =========================================================

def analyze_shadow_setup(
    ticker,
    analysis,
    one_minute_candles
):
    if not analysis:
        return None

    if (
        analysis["seconds_left"]
        < SHADOW_MIN_SECONDS
    ):
        return None

    # If V4.2 already has a confirmed ENTER,
    # there is no reason to create a new
    # early-warning formation.
    if (
        analysis["confirmed"]
        and analysis["action"].startswith(
            "🗣️ ENTER"
        )
    ):
        return None

    pa1 = analyze_1m_price_action(
        one_minute_candles
    )

    if not pa1:
        return None

    target_motion = (
        get_recent_target_motion(
            ticker
        )
    )

    kalshi_motion = (
        get_recent_kalshi_motion(
            ticker
        )
    )

    up_score = 0
    down_score = 0

    up_evidence = []
    down_evidence = []

    # -----------------------------------------------------
    # 1M PRICE ACTION
    # -----------------------------------------------------

    if pa1["rising_closes"]:
        up_score += 2
        up_evidence.append(
            "1m closes rising"
        )

    if pa1["falling_closes"]:
        down_score += 2
        down_evidence.append(
            "1m closes falling"
        )

    if pa1["higher_lows"]:
        up_score += 1
        up_evidence.append(
            "higher 1m lows"
        )

    if pa1["lower_highs"]:
        down_score += 1
        down_evidence.append(
            "lower 1m highs"
        )

    if pa1["bullish_pressure"]:
        up_score += 1
        up_evidence.append(
            "green candle pressure"
        )

    if pa1["bearish_pressure"]:
        down_score += 1
        down_evidence.append(
            "red candle pressure"
        )

    if pa1["strong_green"]:
        up_score += 1
        up_evidence.append(
            "strong green 1m body"
        )

    if pa1["strong_red"]:
        down_score += 1
        down_evidence.append(
            "strong red 1m body"
        )

    if pa1["bullish_rejection"]:
        up_score += 1
        up_evidence.append(
            "lower-wick rejection"
        )

    if pa1["bearish_rejection"]:
        down_score += 1
        down_evidence.append(
            "upper-wick rejection"
        )

    # -----------------------------------------------------
    # 15M STRUCTURE CONTEXT
    # -----------------------------------------------------

    if analysis["trend"] == "BULLISH":
        up_score += 1
        up_evidence.append(
            "15m trend bullish"
        )

    elif analysis["trend"] == "BEARISH":
        down_score += 1
        down_evidence.append(
            "15m trend bearish"
        )

    if analysis["near_support"]:
        # Near support is useful context
        # only if short-term price action
        # is already leaning upward.
        if (
            pa1["bullish_pressure"]
            or pa1["strong_green"]
            or pa1["bullish_rejection"]
        ):
            up_score += 1
            up_evidence.append(
                "bullish action near support"
            )

    if analysis["near_resistance"]:
        if (
            pa1["bearish_pressure"]
            or pa1["strong_red"]
            or pa1["bearish_rejection"]
        ):
            down_score += 1
            down_evidence.append(
                "bearish action near resistance"
            )

    if analysis["breakout_up"]:
        up_score += 2
        up_evidence.append(
            "3m resistance breakout"
        )

    if analysis["breakout_down"]:
        down_score += 2
        down_evidence.append(
            "3m support breakdown"
        )

    if (
        analysis["armed_direction"]
        == "UP"
    ):
        up_score += 1
        up_evidence.append(
            "UP retest armed"
        )

    elif (
        analysis["armed_direction"]
        == "DOWN"
    ):
        down_score += 1
        down_evidence.append(
            "DOWN retest armed"
        )

    # -----------------------------------------------------
    # CONTRACT TARGET
    # -----------------------------------------------------

    target_distance = analysis.get(
        "target_distance"
    )

    target_velocity = (
        target_motion.get(
            "velocity"
        )
    )

    target_cross = (
        target_motion.get(
            "cross"
        )
    )

    if target_cross == "UP":
        up_score += 2
        up_evidence.append(
            "BTC crossed target upward"
        )

    elif target_cross == "DOWN":
        down_score += 2
        down_evidence.append(
            "BTC crossed target downward"
        )

    if target_velocity is not None:
        # We intentionally avoid pretending
        # there is a proven magic velocity
        # threshold yet.
        #
        # Direction of sustained movement is
        # useful as shadow evidence. V4.3
        # records the actual number so we can
        # later discover useful thresholds.
        if target_velocity > 0:
            up_score += 1
            up_evidence.append(
                "moving toward/above target"
            )

        elif target_velocity < 0:
            down_score += 1
            down_evidence.append(
                "moving toward/below target"
            )

    # If BTC is already on one side of the
    # target AND price action agrees, give
    # that side modest context credit.
    if target_distance is not None:
        if (
            target_distance > 0
            and (
                pa1["bullish_pressure"]
                or pa1["rising_closes"]
            )
        ):
            up_score += 1
            up_evidence.append(
                "BTC above target with "
                "bullish pressure"
            )

        elif (
            target_distance < 0
            and (
                pa1["bearish_pressure"]
                or pa1["falling_closes"]
            )
        ):
            down_score += 1
            down_evidence.append(
                "BTC below target with "
                "bearish pressure"
            )

    # -----------------------------------------------------
    # KALSHI REPRICING
    # -----------------------------------------------------

    yes_bid_change = (
        kalshi_motion.get(
            "yes_bid_change"
        )
    )

    no_bid_change = (
        kalshi_motion.get(
            "no_bid_change"
        )
    )

    if (
        yes_bid_change is not None
        and yes_bid_change > 0
    ):
        up_score += 1
        up_evidence.append(
            "UP bid rising"
        )

    if (
        no_bid_change is not None
        and no_bid_change > 0
    ):
        down_score += 1
        down_evidence.append(
            "DOWN bid rising"
        )

    # -----------------------------------------------------
    # REQUIRE DIRECTIONAL SEPARATION
    #
    # We don't want GET READY because both
    # sides scored similarly in chop.
    # -----------------------------------------------------

    if (
        up_score >= SHADOW_MIN_SCORE
        and up_score >= down_score + 2
    ):
        direction = "UP"
        score = up_score
        evidence = up_evidence

    elif (
        down_score >= SHADOW_MIN_SCORE
        and down_score >= up_score + 2
    ):
        direction = "DOWN"
        score = down_score
        evidence = down_evidence

    else:
        return None

    if direction == "UP":
        entry_ask = analysis.get(
            "yes_ask"
        )

        current_bid = analysis.get(
            "yes_bid"
        )

    else:
        entry_ask = analysis.get(
            "no_ask"
        )

        current_bid = analysis.get(
            "no_bid"
        )

    return {
        "direction": direction,
        "score": score,
        "opposite_score": (
            down_score
            if direction == "UP"
            else up_score
        ),
        "evidence": evidence,
        "entry_ask": entry_ask,
        "current_bid": current_bid,
        "target_velocity": target_velocity,
        "target_cross": target_cross,
        "one_minute": pa1
    }


# =========================================================
# V4.3 SHADOW DATABASE TRACKING
# =========================================================

def create_shadow_signal(
    ticker,
    analysis,
    shadow
):
    global active_shadow

    if not shadow:
        return None

    direction = shadow[
        "direction"
    ]

    # Keep one active formation per ticker
    # and direction instead of inserting
    # a duplicate every 15 seconds.
    if (
        active_shadow["ticker"]
        == ticker
        and active_shadow["direction"]
        == direction
        and active_shadow["shadow_id"]
        is not None
    ):
        return active_shadow[
            "shadow_id"
        ]

    now = int(time.time())

    evidence_text = ", ".join(
        shadow["evidence"]
    )

    entry_ask = shadow.get(
        "entry_ask"
    )

    initial_bid = shadow.get(
        "current_bid"
    )

    try:
        with db_connect() as conn:
            cursor = conn.execute("""
                INSERT INTO shadow_signals (
                    ticker,
                    direction,
                    created_at,
                    seconds_left,
                    btc_price,
                    contract_target,
                    target_distance,
                    target_velocity,
                    support,
                    resistance,
                    trend,
                    entry_ask,
                    initial_bid,
                    score,
                    evidence,
                    best_bid,
                    worst_bid,
                    last_bid,
                    last_updated
                )
                VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?
                )
            """, (
                ticker,
                direction,
                now,
                analysis.get(
                    "seconds_left"
                ),
                analysis.get(
                    "btc_price"
                ),
                analysis.get(
                    "contract_target"
                ),
                analysis.get(
                    "target_distance"
                ),
                shadow.get(
                    "target_velocity"
                ),
                analysis.get(
                    "support"
                ),
                analysis.get(
                    "resistance"
                ),
                analysis.get(
                    "trend"
                ),
                entry_ask,
                initial_bid,
                shadow.get(
                    "score"
                ),
                evidence_text,
                initial_bid,
                initial_bid,
                initial_bid,
                now
            ))

            shadow_id = (
                cursor.lastrowid
            )

            conn.commit()

        active_shadow = {
            "ticker": ticker,
            "direction": direction,
            "shadow_id": shadow_id
        }

        print(
            "Logged GET READY:",
            ticker,
            direction,
            shadow["score"]
        )

        return shadow_id

    except Exception as e:
        print(
            "Create shadow signal error:",
            e
        )

        return None


def update_shadow_signals(
    market
):
    ticker = market.get(
        "ticker"
    )

    if not ticker:
        return

    now = int(time.time())

    try:
        with db_connect() as conn:
            rows = conn.execute("""
                SELECT *
                FROM shadow_signals
                WHERE ticker = ?
                  AND finalized = 0
            """, (
                ticker,
            )).fetchall()

            for row in rows:
                direction = row[
                    "direction"
                ]

                current_bid = (
                    side_bid_for_signal(
                        direction,
                        market
                    )
                )

                if current_bid is None:
                    continue

                best = row["best_bid"]
                worst = row["worst_bid"]

                if best is None:
                    best = current_bid
                else:
                    best = max(
                        float(best),
                        current_bid
                    )

                if worst is None:
                    worst = current_bid
                else:
                    worst = min(
                        float(worst),
                        current_bid
                    )

                entry_ask = row[
                    "entry_ask"
                ]

                hit_3c = int(
                    row["hit_3c"]
                    or 0
                )

                hit_5c = int(
                    row["hit_5c"]
                    or 0
                )

                hit_10c = int(
                    row["hit_10c"]
                    or 0
                )

                if entry_ask is not None:
                    entry_ask = float(
                        entry_ask
                    )

                    if (
                        best
                        >= entry_ask + 0.03
                    ):
                        hit_3c = 1

                    if (
                        best
                        >= entry_ask + 0.05
                    ):
                        hit_5c = 1

                    if (
                        best
                        >= entry_ask + 0.10
                    ):
                        hit_10c = 1

                conn.execute("""
                    UPDATE shadow_signals
                    SET
                        best_bid = ?,
                        worst_bid = ?,
                        hit_3c = ?,
                        hit_5c = ?,
                        hit_10c = ?,
                        last_bid = ?,
                        last_updated = ?
                    WHERE id = ?
                """, (
                    best,
                    worst,
                    hit_3c,
                    hit_5c,
                    hit_10c,
                    current_bid,
                    now,
                    row["id"]
                ))

            conn.commit()

    except Exception as e:
        print(
            "Update shadow error:",
            e
        )


def mark_shadow_official_enter(
    ticker,
    analysis
):
    if (
        not analysis["confirmed"]
        or not analysis[
            "action"
        ].startswith(
            "🗣️ ENTER"
        )
    ):
        return

    direction = analysis.get(
        "direction"
    )

    if not direction:
        return

    now = int(time.time())

    try:
        with db_connect() as conn:
            row = conn.execute("""
                SELECT id
                FROM shadow_signals
                WHERE ticker = ?
                  AND direction = ?
                  AND finalized = 0
                  AND official_enter = 0
                ORDER BY id DESC
                LIMIT 1
            """, (
                ticker,
                direction
            )).fetchone()

            if not row:
                return

            conn.execute("""
                UPDATE shadow_signals
                SET
                    official_enter = 1,
                    official_enter_price = ?,
                    official_enter_at = ?
                WHERE id = ?
            """, (
                analysis.get(
                    "entry_price"
                ),
                now,
                row["id"]
            ))

            conn.commit()

    except Exception as e:
        print(
            "Shadow ENTER link error:",
            e
        )


def clear_active_shadow():
    global active_shadow

    active_shadow = {
        "ticker": None,
        "direction": None,
        "shadow_id": None
    }


# =========================================================
# EXISTING 3M PRICE ACTION + STATEFUL RETEST
# =========================================================

def analyze_price_action(
    support,
    resistance,
    one_minute=None
):
    # V4.3 can reuse the already-fetched
    # 1m candles instead of hitting Kraken
    # twice in the same loop.
    if one_minute is None:
        one_minute = (
            get_completed_1m_candles()
        )

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
        and current["close"]
        > support
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
        retest_state[
            "direction"
        ]
    )

    armed_level = (
        retest_state[
            "level"
        ]
    )

    armed_at = (
        retest_state[
            "armed_at"
        ]
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
            and armed_direction
            == "UP"
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
            candles_since >= 1
            and armed_direction
            == "DOWN"
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

        "candle_time":
            current["time"],

        "candle_open":
            current["open"],

        "candle_high":
            current["high"],

        "candle_low":
            current["low"],

        "candle_close":
            current["close"],

        "candle_color": (
            "GREEN"
            if green
            else "RED"
            if red
            else "FLAT"
        ),

        "near_support":
            near_support,

        "near_resistance":
            near_resistance,

        "above_support":
            above_support,

        "below_support":
            below_support,

        "below_resistance":
            below_resistance,

        "above_resistance":
            above_resistance,

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
            retest_state[
                "direction"
            ]
    }
    # =========================================================
# FINAL ACTION DECISION — V4.2 ENTER BRAIN STAYS FROZEN
# =========================================================

def analyze_market(
    market,
    one_minute
):
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
        structure["resistance"],
        one_minute
    )

    if not pa:
        return None

    contract_target = (
        get_contract_target(
            market
        )
    )

    action = "🗣️ WAIT 🟡"
    setup = "NONE"

    reason = (
        "BTC is between key levels."
    )

    direction = None
    confirmed = False

    # -----------------------------------------------------
    # STRONG CONFIRMATIONS
    # EXISTING V4.2 LOGIC
    # -----------------------------------------------------

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

    elif pa[
        "resistance_rejection"
    ]:
        direction = "DOWN"
        confirmed = True
        setup = (
            "RESISTANCE REJECTION"
        )

        reason = (
            "BTC tested 15m resistance and "
            "the completed 3m candle turned "
            "back down."
        )

    # -----------------------------------------------------
    # DEVELOPING SETUPS
    # EXISTING V4.2 LOGIC
    # -----------------------------------------------------

    elif pa["breakout_up"]:
        setup = "UP BREAKOUT"

        reason = (
            "BTC broke resistance..."
            "waiting for a 3m retest "
            "before entering."
        )

    elif pa["breakout_down"]:
        setup = "DOWN BREAK"

        reason = (
            "BTC broke support..."
            "waiting for a 3m retest "
            "before entering."
        )

    elif (
        pa["armed_direction"]
        == "UP"
    ):
        setup = "UP RETEST ARMED"

        reason = (
            "The resistance breakout is "
            "still armed...waiting for BTC "
            "to retest and stay above "
            "the level."
        )

    elif (
        pa["armed_direction"]
        == "DOWN"
    ):
        setup = "DOWN RETEST ARMED"

        reason = (
            "The support break is still "
            "armed...waiting for BTC to "
            "retest and turn back down."
        )

    elif (
        pa["near_support"]
        and pa["below_support"]
    ):
        setup = "SUPPORT BREAKING"

        reason = (
            "BTC is below 15m support, "
            "but the break is not confirmed "
            "enough for an entry yet...WAIT."
        )

    elif (
        pa["near_support"]
        and pa["above_support"]
    ):
        setup = "WATCH SUPPORT"

        reason = (
            "BTC is just above 15m support..."
            "waiting to see if it bounces "
            "or breaks."
        )

    elif (
        pa["near_resistance"]
        and pa["above_resistance"]
    ):
        setup = (
            "RESISTANCE BREAKING"
        )

        reason = (
            "BTC is above 15m resistance, "
            "but the break is not confirmed "
            "enough for an entry yet...WAIT."
        )

    elif (
        pa["near_resistance"]
        and pa["below_resistance"]
    ):
        setup = "WATCH RESISTANCE"

        reason = (
            "BTC is just below 15m "
            "resistance...waiting to see "
            "if it breaks through or turns "
            "back down."
        )

    # -----------------------------------------------------
    # KALSHI ENTRY DECISION
    # EXISTING V4.2 LOGIC
    # -----------------------------------------------------

    entry_price = None
    scalp = False

    if direction == "UP":
        entry_price = yes_ask

    elif direction == "DOWN":
        entry_price = no_ask

    target_read = target_context(
        pa["price"],
        contract_target,
        direction
    )

    if (
        confirmed
        and contract_target
        is not None
    ):
        gap = abs(
            target_read[
                "distance"
            ]
        )

        if (
            target_read[
                "side_status"
            ]
            == "AHEAD"
        ):
            reason += (
                f" 🎯 {direction} is "
                f"currently AHEAD of the "
                f"${contract_target:,.2f} "
                f"target by ${gap:,.2f}."
            )

        else:
            reason += (
                f" 🎯 {direction} is "
                f"currently BEHIND the "
                f"${contract_target:,.2f} "
                f"target by ${gap:,.2f}; "
                "BTC still needs to move "
                "toward/cross the target."
            )

    if confirmed:

        if entry_price is None:
            action = (
                "🗣️ DON'T TOUCH THIS SHIT 🚫"
            )

            reason += (
                " Kalshi doesn't currently "
                "have a usable ask price."
            )

        elif (
            remaining
            < MIN_SECONDS_FOR_ENTRY
        ):
            action = (
                "🗣️ DON'T TOUCH THIS SHIT 🚫"
            )

            reason += (
                " There's less than a minute "
                "left...too late for a fresh "
                "entry signal."
            )

        elif (
            entry_price
            >= HARD_SKIP_PRICE
        ):
            action = (
                "🗣️ DON'T TOUCH THIS SHIT 🚫"
            )

            reason += (
                f" {direction} is already "
                f"${entry_price:.2f}..."
                "almost all the contract "
                "value is priced in."
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

            if (
                entry_price
                >= SCALP_PRICE
            ):
                scalp = True

                reason += (
                    f" 💰 SCALP: "
                    f"{direction} is "
                    f"${entry_price:.2f}, "
                    "so the remaining room "
                    "is smaller."
                )

            elif (
                remaining
                > MAX_SECONDS_FOR_ENTRY
            ):
                reason += (
                    " Contract is still "
                    "early...expect more "
                    "movement."
                )

    # -----------------------------------------------------
    # RANGE / CHOP
    # -----------------------------------------------------

    if (
        not confirmed
        and setup == "NONE"
        and structure["trend"]
        == "RANGE"
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

        "contract_target":
            contract_target,

        "target_distance":
            target_read[
                "distance"
            ],

        "target_distance_pct":
            target_read[
                "distance_pct"
            ],

        "target_position":
            target_read[
                "position"
            ],

        "target_side_status":
            target_read[
                "side_status"
            ],

        "support":
            structure[
                "support"
            ],

        "resistance":
            structure[
                "resistance"
            ],

        "trend":
            structure[
                "trend"
            ],

        "yes_bid": yes_bid,
        "yes_ask": yes_ask,

        "no_bid": no_bid,
        "no_ask": no_ask,

        "entry_price":
            entry_price,

        "seconds_left":
            remaining,

        "candle_time":
            pa[
                "candle_time"
            ],

        "candle_open":
            pa[
                "candle_open"
            ],

        "candle_high":
            pa[
                "candle_high"
            ],

        "candle_low":
            pa[
                "candle_low"
            ],

        "candle_close":
            pa[
                "candle_close"
            ],

        "candle_color":
            pa[
                "candle_color"
            ],

        "near_support":
            pa[
                "near_support"
            ],

        "near_resistance":
            pa[
                "near_resistance"
            ],

        "support_rejection":
            pa[
                "support_rejection"
            ],

        "resistance_rejection":
            pa[
                "resistance_rejection"
            ],

        "breakout_up":
            pa[
                "breakout_up"
            ],

        "breakout_down":
            pa[
                "breakout_down"
            ],

        "retest_up":
            pa[
                "retest_up"
            ],

        "retest_down":
            pa[
                "retest_down"
            ],

        "armed_direction":
            pa[
                "armed_direction"
            ]
    }


# =========================================================
# V4.3 BLACK-BOX SNAPSHOT RECORDER
# =========================================================

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

    yes_spread = (
        yes_ask - yes_bid
        if (
            yes_ask is not None
            and yes_bid is not None
        )
        else None
    )

    no_spread = (
        no_ask - no_bid
        if (
            no_ask is not None
            and no_bid is not None
        )
        else None
    )

    btc = analysis.get(
        "btc_price"
    )

    support = analysis.get(
        "support"
    )

    resistance = analysis.get(
        "resistance"
    )

    try:
        with db_connect() as conn:

            previous = conn.execute("""
                SELECT
                    target_distance,
                    captured_at
                FROM market_snapshots
                WHERE ticker = ?
                ORDER BY
                    captured_at DESC,
                    id DESC
                LIMIT 1
            """, (
                ticker,
            )).fetchone()

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
                old = float(
                    previous[
                        "target_distance"
                    ]
                )

                dt = max(
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
                    - old
                ) / dt

                if (
                    old <= 0
                    and distance > 0
                ):
                    target_cross = "UP"

                elif (
                    old >= 0
                    and distance < 0
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
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?
                )
            """, (
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
                analysis.get(
                    "trend"
                ),
                (
                    btc - support
                    if (
                        btc is not None
                        and support
                        is not None
                    )
                    else None
                ),
                (
                    resistance - btc
                    if (
                        btc is not None
                        and resistance
                        is not None
                    )
                    else None
                ),
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


# =========================================================
# V4.3 SHADOW FAILURE / RESET LOGIC
# =========================================================

def update_active_shadow_state(
    ticker,
    shadow
):
    global active_shadow

    if (
        active_shadow[
            "ticker"
        ] != ticker
        or active_shadow[
            "shadow_id"
        ] is None
    ):
        return

    active_direction = (
        active_shadow[
            "direction"
        ]
    )

    shadow_id = (
        active_shadow[
            "shadow_id"
        ]
    )

    # If the developing evidence flips
    # strongly to the opposite side, mark
    # the old formation as failed.
    if (
        shadow
        and shadow[
            "direction"
        ] != active_direction
    ):
        try:
            with db_connect() as conn:
                conn.execute("""
                    UPDATE shadow_signals
                    SET
                        failed = 1,
                        finalized = 1
                    WHERE id = ?
                """, (
                    shadow_id,
                ))

                conn.commit()

        except Exception as e:
            print(
                "Shadow failure error:",
                e
            )

        clear_active_shadow()


# =========================================================
# MESSAGE HELPERS
# =========================================================

def target_line(analysis):
    target = analysis.get(
        "contract_target"
    )

    distance = analysis.get(
        "target_distance"
    )

    position = analysis.get(
        "target_position",
        "UNKNOWN"
    )

    side_status = analysis.get(
        "target_side_status",
        "UNKNOWN"
    )

    if (
        target is None
        or distance is None
    ):
        return (
            "Target: unavailable "
            "from Kalshi API ⚠️"
        )

    signed = (
        f"{distance:+,.2f}"
    )

    status = ""

    if analysis.get(
        "direction"
    ):
        status = (
            f" | "
            f"{analysis['direction']} "
            f"is {side_status}"
        )

    return (
        f"Target: "
        f"${target:,.2f}\n"
        f"BTC is {position} target by "
        f"${abs(distance):,.2f} "
        f"({signed} vs target)"
        f"{status}"
    )


def build_message(
    market,
    analysis
):
    seconds = analysis[
        "seconds_left"
    ]

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


def build_shadow_message(
    market,
    analysis,
    shadow
):
    direction = shadow[
        "direction"
    ]

    if direction == "UP":
        emoji = "🟢"

    else:
        emoji = "🔴"

    seconds = analysis[
        "seconds_left"
    ]

    minutes = seconds // 60
    secs = seconds % 60

    evidence = "\n".join(
        f"• {item}"
        for item in shadow[
            "evidence"
        ][:6]
    )

    entry_ask = shadow.get(
        "entry_ask"
    )

    current_bid = shadow.get(
        "current_bid"
    )

    ask_text = (
        cents(entry_ask)
    )

    bid_text = (
        cents(current_bid)
    )

    target_velocity = (
        shadow.get(
            "target_velocity"
        )
    )

    velocity_text = "—"

    if target_velocity is not None:
        velocity_text = (
            f"${target_velocity:+.2f}/sec"
        )

    return (
        f"👀 GET READY {direction} "
        f"{emoji}\n\n"

        "⚠️ EARLY FORMATION ONLY — "
        "NOT AN ENTER CALL.\n\n"

        f"Price-action score: "
        f"{shadow['score']}\n"

        f"Opposite score: "
        f"{shadow['opposite_score']}\n\n"

        f"{evidence}\n\n"

        f"BTC: "
        f"${analysis['btc_price']:,.2f}\n"

        f"Target distance: "
        f"${analysis['target_distance']:+,.2f}\n"
        if (
            analysis.get(
                "target_distance"
            ) is not None
        )
        else (
            f"👀 GET READY {direction} "
            f"{emoji}\n\n"
            "⚠️ EARLY FORMATION ONLY — "
            "NOT AN ENTER CALL.\n\n"
            f"Price-action score: "
            f"{shadow['score']}\n"
            f"Opposite score: "
            f"{shadow['opposite_score']}\n\n"
            f"{evidence}\n\n"
            f"BTC: "
            f"${analysis['btc_price']:,.2f}\n"
        )
    ) + (
        f"Target velocity: "
        f"{velocity_text}\n"
        f"Current ask: "
        f"{ask_text}\n"
        f"Sellable bid: "
        f"{bid_text}\n"
        f"Time left: "
        f"{minutes}:{secs:02d}\n\n"
        "I'm watching this formation 👁️"
    )


# =========================================================
# SHADOW ALERT CONTROL
# =========================================================

def maybe_send_shadow_alert(
    market,
    analysis,
    shadow
):
    global last_shadow_alert
    global last_shadow_alert_time

    if not shadow:
        return

    now = int(time.time())

    key = (
        market.get(
            "ticker"
        ),
        shadow[
            "direction"
        ]
    )

    # Separate alert memory from WAIT/ENTER
    # so shadow alerts cannot interfere with
    # official V4.2 messages.
    if (
        key == last_shadow_alert
        and (
            now
            - last_shadow_alert_time
        )
        < SHADOW_ALERT_COOLDOWN
    ):
        return

    last_shadow_alert = key
    last_shadow_alert_time = now

    message = (
        build_shadow_message(
            market,
            analysis,
            shadow
        )
    )

    send_telegram(
        message
    )

    print(
        message
    )


# =========================================================
# MAIN LOOP
# =========================================================

def main():
    global last_alert
    global last_shadow_alert
    global last_market
    global last_analysis

    print(
        "Kate's BTC Watcher V4.3 "
        "starting..."
    )

    print(
        "Stats database:",
        DB_PATH
    )

    init_db()
    load_logged_signal_keys()
    initialize_telegram_offset()

    send_telegram(
        "👁️ KATE'S BTC WATCHER V4.3 "
        "PRICE-ACTION SHADOW MODE "
        "IS ONLINE.\n\n"

        "Commands:\n"
        "/start — turn alerts ON\n"
        "/stop — turn alerts OFF\n"
        "/status — current read\n"
        "/last — last ENTER call\n"
        "/stats — official ENTER stats\n"
        "/shadowstats — GET READY stats\n\n"

        "🗣️ Official ENTER rules are "
        "still V4.2.\n"

        "👀 GET READY watches developing "
        "price action.\n"

        "🕐 Completed 1m candles are "
        "being saved.\n"

        "🧠 WAIT states are still being "
        "black-box recorded.\n"

        "💵 Scalp opportunities use the "
        "sellable bid.\n\n"

        "GET READY is research — "
        "not an ENTER call.\n\n"

        "I GOT EYES NOW 👁️😂"
    )

    while True:
        try:
            # ---------------------------------------------
            # TELEGRAM COMMANDS ALWAYS WORK
            # ---------------------------------------------

            check_telegram_commands()

            if not watcher_enabled:
                time.sleep(
                    CHECK_EVERY
                )
                continue

            # ---------------------------------------------
            # LIVE KALSHI CONTRACT
            # ---------------------------------------------

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

            # ---------------------------------------------
            # NEW CONTRACT
            # ---------------------------------------------

            if ticker != last_market:
                last_market = ticker

                last_alert = None
                last_shadow_alert = None

                clear_retest()
                clear_active_shadow()

                send_telegram(
                    "🆕 NEW BTC "
                    "15-MIN CONTRACT\n\n"
                    f"{ticker}\n\n"
                    "I'm watching 👀"
                )

            # ---------------------------------------------
            # UPDATE OLD TRACKING FIRST
            # ---------------------------------------------

            update_open_signals(
                market
            )

            update_shadow_signals(
                market
            )

            finalize_old_signals(
                ticker
            )

            # ---------------------------------------------
            # FETCH COMPLETED 1M PRICE ACTION ONCE
            # ---------------------------------------------

            one_minute = (
                get_completed_1m_candles()
            )

            if not one_minute:
                time.sleep(
                    CHECK_EVERY
                )
                continue

            # Preserve the actual 1m sequence.
            save_one_minute_candles(
                ticker,
                one_minute
            )

            # ---------------------------------------------
            # OFFICIAL V4.2 ANALYSIS
            # ---------------------------------------------

            analysis = analyze_market(
                market,
                one_minute
            )

            if not analysis:
                time.sleep(
                    CHECK_EVERY
                )
                continue

            last_analysis = analysis

            # ---------------------------------------------
            # BLACK-BOX SNAPSHOT
            # ---------------------------------------------

            record_market_snapshot(
                market,
                analysis
            )

            # ---------------------------------------------
            # V4.3 SHADOW PRICE-ACTION READ
            #
            # This happens AFTER the current market
            # snapshot is recorded so the database
            # keeps the raw observable state.
            # ---------------------------------------------

            shadow = (
                analyze_shadow_setup(
                    ticker,
                    analysis,
                    one_minute
                )
            )

            analysis[
                "shadow"
            ] = shadow

            # If an existing formation flips,
            # finalize it before creating the
            # new opposite formation.
            update_active_shadow_state(
                ticker,
                shadow
            )

            if shadow:
                create_shadow_signal(
                    ticker,
                    analysis,
                    shadow
                )

                maybe_send_shadow_alert(
                    market,
                    analysis,
                    shadow
                )

            # ---------------------------------------------
            # RECORD REAL OFFICIAL ENTER
            # ---------------------------------------------

            record_enter_signal(
                ticker,
                analysis
            )

            # Link a prior GET READY formation
            # to the later official ENTER.
            mark_shadow_official_enter(
                ticker,
                analysis
            )

            # ---------------------------------------------
            # OFFICIAL ALERT CHANNEL
            # ---------------------------------------------

            signal_key = (
                ticker,
                analysis[
                    "action"
                ],
                analysis[
                    "setup"
                ],
                analysis[
                    "scalp"
                ],
                (
                    analysis[
                        "candle_time"
                    ]
                    if analysis[
                        "confirmed"
                    ]
                    else None
                )
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

                print(
                    message
                )

            time.sleep(
                CHECK_EVERY
            )

        except Exception as e:
            print(
                "Monitor error:",
                e
            )

            time.sleep(15)


if __name__ == "__main__":
    main()
