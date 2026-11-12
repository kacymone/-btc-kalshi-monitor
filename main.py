import os
import time
import sqlite3
import requests
import json
from datetime import datetime, timezone

# =========================================================
# SETTINGS
# =========================================================

KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
SERIES = "KXBTC15M"

KRAKEN_OHLC = "https://api.kraken.com/0/public/OHLC"
KRAKEN_TICKER = "https://api.kraken.com/0/public/Ticker"
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

# V4.3.1 fast-brain gates. These are experimental and logged for review.
GET_READY_SCORE = 4
STRENGTHEN_SCORE = 6
EARLY_ENTER_SCORE = 7
EARLY_ENTER_MARGIN = 4
GET_READY_MAX_PRICE = 0.88
GET_READY_MIN_SECONDS = 180

# Trader Mode: position-facing updates after a bot-issued ENTER.
POSITION_UPDATE_COOLDOWN = 60
POSITION_HISTORY_SECONDS = 180

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
last_shadow_alert = None
last_market = None
last_analysis = None

# V4.3.1 remembers the developing setup inside each 15m contract.
formation_state = {
    "ticker": None, "direction": None, "stage": "WATCH",
    "first_score": 0, "peak_score": 0, "started_at": None,
    "entry_sent": False, "last_notice": None
}

# Once the bot itself calls ENTER, Trader Mode treats that side as the active
# thesis. Shadow noise still feeds the brain, but Telegram becomes position-facing.
active_position = {
    "ticker": None, "side": None, "entry_price": None, "entered_at": None,
    "entry_btc": None, "entry_target_distance": None, "best_bid": None,
    "last_status": None, "last_sent_at": 0, "source_setup": None
}

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
                yes_bid REAL, yes_ask REAL, no_bid REAL, no_ask REAL,
                yes_spread REAL, no_spread REAL,
                support REAL, resistance REAL, trend TEXT,
                distance_to_support REAL, distance_to_resistance REAL,
                candle_time INTEGER, candle_color TEXT,
                candle_open REAL, candle_high REAL, candle_low REAL, candle_close REAL,
                near_support INTEGER, near_resistance INTEGER,
                support_rejection INTEGER, resistance_rejection INTEGER,
                breakout_up INTEGER, breakout_down INTEGER,
                retest_up INTEGER, retest_down INTEGER, armed_direction TEXT,
                target_cross TEXT, target_velocity REAL,
                action TEXT, setup TEXT, direction TEXT, confirmed INTEGER, scalp INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS one_minute_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                candle_time INTEGER NOT NULL,
                captured_at INTEGER NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                body_ratio REAL,
                up_count_3 INTEGER, down_count_3 INTEGER,
                higher_lows INTEGER, lower_highs INTEGER,
                rising_closes INTEGER, falling_closes INTEGER,
                UNIQUE(ticker, candle_time)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS shadow_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shadow_key TEXT UNIQUE NOT NULL,
                ticker TEXT NOT NULL,
                direction TEXT NOT NULL,
                detected_at INTEGER NOT NULL,
                seconds_left INTEGER,
                btc_price REAL,
                contract_target REAL,
                target_distance REAL,
                entry_ask REAL,
                entry_bid REAL,
                score INTEGER,
                reasons TEXT,
                candle_time INTEGER,
                official_enter INTEGER DEFAULT 0,
                best_bid REAL,
                worst_bid REAL,
                hit_3c INTEGER DEFAULT 0,
                hit_5c INTEGER DEFAULT 0,
                hit_10c INTEGER DEFAULT 0,
                finalized INTEGER DEFAULT 0,
                last_updated INTEGER
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_shadow_ticker_time
            ON shadow_signals(ticker, detected_at)
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

    # Entry cost is the ask, but the price we could actually sell into
    # at that moment is the bid. Keep those separate so scalp stats do
    # not pretend the entry ask was already an achievable exit price.
    if analysis["direction"] == "UP":
        initial_exit_bid = analysis.get("yes_bid")
    else:
        initial_exit_bid = analysis.get("no_bid")

    if initial_exit_bid is not None:
        initial_exit_bid = float(initial_exit_bid)

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
                initial_exit_bid,
                initial_exit_bid,
                initial_exit_bid,
                now
            ))
            conn.commit()

        logged_signal_keys.add(signal_key)
        print("Logged ENTER signal:", signal_key)

    except Exception as e:
        print("Record signal error:", e)


def side_bid_for_signal(side, market):
    # Entry is recorded from the ask; post-entry opportunity is measured
    # from the bid, which is the side available to sell into.
    if side == "UP":
        return money(market.get("yes_bid_dollars"))
    return money(market.get("no_bid_dollars"))


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
                current_price = side_bid_for_signal(
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


TELEGRAM_KEYBOARD = {
    "keyboard": [
        [{"text": "👀 STATUS"}, {"text": "📊 STATS"}],
        [{"text": "🕒 LAST"}, {"text": "🧠 MARKET READ"}],
        [{"text": "🟢 START"}, {"text": "🔴 STOP"}],
    ],
    "resize_keyboard": True,
    "is_persistent": True,
    "input_field_placeholder": "Tap a watcher control…",
}


def send_telegram(message, show_keyboard=True):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram variables missing.")
        return

    try:
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
        }
        if show_keyboard:
            data["reply_markup"] = json.dumps(TELEGRAM_KEYBOARD)

        response = requests.post(
            telegram_url("sendMessage"),
            data=data,
            timeout=10
        )
        response.raise_for_status()
    except Exception as e:
        print("Telegram send error:", e)


def register_telegram_commands():
    """Populate Telegram's built-in slash-command menu as a backup to the tap buttons."""
    if not TELEGRAM_TOKEN:
        return

    commands = [
        {"command": "start", "description": "Start the watcher"},
        {"command": "stop", "description": "Stop the watcher"},
        {"command": "status", "description": "Current market read"},
        {"command": "stats", "description": "Performance stats"},
        {"command": "last", "description": "Last ENTER call"},
    ]

    try:
        response = requests.post(
            telegram_url("setMyCommands"),
            data={"commands": json.dumps(commands)},
            timeout=10,
        )
        response.raise_for_status()
    except Exception as e:
        print("Telegram command-menu registration error:", e)


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

            # Persistent keyboard buttons are shortcuts for the same slash commands.
            button_commands = {
                "🔴 stop": "/stop",
                "🟢 start": "/start",
                "👀 status": "/status",
                "📊 stats": "/stats",
                "🕒 last": "/last",
                "🧠 market read": "/status",
            }
            text = button_commands.get(text, text)

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


def synthesize_market_briefing(ticker, analysis):
    """One shared, evidence-labeled read for the active contract."""
    memory = contract_case_file(ticker, analysis)
    settlement = settlement_outlook({"ticker": ticker}, analysis, memory)
    thesis = memory.get("thesis", "NEUTRAL")
    strength = memory.get("strength", "WEAK")
    trajectory = memory.get("trajectory", "FLAT")
    w60 = memory.get("w60", {})
    distance = analysis.get("target_distance")
    seconds = int(analysis.get("seconds_left") or 0)
    side = settlement.get("side", "UNKNOWN")
    ask = analysis.get("yes_ask") if side == "UP" else analysis.get("no_ask")
    bid = analysis.get("yes_bid") if side == "UP" else analysis.get("no_bid")
    delta = w60.get("btc_delta", 0)
    observations = []
    if w60.get("samples", 0) >= 2:
        observations.append(f"BTC {'rose' if delta > 0 else 'fell' if delta < 0 else 'was flat'} ${abs(delta):.2f} across the observed last-minute window")
    if distance is not None:
        observations.append(f"BTC is ${abs(distance):.2f} {'above' if distance >= 0 else 'below'} target")
    if trajectory != "FLAT":
        observations.append(f"target cushion trajectory: {trajectory.lower()}")
    crosses = memory.get("crosses_up", 0) + memory.get("crosses_down", 0)
    if crosses:
        observations.append(f"{crosses} recorded target crosses this contract")
    conflicts = (thesis in ("UP", "DOWN") and side in ("UP", "DOWN") and thesis != side)
    if conflicts:
        interpretation = f"MIXED: contract pressure favors {thesis}, but BTC is currently on the {side} side of target."
    elif thesis == "NEUTRAL":
        interpretation = "No clear contract-wide directional thesis yet."
    else:
        interpretation = f"Contract pressure favors {thesis} ({strength.lower()}); recent trajectory is {trajectory.lower()}."
    if seconds <= 0:
        action = "Contract timer expired; verify official settlement."
    elif distance is None:
        action = "WAIT: settlement target unavailable; don't infer a settlement edge."
    elif conflicts:
        action = "WAIT: conflicting evidence. Watch for a sustained target reclaim or rejection."
    elif ask is not None and ask >= .90:
        action = f"NO NEW CHASE: {side} costs {ask*100:.1f}¢; limited gross upside before fees."
    elif thesis == "NEUTRAL" or memory.get("samples", 0) < 3:
        action = "WAIT: need more contract observations and a clean entry trigger."
    else:
        action = f"WATCH {thesis}: thesis is developing, but this briefing alone is NOT an ENTER call."
    triggers = []
    if analysis.get("contract_target") is not None:
        triggers.append(f"Target ${analysis['contract_target']:,.2f}: watch crosses, holds and failed reclaims")
    btc_now = analysis.get("live_btc_price") or analysis.get("btc_price")
    for key, label in (("support", "Support"), ("resistance", "Resistance")):
        level = analysis.get(key)
        if level is not None and btc_now is not None and abs(level - btc_now) <= 150:
            triggers.append(f"{label} ${level:,.2f}: monitor a sustained test, not one tick")
    if w60.get("samples", 0) < 2:
        interpretation += " Recent-window evidence is insufficient."
    return {"memory": memory, "settlement": settlement, "observations": observations,
            "interpretation": interpretation, "action": action, "bid": bid,
            "ask": ask, "conflicts": conflicts, "triggers": triggers}


def send_status():
    if not watcher_enabled:
        send_telegram("💤 WATCHER STATUS: OFF\n\nSend /start when you're ready.")
        return
    if not last_analysis or not last_market:
        send_telegram("🟢 WATCHER STATUS: ON\n\nWaiting for a current market snapshot.")
        return
    analysis = last_analysis
    ticker = last_market
    briefing = synthesize_market_briefing(ticker, analysis)
    memory = briefing["memory"]
    settlement = briefing["settlement"]
    seconds = max(0, int(analysis.get("seconds_left") or 0))
    last_seen = memory.get("last_captured_at")
    age = int(time.time()) - int(last_seen) if last_seen else None
    freshness = f"Data age: {age}s" if age is not None else "Data freshness: UNKNOWN"
    if age is None or age > max(45, CHECK_EVERY * 3):
        freshness += " ⚠️ STALE — do not act on this briefing"
    observations = "\n".join("• " + item for item in briefing["observations"]) or "• Not enough history yet"
    dist = analysis.get("target_distance")
    cushion = f"${abs(dist):,.2f} {'above' if dist >= 0 else 'below'} target" if dist is not None else "unknown"
    trigger_text = "\n".join("• " + t for t in briefing["triggers"]) or "• Levels unavailable"
    position_text = ""
    if active_position.get("ticker") == ticker:
        position_text = f"\n💰 Monitored bot-call position: {active_position.get('side')} (not verified as your actual trade)."
    send_telegram(
        "👀 LIVE CONTRACT BRIEFING\n"
        f"{freshness}\n"
        f"⏱ {seconds//60}:{seconds%60:02d} left\n\n"
        f"🧠 CONTRACT THESIS: {memory.get('thesis', 'NEUTRAL')} — {memory.get('strength', 'WEAK')}\n"
        f"{briefing['interpretation']}\n\n"
        f"📍 OBSERVED\n{observations}\n"
        f"BTC: ${analysis.get('live_btc_price') or analysis.get('btc_price') or 0:,.2f}\n"
        f"UP ask: {analysis.get('yes_ask')} | DOWN ask: {analysis.get('no_ask')}\n\n"
        f"🏁 SETTLEMENT: {settlement.get('side')} — {settlement.get('strength')}\n"
        f"Cushion: {cushion}\n"
        f"Reversal difficulty: {settlement.get('reversal_difficulty')} (heuristic, uncalibrated)\n\n"
        f"🎯 NEXT: {briefing['action']}\n"
        f"📍 WATCH LEVELS\n{trigger_text}\n"
        f"Official entry signal: {analysis.get('action')}\n"
        f"{position_text}"
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


def get_live_btc_price():
    """Fast BTC price for V4.3.1 shadow logic only. Official V4.2 stays on completed candles."""
    response = requests.get(KRAKEN_TICKER, params={"pair": KRAKEN_PAIR}, timeout=10)
    response.raise_for_status()
    data = response.json()
    if data.get("error"):
        raise Exception(f"Kraken ticker error: {data['error']}")
    result = data.get("result", {})
    for value in result.values():
        try:
            return float(value["c"][0])
        except Exception:
            continue
    return None


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

    now = int(time.time())
    completed_1m = [c for c in one_minute if c["time"] + 60 <= now]

    if len(candles) < 3 or len(completed_1m) < 4:
        return None

    recent_1m = completed_1m[-4:]
    last_1m = recent_1m[-1]
    one_minute_up_count = sum(1 for c in recent_1m[-3:] if c["close"] > c["open"])
    one_minute_down_count = sum(1 for c in recent_1m[-3:] if c["close"] < c["open"])
    higher_lows_1m = all(recent_1m[i]["low"] >= recent_1m[i-1]["low"] for i in range(1, len(recent_1m)))
    lower_highs_1m = all(recent_1m[i]["high"] <= recent_1m[i-1]["high"] for i in range(1, len(recent_1m)))
    rising_closes_1m = all(recent_1m[i]["close"] > recent_1m[i-1]["close"] for i in range(1, len(recent_1m)))
    falling_closes_1m = all(recent_1m[i]["close"] < recent_1m[i-1]["close"] for i in range(1, len(recent_1m)))
    last_1m_body = abs(last_1m["close"] - last_1m["open"])
    last_1m_range = max(0.01, last_1m["high"] - last_1m["low"])
    last_1m_body_ratio = last_1m_body / last_1m_range

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
        "candle_open": current["open"],
        "candle_high": current["high"],
        "candle_low": current["low"],
        "candle_close": current["close"],
        "candle_color": "GREEN" if green else "RED" if red else "FLAT",

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

        "armed_direction": retest_state["direction"],

        # V4.3 shadow-mode eyesight. These do NOT change official ENTER rules.
        "one_minute_time": last_1m["time"],
        "one_minute_open": last_1m["open"],
        "one_minute_high": last_1m["high"],
        "one_minute_low": last_1m["low"],
        "one_minute_close": last_1m["close"],
        "one_minute_body_ratio": last_1m_body_ratio,
        "one_minute_up_count": one_minute_up_count,
        "one_minute_down_count": one_minute_down_count,
        "higher_lows_1m": higher_lows_1m,
        "lower_highs_1m": lower_highs_1m,
        "rising_closes_1m": rising_closes_1m,
        "falling_closes_1m": falling_closes_1m
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
        "candle_time": pa["candle_time"],
        "candle_open": pa["candle_open"],
        "candle_high": pa["candle_high"],
        "candle_low": pa["candle_low"],
        "candle_close": pa["candle_close"],
        "candle_color": pa["candle_color"],
        "near_support": pa["near_support"],
        "near_resistance": pa["near_resistance"],
        "support_rejection": pa["support_rejection"],
        "resistance_rejection": pa["resistance_rejection"],
        "breakout_up": pa["breakout_up"],
        "breakout_down": pa["breakout_down"],
        "retest_up": pa["retest_up"],
        "retest_down": pa["retest_down"],
        "armed_direction": pa["armed_direction"],
        "one_minute_time": pa["one_minute_time"],
        "one_minute_open": pa["one_minute_open"],
        "one_minute_high": pa["one_minute_high"],
        "one_minute_low": pa["one_minute_low"],
        "one_minute_close": pa["one_minute_close"],
        "one_minute_body_ratio": pa["one_minute_body_ratio"],
        "one_minute_up_count": pa["one_minute_up_count"],
        "one_minute_down_count": pa["one_minute_down_count"],
        "higher_lows_1m": pa["higher_lows_1m"],
        "lower_highs_1m": pa["lower_highs_1m"],
        "rising_closes_1m": pa["rising_closes_1m"],
        "falling_closes_1m": pa["falling_closes_1m"]
    }


# =========================================================
# V4.3 PRICE-ACTION SHADOW BRAIN
# =========================================================

def get_previous_snapshot(ticker):
    try:
        with db_connect() as conn:
            return conn.execute("""
                SELECT * FROM market_snapshots
                WHERE ticker = ?
                ORDER BY captured_at DESC, id DESC
                LIMIT 1
            """, (ticker,)).fetchone()
    except Exception:
        return None


def record_one_minute_snapshot(market, analysis):
    ticker = market.get("ticker")
    candle_time = analysis.get("one_minute_time")
    if not ticker or candle_time is None:
        return
    try:
        with db_connect() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO one_minute_snapshots (
                    ticker,candle_time,captured_at,open,high,low,close,body_ratio,
                    up_count_3,down_count_3,higher_lows,lower_highs,rising_closes,falling_closes
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ticker,candle_time,int(time.time()),analysis.get("one_minute_open"),
                analysis.get("one_minute_high"),analysis.get("one_minute_low"),analysis.get("one_minute_close"),
                analysis.get("one_minute_body_ratio"),analysis.get("one_minute_up_count"),
                analysis.get("one_minute_down_count"),int(bool(analysis.get("higher_lows_1m"))),
                int(bool(analysis.get("lower_highs_1m"))),int(bool(analysis.get("rising_closes_1m"))),
                int(bool(analysis.get("falling_closes_1m")))
            ))
            conn.commit()
    except Exception as e:
        print("1m snapshot error:", e)


def analyze_shadow_setup(market, analysis):
    """Score BOTH sides using fast BTC + completed 1m structure. Does not blindly enter on score alone."""
    ticker = market.get("ticker")
    previous = get_previous_snapshot(ticker) if ticker else None
    up_score = down_score = 0
    up_reasons, down_reasons = [], []

    memory = contract_case_file(ticker, analysis)
    if memory.get("thesis") == "UP":
        bonus = 2 if memory.get("strength") == "STRONG" else 1
        up_score += bonus; up_reasons.append(f"contract memory {memory.get('strength','').lower()} UP")
    elif memory.get("thesis") == "DOWN":
        bonus = 2 if memory.get("strength") == "STRONG" else 1
        down_score += bonus; down_reasons.append(f"contract memory {memory.get('strength','').lower()} DOWN")

    if analysis.get("rising_closes_1m"):
        up_score += 2; up_reasons.append("1m closes rising")
    if analysis.get("falling_closes_1m"):
        down_score += 2; down_reasons.append("1m closes falling")
    if analysis.get("higher_lows_1m"):
        up_score += 1; up_reasons.append("higher 1m lows")
    if analysis.get("lower_highs_1m"):
        down_score += 1; down_reasons.append("lower 1m highs")
    if analysis.get("one_minute_up_count", 0) >= 2:
        up_score += 1; up_reasons.append("green candle pressure")
    if analysis.get("one_minute_down_count", 0) >= 2:
        down_score += 1; down_reasons.append("red candle pressure")

    body_ratio = analysis.get("one_minute_body_ratio") or 0
    o, c = analysis.get("one_minute_open"), analysis.get("one_minute_close")
    if body_ratio >= .60 and o is not None and c is not None:
        if c > o: up_score += 1; up_reasons.append("strong bullish 1m body")
        elif c < o: down_score += 1; down_reasons.append("strong bearish 1m body")

    if analysis.get("near_support") or analysis.get("armed_direction") == "UP":
        up_score += 1; up_reasons.append("bullish structure area")
    if analysis.get("near_resistance") or analysis.get("armed_direction") == "DOWN":
        down_score += 1; down_reasons.append("bearish structure area")

    live = analysis.get("live_btc_price") or analysis.get("btc_price")
    target = analysis.get("contract_target")
    distance = (live - target) if live is not None and target is not None else None
    target_cross = None
    target_delta = None

    if previous and distance is not None and previous["target_distance"] is not None:
        old = float(previous["target_distance"]); target_delta = distance - old
        if target_delta >= 8:
            up_score += 2; up_reasons.append("moving toward/above target")
        elif target_delta <= -8:
            down_score += 2; down_reasons.append("moving toward/below target")
        if old <= 0 < distance:
            target_cross = "UP"; up_score += 2; up_reasons.append("BTC crossed target upward")
        elif old >= 0 > distance:
            target_cross = "DOWN"; down_score += 2; down_reasons.append("BTC crossed target downward")

        yb, nb = analysis.get("yes_bid"), analysis.get("no_bid")
        pyb, pnb = previous["yes_bid"], previous["no_bid"]
        if yb is not None and pyb is not None and float(yb)-float(pyb) >= .02:
            up_score += 1; up_reasons.append("UP bid rising")
        if nb is not None and pnb is not None and float(nb)-float(pnb) >= .02:
            down_score += 1; down_reasons.append("DOWN bid rising")

    if up_score >= GET_READY_SCORE and up_score >= down_score + 2:
        direction, score, opposite, reasons = "UP", up_score, down_score, up_reasons
    elif down_score >= GET_READY_SCORE and down_score >= up_score + 2:
        direction, score, opposite, reasons = "DOWN", down_score, up_score, down_reasons
    else:
        direction = None; score = max(up_score, down_score); opposite = min(up_score, down_score); reasons = []

    return {"direction": direction, "score": score, "opposite_score": opposite,
            "up_score": up_score, "down_score": down_score, "reasons": reasons,
            "live_btc_price": live, "live_target_distance": distance,
            "target_cross": target_cross, "target_delta": target_delta}


def reset_formation(ticker=None):
    formation_state.update({"ticker": ticker, "direction": None, "stage": "WATCH",
                            "first_score": 0, "peak_score": 0, "started_at": None,
                            "entry_sent": False, "last_notice": None})


def opportunity_ok(analysis, direction):
    ask = analysis.get("yes_ask") if direction == "UP" else analysis.get("no_ask")
    return ask is not None and ask < HARD_SKIP_PRICE and analysis.get("seconds_left", 0) >= MIN_SECONDS_FOR_ENTRY


def get_ready_actionable(analysis, direction):
    ask = analysis.get("yes_ask") if direction == "UP" else analysis.get("no_ask")
    return ask is not None and ask <= GET_READY_MAX_PRICE and analysis.get("seconds_left", 0) >= GET_READY_MIN_SECONDS


def strong_promotion(shadow, analysis):
    direction = shadow.get("direction")
    if not direction or shadow["score"] < EARLY_ENTER_SCORE:
        return False
    if shadow["score"] - shadow["opposite_score"] < EARLY_ENTER_MARGIN:
        return False
    if not opportunity_ok(analysis, direction):
        return False
    shared = analysis.get("shared_briefing") or {}
    mem = shared.get("memory") or {}
    if (mem.get("strength") == "STRONG" and mem.get("thesis") in ("UP", "DOWN")
            and mem.get("thesis") != direction and shadow.get("target_cross") != direction):
        return False
    reasons = " ".join(shadow.get("reasons", [])).lower()
    # Require a meaningful trigger, not merely a pile of correlated candle points.
    trigger = (shadow.get("target_cross") == direction or
               "bid rising" in reasons or
               (direction == "UP" and shadow.get("target_delta") is not None and shadow["target_delta"] >= 8) or
               (direction == "DOWN" and shadow.get("target_delta") is not None and shadow["target_delta"] <= -8))
    return bool(trigger)


# Contract-scoped alert memory; never affects historical snapshot recording.
formation_notice_memory = {"ticker": None, "last_ready": {"UP": 0, "DOWN": 0},
                           "fades": 0, "last_direction": None}
READY_COOLDOWN_SECONDS = 60
FADE_CONFIRM_SNAPSHOTS = 2

def quiet_ready_notice(ticker, direction):
    now = int(time.time())
    if formation_notice_memory["ticker"] != ticker:
        formation_notice_memory.update({"ticker": ticker, "last_ready": {"UP": 0, "DOWN": 0},
                                        "fades": 0, "last_direction": None})
    if now - formation_notice_memory["last_ready"].get(direction, 0) < READY_COOLDOWN_SECONDS:
        return False
    formation_notice_memory["last_ready"][direction] = now
    return True


def apply_fast_brain(market, analysis, shadow):
    """State machine: WATCH -> GET READY -> STRENGTHEN/FADING -> EARLY ENTER -> TRACK."""
    ticker = market.get("ticker")
    notices = []
    if formation_state["ticker"] != ticker:
        reset_formation(ticker)

    direction = shadow.get("direction") if shadow else None
    score = shadow.get("score", 0) if shadow else 0

    if formation_state["entry_sent"]:
        return notices

    old_dir = formation_state["direction"]
    if old_dir and direction != old_dir:
        old_score = shadow.get("down_score" if old_dir == "DOWN" else "up_score", 0) if shadow else 0
        opp_score = shadow.get("up_score" if old_dir == "DOWN" else "down_score", 0) if shadow else 0
        if old_score < GET_READY_SCORE or opp_score >= old_score:
            formation_notice_memory["fades"] += 1
        else:
            formation_notice_memory["fades"] = 0
        if formation_notice_memory["fades"] >= FADE_CONFIRM_SNAPSHOTS:
            formation_notice_memory["fades"] = 0
            # Quietly retire a transient formation. This is NOT a contract-thesis reversal.
            # The state machine and database continue updating; no noisy Telegram fade alert.
            reset_formation(ticker)

    if direction == old_dir:
        formation_notice_memory["fades"] = 0
    if not direction:
        return notices

    if formation_state["direction"] is None:
        formation_state.update({"direction": direction, "stage": "GET_READY", "first_score": score,
                                "peak_score": score, "started_at": int(time.time())})
        if get_ready_actionable(analysis, direction) and quiet_ready_notice(ticker, direction):
            notices.append(build_shadow_message(analysis, shadow))
    elif formation_state["direction"] == direction:
        old_peak = formation_state["peak_score"]
        formation_state["peak_score"] = max(old_peak, score)
        if score >= STRENGTHEN_SCORE and old_peak < STRENGTHEN_SCORE and get_ready_actionable(analysis, direction):
            arrow = "🟢" if direction == "UP" else "🔴"
            notices.append(f"🔥 {direction} SETUP STRENGTHENING {arrow}\n\nScore: {score}-{shadow['opposite_score']}\nStill watching for enough evidence to ENTER.")

    if strong_promotion(shadow, analysis):
        direction = shadow["direction"]
        ask = analysis.get("yes_ask") if direction == "UP" else analysis.get("no_ask")
        analysis["direction"] = direction
        analysis["confirmed"] = True
        analysis["setup"] = "EARLY PRICE ACTION"
        analysis["entry_price"] = ask
        analysis["scalp"] = bool(ask is not None and ask >= SCALP_PRICE)
        analysis["action"] = f"🗣️ ENTER {direction} " + ("🟢" if direction == "UP" else "🔴")
        analysis["reason"] = "Fast brain promoted the developing setup before the completed 3m confirmation. " + "; ".join(shadow.get("reasons", [])[:5])
        formation_state["entry_sent"] = True
        formation_state["stage"] = "TRACK"
        notices = []  # ENTER replaces any same-loop GET READY/STRENGTHEN message.

    return notices


def record_shadow_signal(market, analysis, shadow):
    if not shadow:
        return
    ticker = market.get("ticker")
    direction = shadow["direction"]
    candle_time = analysis.get("one_minute_time") or analysis.get("candle_time")
    shadow_key = f"{ticker}|{direction}|{candle_time}"
    ask = analysis.get("yes_ask") if direction == "UP" else analysis.get("no_ask")
    bid = analysis.get("yes_bid") if direction == "UP" else analysis.get("no_bid")
    now = int(time.time())
    try:
        with db_connect() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO shadow_signals (
                    shadow_key,ticker,direction,detected_at,seconds_left,btc_price,
                    contract_target,target_distance,entry_ask,entry_bid,score,reasons,
                    candle_time,best_bid,worst_bid,last_updated
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                shadow_key,ticker,direction,now,analysis.get("seconds_left"),analysis.get("btc_price"),
                analysis.get("contract_target"),analysis.get("target_distance"),ask,bid,shadow["score"],
                "; ".join(shadow["reasons"]),candle_time,bid,bid,now
            ))
            conn.commit()
    except Exception as e:
        print("Shadow signal record error:", e)


def update_shadow_signals(market, analysis):
    ticker = market.get("ticker")
    if not ticker:
        return
    now = int(time.time())
    try:
        with db_connect() as conn:
            rows = conn.execute("SELECT * FROM shadow_signals WHERE ticker=? AND finalized=0", (ticker,)).fetchall()
            for row in rows:
                bid = analysis.get("yes_bid") if row["direction"] == "UP" else analysis.get("no_bid")
                if bid is None:
                    continue
                best = float(bid) if row["best_bid"] is None else max(float(row["best_bid"]), float(bid))
                worst = float(bid) if row["worst_bid"] is None else min(float(row["worst_bid"]), float(bid))
                ask = row["entry_ask"]
                hit3 = row["hit_3c"]; hit5 = row["hit_5c"]; hit10 = row["hit_10c"]
                if ask is not None:
                    hit3 = 1 if best >= float(ask)+0.03 else hit3
                    hit5 = 1 if best >= float(ask)+0.05 else hit5
                    hit10 = 1 if best >= float(ask)+0.10 else hit10
                official = int(row["official_enter"] or 0)
                if analysis.get("confirmed") and analysis.get("direction") == row["direction"]:
                    official = 1
                conn.execute("""UPDATE shadow_signals SET best_bid=?,worst_bid=?,hit_3c=?,hit_5c=?,hit_10c=?,official_enter=?,last_updated=? WHERE id=?""",
                             (best,worst,hit3,hit5,hit10,official,now,row["id"]))
            conn.commit()
    except Exception as e:
        print("Shadow tracking error:", e)


def finalize_old_shadow_signals(current_ticker):
    try:
        with db_connect() as conn:
            conn.execute("UPDATE shadow_signals SET finalized=1 WHERE ticker != ? AND finalized=0", (current_ticker,))
            conn.commit()
    except Exception as e:
        print("Finalize shadow error:", e)


def build_shadow_message(analysis, shadow):
    direction = shadow["direction"]
    ask = analysis.get("yes_ask") if direction == "UP" else analysis.get("no_ask")
    arrow = "🟢" if direction == "UP" else "🔴"
    reasons = ", ".join(shadow["reasons"][:4])
    return (
        f"👀 GET READY {direction} {arrow}\n\n"
        f"Shadow score: {shadow['score']}\n"
        f"BTC: ${analysis['btc_price']:,.2f}\n"
        f"{direction} ask: {ask}\n"
        f"Time left: {analysis['seconds_left']//60}:{analysis['seconds_left']%60:02d}\n\n"
        f"Seeing: {reasons}.\n\n"
        "⚠️ EARLY FORMATION ONLY — NOT AN ENTER CALL."
    )



# =========================================================
# V4.5 CONTRACT MEMORY — CURRENT CONTRACT CASE FILE
# =========================================================

def contract_case_file(ticker, analysis=None):
    """Build an evolving case file from every snapshot in the active 15m contract."""
    if not ticker:
        return {}
    try:
        with db_connect() as conn:
            rows = conn.execute("""
                SELECT captured_at, seconds_left, btc_price, target_distance,
                       yes_bid, no_bid, target_cross, target_velocity
                FROM market_snapshots WHERE ticker=?
                ORDER BY captured_at ASC, id ASC
            """, (ticker,)).fetchall()
    except Exception as e:
        print("Contract memory error:", e); rows=[]
    if not rows:
        return {"samples":0,"thesis":"NEUTRAL","strength":"WEAK","trajectory":"FLAT"}
    now=int(time.time())
    def window(sec):
        rr=[r for r in rows if now-int(r["captured_at"])<=sec]
        if not rr: return {"btc_delta":0.0,"distance_delta":0.0,"yes_bid_delta":0.0,"no_bid_delta":0.0,"samples":0}
        a,b=rr[0],rr[-1]
        def d(k): return float(b[k])-float(a[k]) if a[k] is not None and b[k] is not None else 0.0
        return {"btc_delta":d("btc_price"),"distance_delta":d("target_distance"),"yes_bid_delta":d("yes_bid"),"no_bid_delta":d("no_bid"),"samples":len(rr)}
    w30,w60,w120,w180=window(30),window(60),window(120),window(180)
    ds=[float(r["target_distance"]) for r in rows if r["target_distance"] is not None]
    bs=[float(r["btc_price"]) for r in rows if r["btc_price"] is not None]
    current=ds[-1] if ds else None
    points=0
    for w,weight in ((w30,3),(w60,3),(w120,2),(w180,1)):
        if w["samples"]>=2:
            if w["btc_delta"]>12: points+=weight
            elif w["btc_delta"]<-12: points-=weight
            if w["yes_bid_delta"]>=.02: points+=max(1,weight//2)
            if w["no_bid_delta"]>=.02: points-=max(1,weight//2)
    if current is not None:
        points += 2 if current>50 else (-2 if current<-50 else (1 if current>0 else -1 if current<0 else 0))
    contract_move=(bs[-1]-bs[0]) if len(bs)>=2 else 0.0
    if contract_move>40: points+=1
    elif contract_move<-40: points-=1
    if points>=5: thesis,strength="UP","STRONG"
    elif points>=2: thesis,strength="UP","MODERATE"
    elif points<=-5: thesis,strength="DOWN","STRONG"
    elif points<=-2: thesis,strength="DOWN","MODERATE"
    else: thesis,strength="NEUTRAL","WEAK"
    trajectory="FLAT"
    if w60["distance_delta"]>15: trajectory="WIDENING UP"
    elif w60["distance_delta"]<-15: trajectory="WIDENING DOWN"
    elif w120["distance_delta"]>20: trajectory="DRIFTING UP"
    elif w120["distance_delta"]<-20: trajectory="DRIFTING DOWN"
    return {"last_captured_at":int(rows[-1]["captured_at"]),"samples":len(rows),"thesis":thesis,"strength":strength,"points":points,"current_distance":current,
            "max_above":max(ds) if ds else None,"max_below":min(ds) if ds else None,
            "crosses_up":sum(1 for r in rows if r["target_cross"]=="UP"),
            "crosses_down":sum(1 for r in rows if r["target_cross"]=="DOWN"),
            "contract_btc_move":contract_move,"contract_range":max(bs)-min(bs) if bs else 0.0,
            "trajectory":trajectory,"w30":w30,"w60":w60,"w120":w120,"w180":w180}

def settlement_outlook(market, analysis, memory=None):
    """Evidence-based expiration read: cushion + time + movement actually observed this contract."""
    memory=memory or contract_case_file(market.get("ticker"),analysis)
    seconds=int(analysis.get("seconds_left") or 0); distance=analysis.get("target_distance")
    if distance is None: return {"side":"UNKNOWN","strength":"UNKNOWN","reversal_difficulty":"UNKNOWN"}
    distance=float(distance); side="UP" if distance>=0 else "DOWN"; cushion=abs(distance)
    req_per_min=cushion/max(seconds/60.0,1/60)
    recent=max(abs(memory.get("w30",{}).get("btc_delta",0))*2,abs(memory.get("w60",{}).get("btc_delta",0)),abs(memory.get("w120",{}).get("btc_delta",0))/2,1.0)
    ratio=req_per_min/recent; traj=memory.get("trajectory","FLAT")
    helping=(side=="UP" and "UP" in traj) or (side=="DOWN" and "DOWN" in traj)
    hurting=(side=="UP" and "DOWN" in traj) or (side=="DOWN" and "UP" in traj)
    if seconds<=60 and cushion>=recent*1.5 and not hurting: strength="VERY STRONG"
    elif ratio>=1.4 and not hurting: strength="STRONG"
    elif ratio>=.75 or helping: strength="MODERATE"
    else: strength="WEAK"
    difficulty="HIGH" if ratio>=1.5 else "MODERATE" if ratio>=.75 else "LOW"
    return {"side":side,"strength":strength,"cushion":cushion,"required_per_min":req_per_min,
            "recent_move_per_min":recent,"reversal_difficulty":difficulty,"trajectory":traj}

# =========================================================
# V4.4 TRADER MODE — USE HISTORY, DON'T JUST STORE IT
# =========================================================

def clear_active_position():
    active_position.update({
        "ticker": None, "side": None, "entry_price": None, "entered_at": None,
        "entry_btc": None, "entry_target_distance": None, "best_bid": None,
        "last_status": None, "last_sent_at": 0, "source_setup": None
    })


def activate_position_from_enter(ticker, analysis):
    if not analysis.get("confirmed") or not str(analysis.get("action", "")).startswith("🗣️ ENTER"):
        return False
    side = analysis.get("direction")
    if side not in ("UP", "DOWN") or analysis.get("entry_price") is None:
        return False
    # Do not restart the same position every loop when the old 3m confirmation repeats.
    if active_position.get("ticker") == ticker and active_position.get("side") == side:
        return False
    bid = analysis.get("yes_bid") if side == "UP" else analysis.get("no_bid")
    active_position.update({
        "ticker": ticker, "side": side, "entry_price": float(analysis["entry_price"]),
        "entered_at": int(time.time()), "entry_btc": analysis.get("live_btc_price") or analysis.get("btc_price"),
        "entry_target_distance": analysis.get("target_distance"), "best_bid": bid,
        "last_status": None, "last_sent_at": 0, "source_setup": analysis.get("setup")
    })
    return True


def recent_market_history(ticker, seconds=POSITION_HISTORY_SECONDS):
    cutoff = int(time.time()) - seconds
    try:
        with db_connect() as conn:
            return conn.execute("""
                SELECT captured_at, btc_price, target_distance, target_velocity,
                       yes_bid, yes_ask, no_bid, no_ask, trend
                FROM market_snapshots
                WHERE ticker=? AND captured_at>=?
                ORDER BY captured_at ASC, id ASC
            """, (ticker, cutoff)).fetchall()
    except Exception as e:
        print("Trader history error:", e)
        return []


def trader_position_read(market, analysis, shadow):
    if active_position.get("ticker") != market.get("ticker"):
        return None
    side = active_position.get("side")
    if side not in ("UP", "DOWN"):
        return None

    now = int(time.time())
    seconds_left = int(analysis.get("seconds_left") or 0)
    live = analysis.get("live_btc_price") or analysis.get("btc_price")
    target = analysis.get("contract_target")
    distance = (live - target) if live is not None and target is not None else analysis.get("target_distance")
    signed_cushion = distance if side == "UP" else (-distance if distance is not None else None)
    bid = analysis.get("yes_bid") if side == "UP" else analysis.get("no_bid")
    ask = analysis.get("yes_ask") if side == "UP" else analysis.get("no_ask")
    opp_bid = analysis.get("no_bid") if side == "UP" else analysis.get("yes_bid")
    if bid is not None:
        b = float(bid)
        old_best = active_position.get("best_bid")
        active_position["best_bid"] = b if old_best is None else max(float(old_best), b)

    hist = recent_market_history(market.get("ticker"))
    shared = analysis.get("shared_briefing") or synthesize_market_briefing(market.get("ticker"), analysis)
    memory = shared["memory"]
    settlement_market = shared["settlement"]
    btc_delta = 0.0
    cushion_delta = 0.0
    elapsed = 0
    if hist and live is not None:
        first = hist[0]
        if first["btc_price"] is not None:
            btc_delta = float(live) - float(first["btc_price"])
        if distance is not None and first["target_distance"] is not None:
            cushion_delta = float(distance) - float(first["target_distance"])
        elapsed = max(1, now - int(first["captured_at"]))

    # Signed momentum is positive when it helps the held side.
    side_momentum = btc_delta if side == "UP" else -btc_delta
    side_cushion_change = cushion_delta if side == "UP" else -cushion_delta
    speed_against = max(0.0, -side_momentum / max(1, elapsed))
    projected_adverse = speed_against * seconds_left

    own_score = shadow.get("up_score" if side == "UP" else "down_score", 0) if shadow else 0
    opp_score = shadow.get("down_score" if side == "UP" else "up_score", 0) if shadow else 0

    # Overall lean deliberately synthesizes history + current evidence.
    lean_points = 0
    if signed_cushion is not None:
        lean_points += 2 if signed_cushion > 50 else (1 if signed_cushion > 0 else -3)
    lean_points += 1 if side_momentum > 20 else (-1 if side_momentum < -20 else 0)
    lean_points += 1 if side_cushion_change > 15 else (-1 if side_cushion_change < -15 else 0)
    lean_points += 1 if own_score >= opp_score + 2 else (-1 if opp_score >= own_score + 2 else 0)
    if side == "UP" and analysis.get("near_support"): lean_points += 1
    if side == "DOWN" and analysis.get("near_resistance"): lean_points += 1
    if memory.get("thesis") == side:
        lean_points += 2 if memory.get("strength") == "STRONG" else 1
    elif memory.get("thesis") not in (None, "NEUTRAL", side):
        lean_points -= 2 if memory.get("strength") == "STRONG" else 1

    if lean_points >= 3:
        overall = f"{side} LEAN — STRONG"
    elif lean_points >= 1:
        overall = f"{side} LEAN — MODERATE"
    elif lean_points >= 0:
        overall = "TOSS-UP"
    else:
        overall = f"{'DOWN' if side == 'UP' else 'UP'} LEAN"

    # V4.5 settlement brain is contract-wide and independent of entry/scalp quality.
    settle_side = settlement_market.get("side", "UNKNOWN")
    settle_strength = settlement_market.get("strength", "UNKNOWN")
    settlement = f"{settle_side} — {settle_strength}"

    entry = active_position.get("entry_price")
    scalp_cents = None
    if bid is not None and entry is not None:
        scalp_cents = round((float(bid) - float(entry)) * 100, 1)

    target_lost = signed_cushion is not None and signed_cushion <= 0
    hard_opposition = opp_score >= max(5, own_score + 3)
    sustained_against = side_momentum < -25 and side_cushion_change < -20
    structure_broken = (side == "UP" and live is not None and analysis.get("support") is not None and live < analysis["support"]) or \
                       (side == "DOWN" and live is not None and analysis.get("resistance") is not None and live > analysis["resistance"])

    if (target_lost and (hard_opposition or sustained_against)) or structure_broken:
        status = "EXIT"
    elif hard_opposition or sustained_against or settle_side != side and settle_strength in ("STRONG", "VERY STRONG") and side_momentum < -15:
        status = "PROTECT"
    else:
        status = "HOLD"

    if scalp_cents is None:
        scalp_read = "No clean sellable-bid P/L read yet."
    elif scalp_cents >= 5:
        scalp_read = f"+{scalp_cents:.1f}¢ available at the sellable bid — profit is on the table."
    elif scalp_cents >= 2:
        scalp_read = f"+{scalp_cents:.1f}¢ at the sellable bid — small scalp available."
    elif scalp_cents >= 0:
        scalp_read = f"{scalp_cents:+.1f}¢ at the sellable bid — little scalp cushion."
    else:
        scalp_read = f"{scalp_cents:.1f}¢ vs entry at the sellable bid — position is below entry."

    if side_momentum > 20:
        momentum_text = f"BTC moved ${abs(side_momentum):,.0f} in favor of {side} over the recent window."
    elif side_momentum < -20:
        momentum_text = f"BTC moved ${abs(side_momentum):,.0f} against {side} over the recent window."
    else:
        momentum_text = "Recent BTC movement is choppy/flat rather than decisive."

    if status == "EXIT":
        header = f"🚨 EXIT {side}"
        action = "The original thesis is materially invalidated."
    elif status == "PROTECT":
        header = f"⚠️ {side} WEAKENING — PROTECT PROFIT"
        action = "Opposing pressure is becoming meaningful; don't treat the original ENTER as unchanged."
    else:
        header = f"📡 HOLD {side} {'🟢' if side == 'UP' else '🔴'}"
        action = "The position thesis is still intact."

    mins, secs = divmod(max(0, seconds_left), 60)
    cushion_text = "n/a" if signed_cushion is None else f"${signed_cushion:+,.2f} for {side}"
    return {
        "status": status,
        "message": (
            f"{header}\n\n"
            f"Overall read: {overall}\n"
            f"⏱ Time left: {mins}:{secs:02d}\n"
            f"🎯 Target cushion: {cushion_text}\n"
            f"📈 Momentum: {momentum_text}\n\n"
            f"💰 Scalp read: {scalp_read}\n"
            f"⏳ Settlement read: {settlement} | reversal difficulty {settlement_market.get('reversal_difficulty','UNKNOWN')}\n"
            f"🧠 Contract thesis: {memory.get('thesis','NEUTRAL')} — {memory.get('strength','WEAK')} | {memory.get('trajectory','FLAT')}\n\n"
            f"{action}"
        )
    }


def maybe_send_position_update(market, analysis, shadow, force=False):
    read = trader_position_read(market, analysis, shadow)
    if not read:
        return False
    now = int(time.time())
    changed = read["status"] != active_position.get("last_status")
    due = now - int(active_position.get("last_sent_at") or 0) >= POSITION_UPDATE_COOLDOWN
    if force or changed or due:
        send_telegram(read["message"])
        active_position["last_status"] = read["status"]
        active_position["last_sent_at"] = now
    if read["status"] == "EXIT":
        clear_active_position()
        reset_formation(market.get("ticker"))
    return True

# =========================================================
# V4.3 BLACK-BOX SNAPSHOT RECORDER
# =========================================================

def record_market_snapshot(market, analysis):
    """Save the observable market state every monitor loop without changing signals."""
    ticker = market.get("ticker")
    if not ticker or not analysis:
        return

    now = int(time.time())
    yes_bid, yes_ask = analysis.get("yes_bid"), analysis.get("yes_ask")
    no_bid, no_ask = analysis.get("no_bid"), analysis.get("no_ask")
    yes_spread = (yes_ask - yes_bid) if yes_ask is not None and yes_bid is not None else None
    no_spread = (no_ask - no_bid) if no_ask is not None and no_bid is not None else None
    btc = analysis.get("live_btc_price") or analysis.get("btc_price")
    support, resistance = analysis.get("support"), analysis.get("resistance")

    try:
        with db_connect() as conn:
            previous = conn.execute("""
                SELECT target_distance, captured_at
                FROM market_snapshots
                WHERE ticker = ?
                ORDER BY captured_at DESC, id DESC
                LIMIT 1
            """, (ticker,)).fetchone()

            target_cross = None
            target_velocity = None
            distance = analysis.get("target_distance")

            if previous and distance is not None and previous["target_distance"] is not None:
                old = float(previous["target_distance"])
                dt = max(1, now - int(previous["captured_at"]))
                target_velocity = (float(distance) - old) / dt
                if old <= 0 < distance:
                    target_cross = "UP"
                elif old >= 0 > distance:
                    target_cross = "DOWN"

            conn.execute("""
                INSERT INTO market_snapshots (
                    captured_at,ticker,seconds_left,btc_price,contract_target,
                    target_distance,target_distance_pct,target_position,
                    yes_bid,yes_ask,no_bid,no_ask,yes_spread,no_spread,
                    support,resistance,trend,distance_to_support,distance_to_resistance,
                    candle_time,candle_color,candle_open,candle_high,candle_low,candle_close,
                    near_support,near_resistance,support_rejection,resistance_rejection,
                    breakout_up,breakout_down,retest_up,retest_down,armed_direction,
                    target_cross,target_velocity,action,setup,direction,confirmed,scalp
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                now,ticker,analysis.get("seconds_left"),btc,analysis.get("contract_target"),
                distance,analysis.get("target_distance_pct"),analysis.get("target_position"),
                yes_bid,yes_ask,no_bid,no_ask,yes_spread,no_spread,
                support,resistance,analysis.get("trend"),
                (btc-support) if btc is not None and support is not None else None,
                (resistance-btc) if btc is not None and resistance is not None else None,
                analysis.get("candle_time"),analysis.get("candle_color"),analysis.get("candle_open"),
                analysis.get("candle_high"),analysis.get("candle_low"),analysis.get("candle_close"),
                int(bool(analysis.get("near_support"))),int(bool(analysis.get("near_resistance"))),
                int(bool(analysis.get("support_rejection"))),int(bool(analysis.get("resistance_rejection"))),
                int(bool(analysis.get("breakout_up"))),int(bool(analysis.get("breakout_down"))),
                int(bool(analysis.get("retest_up"))),int(bool(analysis.get("retest_down"))),
                analysis.get("armed_direction"),target_cross,target_velocity,
                analysis.get("action"),analysis.get("setup"),analysis.get("direction"),
                int(bool(analysis.get("confirmed"))),int(bool(analysis.get("scalp")))
            ))
            conn.commit()
    except Exception as e:
        print("Snapshot recorder error:", e)

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
    shared = analysis.get("shared_briefing")
    shared_line = ""
    if shared:
        mem, settle = shared["memory"], shared["settlement"]
        shared_line = (
            f"\n🧠 CONTRACT: {mem.get('thesis', 'NEUTRAL')} — {mem.get('strength', 'WEAK')}\n"
            f"🏁 SETTLEMENT: {settle.get('side', 'UNKNOWN')} — {settle.get('strength', 'UNKNOWN')} (uncalibrated)\n"
            f"🎯 NEXT: {shared['action']}\n"
        )
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

{shared_line}
⚠️ Signal only — no automatic trade.
""".strip()


# =========================================================
# MAIN LOOP
# =========================================================

def main():
    global last_alert
    global last_shadow_alert
    global last_market
    global last_analysis

    print("Kate's BTC Watcher V4.8 Integrated Quiet Read starting...")
    print("Stats database:", DB_PATH)

    init_db()
    load_logged_signal_keys()
    register_telegram_commands()
    initialize_telegram_offset()

    send_telegram(
        "🧠 KATE'S BTC WATCHER V4.8 INTEGRATED QUIET READ IS ONLINE.\n\n"
        "Commands:\n"
        "/start — turn alerts ON\n"
        "/stop — turn alerts OFF\n"
        "/status — current read\n"
        "/last — last ENTER call\n"
        "/stats — performance stats\n\n"
        "📊 ENTER calls are being tracked.\n"
        "🎯 Contract target is being read.\n"
        "🧠 Black-box snapshots are recording every loop.\n"
        "👀 Shadow evidence is used internally and tracked separately.\n"
        "📡 Bot-issued ENTERs automatically switch into HOLD / PROTECT / EXIT mode.\n"
        "⏱ Time + target cushion + recent momentum feed the position read.\n"
        "🧠 Every 15m contract keeps an active case file; recent history strengthens or weakens the thesis.\n"
        "🏁 Settlement brain uses cushion + time + movement actually observed this contract.\n"
        "📈 1m price action is now part of the early read.\n"
        "💵 Scalp exits are measured from the sellable bid.\n"
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
            finalize_old_shadow_signals(ticker)

            analysis = analyze_market(market)

            if not analysis:
                time.sleep(CHECK_EVERY)
                continue

            # V4.3.1: fast BTC is for shadow/early-entry logic only.
            try:
                live_btc = get_live_btc_price()
            except Exception as e:
                print("Live BTC error:", e)
                live_btc = analysis.get("btc_price")
            analysis["live_btc_price"] = live_btc
            if analysis.get("contract_target") is not None and live_btc is not None:
                analysis["target_distance"] = live_btc - analysis["contract_target"]
                analysis["target_distance_pct"] = analysis["target_distance"] / analysis["contract_target"] * 100
                analysis["target_position"] = "ABOVE" if analysis["target_distance"] >= 0 else "BELOW"

            last_analysis = analysis

            # Persist the completed 1m candle so early formations are not thrown away.
            record_one_minute_snapshot(market, analysis)

            # Shared contract interpretation: advisory, not a replacement for validated entry gates.
            analysis["shared_briefing"] = synthesize_market_briefing(ticker, analysis)

            # V4.3.1 scores BOTH directions against the prior snapshot.
            shadow = analyze_shadow_setup(market, analysis)
            notices = apply_fast_brain(market, analysis, shadow)
            # While in a position, shadow chatter becomes internal evidence instead of Telegram spam.
            if active_position.get("ticker") == ticker:
                notices = []
            if shadow and shadow.get("direction"):
                record_shadow_signal(market, analysis, shadow)
            update_shadow_signals(market, analysis)

            # Record every observed state, including WAITs and fast-brain reads.
            record_market_snapshot(market, analysis)

            # ---------------------------------
            # NEW CONTRACT
            # ---------------------------------

            if ticker != last_market:
                last_market = ticker
                last_alert = None
                last_shadow_alert = None

                # Don't carry a retest setup into
                # another Kalshi contract.
                clear_retest()
                if active_position.get("ticker") and active_position.get("ticker") != ticker:
                    clear_active_position()
                reset_formation(ticker)

                send_telegram(
                    "🆕 NEW BTC 15-MIN CONTRACT\n\n"
                    f"{ticker}\n\n"
                    "I'm watching 👀"
                )

            # ---------------------------------
            # V4.3.1 FORMATION STATE ALERTS
            # ---------------------------------
            for notice in notices:
                send_telegram(notice)

            # ---------------------------------
            # RECORD EVERY REAL ENTER CALL
            # ---------------------------------

            record_enter_signal(
                ticker,
                analysis
            )
            new_position = activate_position_from_enter(ticker, analysis)

            # Once an ENTER exists, Trader Mode owns the user-facing updates.
            position_active = active_position.get("ticker") == ticker
            if position_active and not new_position:
                maybe_send_position_update(market, analysis, shadow)

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

            if signal_key != last_alert and (not position_active or new_position):
                last_alert = signal_key

                message = build_message(
                    market,
                    analysis
                )

                send_telegram(message)
                print(message)
                if new_position:
                    maybe_send_position_update(market, analysis, shadow, force=True)

            time.sleep(CHECK_EVERY)

        except Exception as e:
            print("Monitor error:", e)
            time.sleep(15)


if __name__ == "__main__":
    main()
