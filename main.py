"""KeKe BTC 15-minute indicator bot — single-file main.py for GitHub/Railway.
Read-only: NEVER places orders. Signals are experimental, not validated profits.
Requires requests and cryptography. Existing Railway credentials are reused.
Keeps existing SQLite database; does not remove legacy tables.
"""
import base64
import csv
import io
import json
import logging
import math
import os
import re
import sqlite3
import threading
import time
import zipfile
import tempfile
from contextlib import contextmanager
import queue
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, padding

BASE = 'https://external-api.kalshi.com/trade-api/v2'
SERIES = 'KXBTC15M'
TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
CHAT = os.environ.get('TELEGRAM_CHAT_ID', '')
DATA_DIR = Path(os.environ.get('DATA_DIR', '/data'))
MAX_ASK = float(os.environ.get('KEKE_MAX_ASK', '0.70'))
MAX_SPREAD = float(os.environ.get('KEKE_MAX_SPREAD', '0.10'))
LOG = logging.getLogger('keke')
KEYBOARD = {'keyboard': [
    [{'text': '👁️ LIVE'}, {'text': '📍 POSITION'}],
    [{'text': '📊 STATS'}, {'text': '📜 HISTORY'}],
    [{'text': '📤 EXPORT'}, {'text': '⚙️ SETTINGS'}],
    [{'text': '☀️ WAKE'}, {'text': '🌙 SLEEP'}],
], 'resize_keyboard': True}


def num(x):
    try:
        v = float(x)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def payout_multiplier(ask):
    """Gross $1 settlement payout / current ask, before fees and slippage."""
    price = num(ask)
    return 1 / price if price is not None and 0 < price <= 1 else None


def multiplier_label(ask):
    mult = payout_multiplier(ask)
    return f'{mult:.2f}×' if mult is not None else '—'


def stamp(value):
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()
    except (TypeError, ValueError):
        return None


def target(m):
    for key in ('floor_strike', 'functional_strike'):
        x = num(m.get(key))
        if x and 10000 <= x <= 1000000:
            return x
    for key in ('title', 'subtitle', 'yes_sub_title', 'no_sub_title'):
        for s in re.findall(r'\$?([0-9]{2,3}(?:,[0-9]{3})+(?:\.[0-9]+)?|[0-9]{4,6}(?:\.[0-9]+)?)', str(m.get(key) or '')):
            x = num(s.replace(',', ''))
            if x and 10000 <= x <= 1000000:
                return x
    return None


@contextmanager
def db():
    con = sqlite3.connect(str(DATA_DIR / 'kate_btc_watcher.db'), timeout=15)
    con.row_factory = sqlite3.Row
    con.execute('PRAGMA busy_timeout=15000')
    try:
        yield con
        con.commit()
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


def setup():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with db() as con:
        con.execute('PRAGMA journal_mode=WAL')
        con.executescript('''
          CREATE TABLE IF NOT EXISTS ki_ticks(t REAL PRIMARY KEY, price REAL NOT NULL, received REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS ki_books(id INTEGER PRIMARY KEY, ticker TEXT, at REAL,
            yes_bid REAL, no_bid REAL, yes_ask REAL, no_ask REAL,
            yes_qty REAL, no_qty REAL, raw_json TEXT);
          CREATE TABLE IF NOT EXISTS ki_markets(ticker TEXT PRIMARY KEY,first_at REAL,first_seconds INTEGER,eligible INTEGER,official_side TEXT);
          CREATE TABLE IF NOT EXISTS ki_signals(ticker TEXT PRIMARY KEY, at REAL, seconds INTEGER,
            side TEXT, kind TEXT, ask REAL, btc REAL, target REAL, reason TEXT,
            official_side TEXT, grade TEXT DEFAULT 'PENDING');
          CREATE TABLE IF NOT EXISTS ki_observations(id INTEGER PRIMARY KEY, at REAL,
            ticker TEXT, seconds INTEGER, btc REAL, target REAL,
            trend15 TEXT, setup3 TEXT, confirm1 TEXT, action TEXT, reason TEXT,
            ask_up REAL, ask_down REAL);
          CREATE TABLE IF NOT EXISTS ki_settings(key TEXT PRIMARY KEY, value TEXT);
          CREATE INDEX IF NOT EXISTS ki_obs_idx ON ki_observations(ticker,at);
        ''')
        # Reuse BRTI history from previous KeKe versions when available.
        existing = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        cutoff = time.time() - 7200
        if 'keke_brti_ticks' in existing:
            con.execute('INSERT OR IGNORE INTO ki_ticks SELECT timestamp,price,? FROM keke_brti_ticks WHERE timestamp>=?', (time.time(),cutoff))
        if 'k61_brti_ticks' in existing:
            con.execute('INSERT OR IGNORE INTO ki_ticks SELECT source_at,price,first_received_at FROM k61_brti_ticks WHERE source_at>=?', (cutoff,))


def setting(key, default=None):
    with db() as con:
        row = con.execute('SELECT value FROM ki_settings WHERE key=?', (key,)).fetchone()
    return json.loads(row[0]) if row else default


def set_setting(key, value):
    with db() as con:
        con.execute('INSERT OR REPLACE INTO ki_settings VALUES (?,?)', (key, json.dumps(value)))


class Kalshi:
    def __init__(self):
        self.local = threading.local()
        self.key_id = os.environ.get('KALSHI_API_KEY_ID', '').strip()
        pem = os.environ.get('KALSHI_PRIVATE_KEY', '').replace('\\n', '\n').strip()
        path = os.environ.get('KALSHI_PRIVATE_KEY_PATH', '').strip()
        if path:
            pem = Path(path).read_text()
        self.key = serialization.load_pem_private_key(pem.encode(), password=None) if pem else None
        self.cooldown_until = 0
        self.lock = threading.Lock()

    def get(self, path, params=None, private=False):
        with self.lock:
            if time.monotonic() < self.cooldown_until:
                raise RuntimeError('Kalshi rate cooldown')
        if not hasattr(self.local, 'http'):
            self.local.http = requests.Session()
        headers = {}
        if private:
            if not self.key_id or not self.key:
                raise RuntimeError('Kalshi BRTI/portfolio credentials missing')
            ts = str(int(time.time() * 1000))
            message = (ts + 'GET' + '/trade-api/v2' + path).encode()
            if isinstance(self.key, ed25519.Ed25519PrivateKey):
                signature = self.key.sign(message)
            else:
                signature = self.key.sign(message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
            headers = {'KALSHI-ACCESS-KEY': self.key_id,
                'KALSHI-ACCESS-TIMESTAMP': ts,
                'KALSHI-ACCESS-SIGNATURE': base64.b64encode(signature).decode()}
        response = self.local.http.get(BASE + path, params=params, headers=headers,
            timeout=(3, 7), allow_redirects=False)
        if response.status_code == 429:
            with self.lock:
                self.cooldown_until = time.monotonic() + 15
        response.raise_for_status()
        return response.json()

    def market(self):
        now = time.time()
        markets = self.get('/markets', {'series_ticker': SERIES, 'status': 'open', 'limit': 100}).get('markets', [])
        active = [m for m in markets if stamp(m.get('close_time')) and
                  (stamp(m.get('open_time')) or stamp(m.get('close_time')) - 900) <= now < stamp(m['close_time'])]
        return min(active, key=lambda m: stamp(m['close_time'])) if active else None

    def brti(self):
        started = time.time()
        obj = self.get('/cfbenchmarks/values', {'id': 'BRTI'}, private=True)
        received = time.time()
        points = (obj.get('data') or {}).get('payload') or []
        rows = []
        for item in points:
            t, p = num(item.get('time')), num(item.get('value'))
            if t is None or p is None:
                continue
            if t > 1e11:
                t /= 1000
            if received - 7200 <= t <= received and 10000 < p < 1000000:
                rows.append((t, p, received))
        if not rows:
            raise ValueError('No timestamped BRTI data')
        with db() as con:
            con.executemany('INSERT OR IGNORE INTO ki_ticks VALUES (?,?,?)', rows)
            con.execute('DELETE FROM ki_ticks WHERE t<?', (received - 86400,))
        if received - max(t for t, _, _ in rows) > 10:
            raise ValueError('BRTI feed stale')
        return max(rows, key=lambda r: r[0])

    def orderbook(self, ticker):
        raw = self.get('/markets/' + ticker + '/orderbook', {'depth': 20})
        received = time.time()
        book = raw.get('orderbook_fp') or raw.get('orderbook') or {}
        def levels(side):
            arr = book.get(side + '_dollars') or []
            return sorted([(p, q) for item in arr if len(item) >= 2
                           for p, q in [(num(item[0]), num(item[1]))]
                           if p is not None and q is not None and 0 < p < 1 and q > 0], reverse=True)
        yes, no = levels('yes'), levels('no')
        y = yes[0] if yes else (None, None)
        n = no[0] if no else (None, None)
        out = {'ticker': ticker, 'at': received, 'yes_bid': y[0], 'no_bid': n[0],
               'yes_ask': 1 - n[0] if n[0] is not None else None,
               'no_ask': 1 - y[0] if y[0] is not None else None,
               'yes_qty': n[1], 'no_qty': y[1]}
        with db() as con:
            con.execute('''INSERT INTO ki_books(ticker,at,yes_bid,no_bid,yes_ask,no_ask,yes_qty,no_qty,raw_json)
                           VALUES (?,?,?,?,?,?,?,?,?)''', (ticker, received, y[0], n[0],
                out['yes_ask'], out['no_ask'], n[1], y[1], json.dumps(raw)))
        return out

    def positions(self):
        rows = []
        cursor = None
        while True:
            params = {'limit': 100, 'count_filter': 'position'}
            if cursor:
                params['cursor'] = cursor
            obj = self.get('/portfolio/positions', params, private=True)
            for p in obj.get('market_positions', []):
                if not str(p.get('ticker', '')).startswith(SERIES + '-'):
                    continue
                qty = num(p.get('position_fp'))
                if qty is None:
                    qty = num(p.get('position'))
                if not qty:
                    continue
                basis = num(p.get('market_exposure_dollars'))
                if basis is None:
                    cents = num(p.get('market_exposure'))
                    basis = cents / 100 if cents is not None else None
                rows.append({'ticker': p['ticker'], 'side': 'UP' if qty > 0 else 'DOWN',
                             'qty': abs(qty), 'basis': basis})
            new_cursor = obj.get('cursor')
            if not new_cursor or new_cursor == cursor:
                break
            cursor = new_cursor
        return rows

    def result(self, ticker):
        try:
            obj = self.get('/markets/' + ticker)
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 404:
                raise
            obj = self.get('/historical/markets/' + ticker)
        side = str((obj.get('market') or {}).get('result', '')).lower()
        return {'yes': 'UP', 'no': 'DOWN'}.get(side)


def bars_from_ticks(ticks, now):
    """Complete 1m candles, never partial candles or manufactured gap fills."""
    groups = {}
    for t, price in ticks:
        minute = int(t) // 60 * 60
        if minute + 60 > now:
            continue
        groups.setdefault(minute, []).append((t, price))
    out = []
    for minute, rows in sorted(groups.items()):
        rows.sort()
        if rows[0][0] > minute + 15 or rows[-1][0] < minute + 45:
            continue
        prices = [p for _, p in rows]
        out.append({'t': minute, 'o': prices[0], 'h': max(prices),
                    'l': min(prices), 'c': prices[-1]})
    return out


def resample(one, minutes):
    out, groups = [], {}
    width = minutes * 60
    for bar in one:
        groups.setdefault(bar['t'] // width * width, []).append(bar)
    for t, rows in sorted(groups.items()):
        rows.sort(key=lambda x: x['t'])
        if len(rows) != minutes or any(r['t'] != t + 60 * i for i, r in enumerate(rows)):
            continue
        out.append({'t': t, 'o': rows[0]['o'], 'h': max(x['h'] for x in rows),
                    'l': min(x['l'] for x in rows), 'c': rows[-1]['c']})
    return out


def ema(values, period):
    if len(values) < period:
        return None
    value = sum(values[:period]) / period
    alpha = 2 / (period + 1)
    for x in values[period:]:
        value += alpha * (x - value)
    return value


def contiguous(rows, width):
    result = []
    for row in reversed(rows):
        if result and result[-1]['t'] - row['t'] != width:
            break
        result.append(row)
    return list(reversed(result))


def indicator(ticks, market, now):
    """15m structure + 3m setup + 1m confirmation. All bars are closed and UTC-aligned."""
    out = {'trend15': 'COLLECTING', 'setup3': 'COLLECTING', 'confirm1': 'COLLECTING',
           'action': 'WAIT', 'side': None, 'reason': 'Collecting completed BRTI candles',
           'support': None, 'resistance': None, 'btc': None, 'target': target(market)}
    if not ticks or out['target'] is None:
        return out
    source, price = ticks[-1]
    out['btc'] = price
    if not 0 <= now - source <= 10:
        out['reason'] = 'Stale/invalid BRTI timestamp • no signal'
        return out
    one = contiguous(bars_from_ticks(ticks, now), 60)
    three = contiguous(resample(one, 3), 180)
    fifteen = contiguous(resample(one, 15), 900)
    if len(one) < 5 or len(three) < 5 or len(fifteen) < 5:
        out['reason'] = f'Warming up: {len(one)} 1m / {len(three)} 3m / {len(fifteen)} 15m bars'
        return out
    c15 = [b['c'] for b in fifteen]
    fast15, slow15 = ema(c15, 3), ema(c15, 5)
    spread15 = abs(fast15 - slow15)
    typical15 = sum(b['h'] - b['l'] for b in fifteen[-5:]) / 5
    flat = spread15 < max(5, typical15 * .07)
    trend = 'RANGE' if flat else 'UP' if fast15 > slow15 and c15[-1] > c15[-3] else 'DOWN' if fast15 < slow15 and c15[-1] < c15[-3] else 'MIXED'
    out['trend15'] = trend
    out['support'] = min(b['l'] for b in fifteen[-4:])
    out['resistance'] = max(b['h'] for b in fifteen[-4:])
    c3 = [b['c'] for b in three]
    avg3 = ema(c3, 5)
    last3, prev3 = three[-1], three[-2]
    breakout_up = last3['c'] > max(b['h'] for b in three[-4:-1])
    breakout_dn = last3['c'] < min(b['l'] for b in three[-4:-1])
    retest_up = last3['l'] <= avg3 and last3['c'] > avg3 and last3['c'] > last3['o'] and last3['c'] > prev3['c']
    retest_dn = last3['h'] >= avg3 and last3['c'] < avg3 and last3['c'] < last3['o'] and last3['c'] < prev3['c']
    setup = 'UP' if breakout_up or retest_up else 'DOWN' if breakout_dn or retest_dn else 'NONE'
    out['setup3'] = ('BREAKOUT ' if breakout_up or breakout_dn else 'RETEST ' if retest_up or retest_dn else '') + setup
    c1 = [b['c'] for b in one]
    last1, prev1 = one[-1], one[-2]
    avg1 = ema(c1, 3)
    confirm_up = last1['c'] > avg1 and last1['c'] > prev1['c'] and last1['c'] > last1['o']
    confirm_dn = last1['c'] < avg1 and last1['c'] < prev1['c'] and last1['c'] < last1['o']
    out['confirm1'] = 'UP' if confirm_up else 'DOWN' if confirm_dn else 'NONE'
    if trend not in ('UP', 'DOWN'):
        out['reason'] = '15m range/mixed • no directional confirmation'
    elif setup != trend:
        out['reason'] = '3m setup does not agree with 15m trend'
    elif out['confirm1'] != trend:
        out['reason'] = 'Waiting for closed 1m confirmation'
    else:
        out['side'] = trend
        out['action'] = 'WATCH ' + trend
        out['reason'] = '15m trend + 3m breakout/retest + 1m confirmation'
    return out


def decide(ind, market, book, now):
    """Never claim an entry without a fresh book, target-side lead, and price cap."""
    side = ind.get('side')
    remaining = stamp(market['close_time']) - now
    if not side:
        return ind
    if not 90 <= remaining <= 870:
        ind.update(action='WAIT', reason='Outside entry window (1:30–14:30 left)')
        return ind
    if (ind['btc'] - ind['target']) * (1 if side == 'UP' else -1) <= 0:
        ind.update(action='WATCH ' + side, reason='Indicators align, but BTC has not crossed target')
        return ind
    if not book or book.get('ticker') != market['ticker'] or now - book.get('at', 0) > 6:
        ind.update(action='WAIT', reason='Fresh Kalshi orderbook unavailable')
        return ind
    prefix = 'yes' if side == 'UP' else 'no'
    ask, bid, qty = (book.get(prefix + '_ask'), book.get(prefix + '_bid'), book.get(prefix + '_qty'))
    ind['ask'] = ask
    if ask is None or bid is None or qty is None or qty < 1:
        ind.update(action='WAIT', reason='No confirmed one-contract liquidity')
    elif ask > MAX_ASK:
        ind.update(action='WATCH ' + side, reason=f'Payout multiplier {multiplier_label(ask)} below {multiplier_label(MAX_ASK)} minimum')
    elif ask - bid > MAX_SPREAD:
        ind.update(action='WATCH ' + side, reason=f'Wide spread: {ask - bid:.0%}')
    else:
        ind.update(action='ENTER ' + side, reason='Experimental indicator setup • orderbook conditions passed')
    return ind


class Bot:
    def __init__(self):
        self.api = Kalshi()
        self.lock = threading.Lock()
        self.state = {'market': None, 'market_at': 0, 'book': None, 'brti_at': 0,
                      'read': None, 'positions': [], 'positions_at': 0}
        self.offset = setting('offset')
        self.awake = setting('awake', True)
        self.last_alert = {}
        self.last_error = {}
        self.stop = threading.Event()
        self.last_signal_ticker = None
        self.sent_at = {}
        self.outbox = queue.Queue(maxsize=30)

    def set(self, **kwargs):
        with self.lock:
            self.state.update(kwargs)

    def snap(self):
        with self.lock:
            return dict(self.state)

    def send(self, text):
        if TOKEN and CHAT:
            try:
                self.outbox.put_nowait(text[:3900])
            except queue.Full:
                LOG.warning('Telegram queue full; skipping message')

    def sender(self):
        while not self.stop.is_set():
            try:
                text = self.outbox.get(timeout=.5)
            except queue.Empty:
                continue
            try:
                response = requests.post(f'https://api.telegram.org/bot{TOKEN}/sendMessage',
                    data={'chat_id': CHAT, 'text': text, 'reply_markup': json.dumps(KEYBOARD)}, timeout=12)
                response.raise_for_status()
            except requests.RequestException as exc:
                LOG.warning('Telegram send delayed: %s', type(exc).__name__)
            self.stop.wait(1.1)

    def send_file(self, name, contents):
        try:
            requests.post(f'https://api.telegram.org/bot{TOKEN}/sendDocument',
                data={'chat_id': CHAT}, files={'document': (name, contents, 'application/zip')}, timeout=60).raise_for_status()
        except requests.RequestException as exc:
            LOG.warning('Telegram export failed: %s', type(exc).__name__)
            self.send('Export delivery failed; retry /export')

    def live(self):
        s = self.snap()
        m, r = s.get('market'), s.get('read')
        if not m or not r:
            return '👁️ KEKE | BTC 15M\n⚪ WAIT\n\n⏳ Collecting fresh market and indicator data'

        now = time.time()
        seconds = max(0, int(stamp(m['close_time']) - now))
        stale = now - s['market_at'] > 12 or now - s['brti_at'] > 12
        action = 'WAIT' if stale else r['action']
        book = s.get('book') or {}
        book_ok = book.get('ticker') == m['ticker'] and now - book.get('at', 0) <= 6

        def usd(x):
            value = num(x)
            return f'${value:,.2f}' if value is not None else '—'

        btc, strike = num(r.get('btc')), num(r.get('target'))
        if btc is None or strike is None:
            distance = 'Distance unavailable'
        else:
            difference = btc - strike
            direction = 'ABOVE' if difference > 0 else 'BELOW' if difference < 0 else 'AT'
            symbol = '🔺' if difference > 0 else '🔻' if difference < 0 else '➖'
            distance = f'{symbol} {usd(abs(difference))} {direction} target'

        up = multiplier_label(book.get('yes_ask')) if book_ok else '—'
        down = multiplier_label(book.get('no_ask')) if book_ok else '—'
        state_icon = '🟢' if action == 'ENTER UP' else '🔴' if action == 'ENTER DOWN' else '🟡' if action.startswith('WATCH') else '⚪'
        verdict = '✅ ENTRY SIGNAL' if action.startswith('ENTER') else '👀 WATCH ONLY' if action.startswith('WATCH') else '⏸️ NO ENTRY'
        reason = 'Stale feed — no entry' if stale else r.get('reason', 'No confirmed setup')

        return '\n'.join([
            '👁️ KEKE | BTC 15M',
            f'{state_icon} {action}  •  ⏳ {seconds//60}:{seconds%60:02d} LEFT',
            '',
            '📊 INDICATORS',
            f"15M Trend: {r['trend15']}",
            f"3M Setup: {r['setup3']}",
            f"1M Momentum: {r['confirm1']}",
            '',
            '🎯 BTC vs TARGET',
            f'BTC: {usd(btc)}',
            f'Target: {usd(strike)}',
            distance,
            '',
            '💰 PAYOUT MULTIPLIERS',
            f'🟢 UP: {up}',
            f'🔴 DOWN: {down}',
            'Gross if correct • before fees',
            '',
            '🧱 KEY LEVELS',
            f"Support: {usd(r.get('support'))}",
            f"Resistance: {usd(r.get('resistance'))}",
            '',
            verdict,
            reason,
        ])

    def position_text(self):
        s = self.snap()
        if time.time() - s['positions_at'] > 20:
            return '📍 POSITION • Awaiting fresh portfolio data'
        if not s['positions']:
            return '📍 POSITION • No open BTC 15M holdings'
        lines = ['📍 KEKE POSITION']
        for p in s['positions']:
            book = s.get('book') or {}
            fresh = book.get('ticker') == p['ticker'] and time.time() - book.get('at', 0) <= 6
            bid = book.get('yes_bid' if p['side'] == 'UP' else 'no_bid') if fresh else None
            gross = p['qty'] * bid - p['basis'] if bid is not None and p['basis'] is not None else None
            lines.append(f"{p['side']} • {p['qty']:g} contracts • {p['ticker']}")
            lines.append('Gross exit P/L ' + (f'${gross:+.2f}' if gross is not None else 'unavailable (no fresh bid)'))
        lines.append('Fees, depth and realized P/L excluded')
        return '\n'.join(lines)

    def stats_text(self):
        with db() as con:
            rows = con.execute("SELECT side,grade,COUNT(*) n FROM ki_signals GROUP BY side,grade").fetchall()
            eligible = con.execute("SELECT COUNT(*) FROM ki_markets WHERE eligible=1 AND official_side IN ('UP','DOWN')").fetchone()[0]
            called = con.execute("SELECT COUNT(*) FROM ki_signals s JOIN ki_markets m USING(ticker) WHERE m.eligible=1 AND m.official_side IN ('UP','DOWN')").fetchone()[0]
        d = {(r['side'], r['grade']): r['n'] for r in rows}
        w = d.get(('UP','WIN'),0) + d.get(('DOWN','WIN'),0)
        l = d.get(('UP','LOSS'),0) + d.get(('DOWN','LOSS'),0)
        pending = sum(r['n'] for r in rows if r['grade'] == 'PENDING')
        return (f'📊 KEKE INDICATOR • NEW COHORT\n'
                f'Win rate: {w}W / {l}L • {w/(w+l):.1%}' if w+l else
                '📊 KEKE INDICATOR • Awaiting graded signals') + (
                f'\nEligible coverage: {called}/{eligible} • {called/eligible:.1%}' if eligible else
                '\nCoverage: collecting eligible markets') + (
                f'\nPending: {pending}\nWAIT counts against coverage, not as a loss.\nResearch-stage signals • no proven edge')

    def history_text(self):
        with db() as con:
            rows = con.execute('SELECT ticker,side,kind,grade FROM ki_signals ORDER BY at DESC LIMIT 12').fetchall()
        return '📜 KEKE HISTORY\n' + ('\n'.join(f"{r['ticker']} • {r['kind']} {r['side']} • {r['grade']}" for r in rows) or 'No indicator signals yet')

    def export(self, kalshi=False):
        """Includes legacy research tables without changing their schema."""
        with db() as con:
            tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
            with io.BytesIO() as mem:
                with zipfile.ZipFile(mem, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                    for table in tables:
                        if table in ('ki_settings', 'k61_settings', 'k6_settings', 'keke_runtime'):
                            continue
                        if kalshi and not any(k in table for k in ('tick','book','raw','observation','transition','shadow','speed')):
                            continue
                        if not re.fullmatch(r'[A-Za-z0-9_]+', table):
                            continue
                        cursor = con.execute(f'SELECT * FROM "{table}"')
                        text = io.StringIO()
                        writer = csv.writer(text)
                        writer.writerow([d[0] for d in cursor.description])
                        for row_num, row in enumerate(cursor):
                            clean = []
                            for col_num, value in enumerate(row):
                                if isinstance(value, bytes):
                                    blob_name = f'raw/{table}/{row_num}_{col_num}.bin'
                                    archive.writestr(blob_name, value)
                                    value = blob_name
                                clean.append(value)
                            writer.writerow(clean)
                        archive.writestr(table + '.csv', text.getvalue())
                return mem.getvalue()

    def commands(self):
        while not self.stop.is_set():
            try:
                data = {'timeout': 20}
                if self.offset is not None:
                    data['offset'] = self.offset
                response = requests.post(f'https://api.telegram.org/bot{TOKEN}/getUpdates',
                    data=data, timeout=27).json()
                for update in response.get('result', []):
                    self.offset = update['update_id'] + 1
                    set_setting('offset', self.offset)
                    msg = update.get('message') or {}
                    if str((msg.get('chat') or {}).get('id')) != str(CHAT):
                        continue
                    raw = (msg.get('text') or '').strip().lower()
                    cmd = raw.split()[0].split('@')[0].lstrip('/') if raw.startswith('/') else raw.split()[-1] if raw else ''
                    cmd = re.sub(r'[^a-z]', '', cmd)
                    if cmd in ('start', 'help', 'live'):
                        self.send(self.live())
                    elif cmd == 'position':
                        self.send(self.position_text())
                    elif cmd in ('stats', 'research'):
                        self.send(self.stats_text())
                    elif cmd == 'history':
                        self.send(self.history_text())
                    elif cmd == 'settings':
                        self.send(f'⚙️ Alerts: {"ON" if self.awake else "OFF"}\nMinimum payout: {multiplier_label(MAX_ASK)} gross • Max spread {MAX_SPREAD:.0%}\n15m / 3m / 1m indicator mode')
                    elif cmd in ('wake', 'sleep'):
                        self.awake = cmd == 'wake'
                        set_setting('awake', self.awake)
                        self.send('☀️ Alerts awake' if self.awake else '🌙 Alerts asleep • data collection continues')
                    elif cmd in ('export', 'kalshiexport'):
                        self.send('📤 Preparing research export')
                        threading.Thread(target=self._export_job, args=(cmd,), daemon=True).start()
                    else:
                        self.send('Commands: /live /position /stats /history /export /kalshiexport /wake /sleep')
            except Exception as exc:
                LOG.warning('Telegram poll delayed: %s', type(exc).__name__)
                self.stop.wait(2)

    def _export_job(self, cmd):
        try:
            name = 'keke_kalshi_research.zip' if cmd == 'kalshiexport' else 'keke_research_export.zip'
            payload = self.export(cmd == 'kalshiexport')
            chunk = 44 * 1024 * 1024
            if len(payload) <= chunk:
                self.send_file(name, payload)
            else:
                parts = (len(payload) + chunk - 1) // chunk
                self.send(f'📤 Large export: {parts} parts; save every part in order')
                for i in range(parts):
                    self.send_file(f'{name}.part{i+1:03d}', payload[i*chunk:(i+1)*chunk])
        except Exception as exc:
            LOG.warning('Export failed: %s', type(exc).__name__)
            self.send('Export failed; try again later')

    def grade(self):
        with db() as con:
            pending = [r['ticker'] for r in con.execute("SELECT ticker FROM ki_markets WHERE official_side IS NULL AND first_at<? ORDER BY first_at LIMIT 8", (time.time()-900,))]
        for ticker in pending:
            try:
                result = self.api.result(ticker)
                if result:
                    with db() as con:
                        con.execute('UPDATE ki_markets SET official_side=? WHERE ticker=?', (result,ticker))
                        con.execute("UPDATE ki_signals SET official_side=?,grade=CASE WHEN side=? THEN 'WIN' ELSE 'LOSS' END WHERE ticker=? AND grade='PENDING'", (result,result,ticker))
            except Exception as exc:
                LOG.warning('Grade delayed: %s', type(exc).__name__)

    def cycle(self):
        now = time.time()
        s = self.snap()
        if now - s['market_at'] >= 5:
            m = self.api.market()
            self.set(market=m, market_at=time.time())
        else:
            m = s['market']
        if not m:
            return
        ticker = m['ticker']
        remaining = int(stamp(m['close_time']) - now)
        with db() as con:
            con.execute('INSERT OR IGNORE INTO ki_markets(ticker,first_at,first_seconds,eligible) VALUES (?,?,?,?)',
                        (ticker,now,remaining,int(720 <= remaining <= 900)))
        if now - s['brti_at'] >= 2:
            t, p, received = self.api.brti()
            self.set(brti_at=received)
        if not s['book'] or s['book'].get('ticker') != ticker or now - s['book']['at'] >= 3:
            self.set(book=self.api.orderbook(ticker))
        now = time.time()  # Evaluate after requests so fresh BRTI timestamps are not treated as future data.
        remaining = int(stamp(m['close_time']) - now)
        with db() as con:
            ticks = [(r['t'], r['price']) for r in con.execute('SELECT t,price FROM ki_ticks WHERE t>=? ORDER BY t', (now-7200,))]
        read = decide(indicator(ticks, m, now), m, self.snap().get('book'), now)
        self.set(read=read)
        book = self.snap().get('book') or {}
        with db() as con:
            con.execute('''INSERT INTO ki_observations(at,ticker,seconds,btc,target,trend15,setup3,confirm1,action,reason,ask_up,ask_down)
                           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''', (now,ticker,remaining,read.get('btc'),read.get('target'),
                            read['trend15'],read['setup3'],read['confirm1'],read['action'],read['reason'],book.get('yes_ask'),book.get('no_ask')))
            if read['action'].startswith('ENTER '):
                side = read['side']
                con.execute("INSERT OR IGNORE INTO ki_signals(ticker,at,seconds,side,kind,ask,btc,target,reason) VALUES (?,?,?,?,?,?,?,?,?)",
                            (ticker,now,remaining,side,'INDICATOR',read.get('ask'),read.get('btc'),read.get('target'),read['reason']))
        old = self.last_alert.get(ticker)
        action = read['action']
        if self.awake and action.startswith('ENTER ') and old != action:
            self.last_alert[ticker] = action
            if now - self.sent_at.get((ticker, action), 0) >= 90:
                self.sent_at[(ticker, action)] = now
                self.send(self.live())
        elif self.awake and old and action != old:
            self.last_alert[ticker] = None
            self.send('⚠️ KEKE • previous ' + old + ' setup no longer confirmed\n' + self.live())

    def position_cycle(self):
        positions = self.api.positions()
        self.set(positions=positions, positions_at=time.time())
        book = self.snap().get('book') or {}
        for p in positions:
            if p['ticker'] != book.get('ticker') or time.time() - book.get('at', 0) > 6:
                continue
            bid = book.get('yes_bid' if p['side'] == 'UP' else 'no_bid')
            basis = p.get('basis')
            if bid is None or basis is None or basis <= 0:
                continue
            gross = p['qty'] * bid - basis
            change = gross / basis
            status = 'EXIT WATCH' if change <= -.20 else 'LOSS WATCH' if change <= -.12 else 'PROTECT PROFIT' if change >= .20 else 'NORMAL'
            key = (p['ticker'], p['side'], status)
            if status != 'NORMAL' and self.awake and time.time() - self.sent_at.get(key, 0) >= 120:
                self.sent_at[key] = time.time()
                self.send(f'⚠️ {status} • {p["side"]} {p["qty"]:g} contracts\nGross bid P/L ${gross:+.2f} ({change:+.1%})\nNot an automatic exit; fees/depth excluded')

    def health(self):
        bot = self
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({'running': True, 'version': 'indicator-1',
                                   'fresh': time.time()-bot.snap().get('brti_at',0)<15}).encode()
                self.send_response(200 if self.path == '/health' else 404)
                self.send_header('Content-Type','application/json')
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *args):
                pass
        server = ThreadingHTTPServer(('0.0.0.0', int(os.environ.get('PORT', '8080'))), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()

    def run(self):
        self.health()
        threading.Thread(target=self.sender, daemon=True).start()
        threading.Thread(target=self.commands, daemon=True).start()
        next_position = next_grade = 0
        self.send('💚 KeKe BTC 15M indicator mode online • 15m structure / 3m setup / 1m confirmation • research-stage alerts')
        while not self.stop.is_set():
            start = time.monotonic()
            try:
                self.cycle()
            except Exception as exc:
                key = type(exc).__name__ + str(exc)[:50]
                if time.time()-self.last_error.get(key,0)>60:
                    LOG.warning('Market cycle delayed: %s', key)
                    self.last_error[key] = time.time()
            if time.monotonic() >= next_position:
                next_position = time.monotonic() + 10
                try:
                    self.position_cycle()
                except Exception as exc:
                    LOG.warning('Position poll delayed: %s', type(exc).__name__)
            if time.monotonic() >= next_grade:
                next_grade = time.monotonic() + 60
                self.grade()
            self.stop.wait(max(0.1, 1.0-(time.monotonic()-start)))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    if not TOKEN or not CHAT:
        raise SystemExit('Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Railway')
    setup()
    Bot().run()
