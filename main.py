"""KeKe V5.27 BRTI-only settlement forecast alerts — settlement-first, observation-only Kalshi BTC 15m watcher.
Requires: requests. Existing SQLite tables are preserved; new tables are additive.
"""
import os, time, json, sqlite3, re, logging, base64, math
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, ed25519
from datetime import datetime, timezone
import requests
import csv, io, zipfile
import threading
import gzip
import keke_ai_lab as ai_lab

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
LOG = logging.getLogger('keke')
BASE='https://external-api.kalshi.com/trade-api/v2'
SERIES='KXBTC15M'
# BRTI is the sole BTC price source; no exchange-price fallback.
TOKEN=os.getenv('TELEGRAM_BOT_TOKEN')
CHAT=os.getenv('TELEGRAM_CHAT_ID')
DATA_DIR=os.getenv('DATA_DIR','/data')
DB_NAME='kate_btc_watcher.db'
CHECK_EVERY=5
HEARTBEAT_SECONDS=60
THREAT_SIGNAL=0.20
THREAT_COOLDOWN=90
GRADE_EVERY=60
GRADE_BATCH=12
MAX_DATA_AGE=75
MIN_ENTRY_SECONDS=90
MAX_ENTRY_ASK=.92
MIN_NET_EDGE=.05  # conservative heuristic margin, NOT a calibrated probability edge
MIN_CUSHION=20  # USD; below this, don't issue directional ENTER
ALERT_COOLDOWN=90
BRTI_PATH='/trade-api/v2/cfbenchmarks/values'
BRTI_MAX_AGE=30

session=requests.Session()
state={'ticker':None,'last_alert':None,'last_sent':0,'last_side':None,'last_read':None,
       'enabled':True,'offset':None,'entry_side':None,'entry_at':None,'last_btc':None,
       'last_btc_at':None,'last_poll':0,'last_grade':0,
       'heartbeat_ticker':None,'heartbeat_at':0,'threat_ticker':None,
       'threat_side':None,'threat_at':0}

# Do not silently fall back to an ephemeral disk after a volume has been attached.
def database_path():
    os.makedirs(DATA_DIR,exist_ok=True)
    p=os.path.join(DATA_DIR,DB_NAME)
    with sqlite3.connect(p,timeout=10) as c:
        c.execute('PRAGMA journal_mode=WAL')
        c.execute('CREATE TABLE IF NOT EXISTS keke_v5_health (id INTEGER PRIMARY KEY, checked_at INTEGER)')
    return p

DB_PATH=database_path()

def connect():
    c=sqlite3.connect(DB_PATH,timeout=10)
    c.row_factory=sqlite3.Row
    return c

def init_db():
    with connect() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS settlement_observations (
          id INTEGER PRIMARY KEY AUTOINCREMENT, captured_at INTEGER NOT NULL,
          ticker TEXT NOT NULL, seconds_left INTEGER, btc_price REAL,
          target REAL, distance REAL, yes_bid REAL, yes_ask REAL, no_bid REAL, no_ask REAL,
          trend TEXT, momentum_1m REAL, momentum_3m REAL, volatility_1m REAL,
          favored_side TEXT, confidence TEXT, decision TEXT, reason TEXT,
          model_version TEXT NOT NULL DEFAULT '5.0')''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_v5_obs ON settlement_observations(ticker,captured_at)')
        c.execute('''CREATE TABLE IF NOT EXISTS settlement_calls (
          id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL,
          created_at INTEGER NOT NULL, seconds_left INTEGER, side TEXT NOT NULL,
          ask REAL, btc_price REAL, target REAL, confidence TEXT, reason TEXT,
          result TEXT DEFAULT 'PENDING', result_source TEXT,
          UNIQUE(ticker,side))''')
        c.execute('''CREATE TABLE IF NOT EXISTS settlement_results (
          ticker TEXT PRIMARY KEY, settled_side TEXT, source TEXT,
          recorded_at INTEGER, verified INTEGER NOT NULL DEFAULT 0)''')
        c.execute('''CREATE TABLE IF NOT EXISTS settlement_predictions (
          ticker TEXT PRIMARY KEY, captured_at INTEGER NOT NULL,
          seconds_left INTEGER, side TEXT NOT NULL, source TEXT NOT NULL,
          distance REAL, ask REAL, official_side TEXT,
          graded_at INTEGER, grade TEXT DEFAULT 'PENDING')''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_v52_predictions_grade ON settlement_predictions(grade,captured_at)')
        c.execute('''CREATE TABLE IF NOT EXISTS v53_opportunities (
            ticker TEXT NOT NULL, side TEXT NOT NULL, captured_at INTEGER NOT NULL,
            seconds_left INTEGER, stage TEXT, ask REAL, multiplier REAL,
            score REAL, brti REAL, target REAL, distance REAL, indicators TEXT,
            official_side TEXT, grade TEXT DEFAULT 'PENDING', graded_at INTEGER,
            PRIMARY KEY(ticker,side,stage))''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_v53_grade ON v53_opportunities(grade,captured_at)')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_thesis_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL,
            captured_at INTEGER NOT NULL, seconds_left INTEGER,
            event TEXT NOT NULL, side TEXT NOT NULL, prior_side TEXT,
            score REAL, ask REAL, target REAL, brti REAL, reason TEXT,
            official_side TEXT, grade TEXT DEFAULT 'PENDING', graded_at INTEGER)''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_thesis_ticker ON keke_thesis_events(ticker,captured_at)')
        c.execute('CREATE INDEX IF NOT EXISTS idx_thesis_grade ON keke_thesis_events(grade,captured_at)')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_v56_forecasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL,
            captured_at INTEGER NOT NULL, seconds_left INTEGER, event TEXT NOT NULL,
            side TEXT NOT NULL, previous_side TEXT, signal REAL, ask REAL,
            target REAL, brti REAL, reason TEXT,
            official_side TEXT, grade TEXT DEFAULT 'PENDING', graded_at INTEGER)''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_v56_forecasts ON keke_v56_forecasts(ticker,captured_at)')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_shadow_predictions (
            ticker TEXT NOT NULL, checkpoint INTEGER NOT NULL,
            captured_at INTEGER NOT NULL, seconds_left INTEGER NOT NULL,
            side TEXT NOT NULL, score REAL NOT NULL, reason TEXT,
            ask REAL, brti REAL, target REAL,
            official_side TEXT, grade TEXT DEFAULT 'PENDING', graded_at INTEGER,
            PRIMARY KEY(ticker,checkpoint))''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_shadow_grade ON keke_shadow_predictions(grade,ticker)')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_paper_scalps (
            ticker TEXT NOT NULL, side TEXT NOT NULL, opened_at INTEGER NOT NULL,
            entry_ask REAL NOT NULL, entry_bid REAL, closed_at INTEGER, exit_bid REAL,
            gross_per_contract REAL, exit_reason TEXT,
            PRIMARY KEY(ticker,side))''')
        c.execute('CREATE TABLE IF NOT EXISTS keke_brti_ticks (timestamp INTEGER PRIMARY KEY, price REAL NOT NULL)')
        c.execute("""CREATE TABLE IF NOT EXISTS keke_range_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL,
            created_at INTEGER NOT NULL, pattern TEXT NOT NULL, side TEXT NOT NULL,
            support REAL NOT NULL, resistance REAL NOT NULL, brti REAL NOT NULL,
            entry_ask REAL, entry_bid REAL, exit_at INTEGER, exit_bid REAL,
            gross_per_contract REAL, exit_reason TEXT,
            UNIQUE(ticker,pattern,side))""")
        c.execute('CREATE TABLE IF NOT EXISTS keke_model_epochs (version TEXT PRIMARY KEY, started_at INTEGER NOT NULL)')
        c.execute('INSERT OR IGNORE INTO keke_model_epochs(version,started_at) VALUES (?,?)',('5.9-BRTI-STATS',int(time.time())))
        c.execute('INSERT OR IGNORE INTO keke_model_epochs(version,started_at) VALUES (?,?)',('5.16-EARLY-PICKS',int(time.time())))
        c.execute('INSERT OR IGNORE INTO keke_model_epochs(version,started_at) VALUES (?,?)',('5.19-REPAIRED',int(time.time())))
        c.execute('CREATE TABLE IF NOT EXISTS keke_runtime (key TEXT PRIMARY KEY, value TEXT NOT NULL)')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_retest_alerts (ticker TEXT NOT NULL, break_time INTEGER NOT NULL, event TEXT NOT NULL, level REAL NOT NULL, confirmed_at INTEGER NOT NULL, PRIMARY KEY(ticker,break_time,event))''')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_signal_state (
          ticker TEXT NOT NULL, strategy TEXT NOT NULL, side TEXT NOT NULL,
          candle_time INTEGER NOT NULL, captured_at INTEGER NOT NULL,
          PRIMARY KEY(ticker,strategy,side,candle_time))''')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_early_opportunities (
          ticker TEXT PRIMARY KEY, first_seen INTEGER NOT NULL, first_seconds INTEGER,
          had_history INTEGER NOT NULL DEFAULT 0)''')
        c.execute('CREATE TABLE IF NOT EXISTS keke_position_snapshots (ticker TEXT PRIMARY KEY, side TEXT NOT NULL, qty REAL NOT NULL, basis REAL, bid REAL, updated_at INTEGER NOT NULL)')
        c.execute('CREATE TABLE IF NOT EXISTS keke_position_events (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL, event TEXT NOT NULL, side TEXT, old_qty REAL, new_qty REAL, recorded_at INTEGER NOT NULL, UNIQUE(ticker,event,recorded_at))')
        c.execute('CREATE TABLE IF NOT EXISTS keke_opening_health (ticker TEXT PRIMARY KEY, first_seen INTEGER, first_seconds INTEGER, had_history INTEGER NOT NULL)')
        c.execute('CREATE TABLE IF NOT EXISTS keke_v518_events (id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL, event TEXT NOT NULL, captured_at INTEGER NOT NULL, detail TEXT)')


def runtime_get(key, default=None):
    with connect() as c:
        row=c.execute('SELECT value FROM keke_runtime WHERE key=?',(key,)).fetchone()
    return json.loads(row['value']) if row else default


def runtime_set(key, value):
    with connect() as c:
        c.execute('INSERT INTO keke_runtime(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                  (key,json.dumps(value)))


def opening_seen(ticker, seconds, had_history):
    with connect() as c:
        c.execute('INSERT OR IGNORE INTO keke_opening_health(ticker,first_seen,first_seconds,had_history) VALUES(?,?,?,?)',
                  (ticker,int(time.time()),seconds,int(bool(had_history))))
        if had_history:
            c.execute('UPDATE keke_opening_health SET had_history=1 WHERE ticker=?',(ticker,))


def record_event(ticker,event,detail=''):
    with connect() as c:
        c.execute('INSERT INTO keke_v518_events(ticker,event,captured_at,detail) VALUES(?,?,?,?)',
                  (ticker,event,int(time.time()),detail))


def http_json(url,params=None,timeout=6):
    r=session.get(url,params=params,timeout=timeout)
    r.raise_for_status()
    obj=r.json()
    if isinstance(obj,dict) and obj.get('error'):
        raise ValueError(str(obj['error']))
    return obj

def number(v):
    try:
        if v is None or v=='': return None
        return float(v)
    except (ValueError,TypeError): return None

def quote(m,side,kind):
    # Modern dollar fields first; integer cent fields only as a fallback.
    prefix='yes' if side=='UP' else 'no'
    value=number(m.get(f'{prefix}_{kind}_dollars'))
    if value is None:
        raw=number(m.get(f'{prefix}_{kind}'))
        value=raw/100 if raw is not None else None
    if value is not None and not (0 <= value <= 1): return None
    return value

def target_of(m):
    for key in ('floor_strike','functional_strike'):
        n=number(m.get(key))
        if n is not None and 10000<=n<=1000000: return n
    for key in ('title','subtitle','yes_sub_title','no_sub_title'):
        for found in re.findall(r'\$?([0-9]{2,3}(?:,[0-9]{3})+(?:\.[0-9]+)?|[0-9]{4,6}(?:\.[0-9]+)?)',str(m.get(key) or '')):
            n=number(found.replace(',',''))
            if n is not None and 10000<=n<=1000000: return n
    return None

def get_market():
    obj=http_json(BASE+'/markets',{'series_ticker':SERIES,'status':'open','limit':100})
    now=datetime.now(timezone.utc)
    eligible=[]
    for m in obj.get('markets',[]):
        try:
            close=datetime.fromisoformat(m['close_time'].replace('Z','+00:00'))
            if close>now:
                eligible.append((close,m))
        except (KeyError,TypeError,ValueError): pass
    return min(eligible,key=lambda x:x[0])[1] if eligible else None

def seconds_remaining(m):
    close=datetime.fromisoformat(m['close_time'].replace('Z','+00:00'))
    return max(0,int((close-datetime.now(timezone.utc)).total_seconds()))

def save_brti_ticks():
    """Persist authentic index observations, including any history in the response."""
    obj=brti_raw()
    data=obj.get('data')
    payload=data.get('payload') if isinstance(data,dict) else None
    if not isinstance(payload,list) or not payload:
        raise ValueError('No BRTI payload')
    now=time.time(); ticks=[]
    for item in payload:
        if not isinstance(item,dict):continue
        price=number(item.get('value'));stamp=number(item.get('time'))
        # Repeat markers without a numeric value are not independently reconstructible.
        if item.get('repeatOfPreviousValue') is True and price is None:continue
        if price is None or stamp is None or not math.isfinite(price) or not math.isfinite(stamp) or not (10000<price<1000000):continue
        ts=stamp/1000 if stamp>1e11 else stamp
        if not -5<=now-ts<=7200:continue
        ticks.append((int(ts),price))
    if not ticks:raise ValueError('No valid BRTI observations')
    with connect() as c:
        c.executemany('INSERT OR IGNORE INTO keke_brti_ticks(timestamp,price) VALUES (?,?)',sorted(ticks))
        c.execute('DELETE FROM keke_brti_ticks WHERE timestamp<?',(int(now)-86400,))
    latest_ts,latest_price=max(ticks)
    if now-latest_ts>BRTI_MAX_AGE:raise ValueError('Stale BRTI feed')
    return latest_price,latest_ts

def brti_candles():
    """Only complete UTC minutes with observed data near both minute boundaries.
    Missing minutes are never interpolated or silently filled.
    """
    now=int(time.time());cutoff=now//60*60
    with connect() as c:
        rows=c.execute('SELECT timestamp,price FROM keke_brti_ticks WHERE timestamp>=? AND timestamp<? ORDER BY timestamp',
                       (cutoff-7200,cutoff)).fetchall()
    buckets={}
    for row in rows:
        minute=row['timestamp']//60*60
        buckets.setdefault(minute,[]).append((row['timestamp'],row['price']))
    candles=[]
    for t,values in sorted(buckets.items()):
        # Require actual samples from both halves of each completed minute.
        if values[0][0]>t+20 or values[-1][0]<t+40:continue
        prices=[v[1] for v in values]
        candles.append({'time':t,'open':prices[0],'high':max(prices),
                        'low':min(prices),'close':prices[-1],'volume':0.0})
    return candles

def brti_raw():
    """Fetch authenticated index response; do not log credentials or payload."""
    key_id=os.getenv('KALSHI_API_KEY_ID','').strip()
    pem=os.getenv('KALSHI_PRIVATE_KEY','').replace('\\n','\n').strip()
    pem_path=os.getenv('KALSHI_PRIVATE_KEY_PATH','').strip()
    if not key_id or not (pem or pem_path):
        raise ValueError('Missing BRTI credentials')
    key_bytes=open(pem_path,'rb').read() if pem_path else pem.encode()
    key=serialization.load_pem_private_key(key_bytes,password=None)
    ts=str(int(time.time()*1000))
    signed=f'{ts}GET{BRTI_PATH}'.encode()
    if isinstance(key,ed25519.Ed25519PrivateKey):
        signature=key.sign(signed)
    else:
        signature=key.sign(signed,padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=padding.PSS.DIGEST_LENGTH),hashes.SHA256())
    headers={'KALSHI-ACCESS-KEY':key_id,'KALSHI-ACCESS-TIMESTAMP':ts,
             'KALSHI-ACCESS-SIGNATURE':base64.b64encode(signature).decode()}
    response=session.get('https://external-api.kalshi.com'+BRTI_PATH,params={'id':'BRTI'},headers=headers,timeout=8)
    response.raise_for_status()
    obj=response.json()
    if not isinstance(obj,dict) or obj.get('error'):
        raise ValueError('BRTI response error')
    return obj

def brti_live():
    """Read the freshest validated BRTI tick from Kalshi data.payload."""
    obj=brti_raw()
    data=obj.get('data')
    payload=data.get('payload') if isinstance(data,dict) else None
    if not isinstance(payload,list) or not payload:
        raise ValueError('BRTI data.payload missing or empty')
    now=time.time()
    valid=[]
    for item in payload:
        if not isinstance(item,dict) or item.get('repeatOfPreviousValue') is True:
            continue
        price=number(item.get('value'))
        stamp=number(item.get('time'))
        if price is None or stamp is None or not math.isfinite(price) or not math.isfinite(stamp):
            continue
        if not 10000 < price < 1000000:
            continue
        stamp_seconds=stamp/1000 if stamp>1e11 else stamp
        age=now-stamp_seconds
        if -5<=age<=BRTI_MAX_AGE:
            valid.append((stamp_seconds,price))
    if not valid:
        raise ValueError('No fresh valid BRTI tick (time/value validation failed)')
    stamp_seconds,price=max(valid,key=lambda x:x[0])
    return price,stamp_seconds

def ema(values, period):
    k=2/(period+1)
    out=values[0]
    for x in values[1:]:out=x*k+out*(1-k)
    return out

def rsi(values, period=14):
    diffs=[values[i]-values[i-1] for i in range(1,len(values))]
    recent=diffs[-period:]
    gains=sum(max(x,0) for x in recent)/period
    losses=sum(max(-x,0) for x in recent)/period
    if gains==0 and losses==0:return 50.0
    if losses==0:return 100.0
    return 100-100/(1+gains/losses)

def assessment(m,btc,candles,brti=None):
    now=int(time.time()); seconds=seconds_remaining(m); target=target_of(m)
    bids={side:quote(m,side,'bid') for side in ('UP','DOWN')}
    asks={side:quote(m,side,'ask') for side in ('UP','DOWN')}
    closes=[c['close'] for c in candles]
    delta1=btc-closes[-1];delta3=btc-closes[-min(3,len(closes))]
    ranges=[c['high']-c['low'] for c in candles[-12:]]
    typical_range=sum(ranges)/len(ranges)
    trend='UP' if len(closes)>=9 and closes[-1]>closes[-min(6,len(closes))] and closes[-3]>closes[-9] else ('DOWN' if len(closes)>=9 and closes[-1]<closes[-6] and closes[-3]<closes[-9] else 'MIXED')
    distance=brti-target if brti is not None and target is not None else None
    favored='UP' if distance is not None and distance>0 else ('DOWN' if distance is not None and distance<0 else None)
    # Completed 1-minute BRTI candles; history is accumulated persistently.
    e5=ema(closes[-50:],5);e13=ema(closes[-50:],13)
    r=rsi(closes)
    vol=[c['volume'] for c in candles[-21:]]
    vol_ratio=None  # BRTI index has no trade volume
    recent=candles[-16:-1] if len(candles)>=17 else candles[:-1] or candles
    resistance=max(c['high'] for c in recent)
    support=min(c['low'] for c in recent)
    vwap_num=sum(((c['high']+c['low']+c['close'])/3)*c['volume'] for c in candles[-15:])
    vwap_den=sum(c['volume'] for c in candles[-15:])
    vwap=None  # BRTI has no exchange volume; do not mislabel an average as VWAP
    # A directional research score, NOT a probability or claimed statistical edge.
    score=0.0
    score+=1.0 if btc>e5 else -1.0
    score+=1.0 if e5>e13 else -1.0
    # No volume-weighted indicators on BRTI
    score+=.75 if delta3>0 else -.75
    score+=.5 if closes[-1]>closes[-min(6,len(closes))] else -.5
    score+=.5 if r>55 else (-.5 if r<45 else 0)
    if btc>resistance:score+=.5
    if btc<support:score-=.5
    # Both candle structure and target distance use BRTI.
    if distance is not None:
        normalized=distance/max(typical_range*max(seconds/60,1)**.5,10)
        score+=max(-2.0,min(2.0,normalized))
    side='UP' if score>0 else 'DOWN'
    strength=abs(score)
    ask=asks[side];bid=bids[side]
    spread=ask-bid if ask is not None and bid is not None else None
    multiple=1/ask if ask is not None and ask>0 else None
    decision='WAIT';confidence='UNVALIDATED';reasons=[]
    if brti is None or target is None:
        reasons.append('BRTI/target unavailable')
    elif seconds<MIN_ENTRY_SECONDS:
        reasons.append('Late contract')
    elif ask is None or bid is None or spread is None or spread<0 or spread>.08:
        reasons.append('Quote/spread unavailable')
    elif ask>=.92:
        reasons.append('Low payout multiple')
    elif strength>=3.0:
        decision=f'PAPER BUY {side}'
        reasons.append('Experimental multi-indicator alignment; not calibrated')
    elif strength>=1.5:
        decision=f'WATCH {side}'
        reasons.append('Directional setup developing')
    else:
        reasons.append('Mixed directional evidence')
    indicators={'ema5':round(e5,2),'ema13':round(e13,2),'rsi14':round(r,1),
                'vwap15':None,'vol_ratio':None,
                'support15':round(support,2),'resistance15':round(resistance,2),
                'momentum1':round(delta1,2),'momentum3':round(delta3,2),
                'momentum5':round(btc-closes[-min(5,len(closes))],2),'momentum15':round(btc-closes[-min(15,len(closes))],2)}
    return {'ticker':m['ticker'],'at':now,'seconds':seconds,'btc':brti,'kraken':btc, # legacy field name; value is BRTI, never Kraken
            'source':'BRTI' if brti is not None else 'UNAVAILABLE','target':target,
            'distance':distance,'favored':favored,'trend':trend,'delta1':delta1,
            'delta3':delta3,'volatility':typical_range,'confidence':confidence,
            'decision':decision,'reason':'; '.join(reasons),'bids':bids,'asks':asks,
            'predicted':side,'score':score,'multiplier':multiple,'indicators':indicators,
            'brti_stamp':state.get('current_brti_stamp'),
            'market_close':m.get('close_time')}

def record_v53(a):
    # First WATCH and first PAPER BUY per side per contract; immutable first-call record.
    if a['source']!='BRTI' or a['decision']=='WAIT':return
    stage='PAPER_BUY' if a['decision'].startswith('PAPER BUY') else 'WATCH'
    side=a['predicted']
    with connect() as c:
        c.execute('''INSERT OR IGNORE INTO v53_opportunities
          (ticker,side,captured_at,seconds_left,stage,ask,multiplier,score,brti,target,distance,indicators)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
          (a['ticker'],side,a['at'],a['seconds'],stage,a['asks'][side],a['multiplier'],
           a['score'],a['btc'],a['target'],a['distance'],json.dumps(a['indicators'])))

def record(a):
    with connect() as c:
        c.execute('''INSERT INTO settlement_observations
          (captured_at,ticker,seconds_left,btc_price,target,distance,yes_bid,yes_ask,no_bid,no_ask,
           trend,momentum_1m,momentum_3m,volatility_1m,favored_side,confidence,decision,reason,model_version)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
          (a['at'],a['ticker'],a['seconds'],a['btc'],a['target'],a['distance'],
           a['bids']['UP'],a['asks']['UP'],a['bids']['DOWN'],a['asks']['DOWN'],
           a['trend'],a['delta1'],a['delta3'],a['volatility'],a['favored'],
           a['confidence'],a['decision'],a['reason'],'5.9-BRTI'))
        if a['decision'].startswith('ENTER '):
            side=a['decision'].split()[-1]
            c.execute('''INSERT OR IGNORE INTO settlement_calls
              (ticker,created_at,seconds_left,side,ask,btc_price,target,confidence,reason)
              VALUES (?,?,?,?,?,?,?,?,?)''',
              (a['ticker'],a['at'],a['seconds'],side,a['asks'][side],a['btc'],
               a['target'],a['confidence'],a['reason']))

def record_early_prediction(a):
    # One independent early directional baseline per contract
    if a['source']!='BRTI' or a['favored'] not in ('UP','DOWN'):
        return
    if a['seconds'] < 600 or a['seconds'] > 900:
        return
    if a['distance'] is None or abs(a['distance']) < MIN_CUSHION:
        return
    with connect() as c:
        c.execute('''INSERT OR IGNORE INTO settlement_predictions
            (ticker,captured_at,seconds_left,side,source,distance,ask)
            VALUES (?,?,?,?,?,?,?)''',
            (a['ticker'],a['at'],a['seconds'],a['favored'],a['source'],
             a['distance'],a['asks'].get(a['favored'])))


def official_result(ticker):
    # Public market result only
    # Tickers are sourced from Kalshi, but still restrict path components.
    if not re.fullmatch(r'[A-Za-z0-9_-]{4,150}',ticker):
        raise ValueError('Invalid market ticker')
    try:
        obj=http_json(BASE+'/markets/'+ticker,timeout=8)
    except requests.exceptions.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 404:
            raise
        obj=http_json(BASE+'/historical/markets/'+ticker,timeout=8)
    m=obj.get('market') if isinstance(obj,dict) else None
    if not isinstance(m,dict) or m.get('ticker') != ticker:
        raise ValueError('Unexpected market response')
    result=str(m.get('result') or '').lower()
    if result=='yes':return 'UP'
    if result=='no':return 'DOWN'
    if result in ('','null','none'):
        return None
    # scalar, void, and unknown outcomes are never scored as a win/loss.
    LOG.warning('Nonbinary market result for %s: %s',ticker,result)
    return 'UNGRADABLE'


def grade_settlements():
    # Grade against Kalshi market results
    with connect() as c:
        pending=c.execute('''SELECT ticker FROM (
            SELECT ticker FROM settlement_calls WHERE result='PENDING'
            UNION SELECT ticker FROM settlement_predictions WHERE grade='PENDING'
            UNION SELECT ticker FROM v53_opportunities WHERE grade='PENDING'
            UNION SELECT ticker FROM keke_thesis_events WHERE grade='PENDING'
            UNION SELECT ticker FROM keke_v56_forecasts WHERE grade='PENDING'
            UNION SELECT ticker FROM keke_shadow_predictions WHERE grade='PENDING'
            UNION SELECT ticker FROM keke_v527_checkpoints WHERE grade='PENDING'
            UNION SELECT ticker FROM keke_locked_forecasts WHERE grade='PENDING'
            UNION SELECT ticker FROM keke_consensus_predictions WHERE grade='PENDING'
        ) ORDER BY ticker LIMIT ?''',(GRADE_BATCH,)).fetchall()
    verified_now=0
    newly_finalized=[]
    notices=[]
    for row in pending:
        ticker=row['ticker']
        try:
            outcome=official_result(ticker)
        except Exception as exc:
            LOG.warning('Official settlement lookup failed for %s: %s',ticker,type(exc).__name__)
            continue
        if outcome is None:
            continue
        now=int(time.time())
        with connect() as c:
            if outcome=='UNGRADABLE':
                research_grade(ticker,outcome,now)
                c.execute("UPDATE keke_v527_checkpoints SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
                c.execute("UPDATE settlement_calls SET result='UNGRADABLE',result_source='Kalshi market result' WHERE ticker=? AND result='PENDING'",(ticker,))
                c.execute("UPDATE settlement_predictions SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
                c.execute("UPDATE v53_opportunities SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
                c.execute("UPDATE keke_thesis_events SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
                c.execute("UPDATE keke_v56_forecasts SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
                c.execute("UPDATE keke_shadow_predictions SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
                continue
            research_grade(ticker,outcome,now)
            v527_grade(ticker,outcome,now)
            c.execute('''INSERT INTO settlement_results(ticker,settled_side,source,recorded_at,verified)
                VALUES (?,?,?,?,1) ON CONFLICT(ticker) DO UPDATE SET
                settled_side=excluded.settled_side,source=excluded.source,
                recorded_at=excluded.recorded_at,verified=1''',
                (ticker,outcome,'Kalshi public market result',now))
            c.execute('''UPDATE settlement_calls SET
                result=CASE WHEN side=? THEN 'WIN' ELSE 'LOSS' END,
                result_source='Kalshi public market result'
                WHERE ticker=? AND result='PENDING' ''',(outcome,ticker))
            c.execute('''UPDATE settlement_predictions SET
                grade=CASE WHEN side=? THEN 'WIN' ELSE 'LOSS' END,
                official_side=?,graded_at=? WHERE ticker=? AND grade='PENDING' ''',
                (outcome,outcome,now,ticker))
            c.execute('''UPDATE v53_opportunities SET grade=CASE WHEN side=? THEN 'WIN' ELSE 'LOSS' END, official_side=?, graded_at=? WHERE ticker=? AND grade='PENDING' ''',(outcome,outcome,now,ticker))
            final_pending=c.execute("SELECT 1 FROM keke_thesis_events WHERE ticker=? AND event='FINAL' AND grade='PENDING'",(ticker,)).fetchone()
            c.execute('''UPDATE keke_thesis_events SET grade=CASE WHEN side=? THEN 'WIN' ELSE 'LOSS' END, official_side=?, graded_at=? WHERE ticker=? AND grade='PENDING' ''',(outcome,outcome,now,ticker))
            if final_pending:newly_finalized.append(ticker)
            new_v56=c.execute("SELECT * FROM keke_v56_forecasts WHERE ticker=? AND grade='PENDING' ORDER BY id",(ticker,)).fetchall()
            c.execute('''UPDATE keke_v56_forecasts SET grade=CASE WHEN side=? THEN 'WIN' ELSE 'LOSS' END,
                official_side=?,graded_at=? WHERE ticker=? AND grade='PENDING' ''',(outcome,outcome,now,ticker))
            c.execute("""UPDATE keke_shadow_predictions
                SET grade=CASE WHEN side=? THEN 'WIN' ELSE 'LOSS' END,
                    official_side=?,graded_at=? WHERE ticker=? AND grade='PENDING'""",
                (outcome,outcome,now,ticker))
            if new_v56:
                first=next((r for r in new_v56 if r['event']=='INITIAL'),None)
                late=next((r for r in new_v56 if r['event']=='LATE'),None)
                last=new_v56[-1]
                notices.append((ticker,outcome,first['side'] if first else None,late['side'] if late else None,last['side']))
            verified_now+=1
    if verified_now:
        LOG.info('Official outcomes newly verified: %d',verified_now)
    if state['enabled']:
        for ticker,outcome,first,late,last in notices:
            send(f'🏁 OFFICIAL RESULT: {outcome}\n🎯 {ticker}\n'
                 f'Early {first or "n/a"}: {("WIN" if first==outcome else "LOSS") if first else "NO PICK"} • '
                 f'Late {late or "n/a"}: {("WIN" if late==outcome else "LOSS") if late else "NO PICK"} • '
                 f'Final {last}: {"WIN" if last==outcome else "LOSS"}\nKalshi official market result')


def stats_epoch():
    with connect() as c:
        row=c.execute('SELECT started_at FROM keke_model_epochs WHERE version=?',('5.9-BRTI-STATS',)).fetchone()
    return int(row['started_at'])


def brti_stats_summary():
    """BRTI-only model epoch; historical stats remain available via /legacy."""
    cutoff=stats_epoch()
    with connect() as c:
        obs=c.execute('SELECT COUNT(*) n, COUNT(DISTINCT ticker) contracts FROM settlement_observations WHERE model_version=? AND captured_at>=?',('5.9-BRTI',cutoff)).fetchone()
        forecasts=c.execute('SELECT ticker,event,side,grade FROM keke_v56_forecasts WHERE captured_at>=? ORDER BY ticker,id',(cutoff,)).fetchall()
        paper=c.execute('SELECT stage,grade FROM v53_opportunities WHERE captured_at>=?',(cutoff,)).fetchall()
    def summary(rows):
        w=sum(r['grade']=='WIN' for r in rows)
        l=sum(r['grade']=='LOSS' for r in rows)
        pending=sum(r['grade']=='PENDING' for r in rows)
        pct=f'{100*w/(w+l):.1f}%' if w+l else 'n/a'
        return f'{w}W / {l}L ({pct}) • {pending} pending'
    initial=[r for r in forecasts if r['event']=='INITIAL']
    late=[r for r in forecasts if r['event']=='LATE']
    reversal=[r for r in forecasts if r['event']=='REVERSAL']
    latest={}
    for r in forecasts: latest[r['ticker']]=r
    paper_buy=[r for r in paper if r['stage']=='PAPER_BUY']
    return (f'📊 KEKE • BRTI-ONLY\n\n'
            f'🎯 Initial: {summary(initial)}\n'
            f'📡 Late: {summary(late)}\n'
            f'🏁 Last recorded (not an early forecast): {summary(list(latest.values()))}\n'
            f'🔄 Reversals: {summary(reversal)}\n\n'
            f'🧪 Paper outcomes: {summary(paper_buy)}\n'
            '💰 Actual realized scalp P/L: requires verified fills\n\n'
            f'📋 {obs["contracts"]} contracts • {obs["n"]} snapshots\n'
            '📚 /legacy = previous model history\n\n'
            '⚠️ Last-recorded accuracy includes reversals and late calls. /audit /research for frozen checkpoints. Accuracy ≠ trading profit.')


def early_pick_stats_summary(now=None):
    """Score only contracts actually observed during their opening window."""
    now = int(time.time()) if now is None else int(now)
    with connect() as c:
        epoch = c.execute("SELECT started_at FROM keke_model_epochs WHERE version=?",
                          ('5.16-EARLY-PICKS',)).fetchone()
        if not epoch:
            return '🎯 EARLY-PICK TEST: waiting for V5.16 initialization.'
        start = int(epoch['started_at'])
        # seconds_left counts down to settlement. End = observation time + remaining.
        # Use MAX so an unusually early/stale observation cannot mark a market finished.
        monitored = c.execute("""
            SELECT ticker, MAX(captured_at + seconds_left) AS contract_end
            FROM settlement_observations
            WHERE captured_at >= ? AND seconds_left BETWEEN 780 AND 900
              AND model_version = '5.9-BRTI'
            GROUP BY ticker
        """, (start,)).fetchall()
        eligible = sorted(r['ticker'] for r in monitored if now >= int(r['contract_end']))
        missed = c.execute('SELECT COUNT(*) AS n FROM keke_opening_health WHERE first_seen>=? AND first_seconds<780', (start,)).fetchone()['n']
        if not eligible:
            return ('🎯 EARLY-PICK TEST • V5.16 onward\n'
                    'No completed opening-window contracts yet. Historical picks excluded.')
        placeholders = ','.join('?' for _ in eligible)
        rows = c.execute(f"""
            SELECT ticker,event,side,grade,seconds_left,ask
            FROM keke_v56_forecasts
            WHERE captured_at >= ? AND ticker IN ({placeholders})
            ORDER BY id
        """, (start, *eligible)).fetchall()
    by_ticker = {}
    for r in rows:
        by_ticker.setdefault(r['ticker'], []).append(r)
    early = {}
    for ticker, events in by_ticker.items():
        qualifying = [r for r in events if r['event'] == 'INITIAL'
                      and r['seconds_left'] is not None
                      and 780 <= r['seconds_left'] <= 900]
        if qualifying:
            early[ticker] = qualifying[0]
    wins = sum(r['grade'] == 'WIN' for r in early.values())
    losses = sum(r['grade'] == 'LOSS' for r in early.values())
    pending = sum(r['grade'] == 'PENDING' for r in early.values())
    ungradable = sum(r['grade'] == 'UNGRADABLE' for r in early.values())
    graded = wins + losses
    accuracy = f'{100 * wins / graded:.1f}%' if graded else 'n/a'
    coverage = f'{100 * len(early) / len(eligible):.1f}%'
    reversed_contracts = sum(any(r['event'] == 'REVERSAL'
                                 for r in by_ticker.get(t, [])) for t in early)
    return (f'🎯 EARLY-PICK TEST • V5.16 onward\n'
            f'Opening-window contracts: {len(eligible)}\n'
            f'First observed after opening window: {missed} (excluded)\n'
            f'Initial picks: {len(early)} • Coverage: {coverage}\n'
            f'No early pick: {len(eligible) - len(early)}\n'
            f'Graded early-pick accuracy: {wins}W / {losses}L ({accuracy}; {graded} graded)'
            f' • {pending} pending • {ungradable} ungradable\n'
            f'Reversed after early pick: {reversed_contracts}/{len(early)}\n'
            'Only monitored, completed contracts count; historical picks excluded.\n'
            'Paper accuracy is not realized trading profit.')


def prospective_early_summary(now=None):
    """V5.19+ eligible openings, including incomplete-history observations."""
    now=int(time.time()) if now is None else int(now)
    with connect() as c:
        start=c.execute("SELECT started_at FROM keke_model_epochs WHERE version='5.19-REPAIRED'").fetchone()
        if not start:return '🎯 V5.19 EARLY TEST: waiting for initialization'
        start=start['started_at']
        rows=c.execute('''SELECT ticker,MIN(first_seen) first_seen,MAX(first_seconds) first_seconds,
                          MAX(had_history) had_history FROM keke_opening_health
                          WHERE first_seen>=? GROUP BY ticker''',(start,)).fetchall()
        completed=[r for r in rows if r['first_seconds'] is not None and now>=r['first_seen']+r['first_seconds']]
        eligible=[r for r in completed if r['first_seconds']>=780]
        missed=[r for r in completed if r['first_seconds']<780]
        lacking=sum(not r['had_history'] for r in eligible)
        picks=[]
        for r in eligible:
            pick=c.execute('''SELECT * FROM keke_v56_forecasts WHERE ticker=? AND event='INITIAL'
                              AND seconds_left BETWEEN 780 AND 900 AND captured_at>=?
                              ORDER BY id LIMIT 1''',(r['ticker'],start)).fetchone()
            if pick:picks.append(pick)
    wins=sum(p['grade']=='WIN' for p in picks)
    losses=sum(p['grade']=='LOSS' for p in picks)
    pending=sum(p['grade']=='PENDING' for p in picks)
    graded=wins+losses
    pct=f'{100*wins/graded:.1f}%' if graded else 'n/a'
    cov=f'{100*len(picks)/len(eligible):.1f}%' if eligible else 'n/a'
    return (f'🎯 V5.19 EARLY-PICK COHORT\n'
            f'Completed monitored openings: {len(eligible)} • Coverage: {cov}\n'
            f'Early picks: {len(picks)} • No pick: {len(eligible)-len(picks)}\n'
            f'Graded: {wins}W/{losses}L ({pct}) • Pending: {pending}\n'
            f'Insufficient history at last observation: {lacking}\n'
            f'First seen too late (excluded): {len(missed)}\n'
            'Historical pre-V5.19 picks excluded. No real trade P/L.')


def grade_summary():
    with connect() as c:
        observations=c.execute('SELECT COUNT(*) FROM settlement_observations').fetchone()[0]
        contracts=c.execute('SELECT COUNT(DISTINCT ticker) FROM settlement_observations').fetchone()[0]
        calls=c.execute("SELECT result,COUNT(*) n FROM settlement_calls GROUP BY result").fetchall()
        predictions=c.execute("SELECT grade,COUNT(*) n FROM settlement_predictions GROUP BY grade").fetchall()
        verified=c.execute('SELECT COUNT(*) FROM settlement_results WHERE verified=1').fetchone()[0]
        v53=c.execute("SELECT stage,grade,COUNT(*) n FROM v53_opportunities GROUP BY stage,grade").fetchall()
        returns=c.execute("SELECT ask,result FROM settlement_calls WHERE result IN ('WIN','LOSS') AND ask IS NOT NULL AND ask>0 AND ask<1").fetchall()
        thesis=c.execute("SELECT event,grade,ask FROM keke_thesis_events").fetchall()
        v56=c.execute("SELECT event,grade,ask FROM keke_v56_forecasts").fetchall()
        shadow_rows=c.execute("SELECT checkpoint,grade FROM keke_shadow_predictions").fetchall()
    call_counts={r['result']:r['n'] for r in calls}
    pred_counts={r['grade']:r['n'] for r in predictions}
    cw=call_counts.get('WIN',0);cl=call_counts.get('LOSS',0)
    pw=pred_counts.get('WIN',0);pl=pred_counts.get('LOSS',0)
    def pct(w,l):return f'{100*w/(w+l):.1f}%' if w+l else 'n/a'
    # Illustrative payoff per one contract at the recorded ask; excludes fees,
    # slippage, missed fills, and market impact. Not realized profit.
    gross=sum((1-r['ask']) if r['result']=='WIN' else -r['ask'] for r in returns)
    research={(x['stage'],x['grade']):x['n'] for x in v53}
    bw=research.get(('PAPER_BUY','WIN'),0);bl=research.get(('PAPER_BUY','LOSS'),0)
    def event_line(event):
        rows=[r for r in thesis if r['event']==event]
        w=sum(r['grade']=='WIN' for r in rows);l=sum(r['grade']=='LOSS' for r in rows)
        pending=sum(r['grade']=='PENDING' for r in rows)
        # Per $1 hypothetical stake; excludes fees and fills.
        net=sum(((1/r['ask']-1) if r['grade']=='WIN' else -1)
                for r in rows if r['grade'] in ('WIN','LOSS') and r['ask'] is not None and 0<r['ask']<=1)
        suffix=f' • illustrative ${net:+.2f}' if event!='FINAL' else ''
        return f'{event}: {w}W/{l}L ({pct(w,l)}) pending {pending}{suffix}'
    def v56_line(event):
        rows=[r for r in v56 if r['event']==event]
        w=sum(r['grade']=='WIN' for r in rows);l=sum(r['grade']=='LOSS' for r in rows)
        return f'V5.6 {event}: {w}W/{l}L • {pct(w,l)} • pending {sum(r["grade"]=="PENDING" for r in rows)}'
    shadow_lines=[]
    for checkpoint in SHADOW_CHECKPOINTS:
        rows=[r for r in shadow_rows if r['checkpoint']==checkpoint]
        w=sum(r['grade']=='WIN' for r in rows)
        l=sum(r['grade']=='LOSS' for r in rows)
        pending=sum(r['grade']=='PENDING' for r in rows)
        shadow_lines.append(f'Shadow {checkpoint//60}m: {w}W/{l}L • {pct(w,l)} • pending {pending}')
    shadow_report='\n'.join(shadow_lines)
    return (f'📊 KEKE REPORT CARD\n' 
            f'Contracts {contracts} • Snapshots {observations} • Official results {verified}\n'
            f'🚨 ENTER: {cw}W / {cl}L • {pct(cw,cl)} • pending {call_counts.get("PENDING",0)}\n'
            f'🔎 EARLY LEAN: {pw}W / {pl}L • {pct(pw,pl)} • pending {pred_counts.get("PENDING",0)}\n'
            f'🧪 V5.3 PAPER: {bw}W / {bl}L • {pct(bw,bl)} • pending {research.get(("PAPER_BUY","PENDING"),0)}\n'
            f'Legacy {event_line("INITIAL")}\n'
            f'Legacy {event_line("REVERSAL")}\n'
            f'Legacy {event_line("FINAL")}\n'
            f'{v56_line("INITIAL")}\n'
            f'{v56_line("REVERSAL")}\n'
             f'{shadow_report}\n'
            f'Illustrative ENTER gross: ${gross:+.2f} across {len(returns)} graded calls (1 contract each; NO fees/fills)\n'
            'Official outcomes only • No real trades placed')

def tg(method,**data):
    if not TOKEN: return None
    r=session.post(f'https://api.telegram.org/bot{TOKEN}/{method}',data=data,timeout=7)
    r.raise_for_status()
    return r.json()

# V5.30: compact user-facing keyboard; legacy slash commands remain available.
KEYBOARD={'keyboard':[[{'text':'📡 LIVE'},{'text':'📊 STATS'}],
                      [{'text':'📍 POSITION'},{'text':'📜 HISTORY'}],
                      [{'text':'😴 SLEEP'},{'text':'☀️ WAKE'}],
                      [{'text':'⚙️ SETTINGS'},{'text':'❓ HELP'}]],
          'resize_keyboard':True,'is_persistent':True}

def confidence_report():
    """Explain experimental directional strength without claiming win probability."""
    read=shadow_view.get('last')
    if not read:
        return ('🎯 KEKE CONFIDENCE • RESEARCH ONLY\n'
                'Waiting for fresh, contiguous BRTI candles and a verified target.\n'
                'No calibrated win probability available.')
    score=abs(float(read['score']))
    strength=('STRONG' if score>=1.25 else 'FAVORABLE' if score>=.75
              else 'TENTATIVE' if score>=.45 else 'WAIT')
    side=read['side']
    return (f'🎯 KEKE CONFIDENCE • EXPERIMENTAL\n'
            f'Settlement lean: {side} | Evidence: {strength}\n'
            f'Regime: {read["regime"]} | Time left: {read["seconds"]//60}m {read["seconds"]%60:02d}s\n'
            f'Distance/time component: {read["base"]:+.2f}\n'
            f'Structure adjustment: {read["structure"]:+.2f}\n'
            'This is NOT a win probability or a trade recommendation. '
            'Official predictions remain unchanged; compare with verified settlements.')

def menu_settings():
    return ('⚙️ KEKE SETTINGS\n'
            '😴 Sleep pauses Telegram alerts; ☀️ Wake resumes them. Research continues.\n'
            'Advanced research: /shadow, /shadowon, /shadowoff, /audit, /research, /export\n'
            'Check experimental confidence: /confidence')

def menu_help():
    return ('❓ KEKE HELP\n'
            '📡 Live: fresh market read | 📊 Stats: verified results\n'
            '📍 Position: current position | 📜 History: latest settlement pick\n'
            '😴 Sleep / ☀️ Wake: pause or resume alerts\n'
            '⚙️ Settings: advanced research controls\n'
            'Confidence is an experimental evidence score, NOT a guaranteed outcome.')

def send(message):
    if not TOKEN or not CHAT:
        LOG.info('Telegram unavailable; %s',message)
        return
    try: tg('sendMessage',chat_id=CHAT,text=message,reply_markup=json.dumps(KEYBOARD))
    except Exception: LOG.exception('Telegram send failed')

def money_text(x): return f'${x:,.2f}' if x is not None else 'UNKNOWN'
def ask_text(x): return f'{x*100:.1f}¢' if x is not None else 'n/a'

def tiny_money(x):
    if x is None:return 'unknown'
    return f'${x:+.4f}' if abs(x)<.01 else f'${x:+.2f}'

def total_money(x):
    if x is None:return 'unknown'
    return f'${x:.4f}' if 0<=x<.01 else f'${x:.2f}'


def brti_opening_levels(candles,live):
    # Levels are always from verified completed BRTI minutes, not an exchange.
    if not candles:return '🟡 Support/resistance: collecting BRTI history'
    latest=candles[-1]['time']
    recent=[c for c in candles if latest-c['time']<=900]
    if len(recent)<5:return '🟡 Support/resistance: collecting BRTI history'
    if any(recent[i]['time']-recent[i-1]['time']!=60 for i in range(1,len(recent))):
        return '🟡 Support/resistance: gaps in BRTI history'
    low=min(c['low'] for c in recent)
    high=max(c['high'] for c in recent)
    window=f'{len(recent)}m' if len(recent)<15 else '15m'
    return (f'🟢 BRTI support ({window}): ${low:,.2f} (BRTI − support: ${live-low:+.2f})\n'
            f'🔴 BRTI resistance ({window}): ${high:,.2f} (resistance − BRTI: ${high-live:+.2f})')

def banner_levels(a):
    """First-line levels from completed BRTI candles; never invent missing levels."""
    raw=a.get('brti_levels') or ''
    found=re.findall(r'(?:BRTI )?(support|resistance)(?: \([^)]*\))?: \$([\d,]+(?:\.\d+)?)',raw,re.I)
    values={name.lower():float(value.replace(',','')) for name,value in found}
    if 'support' in values and 'resistance' in values:
        return f'🆕 S ${values["support"]:,.0f} | R ${values["resistance"]:,.0f}'
    return '🆕 BTC OPEN | S/R collecting'

def brti_levels_text(a):
    return a.get('brti_levels','🟡 BRTI levels unavailable')

def format_read(a, headline=None):
    """Compact Telegram message with whitespace for rapid scanning."""
    mm,ss=divmod(max(0,int(a['seconds'])),60)
    side=a.get('v56_side')
    label=headline or ('🟢 SETTLEMENT: UP' if side=='UP' else '🔴 SETTLEMENT: DOWN' if side=='DOWN' else '👀 ANALYZING')
    if a.get('source')!='BRTI' or a.get('target') is None:
        return f'{label}\n\n🎯 Target: {money_text(a.get("target"))}\n⏱ {mm}:{ss:02d} left\n⚠️ BRTI unavailable'
    price=a.get('btc')
    distance=a.get('distance')
    direction=('above' if distance>=0 else 'below') if distance is not None else 'from'
    relation=f'{abs(distance):,.2f} {direction} target' if distance is not None else 'distance unavailable'
    levels=a.get('brti_levels','')
    # Level text may be generated by an older version of the helper.
    import re as _re
    levels=_re.sub(r'BRTI support \([^)]*\)', 'Support', levels)
    levels=_re.sub(r'BRTI resistance \([^)]*\)', 'Resistance', levels)
    levels=_re.sub(r' \(BRTI − support: [^)]*\)', '', levels)
    levels=_re.sub(r' \(resistance − BRTI: [^)]*\)', '', levels)
    ask=a.get('asks',{}).get(side) if side else None
    price_line=f'📍 BRTI: {money_text(price)}'
    payout=f' • {ask_text(ask)} ask' if ask is not None else ''
    return (f'{label}\n⏱ {mm}:{ss:02d} left{payout}\n\n'
            f'🎯 Target: {money_text(a.get("target"))}\n'
            f'{price_line}\n📏 ${relation}\n\n'
            f'{levels}' if levels else
            f'{label}\n⏱ {mm}:{ss:02d} left{payout}\n\n'
            f'🎯 Target: {money_text(a.get("target"))}\n'
            f'{price_line}\n📏 ${relation}')

def safe_shape(value, depth=0):
    """Show only JSON key names and types; never values or array contents."""
    if isinstance(value, dict):
        if depth >= 3: return '{object}'
        parts=[]
        for k,v in list(value.items())[:12]:
            name=str(k)
            if any(term in name.lower() for term in ('secret','token','password','signature','private','credential','authorization','key')):
                continue
            parts.append(f'{name}: {safe_shape(v,depth+1)}')
        return '{' + ', '.join(parts) + '}'
    if isinstance(value,list):
        return f'list[{len(value)}] ' + (safe_shape(value[0],depth+1) if value and depth<3 else '')
    return type(value).__name__

def brti_record_shape():
    """Report BRTI record field names/types only, never API keys or values."""
    obj=brti_raw()
    data=obj.get('data')
    payload=data.get('payload') if isinstance(data,dict) else None
    if not isinstance(payload,list) or not payload:
        return 'Unexpected response wrapper: '+safe_shape(obj)
    samples=[('FIRST',payload[0]),('LATEST',payload[-1])]
    lines=[f'BRTI records: {len(payload)}']
    for label,item in samples:
        lines.append(f'{label} record: {safe_shape(item,0)}')
    return '\n'.join(lines)

def run_brti_test():
    try:
        price,stamp=brti_live()
        send(f'🟢 BRTI LIVE: ${price:,.2f}\nFeed age: {time.time()-stamp:.0f}s\nSettlement and chart readings use BRTI.')
    except Exception as exc:
        LOG.warning('BRTI test failed: %s',type(exc).__name__)
        try:
            shape=brti_record_shape()
            send('🔎 BRTI RECORD FIELDS (names/types only):\n'+shape[:1600]+'\n⚪ Settlement ENTER remains disabled.')
        except Exception as diag_exc:
            send(f'🔴 BRTI TEST FAILED: {type(exc).__name__}\nDiagnostic fetch: {type(diag_exc).__name__}\nNo settlement ENTER.')

def export_research():
    """Send a consistent, read-only CSV archive; never alter the live database."""
    if not TOKEN or not CHAT:
        return
    tables=('settlement_observations','settlement_calls','settlement_results',
            'settlement_predictions','v53_opportunities','keke_thesis_events','keke_v56_forecasts',
            'keke_v527_checkpoints','keke_v527_warnings',
            'keke_exchange_samples','keke_contract_transitions','keke_transition_snapshots',
            'keke_locked_forecasts','keke_consensus_predictions')
    try:
        # SQLite online backup takes a consistent snapshot, including WAL changes.
        snapshot=sqlite3.connect(':memory:')
        try:
            with connect() as live:
                live.backup(snapshot, pages=100, sleep=0.05)
            output=io.BytesIO()
            counts=[]
            with zipfile.ZipFile(output,'w',compression=zipfile.ZIP_DEFLATED) as z:
                for table in tables:
                    exists=snapshot.execute('SELECT 1 FROM sqlite_master WHERE type=? AND name=?',('table',table)).fetchone()
                    if not exists:
                        continue
                    cur=snapshot.execute('SELECT * FROM "'+table+'"')
                    buf=io.StringIO(newline='')
                    writer=csv.writer(buf)
                    writer.writerow([d[0] for d in cur.description])
                    n=0
                    for row in cur:
                        writer.writerow(row)
                        n+=1
                    z.writestr(table+'.csv',buf.getvalue())
                    counts.append(f'{table}: {n}')
                z.writestr('README.txt','KeKe research export (read-only SQLite snapshot).\n'+ '\n'.join(counts)+'\nNo trading orders or API credentials included.\n')
            output.seek(0)
            if output.getbuffer().nbytes>45*1024*1024:
                send('⚠️ Export is over 45 MB. No data was changed; ask for a smaller export.')
                return
            response=session.post(f'https://api.telegram.org/bot{TOKEN}/sendDocument',
                data={'chat_id':CHAT,'caption':'📦 KeKe research export • CSV files • read-only'},
                files={'document':('keke_research_export.zip',output,'application/zip')},timeout=90)
            response.raise_for_status()
            if not response.json().get('ok'):
                raise ValueError('Telegram rejected export')
        finally:
            snapshot.close()
    except Exception:
        LOG.exception('Research export failed')
        send('⚠️ Export failed. Your existing database was not changed.')

# Independent read-only Kalshi app/chart data observer. Never participates in predictions.
KALSHI_LIVE_POLL_SECONDS = max(5, int(os.getenv('KEKE_KALSHI_LIVE_POLL_SECONDS','5')))

def kalshi_live_init():
    with connect() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS keke_kalshi_live_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, received_utc TEXT NOT NULL,
            request_started_utc TEXT NOT NULL, request_finished_utc TEXT NOT NULL,
            event_ticker TEXT NOT NULL, market_ticker TEXT,
            http_status INTEGER, data_type TEXT, latest_value REAL,
            latest_point_price REAL, latest_point_timestamp TEXT,
            chart_point_path TEXT, response_keys TEXT, details_keys TEXT,
            brti_price REAL, brti_source_utc TEXT,
            yes_bid TEXT, yes_ask TEXT, no_bid TEXT, no_ask TEXT,
            error TEXT)''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_kalshi_live_time ON keke_kalshi_live_observations(received_utc)')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_kalshi_live_raw_samples (
            event_ticker TEXT PRIMARY KEY, captured_utc TEXT NOT NULL,
            response_gzip BLOB NOT NULL)''')

def kalshi_utc(ts=None):
    return datetime.fromtimestamp(time.time() if ts is None else ts,timezone.utc).isoformat(timespec='milliseconds')

def kalshi_timestamp(value):
    if isinstance(value,(int,float)):
        if value>1e12: value=value/1000
        if 1e9<value<3e9:return kalshi_utc(value)
    if isinstance(value,str):
        try:
            dt=datetime.fromisoformat(value.replace('Z','+00:00'))
            if dt.tzinfo:return dt.astimezone(timezone.utc).isoformat(timespec='milliseconds')
        except ValueError:pass
    return None

def kalshi_live_point(payload):
    """Conservatively identify explicitly timestamped BTC price points."""
    found=[]
    def visit(obj,path,depth=0):
        if depth>9:return
        if isinstance(obj,dict):
            stamp=None
            for k in ('timestamp','time','ts','timestamp_ms','time_ms','datetime'):
                if k in obj:
                    stamp=kalshi_timestamp(obj[k])
                    if stamp:break
            if stamp:
                for k in ('value','price','btc_price','index_value'):
                    v=number(obj.get(k))
                    if v is not None and 10000<v<1000000:
                        found.append((stamp,float(v),path+'.'+k))
                        break
            for k,v in obj.items():
                if isinstance(v,(dict,list)):visit(v,path+'.'+str(k),depth+1)
        elif isinstance(obj,list):
            for i,item in enumerate(obj[-15:]):visit(item,path+'['+str(i)+']',depth+1)
    visit(payload,'root')
    return max(found,key=lambda x:x[0]) if found else (None,None,None)

def kalshi_live_worker():
    """Collect chart API snapshots independently, including HTTP failures and raw sample."""
    last_error=0
    while True:
        started=time.monotonic()
        market=None
        try:
            market=get_market()
            if market:
                event=market.get('event_ticker') or market.get('ticker')
                started_utc=kalshi_utc()
                response=session.get(BASE+'/live_data/events/'+str(event),params={'range':'15min'},timeout=10)
                finished_utc=kalshi_utc()
                obj=response.json() if response.ok else {}
                data=obj.get('live_data',obj) if isinstance(obj,dict) else {}
                details=data.get('details',{}) if isinstance(data,dict) else {}
                if not isinstance(details,dict):details={}
                point_ts,point_price,point_path=kalshi_live_point(details)
                latest=number(details.get('latest_value'))
                if latest is None:latest=number(data.get('latest_value')) if isinstance(data,dict) else None
                if latest is not None and not 10000<latest<1000000:latest=None
                brti_stamp=state.get('current_brti_stamp')
                brti_price=(state.get('last_read') or {}).get('btc')
                with connect() as c:
                    c.execute('''INSERT INTO keke_kalshi_live_observations (
                        received_utc,request_started_utc,request_finished_utc,event_ticker,market_ticker,
                        http_status,data_type,latest_value,latest_point_price,latest_point_timestamp,
                        chart_point_path,response_keys,details_keys,brti_price,brti_source_utc,
                        yes_bid,yes_ask,no_bid,no_ask,error)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                        (kalshi_utc(),started_utc,finished_utc,event,market.get('ticker'),
                         response.status_code,data.get('type') if isinstance(data,dict) else None,
                         latest,point_price,point_ts,point_path,
                         json.dumps(list(data.keys()) if isinstance(data,dict) else []),
                         json.dumps(list(details.keys())),brti_price,
                         kalshi_utc(brti_stamp) if isinstance(brti_stamp,(int,float)) else None,
                         str(market.get('yes_bid_dollars') or market.get('yes_bid') or ''),
                         str(market.get('yes_ask_dollars') or market.get('yes_ask') or ''),
                         str(market.get('no_bid_dollars') or market.get('no_bid') or ''),
                         str(market.get('no_ask_dollars') or market.get('no_ask') or ''),
                         None if response.ok else ('HTTP '+str(response.status_code))))
                    if response.ok and isinstance(obj,dict):
                        c.execute('INSERT OR IGNORE INTO keke_kalshi_live_raw_samples VALUES (?,?,?)',
                                  (event,finished_utc,gzip.compress(json.dumps(obj,separators=(',',':')).encode())))
        except Exception as exc:
            if time.time()-last_error>300:
                LOG.warning('Kalshi chart observer: %s',str(exc)[:180]);last_error=time.time()
            try:
                with connect() as c:
                    c.execute('''INSERT INTO keke_kalshi_live_observations
                      (received_utc,request_started_utc,request_finished_utc,event_ticker,market_ticker,error)
                      VALUES (?,?,?,?,?,?)''',
                      (kalshi_utc(),kalshi_utc(),kalshi_utc(),
                       (market or {}).get('event_ticker','UNKNOWN'),
                       (market or {}).get('ticker'),str(exc)[:300]))
            except Exception:LOG.exception('Kalshi observer error logging failed')
        time.sleep(max(1,KALSHI_LIVE_POLL_SECONDS-(time.monotonic()-started)))

def export_kalshi_research():
    """Separate read-only timestamp research export; no trading or signal changes."""
    if not TOKEN or not CHAT:
        return
    tables=('keke_transition_snapshots','keke_contract_transitions',
            'settlement_observations','keke_brti_ticks',
            'keke_kalshi_live_observations')
    try:
        snapshot=sqlite3.connect(':memory:')
        try:
            with connect() as live:
                live.backup(snapshot, pages=100, sleep=0.05)
            output=io.BytesIO()
            counts=[]
            with zipfile.ZipFile(output,'w',compression=zipfile.ZIP_DEFLATED) as z:
                for table in tables:
                    if not snapshot.execute('SELECT 1 FROM sqlite_master WHERE type=? AND name=?',('table',table)).fetchone():
                        continue
                    cur=snapshot.execute('SELECT * FROM "'+table+'"')
                    buf=io.StringIO(newline='')
                    writer=csv.writer(buf)
                    writer.writerow([d[0] for d in cur.description])
                    count=0
                    for record in cur:
                        writer.writerow(record)
                        count+=1
                    z.writestr(table+'.csv',buf.getvalue())
                    counts.append(f'{table}: {count} rows')
                if snapshot.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='keke_kalshi_live_raw_samples'").fetchone():
                    for event,blob in snapshot.execute('SELECT event_ticker,response_gzip FROM keke_kalshi_live_raw_samples'):
                        z.writestr('raw_event_samples/'+re.sub(r'[^A-Za-z0-9_-]','_',event)+'.json',gzip.decompress(blob))
                z.writestr('README.txt',
                    'KeKe Kalshi research export (read-only).\n'
                    'This export includes Kalshi event live_data API observations where available.\n'
                    'The event live_data price is NOT verified as identical to the app BTC Now label.\n'
                    'Raw API JSON sample is saved once per event to inspect actual schema.\n'
                    'Observed timestamps reflect KeKe collection times unless explicitly named as source timestamps.\n'
                    'Market quote timestamps are not independently exchange-verified in this export.\n'
                    'Do not infer a price-feed lead or executable trading advantage from these files alone.\n\n'
                    + '\n'.join(counts) + '\n')
            output.seek(0)
            if output.getbuffer().nbytes>45*1024*1024:
                send('⚠️ Kalshi export exceeds 45 MB; existing data was not changed.')
                return
            response=session.post(f'https://api.telegram.org/bot{TOKEN}/sendDocument',
                data={'chat_id':CHAT,'caption':'🔬 KeKe Kalshi research • existing timestamped observations • ZIP of CSV files'},
                files={'document':('keke_kalshi_research.zip',output,'application/zip')},timeout=90)
            response.raise_for_status()
            if not response.json().get('ok'):
                raise ValueError('Telegram rejected Kalshi export')
        finally:
            snapshot.close()
    except Exception:
        LOG.exception('Kalshi research export failed')
        send('⚠️ Kalshi research export failed. Existing data was not changed.')

def range_stats_summary():
    with connect() as c:
        rows=c.execute("SELECT pattern,entry_ask,exit_bid,gross_per_contract,exit_at FROM keke_range_signals").fetchall()
    entries=[r for r in rows if r['entry_ask'] is not None]
    closed=[r for r in entries if r['exit_at'] is not None]
    wins=sum(r['gross_per_contract']>0 for r in closed)
    losses=sum(r['gross_per_contract']<0 for r in closed)
    gross=sum(r['gross_per_contract'] for r in closed)
    return (f'↔ RANGE STRATEGY • Paper only\n'
            f'Patterns: {len(rows)} • Entries: {len(entries)}\n'
            f'Closed: {len(closed)} ({wins}W/{losses}L) • Open: {len(entries)-len(closed)}\n'
            f'Observed gross: ${gross:+.2f} / 1 contract each\n'
            'Fees and unfilled orders excluded; not realized profit.')


def paper_scalps_summary():
    with connect() as c:
        rows=c.execute('SELECT * FROM keke_paper_scalps ORDER BY opened_at DESC').fetchall()
    closed=[r for r in rows if r['closed_at'] is not None and r['gross_per_contract'] is not None]
    unpriced=sum(r['closed_at'] is not None and r['gross_per_contract'] is None for r in rows)
    open_count=sum(r['closed_at'] is None for r in rows)
    wins=sum(r['gross_per_contract']>0 for r in closed)
    losses=sum(r['gross_per_contract']<0 for r in closed)
    flat=len(closed)-wins-losses
    gross=sum(r['gross_per_contract'] for r in closed)
    return (f'⚡ PAPER SCALPS • 1 hypothetical contract each\n'
            f'Priced exits: {len(closed)} ({wins}W / {losses}L / {flat} flat) • Open: {open_count} • Unpriced: {unpriced}\n'
            f'Gross total: ${gross:+.2f}\n'
            'Uses observed ask on alert and later bid, not verified fills.\n'
            'Fees, slippage and missed executions excluded.\n'
            '💵 Actual realized P/L: unavailable until fills and fees are reconciled.')


def paper_scalps_update(m, a, read):
    """Research-only one-contract round trips. Does not place or infer actual orders."""
    now=int(time.time())
    ticker=m['ticker']
    with connect() as c:
        open_rows=c.execute('SELECT * FROM keke_paper_scalps WHERE closed_at IS NULL').fetchall()
        for r in open_rows:
            bid=quote(m,r['side'],'bid') if r['ticker']==ticker else None
            if bid is None and r['ticker']==ticker:
                continue  # never fabricate an exit price
            if r['ticker']!=ticker:
                # No executable exit quote; close as UNPRICED, excluded from gross performance.
                c.execute("UPDATE keke_paper_scalps SET closed_at=?,exit_reason='UNPRICED_CONTRACT_ENDED' WHERE ticker=? AND side=? AND closed_at IS NULL",(now,r['ticker'],r['side']))
                continue
            diff=bid-r['entry_ask']
            opposite=read.get('scalp') in ('UP','DOWN') and read['scalp']!=r['side']
            reason=('take_profit' if diff>=TAKE_PROFIT_CENTS else
                    'stop_watch' if diff<=-STOP_WARNING_CENTS else
                    'opposite_momentum' if opposite else
                    'time_exit' if a['seconds']<=90 else None)
            if reason:
                c.execute('UPDATE keke_paper_scalps SET closed_at=?,exit_bid=?,gross_per_contract=?,exit_reason=? WHERE ticker=? AND side=? AND closed_at IS NULL',
                          (now,bid,diff,reason,r['ticker'],r['side']))


def record_paper_entry(a, side, ask, bid):
    with connect() as c:
        c.execute('INSERT OR IGNORE INTO keke_paper_scalps(ticker,side,opened_at,entry_ask,entry_bid) VALUES(?,?,?,?,?)',
                  (a['ticker'],side,int(time.time()),ask,bid))


def health_status():
    """Operational status only. /market owns the analysis."""
    lines=['👀 KEKE STATUS',f'🔔 Alerts: {"ON" if state["enabled"] else "PAUSED"}',
           '🤖 Trading: manual only']
    try:
        m=get_market()
        lines.append('📈 Kalshi market: '+('CONNECTED' if m else 'NO ACTIVE CONTRACT'))
        if m:
            lines.append('🎯 '+str(m['ticker']))
            price,stamp=save_brti_ticks()
            age=max(0,time.time()-stamp)
            lines.append(f'📡 BRTI: {"FRESH" if age<=BRTI_MAX_AGE else "STALE"} ({age:.1f}s)')
            lines.append(f'📚 BRTI candles: {len(brti_candles())} completed 1m')
    except Exception as exc:
        lines.append(f'⚠️ Market/BRTI check: {type(exc).__name__}')
    try:
        with connect() as c:c.execute('SELECT 1').fetchone()
        lines.append('💾 Stats database: CONNECTED')
    except Exception:lines.append('⚠️ Stats database: UNAVAILABLE')
    lines.append('🧠 Trading analysis: /market')
    return '\n'.join(lines)


def send_stats():
    """Compact Telegram stats; retain detailed research in existing commands."""
    try:
        cutoff=stats_epoch()
        with connect() as c:
            rows=c.execute("SELECT ticker,event,grade FROM keke_v56_forecasts WHERE captured_at>=? ORDER BY ticker,id",(cutoff,)).fetchall()
            contracts=c.execute("SELECT COUNT(DISTINCT ticker) n FROM settlement_observations WHERE model_version=? AND captured_at>=?",('5.9-BRTI',cutoff)).fetchone()['n']
        def result(event):
            group=[r for r in rows if r['event']==event]
            w=sum(r['grade']=='WIN' for r in group)
            l=sum(r['grade']=='LOSS' for r in group)
            return f'{w}W/{l}L ({100*w/(w+l):.1f}%)' if w+l else 'Not graded yet'
        early=prospective_early_summary()
        import re
        m=re.search(r'Coverage: ([\d.]+%)',early)
        coverage=m.group(1) if m else 'n/a'
        send('📊 KEKE | PERFORMANCE\n'
             f'🎯 Early: {result("INITIAL")}\n'
             f'📡 Late: {result("LATE")}\n'
             f'🔄 Reversals: {result("REVERSAL")}\n'
             f'📈 Early coverage: {coverage}\n'
             f'📋 Contracts tracked: {contracts}\n'
             '⚠️ Accuracy ≠ trading profit. /legacy for history.')
    except Exception:
        LOG.exception('Compact stats failed')
        send('⚠️ Stats unavailable right now; try again shortly.')

def fresh_status():
    """Fetch a new market and BRTI tick for STATUS; do not change prediction state."""
    try:
        market=get_market()
        if not market:
            return '👀 No currently open BTC 15-minute contract.'
        brti,stamp=save_brti_ticks()
        state['current_brti_stamp']=stamp
        candles=brti_candles()
        levels=brti_opening_levels(candles,brti)
        if len(candles)<15:
            a=fast_opening_assessment(market,brti)
            if a is not None:
                a['brti_levels']=levels
                a['v56_side']=load_v56(a['ticker'])['side']
                return format_read(a)+'\n\n📊 Opening BRTI read; longer candles collecting'
            return (f'📡 BRTI ${brti:,.2f} • Target {money_text(target_of(market))}\n'
                    +levels+'\n⚪ Collecting verified BRTI candles; no trade signal')
        a=assessment(market,brti,candles,brti)
        a['brti_levels']=levels
        # Keep the current stored thesis, without triggering any prediction event.
        if state.get('v56_ticker')==a['ticker']:
            side=state.get('v56',{}).get('side')
        else:
            side=load_v56(a['ticker'])['side']
        a['v56_side']=side
        sig=settlement_signal(a)
        a['v56_signal']=sig
        a['v56_candidate']='UP' if sig is not None and sig>0 else ('DOWN' if sig is not None and sig<0 else None)
        state['last_read']=a
        return format_read(a)+'\n\n🎯 Settlement-only monitoring • scalp signals paused'
    except Exception as exc:
        LOG.warning('Fresh STATUS failed: %s',type(exc).__name__)
        return '⚠️ Could not retrieve a fresh market reading. Please retry; no stale price is being presented as live.'

def telegram_commands():
    if not TOKEN or not CHAT:return
    try:
        params={'timeout':0,'limit':30}
        if state['offset'] is not None:params['offset']=state['offset']
        obj=http_json(f'https://api.telegram.org/bot{TOKEN}/getUpdates',params)
        for upd in obj.get('result',[]):
            state['offset']=upd['update_id']+1
            runtime_set('telegram_offset',state['offset'])
            msg=upd.get('message') or {}
            if str((msg.get('chat') or {}).get('id'))!=str(CHAT):continue
            cmd=str(msg.get('text') or '').strip().lower()
            cmd={'👀 status':'/status','🧠 market read':'/market','📊 stats':'/stats',
                 '💰 position':'/position','🕒 last':'/last','🟢 start':'/start','🔴 stop':'/stop',
                 '📡 live':'/market','📍 position':'/position','📜 history':'/last',
                 '😴 sleep':'/stop','☀️ wake':'/start','⚙️ settings':'/settings',
                 '❓ help':'/help'}.get(cmd,cmd)
            if cmd.startswith('/stop'):
                state['enabled']=False;runtime_set('alerts_enabled',False);send('🔴 Alerts paused. Data collection continues. /start resumes alerts.')
            elif cmd.startswith('/start'):
                state['enabled']=True;runtime_set('alerts_enabled',True);send('🟢 KeKe alerts ON.')
            elif cmd.startswith('/status'):
                send(health_status())
            elif cmd=='/confidence':send(confidence_report())
            elif cmd=='/settings':send(menu_settings())
            elif cmd=='/help':send(menu_help())
            elif cmd.startswith('/market'):
                send(fresh_status())
            elif cmd.startswith('/stats'):send_stats()
            elif cmd.startswith('/exchange'):send(exchange_report())
            elif cmd.startswith('/lead'):send(exchange_research_report())
            elif cmd in ('/aion','/aioff','/ai','/researchon','/researchoff','/auditai','/improvements','/compare'):
                send(ai_lab.command(cmd))
            elif cmd.startswith('/audit'):send(audit_report())
            elif cmd.startswith('/research'):send(research_stats_report())
            elif cmd.startswith('/scorecard'):send(v527_scorecard())
            elif cmd=='/shadowon':
                shadow_view['enabled']=True
                send('🧪 V5.28 shadow updates ON • once per minute. Official calls unchanged. /shadowoff stops updates.')
                send(v528_shadow_message(shadow_view['last'],(state.get('last_read') or {}).get('v56_side')))
            elif cmd=='/shadowoff':
                shadow_view['enabled']=False
                send('🧪 Shadow updates OFF. /shadow still gives the latest reading.')
            elif cmd=='/shadow':send(v528_shadow_message(shadow_view['last'],(state.get('last_read') or {}).get('v56_side')))
            elif cmd.startswith('/preview'):send(preview_report())
            elif cmd.startswith('/legacy'):send('📚 HISTORICAL + LIFETIME REPORT\n'+grade_summary())
            elif cmd.startswith('/position'):send(portfolio_status())
            elif cmd.startswith('/last'):send_last()
            elif cmd.startswith('/brti'):run_brti_test()
            elif cmd == '/export':export_research()
            elif cmd == '/kalshiexport':export_kalshi_research()
    except Exception:LOG.exception('Telegram command polling error')

def initialize_telegram():
    if not TOKEN:return
    try:
        obj=http_json(f'https://api.telegram.org/bot{TOKEN}/getUpdates',{'timeout':0,'limit':100})
        updates=obj.get('result',[])
        if updates and state['offset'] is None:
            state['offset']=updates[-1]['update_id']+1
            runtime_set('telegram_offset',state['offset'])
        tg('setMyCommands',commands=json.dumps([
            {'command':'market','description':'Live BTC settlement read'},
            {'command':'stats','description':'Verified prediction statistics'},
            {'command':'position','description':'Current Kalshi position'},
            {'command':'last','description':'Latest settlement pick'},
            {'command':'stop','description':'Sleep: pause alerts'},
            {'command':'start','description':'Wake: resume alerts'},
            {'command':'settings','description':'Advanced research controls'},
            {'command':'help','description':'KeKe menu help'},
            {'command':'confidence','description':'Experimental confidence explanation'}]))
    except Exception:LOG.exception('Telegram setup error')

# V5.6 uses BRTI distance relative to volatility and time to settlement.
# This is a directional research signal, NOT a calibrated win probability.
V56_FIRST_MIN_SECONDS=600
V56_INITIAL_SIGNAL=0.45
INITIAL_PICK_MIN_SECONDS=780  # opening two minutes of each 900-second contract
V527_EARLY_COVERAGE_GOAL=.60
V527_EARLY_ACCURACY_GOAL=.75
V527_REPORT_WINDOW=86400
V56_REVERSAL_SIGNAL=0.95
V56_FAST_REVERSAL_SIGNAL=1.8
V56_REVERSAL_CONFIRM=2

def fast_opening_assessment(m, brti):
    """Provisional opening signal from actual BRTI ticks, before 1m candles exist.
    Uses target distance; volatility estimated only from observed prices.
    """
    target=target_of(m)
    if target is None:return None
    now=int(time.time()); seconds=seconds_remaining(m)
    if not 0<seconds<=900:return None
    with connect() as c:
        rows=c.execute('SELECT price FROM keke_brti_ticks WHERE timestamp>=? ORDER BY timestamp',
                       (now-90,)).fetchall()
    if len(rows)<3:return None
    prices=[float(r['price']) for r in rows]
    volatility=max(12.0,max(prices)-min(prices))
    distance=brti-target
    return {'ticker':m['ticker'],'at':now,'seconds':seconds,'btc':brti,
            'source':'BRTI','target':target,'distance':distance,
            'volatility':volatility,'delta3':prices[-1]-prices[0],
            'asks':{side:quote(m,side,'ask') for side in ('UP','DOWN')},
            'brti_levels':'📊 Completed candles collecting',
            'favored':'UP' if distance>0 else 'DOWN' if distance<0 else None}


def settlement_signal(a):
    if a['source']!='BRTI' or a['distance'] is None:
        return None
    seconds=max(30,a['seconds'])
    # Completed BRTI candle high-low range approximates 1m variability.
    # The floor prevents tiny measured ranges from creating false certainty.
    range1=max(12.0,float(a['volatility'] or 0))
    scale=max(18.0,range1*math.sqrt(seconds/60))
    # A small, capped directional drift component; target distance dominates.
    momentum=a.get('delta3') or 0
    drift=max(-0.35,min(0.35,0.18*momentum/scale))
    return a['distance']/scale + drift

# Silent experimental model: NEVER changes the live V5.6 predictions or alerts.
SHADOW_CHECKPOINTS=(720,540,360)  # 12, 9, and 6 minutes left
SHADOW_MIN_SCORE=1.25

def shadow_score(a):
    if a['source']!='BRTI' or a['distance'] is None or a['target'] is None:
        return None,None,'BRTI unavailable'
    i=a['indicators']; price=a['kraken']; score=0.0; reasons=[]
    if price>i['ema5']>i['ema13']:
        score+=1.0; reasons.append('rising short-term trend')
    elif price<i['ema5']<i['ema13']:
        score-=1.0; reasons.append('falling short-term trend')
    if i['momentum3']>0 and i['momentum5']>0:
        score+=0.8; reasons.append('upward momentum')
    elif i['momentum3']<0 and i['momentum5']<0:
        score-=0.8; reasons.append('downward momentum')
    if i['rsi14']>=60: score+=0.45
    elif i['rsi14']<=40: score-=0.45
    if price>i['resistance15']:
        score+=0.5; reasons.append('BRTI above recent resistance')
    elif price<i['support15']:
        score-=0.5; reasons.append('BRTI below recent support')
    scale=max(25.0,float(a['volatility'] or 0)*math.sqrt(max(a['seconds'],60)/60))
    score+=max(-1.0,min(1.0,a['distance']/scale))
    return score,('UP' if score>0 else 'DOWN'),'; '.join(reasons) or 'mixed indicators'

def record_shadow(a):
    if not 0<a['seconds']<=900: return
    score,side,reason=shadow_score(a)
    if score is None or abs(score)<SHADOW_MIN_SCORE: return
    for checkpoint in SHADOW_CHECKPOINTS:
        if not checkpoint-45<=a['seconds']<=checkpoint: continue
        with connect() as c:
            c.execute("""INSERT OR IGNORE INTO keke_shadow_predictions
                (ticker,checkpoint,captured_at,seconds_left,side,score,reason,ask,brti,target)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (a['ticker'],checkpoint,a['at'],a['seconds'],side,score,reason,
                 a['asks'].get(side),a['btc'],a['target']))


# V5.28 visible shadow: independent experimental settlement research, never trades or changes V5.27 calls.
# These are heuristic scores, NOT calibrated settlement probabilities.
V528_SHADOW_INTERVAL=60
shadow_view={'enabled':False,'ticker':None,'sent_at':0,'last':None}

def v528_shadow_read(a,candles):
    if not a or a.get('source')!='BRTI' or a.get('target') is None or len(candles)<16:
        return None
    rows=candles[-16:]
    if any(int(rows[j+1]['time'])-int(rows[j]['time'])!=60 for j in range(len(rows)-1)):
        return None
    base=rows[:-2]  # freeze reference range before the two breakout-confirmation bars
    support=min(x['low'] for x in base)
    resistance=max(x['high'] for x in base)
    width=resistance-support
    if width<=0:return None
    target=a['target']; price=a['btc']; seconds=max(1,a['seconds'])
    position=(target-support)/width
    # Independent body/wick measurements; avoid mistaking a long wick for a confirmed reversal.
    last=rows[-1]
    upper=max(0,last['high']-max(last['open'],last['close']))
    lower=max(0,min(last['open'],last['close'])-last['low'])
    span=max(.01,last['high']-last['low'])
    upper_frac=upper/span;lower_frac=lower/span
    upper_rejections=sum(1 for x in rows[-6:] if (x['high']-max(x['open'],x['close']))/max(.01,x['high']-x['low'])>=.45 and x['close']<x['high'])
    lower_rejections=sum(1 for x in rows[-6:] if (min(x['open'],x['close'])-x['low'])/max(.01,x['high']-x['low'])>=.45 and x['close']>x['low'])
    crossings=sum(1 for x in rows[-10:] if x['low']<=target<=x['high'])
    closes_above=sum(1 for x in rows[-10:] if x['close']>target)
    # Regime: range overlap and target-crossings; confirmed breaks require consecutive closes.
    band=max(5.0,width*.05)
    breakout_up=rows[-1]['close']>resistance+band and rows[-2]['close']>resistance+band
    breakout_down=rows[-1]['close']<support-band and rows[-2]['close']<support-band
    regime='BREAKOUT UP' if breakout_up else ('BREAKDOWN' if breakout_down else ('RANGING' if crossings>=3 else 'MIXED / TRENDING'))
    # Target distance and time/volatility dominate; structure is a modest, capped modifier.
    scale=max(18.0,max(12.0,float(a.get('volatility') or 0))*math.sqrt(max(30,seconds)/60))
    base_score=(price-target)/scale
    structure=0.0
    if regime=='RANGING':
        if position>=.70:structure-=.25
        elif position<=.30:structure+=.25
    if upper_rejections>=2:structure-=.15
    if lower_rejections>=2:structure+=.15
    if breakout_up:structure+=.40
    if breakout_down:structure-=.40
    structure=max(-.5,min(.5,structure))
    score=base_score+structure
    side='WAIT' if abs(score)<.45 else ('UP' if score>0 else 'DOWN')
    return {'side':side,'score':score,'base':base_score,'structure':structure,'regime':regime,
            'support':support,'resistance':resistance,'position':position,'crossings':crossings,
            'above':closes_above,'upper':upper_frac,'lower':lower_frac,
            'upper_count':upper_rejections,'lower_count':lower_rejections,'seconds':seconds,
            'price':price,'target':target,'ticker':a['ticker']}

def v528_shadow_message(read,official=None):
    if not read:return '🧪 V5.28 SHADOW\nWaiting for 16 contiguous completed BRTI candles and a valid target.'
    r=read
    pos=f"{r['position']*100:.0f}%" if 0<=r['position']<=1 else 'OUTSIDE RANGE'
    official=official or 'NO OFFICIAL PICK'
    return (f"🧪 V5.28 LIVE SHADOW • EXPERIMENTAL\n"
            f"Official KeKe: {official} | Shadow: {r['side']}\n"
            f"BTC ${r['price']:,.2f} | Target ${r['target']:,.2f}\n"
            f"Time left: {r['seconds']//60}:{r['seconds']%60:02d} | {r['regime']}\n"
            f"Range ${r['support']:,.2f}–${r['resistance']:,.2f} | Target at {pos}\n"
            f"Target touched in {r['crossings']}/10 candles | Closes above {r['above']}/10\n"
            f"Last wick: upper {r['upper']:.0%}, lower {r['lower']:.0%} of candle\n"
            f"6-bar rejection counts: upper {r['upper_count']}, lower {r['lower_count']}\n"
            f"Research score {r['score']:+.2f} (distance/time {r['base']:+.2f}, structure {r['structure']:+.2f})\n"
            f"⚠️ Uncalibrated research only. No live call, trade, or exit change.")

def v528_shadow_tick(a,candles):
    r=v528_shadow_read(a,candles)
    shadow_view['last']=r
    if not shadow_view['enabled'] or r is None:return
    now=int(time.time())
    if shadow_view['ticker']!=a['ticker']:
        shadow_view['ticker']=a['ticker'];shadow_view['sent_at']=0
    if now-shadow_view['sent_at']>=V528_SHADOW_INTERVAL:
        shadow_view['sent_at']=now
        send(v528_shadow_message(r,a.get('v56_side')))

# V5.7 observational multi-timeframe and portfolio extension. GET requests only.
PORTFOLIO_POLL_SECONDS=20
POSITION_ALERT_COOLDOWN=180
portfolio_state={'last_poll':0,'positions':{},'alert_at':{},'error_at':0}

def authenticated_get(path,params=None):
    key_id=os.getenv('KALSHI_API_KEY_ID','').strip()
    pem=os.getenv('KALSHI_PRIVATE_KEY','').replace('\\n','\n').strip()
    pem_path=os.getenv('KALSHI_PRIVATE_KEY_PATH','').strip()
    if not key_id or not (pem or pem_path):
        raise ValueError('Kalshi API credentials missing')
    key_bytes=open(pem_path,'rb').read() if pem_path else pem.encode()
    key=serialization.load_pem_private_key(key_bytes,password=None)
    timestamp=str(int(time.time()*1000))
    signed=(timestamp+'GET'+'/trade-api/v2'+path).encode()
    if isinstance(key,ed25519.Ed25519PrivateKey):
        sig=key.sign(signed)
    else:
        sig=key.sign(signed,padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=padding.PSS.DIGEST_LENGTH),hashes.SHA256())
    headers={'KALSHI-ACCESS-KEY':key_id,'KALSHI-ACCESS-TIMESTAMP':timestamp,
             'KALSHI-ACCESS-SIGNATURE':base64.b64encode(sig).decode()}
    r=session.get(BASE+path,params=params,headers=headers,timeout=10)
    # Diagnostic includes only HTTP status and safe response structure, never secrets.
    if r.status_code >= 400:
        raise RuntimeError(f'Kalshi portfolio HTTP {r.status_code} on {path}')
    try:
        obj=r.json()
    except ValueError:
        raise RuntimeError('Kalshi portfolio returned non-JSON response')
    if not isinstance(obj,dict):
        raise RuntimeError('Kalshi portfolio response was not an object')
    if obj.get('error'):
        raise RuntimeError('Kalshi portfolio API returned an error object')
    return obj

def completed_timeframes(candles,minutes):
    """UTC-aligned complete OHLCV bars only; incomplete buckets are excluded."""
    if minutes==1:return list(candles)
    width=minutes*60; buckets={}
    for c in candles:
        bucket=(int(c['time'])//width)*width
        buckets.setdefault(bucket,[]).append(c)
    out=[]
    for t,rows in sorted(buckets.items()):
        rows=sorted(rows,key=lambda x:x['time'])
        if len(rows)!=minutes or any(int(row['time'])!=t+i*60 for i,row in enumerate(rows)):
            continue
        out.append({'time':t,'open':rows[0]['open'],'high':max(x['high'] for x in rows),
                    'low':min(x['low'] for x in rows),'close':rows[-1]['close'],
                    'volume':sum(x['volume'] for x in rows)})
    return out

def timeframe_signal(rows,live):
    if len(rows)<4:return 'COLLECTING',0
    last=rows[-1];prev=rows[-2]
    slope=last['close']-rows[-4]['close']
    direction='UP' if slope>0 else ('DOWN' if slope<0 else 'FLAT')
    # Simple structure confirmation: candle direction and closing trend must agree.
    if direction=='UP' and last['close']<last['open'] and last['close']<prev['close']:
        direction='MIXED'
    elif direction=='DOWN' and last['close']>last['open'] and last['close']>prev['close']:
        direction='MIXED'
    return direction,slope

def dual_read(a,candles):
    frames={}
    for minutes in (1,3,15):
        rows=completed_timeframes(candles,minutes)
        direction,slope=timeframe_signal(rows,a['kraken'])
        frames[minutes]=direction
    short,mid,long=frames[1],frames[3],frames[15]
    if short in ('UP','DOWN') and short==mid:
        scalp=short
    else:
        scalp='WAIT'
    if long in ('UP','DOWN') and scalp not in ('WAIT',long):
        label='COUNTERTREND / HIGH RISK'
    elif scalp=='WAIT':
        label='NO ENTRY: mixed or insufficient candles'
    else:
        label='WATCH ONLY: confirmation needed'
    # Momentum is descriptive only and does not enter settlement_signal().
    return {'scalp':scalp,'frames':frames,'label':label,'range':range_pattern(candles)}

def dual_text(read):
    f=read['frames']
    symbols={'UP':'↑','DOWN':'↓','MIXED':'↔','FLAT':'→','COLLECTING':'⏳','N/A':'⏳'}
    frames='  •  '.join(f'{m}m {symbols.get(f.get(m),f.get(m,"?"))}' for m in (1,3,15))
    r=read.get('range')
    extra=(f"\n↔ RANGE: {r['pattern']} • S ${r['support']:,.0f} / R ${r['resistance']:,.0f}" if r else "")
    return f"\n\n📊 BRTI momentum: {frames}"

# V5.14: BRTI-only range and breakout research, isolated from settlement picks.
RANGE_LOOKBACK=12
RANGE_MIN_WIDTH=35.0
RANGE_MAX_WIDTH=260.0
RANGE_TOUCH_FRACTION=.18
RANGE_MIN_ENTRY_SECONDS=150
RANGE_MAX_SPREAD=.06
RANGE_MAX_ASK=.80
range_alert_state={'seen':{}}

def range_pattern(candles):
    """Only completed, contiguous 1m BRTI bars. No volume or intrabar guesses."""
    if len(candles)<RANGE_LOOKBACK+2:return None
    rows=candles[-(RANGE_LOOKBACK+2):]
    if any(int(rows[i+1]['time'])-int(rows[i]['time'])!=60 for i in range(len(rows)-1)):
        return None
    base=rows[:RANGE_LOOKBACK]
    last,prev=rows[-1],rows[-2]
    support=min(c['low'] for c in base)
    resistance=max(c['high'] for c in base)
    width=resistance-support
    if not RANGE_MIN_WIDTH<=width<=RANGE_MAX_WIDTH:return None
    band=max(8.0,width*RANGE_TOUCH_FRACTION)
    # Distinct tests of each boundary, separated by at least one minute.
    def distinct_touches(boundary,kind):
        indices=[i for i,c in enumerate(base) if (c['low']<=boundary+band if kind=='low' else c['high']>=boundary-band)]
        count=0;last_i=-3
        for i in indices:
            if i-last_i>=2:count+=1;last_i=i
        return count
    if distinct_touches(support,'low')<2 or distinct_touches(resistance,'high')<2:
        return None
    mid=(support+resistance)/2
    # Two completed closes beyond the *prior* range confirm a break.
    if prev['close']>resistance+band*.25 and last['close']>resistance+band*.25:
        return {'pattern':'BREAKOUT','side':'UP','support':support,'resistance':resistance}
    if prev['close']<support-band*.25 and last['close']<support-band*.25:
        return {'pattern':'BREAKDOWN','side':'DOWN','support':support,'resistance':resistance}
    # Bounce/rejection: prior bar tests the boundary, latest bar confirms movement away.
    if (prev['low']<=support+band and last['close']>prev['close']
            and last['close']>last['open'] and support<last['close']<mid):
        return {'pattern':'SUPPORT BOUNCE','side':'UP','support':support,'resistance':resistance}
    if (prev['high']>=resistance-band and last['close']<prev['close']
            and last['close']<last['open'] and mid<last['close']<resistance):
        return {'pattern':'RESISTANCE REJECTION','side':'DOWN','support':support,'resistance':resistance}
    return {'pattern':'CONSOLIDATING','side':'WAIT','support':support,'resistance':resistance}


# Market-structure observations only. This never opens simulated or real trades.
def detect_retest(candles, tolerance_fraction=0.12):
    """Confirm a prior range break and its subsequent retest using closed 1m bars.

    The baseline ends BEFORE the breakout; levels are frozen for the event.
    A retest is not the same thing as merely being beyond the old level.
    """
    if len(candles)<9:return None
    rows=candles[-40:]
    # Reject gaps: an unobserved minute could invalidate the sequence.
    if any(int(rows[i+1]['time'])-int(rows[i]['time'])!=60 for i in range(len(rows)-1)):
        return None
    last=rows[-1]
    # Break happened 1-5 bars ago, and the final bar is the confirmation.
    for b in range(len(rows)-2,max(5,len(rows)-7),-1):
        base=rows[b-6:b]
        if len(base)!=6:continue
        floor=min(x['low'] for x in base)
        ceiling=max(x['high'] for x in base)
        width=ceiling-floor
        if width<8 or width>500:continue
        tol=max(5.0,min(35.0,width*tolerance_fraction))
        breakout=rows[b]
        after=rows[b+1:]
        if len(after)<1:continue
        # Closing beyond the previous six-bar range establishes a candidate break.
        if breakout['close']>ceiling+tol:
            level=ceiling
            # Subsequent candle must test the frozen level and finish above it.
            tests=[x for x in after if x['low']<=level+tol and x['high']>=level-tol]
            if tests and last['close']>level+tol and last['close']>last['open']:
                return {'event':'RESISTANCE RETEST HELD','side':'UP','level':level,
                        'break_time':breakout['time'],'confirmed_at':last['time']}
            if last['close']<level-tol:
                return {'event':'FAILED UPSIDE BREAKOUT','side':'DOWN','level':level,
                        'break_time':breakout['time'],'confirmed_at':last['time']}
        if breakout['close']<floor-tol:
            level=floor
            tests=[x for x in after if x['high']>=level-tol and x['low']<=level+tol]
            if tests and last['close']<level-tol and last['close']<last['open']:
                return {'event':'SUPPORT RETEST REJECTED','side':'DOWN','level':level,
                        'break_time':breakout['time'],'confirmed_at':last['time']}
            if last['close']>level+tol:
                return {'event':'FAILED DOWNSIDE BREAKDOWN','side':'UP','level':level,
                        'break_time':breakout['time'],'confirmed_at':last['time']}
    return None


def market_structure_alert(m,a,candles):
    """Persistent deduplication; no scalp quote, entry, or paper trade."""
    pattern=detect_retest(candles)
    if not pattern:return None
    # Only report confirmations occurring in the current contract.
    seconds=a.get('seconds')
    if seconds is None:return None
    contract_start=int(time.time())-(900-int(seconds))
    if pattern['confirmed_at']+60<contract_start:return None
    with connect() as c:
        cur=c.execute("""INSERT OR IGNORE INTO keke_retest_alerts
            (ticker,break_time,event,level,confirmed_at) VALUES(?,?,?,?,?)""",
            (m['ticker'],pattern['break_time'],pattern['event'],pattern['level'],pattern['confirmed_at']))
        fresh=cur.rowcount==1
    if fresh and state['enabled']:
        send(f"📍 KEKE • {pattern['event']}\n"
             f"Frozen level ${pattern['level']:,.2f} • BRTI ${a['btc']:,.2f}\n"
             f"Confirmed by completed 1m candle • {seconds//60}:{seconds%60:02d} left\n"
             'Market structure only • not a scalp entry or guaranteed settlement pick')
    return pattern


def range_update_exits(m,a,read):
    """Close simulated positions using an observed sell bid; no settlement assumptions."""
    with connect() as c:
        c.execute('''UPDATE keke_range_signals SET exit_at=?,exit_reason='UNPRICED_CONTRACT_ENDED'
                     WHERE ticker<>? AND entry_ask IS NOT NULL AND exit_at IS NULL''',
                  (int(time.time()),m['ticker']))
        rows=c.execute('SELECT * FROM keke_range_signals WHERE ticker=? AND entry_ask IS NOT NULL AND exit_at IS NULL',(m['ticker'],)).fetchall()
        for r in rows:
            bid=quote(m,r['side'],'bid')
            if bid is None:continue
            diff=bid-r['entry_ask']
            reason=('take_profit' if diff>=TAKE_PROFIT_CENTS else
                    'stop_watch' if diff<=-STOP_WARNING_CENTS else
                    'opposite_momentum' if read.get('scalp') in ('UP','DOWN') and read['scalp']!=r['side'] else
                    'time_exit' if a['seconds']<=90 else None)
            if reason:
                c.execute('UPDATE keke_range_signals SET exit_at=?,exit_bid=?,gross_per_contract=?,exit_reason=? WHERE id=?',
                          (int(time.time()),bid,diff,reason,r['id']))


def range_strategy(m,a,candles,read):
    """Observe patterns; notify on qualified setups only; never place an order."""
    pattern=range_pattern(candles)
    if pattern is None:return
    side=pattern['side'];kind=pattern['pattern'];ticker=a['ticker']
    support=pattern['support'];resistance=pattern['resistance']
    key=(ticker,kind,side)
    # Only one notification of each distinct pattern per contract.
    if key in range_alert_state['seen']:return
    with connect() as c:
        if c.execute('SELECT 1 FROM keke_range_signals WHERE ticker=? AND pattern=? AND side=?',(ticker,kind,side)).fetchone():
            range_alert_state['seen'][key]=int(time.time())
            return
    # Consolidation is descriptive, not an entry; keep it in MARKET READ.
    if side=='WAIT':
        a['range_read']=pattern
        return
    a['range_read']=pattern
    ask=a['asks'].get(side);bid=a['bids'].get(side)
    spread=ask-bid if ask is not None and bid is not None else None
    if not (RANGE_MIN_ENTRY_SECONDS<=a['seconds']<=810 and
            ask is not None and .03<=ask<=RANGE_MAX_ASK and
            spread is not None and 0<=spread<=RANGE_MAX_SPREAD):return
    # Require enough remaining payout to exceed the observed spread and a fee buffer.
    if 1-ask<=spread+.05:return
    range_alert_state['seen'][key]=int(time.time())
    with connect() as c:
        c.execute("""INSERT OR IGNORE INTO keke_range_signals
           (ticker,created_at,pattern,side,support,resistance,brti,entry_ask,entry_bid)
           VALUES (?,?,?,?,?,?,?,?,?)""",
           (ticker,int(time.time()),kind,side,support,resistance,a['btc'],ask,bid))
    if state['enabled']:
        send(f'⚡ {side} SCALP • {kind}\n'
             f'BUY WATCH {ask_text(ask)} • {a["seconds"]//60}:{a["seconds"]%60:02d} left\n\n'
             f'🟢 S ${support:,.0f} | 🔴 R ${resistance:,.0f}\n'
             f'📍 BRTI ${a["btc"]:,.2f} • Spread {spread*100:.1f}¢\n'
             'Confirm live ask, fees and liquidity. Manual entry only.')


# V5.10: read-only trade-opportunity alerts; no order execution.
ENTRY_MIN_SECONDS=150
ENTRY_MAX_SECONDS=810
ENTRY_MAX_ASK=.80
ENTRY_MAX_SPREAD=.06
ENTRY_MIN_CUSHION=35.0
ENTRY_CONFIRM_POLLS=2  # distinct completed 1m candles, not polling cycles
ENTRY_COOLDOWN_SECONDS=180
TAKE_PROFIT_CENTS=.10
STOP_WARNING_CENTS=.08
trade_alert_state={'candidate':None,'confirm':0,'last_entry':{},'last_exit':{}}

def entry_opportunity(a,read):
    """Conservative, executable-quote-filtered watch alert; not a fill guarantee."""
    ticker=a['ticker']; side=read.get('scalp')
    if side not in ('UP','DOWN') or a['source']!='BRTI':
        trade_alert_state['candidate']=None
        trade_alert_state['confirm']=0
        return
    # Only two short timeframes must agree. The 15m trend is context, not a gate.
    seconds=a['seconds']; dist=a.get('distance')
    ask=a['asks'].get(side); bid=a['bids'].get(side)
    spread=ask-bid if ask is not None and bid is not None else None
    cushion=(dist if side=='UP' else -dist) if dist is not None else None
    eligible=(ENTRY_MIN_SECONDS<=seconds<=ENTRY_MAX_SECONDS
              and cushion is not None and cushion>=ENTRY_MIN_CUSHION
              and ask is not None and .03<=ask<=ENTRY_MAX_ASK
              and spread is not None and 0<=spread<=ENTRY_MAX_SPREAD)
    if not eligible:
        trade_alert_state['candidate']=None
        trade_alert_state['confirm']=0
        return
    if not state['enabled']:
        return
    key=(ticker,side)
    candle_time=int(a['at'])//60*60-60
    if trade_alert_state['candidate']!=key:
        trade_alert_state['candidate']=key
        trade_alert_state['confirm']=0
    with connect() as c:
        c.execute('''INSERT OR IGNORE INTO keke_signal_state
                     (ticker,strategy,side,candle_time,captured_at) VALUES (?,?,?,?,?)''',
                  (ticker,'SCALP_CONFIRM',side,candle_time,int(a['at'])))
        recent=c.execute('''SELECT DISTINCT candle_time FROM keke_signal_state
                            WHERE ticker=? AND strategy=? AND side=?
                            ORDER BY candle_time DESC LIMIT ?''',
                         (ticker,'SCALP_CONFIRM',side,ENTRY_CONFIRM_POLLS)).fetchall()
        trade_alert_state['confirm']=len(recent) if (len(recent)==ENTRY_CONFIRM_POLLS
            and all(recent[i]['candle_time']-recent[i+1]['candle_time']==60
                    for i in range(len(recent)-1))) else 1
    now=time.time()
    if trade_alert_state['confirm']<ENTRY_CONFIRM_POLLS or not state['enabled']:
        return
    if now-trade_alert_state['last_entry'].get(key,0)<ENTRY_COOLDOWN_SECONDS:
        return
    # One entry notification per side/contract; no repeated calls on the same move.
    if key in trade_alert_state['last_entry']:
        return
    with connect() as c:
        if c.execute('SELECT 1 FROM keke_paper_scalps WHERE ticker=? AND side=?',key).fetchone():
            return
    trade_alert_state['last_entry'][key]=now
    record_paper_entry(a,side,ask,bid)
    mm,ss=divmod(seconds,60)
    f=read['frames']
    trend={'UP':'↑','DOWN':'↓','MIXED':'↔','COLLECTING':'⏳','N/A':'⏳'}
    send(f'⚡ SCALP WATCH {side} • ASK {ask_text(ask)}\n'
         f'⏱ {mm}:{ss:02d} left • Ask {ask_text(ask)}\n'
         f'📊 1m {trend.get(f.get(1),"?")} • 3m {trend.get(f.get(3),"?")} • 15m {trend.get(f.get(15),"?")}\n\n'
         f'🎯 Target {money_text(a["target"])}\n'
         f'📍 BRTI {money_text(a["btc"])} • Cushion ${cushion:,.2f}\n'
         f'↔ Spread {spread*100:.1f}¢\n\n'
         'Check live Kalshi ask and liquidity before any manual entry.')

def position_exit_alert(position,a,read):
    """Alert on realizable bid estimates, not on settlement payout or paper gains."""
    if not state['enabled'] or position is None:return
    side=position['side']; ticker=position['ticker']
    qty=position['qty']; avg=position['avg']; bid=position['bid']
    if avg is None or bid is None or qty<=0:return
    move=bid-avg
    against=read.get('settlement_side') in ('UP','DOWN') and read['settlement_side']!=side
    if move>=TAKE_PROFIT_CENTS:
        category='TAKE PROFIT WATCH'
    elif move<=-STOP_WARNING_CENTS or against:
        category='EXIT WARNING'
    else:
        return
    key=(ticker,side,category)
    now=time.time()
    if now-trade_alert_state['last_exit'].get(key,0)<POSITION_ALERT_COOLDOWN:return
    trade_alert_state['last_exit'][key]=now
    gross=position['gross']
    detail=' • Settlement forecast opposes holding' if against else ''
    send(f'{"💰 SELL WATCH" if category=="TAKE PROFIT WATCH" else "🚨 EXIT WATCH"} {side} • {ask_text(bid)}\n'
         f'⏱ {a["seconds"]//60}:{a["seconds"]%60:02d} left\n'
         f'Entry {ask_text(avg)} → Sell bid {ask_text(bid)}\n'
         f'Gross P/L {tiny_money(gross)} • {qty:g} contracts{detail}\n\n'
         'Review sell liquidity and fees. No automatic orders.')

def fill_price(fill,side):
    # Kalshi fills use yes_price/no_price (cents) or *_price_dollars.
    prefix='yes' if side=='UP' else 'no'
    value=number(fill.get(prefix+'_price_dollars'))
    if value is None:
        cents=number(fill.get(prefix+'_price'))
        value=cents/100 if cents is not None else None
    return value if value is not None and 0<=value<=1 else None

def get_open_position(m):
    """Read net quantity and reported exposure; fill-based basis not yet verified."""
    ticker=m['ticker']
    data=authenticated_get('/portfolio/positions',{'ticker':ticker,'limit':100})
    positions=data.get('market_positions')
    if not isinstance(positions,list):raise ValueError('Portfolio response missing market_positions; available fields: '+','.join(k for k in data if k in ('market_positions','event_positions','cursor','error','code','message')))
    row=next((p for p in positions if p.get('ticker')==ticker),None)
    if row is None:return None
    # Kalshi may return fixed-point quantity as position_fp instead of legacy position.
    signed=number(row.get('position_fp'))
    if signed is None:
        signed=number(row.get('position'))
    if signed is None:
        safe_fields=','.join(sorted(str(k) for k in row if re.fullmatch(r'[A-Za-z0-9_]{1,60}',str(k))))
        raise ValueError('Position quantity missing; response fields: '+safe_fields[:350])
    if signed==0:return None
    side='UP' if signed>0 else 'DOWN'
    qty=abs(signed)
    # Portfolio market_exposure represents the remaining basis on Kalshi's side.
    basis=number(row.get('market_exposure_dollars'))
    if basis is None:
        cents=number(row.get('market_exposure'))
        basis=cents/100 if cents is not None else None
    if basis is not None and not (0<=basis<=qty):basis=None
    avg=basis/qty if basis is not None else None
    bid=quote(m,side,'bid')
    ask=quote(m,side,'ask')
    gross=(qty*bid-basis) if bid is not None and basis is not None else None
    return {'ticker':ticker,'side':side,'qty':qty,'basis':basis,'avg':avg,
            'bid':bid,'ask':ask,'gross':gross}

def portfolio_poll(m,a,read):
    """Monitor actual account holdings independently of disabled scalp entries.
    Persist observed position changes across restarts; never claim a sale was filled
    without confirmed execution history.
    """
    now=time.time()
    if now-portfolio_state['last_poll']<PORTFOLIO_POLL_SECONDS:return
    portfolio_state['last_poll']=now
    ticker=m['ticker']
    try:
        position=get_open_position(m)
    except Exception as exc:
        if now-portfolio_state['error_at']>600:
            portfolio_state['error_at']=now
            LOG.warning('Position read failed: %s',type(exc).__name__)
            if state['enabled']:send('⚠️ Kalshi position unavailable; exit monitoring temporarily unverified.')
        return
    with connect() as c:
        old=c.execute('SELECT * FROM keke_position_snapshots WHERE ticker=?',(ticker,)).fetchone()
        old=dict(old) if old else None
        if position:
            c.execute("""INSERT INTO keke_position_snapshots(ticker,side,qty,basis,bid,updated_at)
                VALUES(?,?,?,?,?,?) ON CONFLICT(ticker) DO UPDATE SET
                side=excluded.side,qty=excluded.qty,basis=excluded.basis,
                bid=excluded.bid,updated_at=excluded.updated_at""",
                (ticker,position['side'],position['qty'],position['basis'],position['bid'],int(now)))
        elif old:
            c.execute('DELETE FROM keke_position_snapshots WHERE ticker=?',(ticker,))
        event=None
        if old and not position:event='POSITION CLOSED / NO LONGER HELD'
        elif old and position and old['side']!=position['side']:event='POSITION SIDE CHANGED'
        elif old and position and position['qty']<old['qty']-1e-8:event='POSITION REDUCED'
        elif old and position and position['qty']>old['qty']+1e-8:event='POSITION INCREASED'
        elif position and not old:event='POSITION DETECTED'
        if event:
            c.execute('INSERT INTO keke_position_events(ticker,event,side,old_qty,new_qty,recorded_at) VALUES(?,?,?,?,?,?)',
                      (ticker,event,old['side'] if old else position['side'],old['qty'] if old else 0,
                       position['qty'] if position else 0,int(now)))
    if event and state['enabled']:
        if position is None:
            send(f'✅ {event} • {ticker}\nPreviously {old["qty"]:g} {old["side"]}; now 0.\nExit detected from holdings, NOT verified realized P/L.')
        else:
            send(f'💰 {event} • {ticker}\n{position["qty"]:g} {position["side"]} '
                 f'• bid {ask_text(position["bid"])}\nGross P/L {tiny_money(position["gross"])} '
                 '(estimated; fees/fills unverified).')
    if not position:
        portfolio_state['positions'].pop(ticker,None)
        return
    portfolio_state['positions'][ticker]=position
    # Compare the position to the SETTLEMENT prediction, not the disabled scalp model.
    prediction=state.get('v56') or {}
    predicted=prediction.get('side') if state.get('v56_ticker')==ticker else None
    position_exit_alert(position,a,{'settlement_side':predicted})


def portfolio_status():
    try:
        m=get_market()
        if not m:return '💰 No open BTC15m market to inspect.'
        p=get_open_position(m)
        if not p:return '💰 No active position in the current BTC15m contract.'
        pnl=tiny_money(p['gross'])
        return (f"💰 {p['qty']} {p['side']} • Cost basis/contract {ask_text(p['avg'])}"
                f"\nTotal cost {total_money(p['basis'])} • Sell bid {ask_text(p['bid'])} • Gross P/L {pnl}"
                "\nExcludes fees and slippage; current contract only.")
    except Exception as exc:
        LOG.warning('Position command failed: %s: %s',type(exc).__name__,str(exc)[:150])
        detail=str(exc)[:100] if isinstance(exc,(RuntimeError,ValueError)) else type(exc).__name__
        return '⚠️ Position lookup failed: '+detail+'\nNo orders placed. Check Railway logs.'

def load_v56(ticker):
    with connect() as c:
        rows=c.execute("SELECT * FROM keke_v56_forecasts WHERE ticker=? ORDER BY id",(ticker,)).fetchall()
    side=rows[-1]['side'] if rows else None
    return {'side':side,'opposite':runtime_get('v56_opposite_'+ticker,0)}

def record_v56(a,event,side,previous=None,reason=''):
    with connect() as c:
        c.execute('''INSERT INTO keke_v56_forecasts
            (ticker,captured_at,seconds_left,event,side,previous_side,signal,ask,target,brti,reason)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
            (a['ticker'],a['at'],a['seconds'],event,side,previous,
             a['v56_signal'],a['asks'].get(side),a['target'],a['btc'],reason))

def process_v56(a,new_contract):
    if new_contract or state.get('v56_ticker')!=a['ticker']:
        state['v56_ticker']=a['ticker']
        state['v56']=load_v56(a['ticker'])
    t=state['v56']
    signal=settlement_signal(a)
    a['v56_signal']=signal
    a['v56_candidate']=('UP' if signal>0 else 'DOWN') if signal is not None and signal!=0 else None
    a['v56_side']=t['side']
    if signal is None or a['seconds']<=0:return
    candidate=a['v56_candidate']
    if t['side'] is None:
        # Make a qualifying early call, but don't force a coin-flip on a flat signal.
        if abs(signal)>=V56_INITIAL_SIGNAL:
            event='INITIAL' if INITIAL_PICK_MIN_SECONDS<=a['seconds']<=900 else 'LATE'
            t['side']=candidate
            a['v56_side']=candidate
            record_v56(a,event,candidate,reason='First qualifying settlement signal')
            if state['enabled']:send(format_read(a,f'🎯 SETTLEMENT {candidate} • {event} PICK'))
        return
    if candidate==t['side'] or abs(signal)<V56_REVERSAL_SIGNAL:
        t['opposite']=0
        runtime_set('v56_opposite_'+a['ticker'],0)
        return
    t['opposite']+=1
    runtime_set('v56_opposite_'+a['ticker'],t['opposite'])
    if abs(signal)>=V56_FAST_REVERSAL_SIGNAL or t['opposite']>=V56_REVERSAL_CONFIRM:
        old=t['side'];t['side']=candidate;t['opposite']=0
        runtime_set('v56_opposite_'+a['ticker'],0)
        a['v56_side']=candidate
        record_v56(a,'REVERSAL',candidate,old,'Strong opposite settlement signal')
        if state['enabled']:send(format_read(a,f'🎯 SETTLEMENT {candidate} • REVERSED {old}'))

def compact_update(a):
    """First two lines fit Telegram notification previews; detail follows on opening."""
    seconds=max(0,int(a['seconds']))
    mm,ss=divmod(seconds,60)
    side=a.get('v56_side') or 'WAIT'
    distance=a.get('distance')
    delta=f'${distance:+,.2f}' if distance is not None else 'n/a'
    read=a.get('dual_read') or {}
    frames=read.get('frames') or {}
    arrows={'UP':'↑','DOWN':'↓','MIXED':'↔','FLAT':'→','COLLECTING':'⏳','N/A':'⏳'}
    trend=' '.join(f'{m}m{arrows.get(frames.get(m),"⏳")}' for m in (1,3))
    return (f'📡 {side} • {mm}:{ss:02d} LEFT\n'
            f'BRTI {delta} vs target | {trend}\n\n'
            f'🎯 Target {money_text(a.get("target"))}\n'
            f'📍 BRTI {money_text(a.get("btc"))}\n'
            f'{a.get("brti_levels", "S/R collecting")}')

def proactive_updates(a, new_contract):
    if not state['enabled'] or a['source']!='BRTI':return
    ticker=a['ticker'];now=time.time()
    if new_contract or state.get('heartbeat_ticker')!=ticker:
        state['heartbeat_ticker']=ticker
        state['heartbeat_at']=now
        state['heartbeat_status']=None
        state['threat_ticker']=ticker
        state['threat_side']=None
        state['threat_at']=0
        send(banner_levels(a)+'\n'+format_read(a,'🆕 NEW CONTRACT'))
        return
    side=a.get('v56_side');signal=a.get('v56_signal');candidate=a.get('v56_candidate')
    threatened=(side in ('UP','DOWN') and candidate in ('UP','DOWN')
                and candidate!=side and signal is not None and abs(signal)>=THREAT_SIGNAL)
    if not threatened:state['threat_side']=None
    elif state.get('threat_side')!=side and now-state.get('threat_at',0)>=THREAT_COOLDOWN:
        state['threat_side']=side;state['threat_at']=now;state['heartbeat_at']=now
        v527_warning(a,side,candidate,signal)
        send(f'⚠️ {side} PICK AT RISK\nOpposite signal strengthening\n\n'+compact_update(a))
        return
    # Only send a routine update if the displayed direction or scalp state changes.
    status=(a.get('v56_side') or 'WAIT',)
    if now-state.get('heartbeat_at',0)>=HEARTBEAT_SECONDS and status!=state.get('heartbeat_status'):
        state['heartbeat_at']=now
        state['heartbeat_status']=status
        send(compact_update(a))


def send_last():
    with connect() as c:
        row=c.execute("SELECT * FROM keke_v56_forecasts WHERE event IN ('INITIAL','LATE','REVERSAL') ORDER BY id DESC LIMIT 1").fetchone()
    if row is None:send('🕒 No confirmed thesis yet.');return
    current=state.get('ticker')
    label='CURRENT' if row['ticker']==current else 'PREVIOUS CONTRACT'
    send(f'🕒 {label} • {row["ticker"]}\nLAST THESIS: {row["side"]} ({row["event"]})\n🎯 Target: {money_text(row["target"])}\n'
         f'⏱ At {row["seconds_left"]}s remaining • {ask_text(row["ask"])}\nResult: {row["grade"]}')

# Exchange lead/lag experiment — strictly observational, never used by settlement_signal.
# The feeds below are public endpoints; access and instrument availability vary.
EXCHANGE_NAMES=('Coinbase','Kraken','Bitstamp','Gemini','LMAX Digital','Bullish','Crypto.com')
EXCHANGE_POLL_SECONDS=5
EXCHANGE_URLS={
    'Coinbase':'https://api.exchange.coinbase.com/products/BTC-USD/ticker',
    'Kraken':'https://api.kraken.com/0/public/Ticker?pair=XBTUSD',
    'Bitstamp':'https://www.bitstamp.net/api/v2/ticker/btcusd/',
    'Gemini':'https://api.gemini.com/v1/pubticker/btcusd',
    'Crypto.com':'https://api.crypto.com/exchange/v1/public/get-tickers?instrument_name=BTC_USD',
    'Bullish':'https://api.exchange.bullish.com/aggregator-api/v1/orderbook/BTC-USD',
    # LMAX instrument is resolved from the official instrument directory.

}

def exchange_price(name, data):
    if name=='Coinbase': return float(data['price'])
    if name=='Kraken': return float(next(iter(data['result'].values()))['c'][0])
    if name=='Bitstamp': return float(data['last'])
    if name=='Gemini': return float(data['last'])
    if name=='Crypto.com':
        item=data['result']['data'][0]
        if item.get('i') not in ('BTC_USD','BTCUSD'):raise ValueError('Wrong instrument')
        return float(item['a'])  # last traded price, NOT best ask
    if name=='LMAX Digital':
        if data.get('status') not in (None,'OPEN'): raise ValueError('LMAX market not open')
        if data.get('instrument_id') != _lmax_instrument: raise ValueError('LMAX instrument mismatch')
        # LMAX public ticker returns last trade, not necessarily current bid/ask.
        return float(data['last_price'])
    if name=='Bullish':
        # Bullish aggregator orderbook returns [price,quantity] pairs; this is
        # a midpoint snapshot, not a last trade. Keep this distinction visible.
        bid=float(data['bids'][0][0]); ask=float(data['asks'][0][0])
        if bid<=0 or ask<=0 or ask<bid: raise ValueError('Invalid Bullish book')
        return (bid+ask)/2
    raise ValueError('Unknown exchange')

_lmax_instrument=None
_lmax_last_discovery=0

def lmax_url(get):
    global _lmax_instrument,_lmax_last_discovery
    override=os.getenv('KEKE_LMAX_INSTRUMENT','').strip()
    if override:
        _lmax_instrument=override
    if not _lmax_instrument:
        now=time.time()
        if now-_lmax_last_discovery<300: raise ValueError('LMAX BTC/USD instrument not discovered')
        _lmax_last_discovery=now
        r=get('https://public-data-api.london-digital.lmax.com/v1/instruments',timeout=3)
        r.raise_for_status()
        instruments=r.json()
        if not isinstance(instruments,list): raise ValueError('Invalid LMAX instruments response')
        matches=[x for x in instruments if isinstance(x,dict) and
                 str(x.get('symbol','')).upper() in ('BTC/USD','XBT/USD') and
                 x.get('ticker_enabled') is True]
        if len(matches)!=1: raise ValueError('LMAX BTC/USD ticker unavailable or ambiguous')
        _lmax_instrument=str(matches[0]['instrument_id'])
    return 'https://public-data-api.london-digital.lmax.com/v1/ticker/'+_lmax_instrument

def exchange_init():
    with connect() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS keke_exchange_samples (
            id INTEGER PRIMARY KEY AUTOINCREMENT, exchange TEXT NOT NULL,
            received_at_ms INTEGER NOT NULL, source_at_ms INTEGER,
            price REAL NOT NULL, instrument TEXT NOT NULL DEFAULT 'BTC/USD',
            UNIQUE(exchange,received_at_ms))''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_exchange_samples ON keke_exchange_samples(exchange,received_at_ms)')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_exchange_health (
            exchange TEXT PRIMARY KEY, checked_at_ms INTEGER NOT NULL,
            status TEXT NOT NULL, detail TEXT)''')

def exchange_sample(name, data, received_at_ms=None):
    price=exchange_price(name,data)
    if not math.isfinite(price) or not 1000<price<10000000:raise ValueError('Invalid BTC price')
    received_at_ms=received_at_ms or int(time.time()*1000)
    with connect() as c:
        c.execute('INSERT OR IGNORE INTO keke_exchange_samples(exchange,received_at_ms,price) VALUES (?,?,?)',(name,received_at_ms,price))
        c.execute('INSERT OR REPLACE INTO keke_exchange_health VALUES (?,?,?,?)',(name,received_at_ms,'OK','Orderbook midpoint' if name=='Bullish' else 'Public last-trade snapshot'))
    return price

def exchange_cycle(get=None):
    # Separate session and thread: exchange outages must not block Kalshi predictions.
    get=get or requests.get
    for name in EXCHANGE_NAMES:
        url=EXCHANGE_URLS.get(name)
        if name=='Bullish':url=os.getenv('KEKE_BULLISH_URL','').strip() or url
        if not url and name!='LMAX Digital':
            with connect() as c:c.execute('INSERT OR REPLACE INTO keke_exchange_health VALUES (?,?,?,?)',(name,int(time.time()*1000),'UNCONFIGURED','Verified BTC/USD data endpoint required'))
            continue
        try:
            if name=='LMAX Digital':url=lmax_url(get)
            response=get(url,timeout=3)
            response.raise_for_status()
            received=int(time.time()*1000)
            exchange_sample(name,response.json(),received)
        except Exception as exc:
            with connect() as c:c.execute('INSERT OR REPLACE INTO keke_exchange_health VALUES (?,?,?,?)',(name,int(time.time()*1000),'ERROR',(type(exc).__name__+': '+str(exc)[:100])))

def exchange_worker():
    while True:
        try:exchange_cycle()
        except Exception:LOG.exception('Exchange observer cycle failed')
        time.sleep(EXCHANGE_POLL_SECONDS)

def exchange_lead_research(window_seconds=30, horizon_seconds=30, lookback_hours=24):
    """Exploratory, non-overlapping directional follow-through, NOT measured latency.

    At sample t: exchange change over preceding window; BRTI change over next
    horizon. BRTI anchor must be observed at or before t, never in the future.
    This cannot establish causality or a tradable Kalshi edge.
    """
    import bisect
    now_ms=int(time.time()*1000)
    cutoff_ms=now_ms-lookback_hours*3600000
    with connect() as c:
        exchanges=c.execute('SELECT exchange,received_at_ms,price FROM keke_exchange_samples WHERE received_at_ms>=? ORDER BY exchange,received_at_ms',(cutoff_ms,)).fetchall()
        brti=c.execute('SELECT timestamp,price FROM keke_brti_ticks WHERE timestamp>=? ORDER BY timestamp',(cutoff_ms//1000-120,)).fetchall()
    bt=[int(r['timestamp'])*1000 for r in brti]
    bp=[float(r['price']) for r in brti]
    grouped={name:[] for name in EXCHANGE_NAMES}
    for r in exchanges:grouped[r['exchange']].append((int(r['received_at_ms']),float(r['price'])))
    output={}
    for name,points in grouped.items():
        if len(points)<3 or len(bt)<3:
            output[name]={'count':0,'hits':0,'rate':None};continue
        ts=[x[0] for x in points];prices=[x[1] for x in points]
        hits=count=0
        last_event=-10**20
        for t,price in points:
            if t-last_event < horizon_seconds*1000:continue
            prior=bisect.bisect_right(ts,t-window_seconds*1000)-1
            anchor=bisect.bisect_right(bt,t)-1
            future=bisect.bisect_right(bt,t+horizon_seconds*1000)-1
            if prior<0 or anchor<0 or future<=anchor:continue
            # Reject stale anchor, missing history and partial future windows.
            if t-ts[prior]>(window_seconds+12)*1000:continue
            if t-bt[anchor]>4000:continue
            if abs(bt[future]-(t+horizon_seconds*1000))>4000:continue
            delta=price-prices[prior]
            brti_delta=bp[future]-bp[anchor]
            if abs(delta)<10:continue # filter insignificant exchange movements
            count+=1;last_event=t
            if (delta>0 and brti_delta>0) or (delta<0 and brti_delta<0):hits+=1
        output[name]={'count':count,'hits':hits,'rate':hits/count if count else None}
    return output


def exchange_research_report():
    stats=exchange_lead_research()
    lines=['🧪 KEKE • EXCHANGE RESEARCH',
           '30s exchange momentum → next 30s BRTI direction',
           'Last 24h • non-overlapping events • $10 minimum move']
    for name in EXCHANGE_NAMES:
        d=stats[name]
        if d['count']<30:lines.append(f'{name}: {d["count"]} events • insufficient data')
        else:lines.append(f'{name}: {d["rate"]:.0%} follow-through • {d["hits"]}/{d["count"]}')
    lines.extend(['⚠️ Exploratory only: 5s HTTP polling, no synchronized trade timestamps.',
                  'Not a measured lead in seconds, prediction accuracy, or trading profit.',
                  'No impact on settlement picks.'])
    return '\n'.join(lines)

def exchange_report():
    with connect() as c:
        rows={r['exchange']:r for r in c.execute('SELECT * FROM keke_exchange_health').fetchall()}
        counts={r['exchange']:r['n'] for r in c.execute('SELECT exchange,COUNT(*) n FROM keke_exchange_samples GROUP BY exchange').fetchall()}
    now=int(time.time()*1000)
    lines=['🔬 KEKE • 7-EXCHANGE OBSERVER','Research only • no influence on settlement picks']
    for name in EXCHANGE_NAMES:
        r=rows.get(name)
        if r is None:label='NOT CHECKED'
        elif r['status']=='OK' and now-r['checked_at_ms']>30000:label='STALE'
        else:label=r['status']
        lines.append(f'{name}: {label} • {counts.get(name,0)} samples')
    lines.append('Use /lead for exploratory directional follow-through')
    lines.append('Lead/lag and net profitability: NOT YET VALIDATED')
    lines.append('Poll intervals and HTTP delays cannot prove second-level lead; timestamped trades/WebSockets required.')
    return '\n'.join(lines)


# V5.26: Additive, research-only audit / consensus / target-publication instrumentation.
# Historical scores remain intact. No experimental input enters settlement_signal().
# V5.27: observational scorecard and immutable checkpoint. No signal threshold changes.
def v527_init():
    with connect() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS keke_v527_checkpoints (
          ticker TEXT PRIMARY KEY, captured_at INTEGER NOT NULL,
          seconds_left INTEGER NOT NULL, side TEXT, signal REAL,
          target REAL, brti REAL, official_side TEXT,
          grade TEXT NOT NULL DEFAULT 'PENDING', graded_at INTEGER)''')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_v527_warnings (
          id INTEGER PRIMARY KEY AUTOINCREMENT, ticker TEXT NOT NULL,
          captured_at INTEGER NOT NULL, seconds_left INTEGER NOT NULL,
          held_side TEXT NOT NULL, candidate_side TEXT NOT NULL,
          signal REAL NOT NULL, target REAL, brti REAL)''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_v527_warning ON keke_v527_warnings(ticker,captured_at)')

def v527_checkpoint(a):
    # Record first available observation at/below 5 minutes, including WAIT.
    if not (0 < a['seconds'] <= 300):return
    side=a.get('v56_side')
    if side not in ('UP','DOWN'):side=None
    with connect() as c:
        c.execute('''INSERT OR IGNORE INTO keke_v527_checkpoints
          (ticker,captured_at,seconds_left,side,signal,target,brti)
          VALUES(?,?,?,?,?,?,?)''',
          (a['ticker'],a['at'],a['seconds'],side,a.get('v56_signal'),a.get('target'),a.get('btc')))

def v527_warning(a,held_side,candidate,signal):
    # Persist only when the existing warning mechanism actually alerts.
    with connect() as c:
        c.execute('''INSERT INTO keke_v527_warnings
          (ticker,captured_at,seconds_left,held_side,candidate_side,signal,target,brti)
          VALUES(?,?,?,?,?,?,?,?)''',
          (a['ticker'],a['at'],a['seconds'],held_side,candidate,signal,a.get('target'),a.get('btc')))

def v527_grade(ticker,outcome,now=None):
    if outcome not in ('UP','DOWN'):return
    now=int(time.time()) if now is None else now
    with connect() as c:
        c.execute("""UPDATE keke_v527_checkpoints SET official_side=?,
          grade=CASE WHEN side IS NULL THEN 'WAIT' WHEN side=? THEN 'WIN' ELSE 'LOSS' END,
          graded_at=? WHERE ticker=? AND grade='PENDING'""" ,(outcome,outcome,now,ticker))

def v527_scorecard(now=None):
    now=int(time.time()) if now is None else int(now)
    start=now-V527_REPORT_WINDOW
    with connect() as c:
        # First-seen openings are the coverage denominator. Exclude openings first
        # observed after the 120-second window and unfinished/unverified markets.
        rows=c.execute('''SELECT h.ticker,h.first_seconds,r.settled_side,
          (SELECT f.side FROM keke_v56_forecasts f WHERE f.ticker=h.ticker
           AND f.event='INITIAL' AND f.seconds_left BETWEEN 780 AND 900
           ORDER BY f.captured_at,f.id LIMIT 1) early_side,
          (SELECT MIN(f.seconds_left) FROM keke_v56_forecasts f WHERE f.ticker=h.ticker
           AND f.event='INITIAL' AND f.seconds_left BETWEEN 780 AND 900) early_seconds
          FROM keke_opening_health h JOIN settlement_results r ON r.ticker=h.ticker
          WHERE h.first_seen>=? AND h.first_seen<=? AND r.verified=1
            AND h.first_seconds BETWEEN 780 AND 900''',(start,now)).fetchall()
        checkpoints=c.execute("""SELECT grade,seconds_left FROM keke_v527_checkpoints
            WHERE captured_at BETWEEN ? AND ? AND grade!='PENDING'""" ,(start,now)).fetchall()
        warnings=c.execute('''SELECT COUNT(*) n FROM keke_v527_warnings
            WHERE captured_at BETWEEN ? AND ?''',(start,now)).fetchone()['n']
    total=len(rows)
    picks=[r for r in rows if r['early_side'] in ('UP','DOWN')]
    wins=sum(r['early_side']==r['settled_side'] for r in picks)
    cp=[r for r in checkpoints if r['grade'] in ('WIN','LOSS')]
    cpw=sum(r['grade']=='WIN' for r in cp)
    waits=sum(r['grade']=='WAIT' for r in checkpoints)
    pct=lambda n,d:f'{100*n/d:.1f}%' if d else 'n/a'
    return (f'📋 KEKE V5.27 • ROLLING 24H SCORECARD\n'
            f'Eligible completed openings: {total} (late first-seen excluded)\n'
            f'Early calls: {len(picks)} • coverage {pct(len(picks),total)} (goal ≥60%)\n'
            f'Early correct: {wins}/{len(picks)} • accuracy {pct(wins,len(picks))} (goal ≥75%)\n'
            f'5m checkpoint: {cpw}/{len(cp)} • accuracy {pct(cpw,len(cp))} • WAIT {waits}\n'
            f'Weakening warnings logged: {warnings}\n'
            '24h is an initial review, not proof of a predictive edge.\n'
            'V5.26 thresholds unchanged; no automatic trading.')

def v527_daily_report(now=None):
    now=int(time.time()) if now is None else int(now)
    started=runtime_get('v527_started_at',None)
    if started is None:
        runtime_set('v527_started_at',now)
        return
    last=runtime_get('v527_last_report_at',started)
    if now-int(last)<V527_REPORT_WINDOW:return
    # Advance report time only after a successful Telegram send.
    if state['enabled'] and TOKEN and CHAT:
        send(v527_scorecard(now))
        runtime_set('v527_last_report_at',now)

def research_init():
    with connect() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS keke_contract_transitions (
          ticker TEXT PRIMARY KEY, first_seen_ms INTEGER NOT NULL,
          first_target_ms INTEGER, first_target REAL, first_quote_ms INTEGER,
          first_yes_ask REAL, first_no_ask REAL, first_brti REAL,
          first_brti_ms INTEGER, first_consensus TEXT, first_direction TEXT)''')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_transition_snapshots (
          ticker TEXT NOT NULL, observed_ms INTEGER NOT NULL,
          seconds_left INTEGER, target REAL, brti REAL, brti_ms INTEGER,
          up_ask REAL, down_ask REAL, up_bid REAL, down_bid REAL,
          bullish INTEGER, bearish INTEGER, neutral INTEGER,
          momentum_pct REAL, preliminary_side TEXT,
          PRIMARY KEY(ticker,observed_ms))''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_transition_snapshots ON keke_transition_snapshots(ticker,observed_ms)')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_locked_forecasts (
          ticker TEXT PRIMARY KEY, captured_at INTEGER NOT NULL,
          seconds_left INTEGER NOT NULL, side TEXT NOT NULL,
          brti REAL, target REAL, up_ask REAL, down_ask REAL,
          official_side TEXT, grade TEXT NOT NULL DEFAULT 'PENDING', graded_at INTEGER)''')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_consensus_predictions (
          ticker TEXT PRIMARY KEY, captured_at INTEGER NOT NULL,
          seconds_left INTEGER, preliminary_side TEXT NOT NULL,
          bullish INTEGER, bearish INTEGER, neutral INTEGER,
          mean_pct REAL, target_at_prediction REAL,
          official_side TEXT, grade TEXT NOT NULL DEFAULT 'PENDING', graded_at INTEGER)''')

def exchange_consensus(now_ms=None, lookback_s=30):
    # Compare each venue to itself: no averaging heterogeneous price levels.
    now_ms=int(time.time()*1000) if now_ms is None else now_ms
    with connect() as c:
        rows=c.execute('''SELECT exchange,received_at_ms,price FROM keke_exchange_samples
                          WHERE received_at_ms>=? AND received_at_ms<=?
                          ORDER BY exchange,received_at_ms''',
                       (now_ms-(lookback_s+20)*1000,now_ms)).fetchall()
    grouped={name:[] for name in EXCHANGE_NAMES}
    for r in rows: grouped[r['exchange']].append(r)
    bull=bear=neutral=0;changes=[];available=[]
    for name,points in grouped.items():
        if not points:continue
        latest=points[-1]
        if now_ms-latest['received_at_ms']>15000:continue
        baseline=min(points,key=lambda r:abs(r['received_at_ms']-(now_ms-lookback_s*1000)))
        if abs(baseline['received_at_ms']-(now_ms-lookback_s*1000))>10000:continue
        if baseline['received_at_ms']>=latest['received_at_ms'] or baseline['price']<=0:continue
        pct=100*(latest['price']/baseline['price']-1)
        changes.append(pct);available.append(name)
        if pct>0.015:bull+=1
        elif pct< -0.015:bear+=1
        else:neutral+=1
    direction='UP' if bull>=5 and bull>bear else ('DOWN' if bear>=5 and bear>bull else 'WAIT')
    return {'bullish':bull,'bearish':bear,'neutral':neutral,'available':len(changes),
            'mean_pct':sum(changes)/len(changes) if changes else None,
            'direction':direction,'venues':available}

def observe_transition(m, brti=None, brti_stamp=None):
    ticker=m.get('ticker')
    if not ticker:return
    now_ms=int(time.time()*1000)
    remaining=seconds_remaining(m)
    # Observe only near the opening to reduce writes; no fabricated TBD state.
    if remaining<720 or remaining>920:return
    target=target_of(m)
    up_ask=quote(m,'UP','ask');down_ask=quote(m,'DOWN','ask')
    up_bid=quote(m,'UP','bid');down_bid=quote(m,'DOWN','bid')
    consensus=exchange_consensus(now_ms)
    try: bstamp=int(float(brti_stamp)*1000) if brti_stamp is not None else None
    except (TypeError,ValueError):bstamp=None
    # Never treat BRTI as fresh when its own timestamp is stale.
    if bstamp is not None and (bstamp>now_ms+2000 or now_ms-bstamp>30000):brti=None
    with connect() as c:
        c.execute('''INSERT OR IGNORE INTO keke_contract_transitions
                     (ticker,first_seen_ms,first_consensus,first_direction)
                     VALUES(?,?,?,?)''',
                  (ticker,now_ms,json.dumps(consensus),consensus['direction']))
        if target is not None:
            c.execute('''UPDATE keke_contract_transitions SET first_target_ms=?,first_target=?
                         WHERE ticker=? AND first_target_ms IS NULL''',(now_ms,target,ticker))
        if up_ask is not None or down_ask is not None:
            c.execute('''UPDATE keke_contract_transitions SET first_quote_ms=?,first_yes_ask=?,first_no_ask=?
                         WHERE ticker=? AND first_quote_ms IS NULL''',
                      (now_ms,up_ask,down_ask,ticker))
        if brti is not None:
            c.execute('''UPDATE keke_contract_transitions SET first_brti=?,first_brti_ms=?
                         WHERE ticker=? AND first_brti_ms IS NULL''',(brti,bstamp or now_ms,ticker))
        c.execute('''INSERT OR IGNORE INTO keke_transition_snapshots
            (ticker,observed_ms,seconds_left,target,brti,brti_ms,up_ask,down_ask,up_bid,down_bid,
             bullish,bearish,neutral,momentum_pct,preliminary_side)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (ticker,now_ms,remaining,target,brti,bstamp,up_ask,down_ask,up_bid,down_bid,
             consensus['bullish'],consensus['bearish'],consensus['neutral'],consensus['mean_pct'],consensus['direction']))
        if consensus['direction']!='WAIT' and consensus['available']>=5:
            c.execute('''INSERT OR IGNORE INTO keke_consensus_predictions
                (ticker,captured_at,seconds_left,preliminary_side,bullish,bearish,neutral,mean_pct,target_at_prediction)
                VALUES(?,?,?,?,?,?,?,?,?)''',
                (ticker,now_ms//1000,remaining,consensus['direction'],consensus['bullish'],
                 consensus['bearish'],consensus['neutral'],consensus['mean_pct'],target))

def lock_forecast(a):
    # Freeze first available thesis with 5 minutes OR LESS remaining, never revise. Late captures are labeled by seconds_left.
    if not (0<a['seconds']<=300):return
    side=a.get('v56_side')
    if side not in ('UP','DOWN'):return
    with connect() as c:
        c.execute('''INSERT OR IGNORE INTO keke_locked_forecasts
            (ticker,captured_at,seconds_left,side,brti,target,up_ask,down_ask)
            VALUES(?,?,?,?,?,?,?,?)''',
            (a['ticker'],a['at'],a['seconds'],side,a['btc'],a['target'],a['asks'].get('UP'),a['asks'].get('DOWN')))

def research_grade(ticker,outcome,now=None):
    if outcome not in ('UP','DOWN','UNGRADABLE'):return
    now=int(time.time()) if now is None else now
    with connect() as c:
        for table,field in [('keke_locked_forecasts','side'),('keke_consensus_predictions','preliminary_side')]:
            if outcome=='UNGRADABLE':
                c.execute(f"UPDATE {table} SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
            else:
                c.execute(f'''UPDATE {table} SET grade=CASE WHEN {field}=? THEN 'WIN' ELSE 'LOSS' END,
                             official_side=?,graded_at=? WHERE ticker=? AND grade='PENDING' ''',
                          (outcome,outcome,now,ticker))

def audit_report(limit=8):
    with connect() as c:
        rows=c.execute('''SELECT r.ticker,r.settled_side,
          (SELECT side FROM keke_v56_forecasts f WHERE f.ticker=r.ticker ORDER BY id ASC LIMIT 1) first_side,
          (SELECT seconds_left FROM keke_v56_forecasts f WHERE f.ticker=r.ticker ORDER BY id ASC LIMIT 1) first_secs,
          (SELECT side FROM keke_v56_forecasts f WHERE f.ticker=r.ticker ORDER BY id DESC LIMIT 1) last_side,
          (SELECT seconds_left FROM keke_v56_forecasts f WHERE f.ticker=r.ticker ORDER BY id DESC LIMIT 1) last_secs,
          l.side locked_side,l.seconds_left locked_secs
          FROM settlement_results r LEFT JOIN keke_locked_forecasts l ON l.ticker=r.ticker
          WHERE r.verified=1 ORDER BY r.recorded_at DESC LIMIT ?''',(limit,)).fetchall()
    if not rows:return '🧾 No verified contracts yet.'
    lines=['🧾 KEKE • CONTRACT AUDIT (newest first)','First / Locked ≤5m / Last vs official']
    for r in rows:
        def fmt(side,sec):return (f'{side} @{sec}s '+('✓' if side==r['settled_side'] else '✗')) if side else '—'
        lines.append(f"{r['ticker']} • official {r['settled_side']}\n"
                     f"First {fmt(r['first_side'],r['first_secs'])} | "
                     f"Locked {fmt(r['locked_side'],r['locked_secs'])} | "
                     f"Last {fmt(r['last_side'],r['last_secs'])}")
    lines.append('Frozen forecasts begin after deployment; capture may occur later than 5m if no earlier signal. Historical records not reconstructed.')
    return '\n'.join(lines)

def research_stats_report():
    with connect() as c:
        data={}
        for table in ('keke_locked_forecasts','keke_consensus_predictions'):
            rows=c.execute(f'SELECT grade FROM {table}').fetchall()
            w=sum(r['grade']=='WIN' for r in rows);l=sum(r['grade']=='LOSS' for r in rows)
            data[table]=f'{w}W/{l}L ({100*w/(w+l):.1f}%)' if w+l else 'awaiting completed contracts'
        transition=c.execute('SELECT COUNT(*) n, SUM(CASE WHEN first_target_ms>first_seen_ms THEN 1 ELSE 0 END) delayed FROM keke_contract_transitions').fetchone()
    return ('🔬 KEKE • V5.26 RESEARCH\n'
            f"Frozen at ≤5m: {data['keke_locked_forecasts']}\n"
            f"Preliminary exchange consensus: {data['keke_consensus_predictions']}\n"
            f"Transitions observed: {transition['n']}\n"
            'TBD cannot be inferred from a missing target alone; API first-seen timing is not app display latency.\n'
            'No live trades or verified net trading edge.')

def preview_report():
    cns=exchange_consensus()
    with connect() as c:
        row=c.execute('SELECT * FROM keke_contract_transitions ORDER BY first_seen_ms DESC LIMIT 1').fetchone()
    label=(row['ticker'] if row else 'waiting for first contract')
    target=(f"${row['first_target']:,.2f}" if row and row['first_target'] is not None else 'TBD / not observed')
    mean=f"{cns['mean_pct']:+.4f}%" if cns['mean_pct'] is not None else 'n/a'
    return (f'🔮 KEKE • NEXT CONTRACT RESEARCH\nContract: {label}\n'
            f"7-exchange consensus: {cns['bullish']} bullish / {cns['bearish']} bearish / {cns['neutral']} flat "
            f"({cns['available']}/7 usable)\nMean 30s move: {mean}\n"
            f"Preliminary: {cns['direction']} (research only)\n"
            f"First API-observed target: {target}\n"
            'Target and tradable quote timing may differ. No automatic orders or changes to settlement picks.')

def main():
    init_db()
    kalshi_live_init()
    threading.Thread(target=kalshi_live_worker,daemon=True,name='kalshi-chart-observer').start()
    exchange_init()
    research_init()
    v527_init()
    ai_lab.start(DB_PATH,send)
    v527_daily_report()
    if os.getenv("KEKE_EXCHANGE_OBSERVER","1")=="1":
        threading.Thread(target=exchange_worker,daemon=True,name="exchange-observer").start()
    state['enabled']=runtime_get('alerts_enabled',True)
    state['offset']=runtime_get('telegram_offset',None)
    initialize_telegram()
    LOG.info('KeKe V5.27 settlement-only BRTI read-only monitor starting; Stats database: %s',DB_PATH)
    run_brti_test()
    if state['enabled']:send('🎯 KeKe V5.27 BRTI settlement predictions online. '
         'BRTI-only price/candles; no Kraken fallback. Persistent /data database. '
         'Official-result grading active. Scalp signals disabled. No trades. /stats for BRTI-only, /legacy for history' )
    while True:
        started=time.monotonic()
        try:
            telegram_commands()
            if time.monotonic()-state['last_grade']>=GRADE_EVERY:
                state['last_grade']=time.monotonic()
                grade_settlements()
                v527_daily_report()
            ai_lab.schedule_research()
            market=get_market()
            if market:
                try:
                    brti,brti_stamp=save_brti_ticks()
                    state['current_brti_stamp']=brti_stamp
                    state['brti_error_at']=0
                except Exception as exc:
                    brti=None
                    state['current_brti_stamp']=None
                    if time.time()-state.get('brti_error_at',0)>600:
                        LOG.warning('BRTI unavailable: %s',str(exc)[:160])
                        state['brti_error_at']=time.time()
                try: observe_transition(market,brti,brti_stamp)
                except Exception: LOG.exception("Transition observer failed")
                if brti is None:
                    raise ValueError('BRTI unavailable; no candle/settlement signal')
                candles=brti_candles()
                # Never fabricate candles; evaluate early forecasts with available verified ticks.
                if not candles:
                    # First minute: use verified recent BRTI ticks for a provisional
                    # settlement forecast. Never manufacture completed candles.
                    a=fast_opening_assessment(market,brti)
                    if a is not None:
                        new=state['ticker']!=a['ticker']
                        state['ticker']=a['ticker']
                        a['dual_read']={'frames':{1:'COLLECTING',3:'COLLECTING',15:'COLLECTING'},'scalp':'WAIT'}
                        opening_seen(a['ticker'],a['seconds'],True)
                        process_v56(a,new)
                        lock_forecast(a)
                        v527_checkpoint(a)
                        portfolio_poll(market,a,{'scalp':'WAIT'})
                        state['last_read']=a
                        time.sleep(max(1,CHECK_EVERY-(time.monotonic()-started)))
                        continue
                if not candles:
                    # Portfolio reads must keep working while BRTI history warms up.
                    portfolio_poll(market,{'seconds':seconds_remaining(market)}, {'scalp':'WAIT'})
                    opening_seen(market['ticker'],seconds_remaining(market),False)
                    new=state['ticker']!=market['ticker']
                    if new:
                        state['ticker']=market['ticker']
                        state['last_alert']=None
                        state['entry_side']=None
                        if state['enabled']:
                            level=brti_opening_levels(candles,brti)
                            send(banner_levels({'brti_levels':level})+'\n'
                                 +f'🎯 Target {money_text(target_of(market))} • BRTI ${brti:,.2f}\n'
                                 +'📊 Verified BRTI history warming up\n'
                                 +'⚪ NO TRADE SIGNAL • Insufficient BRTI history')
                    state['last_read']=None
                    time.sleep(max(1,CHECK_EVERY-(time.monotonic()-started)))
                    continue
                if time.time()-(candles[-1]['time']+60)>MAX_DATA_AGE:
                    raise ValueError('BRTI candles stale; no directional read')
                btc=brti
                a=assessment(market,btc,candles,brti) if len(candles)>=15 else fast_opening_assessment(market,brti)
                if a is None: raise ValueError('Insufficient observed BRTI samples')
                a['brti_levels']=brti_opening_levels(candles,brti)
                opening_seen(a['ticker'],a['seconds'],True)
                if len(candles)>=15:
                    record(a)
                    record_early_prediction(a)
                    record_v53(a)
                quick=dual_read(a,candles) if len(candles)>=15 else {'scalp':'WAIT','frames':{1:'COLLECTING',3:'COLLECTING',15:'COLLECTING'}}
                a['dual_read']=quick
                # Independent retest observations remain active in settlement-only mode.
                market_structure_alert(market,a,candles)
                # Position monitoring runs after settlement forecast updates.
                # Settlement-only mode: no paper scalp entries, exits, or range-trading alerts.
                # Legacy scalp history remains untouched in SQLite.
                try:
                    if len(candles)>=15: record_shadow(a)
                except Exception:
                    LOG.exception('Silent shadow-model recording failed')
                new=state['ticker']!=a['ticker']
                if new:
                    state['ticker']=a['ticker']
                    state['last_alert']=None
                    state['entry_side']=None
                if new:
                    proactive_updates(a,True)
                process_v56(a,new)
                try:
                    v528_shadow_tick(a,candles)
                    ai_lab.queue_live(a,shadow_view['last'])
                except Exception:LOG.exception('V5.28 AI/shadow tick failed')
                lock_forecast(a)
                v527_checkpoint(a)
                portfolio_poll(market,a,{'scalp':'WAIT'})
                if not new:
                    proactive_updates(a,False)
                state['last_read']=a
                # Diagnostic only: source observation time versus local receive/log time.
                # Does not alter settlement predictions, prices, or Telegram output.
                source_ts = state.get('current_brti_stamp')
                received_ts = time.time()
                if source_ts is not None:
                    LOG.info('%s settlement %s BRTI %s target %s | brti_source_utc=%s received_utc=%s feed_age_s=%.2f',
                             a['ticker'], a.get('v56_side'), brti, a['target'],
                             datetime.fromtimestamp(float(source_ts), timezone.utc).isoformat(timespec='milliseconds'),
                             datetime.fromtimestamp(received_ts, timezone.utc).isoformat(timespec='milliseconds'),
                             received_ts-float(source_ts))
                else:
                    LOG.info('%s settlement %s BRTI %s target %s | brti_source_utc=UNKNOWN',
                             a['ticker'], a.get('v56_side'), brti, a['target'])
            else:LOG.info('No active BTC 15m contract')
        except Exception:LOG.exception('Monitor loop error; will retry')
        time.sleep(max(1,CHECK_EVERY-(time.monotonic()-started)))

if __name__=='__main__':main()
