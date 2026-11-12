"""KeKe V5.5 settlement thesis tracker — settlement-first, observation-only Kalshi BTC 15m watcher.
Requires: requests. Existing SQLite tables are preserved; new tables are additive.
"""
import os, time, json, sqlite3, re, logging, base64, math
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, ed25519
from datetime import datetime, timezone
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
LOG = logging.getLogger('keke')
BASE='https://external-api.kalshi.com/trade-api/v2'
SERIES='KXBTC15M'
KRAKEN='https://api.kraken.com/0/public'
TOKEN=os.getenv('TELEGRAM_BOT_TOKEN')
CHAT=os.getenv('TELEGRAM_CHAT_ID')
DATA_DIR=os.getenv('DATA_DIR','/data')
DB_NAME='kate_btc_watcher.db'
CHECK_EVERY=10
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
       'last_btc_at':None,'last_poll':0,'last_grade':0}

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

def kraken_data():
    # One-minute candles include history BEFORE this Kalshi contract began.
    obj=http_json(KRAKEN+'/OHLC',{'pair':'XBTUSD','interval':1})
    results=obj.get('result',{})
    key=next((k for k in results if k!='last'),None)
    if not key: raise ValueError('No Kraken OHLC series')
    now=time.time()
    candles=[]
    for r in results[key]:
        try:
            t=int(float(r[0]))
            if t+60<=now:
                candles.append({'time':t,'open':float(r[1]),'high':float(r[2]),
                                'low':float(r[3]),'close':float(r[4]),'volume':float(r[6]) if len(r)>6 else 0.0})
        except (IndexError,TypeError,ValueError): pass
    if len(candles)<12: raise ValueError('Not enough completed 1m candles')
    obj=http_json(KRAKEN+'/Ticker',{'pair':'XBTUSD'})
    ticker=next(iter(obj.get('result',{}).values()),None)
    if not ticker: raise ValueError('Kraken ticker unavailable')
    live=float(ticker['c'][0])
    return live,candles

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
    delta1=btc-closes[-1];delta3=btc-closes[-3]
    ranges=[c['high']-c['low'] for c in candles[-12:]]
    typical_range=sum(ranges)/len(ranges)
    trend='UP' if closes[-1]>closes[-6] and closes[-3]>closes[-9] else ('DOWN' if closes[-1]<closes[-6] and closes[-3]<closes[-9] else 'MIXED')
    distance=brti-target if brti is not None and target is not None else None
    favored='UP' if distance is not None and distance>0 else ('DOWN' if distance is not None and distance<0 else None)
    # Completed 1-minute Kraken candles; pre-contract candles available immediately.
    e5=ema(closes[-50:],5);e13=ema(closes[-50:],13)
    r=rsi(closes)
    vol=[c['volume'] for c in candles[-21:]]
    vol_ratio=vol[-1]/max(sum(vol[:-1])/max(1,len(vol)-1),.000001)
    recent=candles[-16:-1]
    resistance=max(c['high'] for c in recent)
    support=min(c['low'] for c in recent)
    vwap_num=sum(((c['high']+c['low']+c['close'])/3)*c['volume'] for c in candles[-15:])
    vwap_den=sum(c['volume'] for c in candles[-15:])
    vwap=vwap_num/vwap_den if vwap_den else closes[-1]
    # A directional research score, NOT a probability or claimed statistical edge.
    score=0.0
    score+=1.0 if btc>e5 else -1.0
    score+=1.0 if e5>e13 else -1.0
    score+=.75 if btc>vwap else -.75
    score+=.75 if delta3>0 else -.75
    score+=.5 if closes[-1]>closes[-6] else -.5
    score+=.5 if r>55 else (-.5 if r<45 else 0)
    if btc>resistance and vol_ratio>1.15:score+=1.0
    if btc<support and vol_ratio>1.15:score-=1.0
    # Distance is BRTI-based; never replace settlement source with Kraken.
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
                'vwap15':round(vwap,2),'vol_ratio':round(vol_ratio,2),
                'support15':round(support,2),'resistance15':round(resistance,2),
                'momentum1':round(delta1,2),'momentum3':round(delta3,2),
                'momentum5':round(btc-closes[-5],2),'momentum15':round(btc-closes[-15],2)}
    return {'ticker':m['ticker'],'at':now,'seconds':seconds,'btc':brti,'kraken':btc,
            'source':'BRTI' if brti is not None else 'UNAVAILABLE','target':target,
            'distance':distance,'favored':favored,'trend':trend,'delta1':delta1,
            'delta3':delta3,'volatility':typical_range,'confidence':confidence,
            'decision':decision,'reason':'; '.join(reasons),'bids':bids,'asks':asks,
            'predicted':side,'score':score,'multiplier':multiple,'indicators':indicators}

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
           a['confidence'],a['decision'],a['reason'],'5.5-THESIS'))
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
        ) ORDER BY ticker LIMIT ?''',(GRADE_BATCH,)).fetchall()
    verified_now=0
    newly_finalized=[]
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
                c.execute("UPDATE settlement_calls SET result='UNGRADABLE',result_source='Kalshi market result' WHERE ticker=? AND result='PENDING'",(ticker,))
                c.execute("UPDATE settlement_predictions SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
                c.execute("UPDATE v53_opportunities SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
                c.execute("UPDATE keke_thesis_events SET grade='UNGRADABLE',graded_at=? WHERE ticker=? AND grade='PENDING'",(now,ticker))
                continue
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
            verified_now+=1
    if verified_now:
        LOG.info('Official outcomes newly verified: %d',verified_now)


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
    return (f'📊 KEKE REPORT CARD\n'
            f'Contracts {contracts} • Snapshots {observations} • Official results {verified}\n'
            f'🚨 ENTER: {cw}W / {cl}L • {pct(cw,cl)} • pending {call_counts.get("PENDING",0)}\n'
            f'🔎 EARLY LEAN: {pw}W / {pl}L • {pct(pw,pl)} • pending {pred_counts.get("PENDING",0)}\n'
            f'🧪 V5.3 PAPER: {bw}W / {bl}L • {pct(bw,bl)} • pending {research.get(("PAPER_BUY","PENDING"),0)}\n'
            f'{event_line("INITIAL")}\n'
            f'{event_line("REVERSAL")}\n'
            f'{event_line("FINAL")}\n'
            f'Illustrative ENTER gross: ${gross:+.2f} across {len(returns)} graded calls (1 contract each; NO fees/fills)\n'
            'Official outcomes only • No real trades placed')

def tg(method,**data):
    if not TOKEN: return None
    r=session.post(f'https://api.telegram.org/bot{TOKEN}/{method}',data=data,timeout=7)
    r.raise_for_status()
    return r.json()

KEYBOARD={'keyboard':[[{'text':'👀 STATUS'},{'text':'📊 STATS'}],
                      [{'text':'🕒 LAST'},{'text':'🧠 MARKET READ'}],
                      [{'text':'🟢 START'},{'text':'🔴 STOP'}]],
          'resize_keyboard':True,'is_persistent':True}

def send(message):
    if not TOKEN or not CHAT:
        LOG.info('Telegram unavailable; %s',message)
        return
    try: tg('sendMessage',chat_id=CHAT,text=message,reply_markup=json.dumps(KEYBOARD))
    except Exception: LOG.exception('Telegram send failed')

def money_text(x): return f'${x:,.2f}' if x is not None else 'UNKNOWN'
def ask_text(x): return f'{x*100:.1f}¢' if x is not None else 'n/a'

def format_read(a, headline=None):
    mm,ss=divmod(max(0,int(a['seconds'])),60)
    target=money_text(a.get('target'))
    if a['source']!='BRTI' or a.get('target') is None:
        return f'⚪ NO SETTLEMENT READ\n🎯 Target: {target}\n⏱ {mm}:{ss:02d} • BRTI unavailable'
    side=a.get('thesis_side') or a['predicted']
    ask=a['asks'].get(side)
    mult=f'{1/ask:.2f}×' if ask and ask>0 else 'n/a'
    icon='🟢' if side=='UP' else '🔴'
    label=headline or (f'{icon} CURRENT THESIS: {side}' if a.get('thesis_side') else '👀 ANALYZING • No initial pick yet')
    return (f'{label}\n🎯 Target: {target}\n'
            f'⏱ {mm}:{ss:02d} • {side} {mult} • score {a["score"]:+.1f}\n'
            '🧪 Research prediction • No trades')

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
        send(f'🟢 BRTI LIVE: ${price:,.2f}\nFeed age: {time.time()-stamp:.0f}s\nSettlement reads use BRTI; Kraken for trend.')
    except Exception as exc:
        LOG.warning('BRTI test failed: %s',type(exc).__name__)
        try:
            shape=brti_record_shape()
            send('🔎 BRTI RECORD FIELDS (names/types only):\n'+shape[:1600]+'\n⚪ Settlement ENTER remains disabled.')
        except Exception as diag_exc:
            send(f'🔴 BRTI TEST FAILED: {type(exc).__name__}\nDiagnostic fetch: {type(diag_exc).__name__}\nNo settlement ENTER.')

def send_stats():
    send(grade_summary())

def telegram_commands():
    if not TOKEN or not CHAT:return
    try:
        params={'timeout':0,'limit':30}
        if state['offset'] is not None:params['offset']=state['offset']
        obj=http_json(f'https://api.telegram.org/bot{TOKEN}/getUpdates',params)
        for upd in obj.get('result',[]):
            state['offset']=upd['update_id']+1
            msg=upd.get('message') or {}
            if str((msg.get('chat') or {}).get('id'))!=str(CHAT):continue
            cmd=str(msg.get('text') or '').strip().lower()
            cmd={'👀 status':'/status','🧠 market read':'/status','📊 stats':'/stats',
                 '🕒 last':'/last','🟢 start':'/start','🔴 stop':'/stop'}.get(cmd,cmd)
            if cmd.startswith('/stop'):
                state['enabled']=False;send('🔴 Alerts paused. Data collection continues. /start resumes alerts.')
            elif cmd.startswith('/start'):
                state['enabled']=True;send('🟢 Settlement alerts ON.')
            elif cmd.startswith('/status'):
                send(format_read(state['last_read']) if state['last_read'] else 'Waiting for fresh market data.')
            elif cmd.startswith('/stats'):send_stats()
            elif cmd.startswith('/last'):send_last()
            elif cmd.startswith('/brti'):run_brti_test()
    except Exception:LOG.exception('Telegram command polling error')

def initialize_telegram():
    if not TOKEN:return
    try:
        obj=http_json(f'https://api.telegram.org/bot{TOKEN}/getUpdates',{'timeout':0,'limit':100})
        updates=obj.get('result',[])
        if updates:state['offset']=updates[-1]['update_id']+1
        tg('setMyCommands',commands=json.dumps([
            {'command':'start','description':'Resume settlement alerts'},
            {'command':'stop','description':'Pause alerts; keep recording'},
            {'command':'status','description':'Latest settlement read'},
            {'command':'stats','description':'Settlement research counts'},
            {'command':'last','description':'Last settlement ENTER'},
            {'command':'brti','description':'Test read-only Kalshi BRTI access'}]))
    except Exception:LOG.exception('Telegram setup error')

# Thesis state is reconstructed from SQLite on restart, so a deploy does not
# silently erase the original pick. Reversals need 3 consecutive 10s readings.
THESIS_START_SCORE=2.5
THESIS_REVERSE_SCORE=3.0
THESIS_REVERSE_POLLS=3
THESIS_WEAKEN_POLLS=2
THESIS_RECOVER_POLLS=2

def load_thesis(ticker):
    with connect() as c:
        events=c.execute("SELECT * FROM keke_thesis_events WHERE ticker=? ORDER BY id",(ticker,)).fetchall()
    side=None;weakened=False
    for e in events:
        if e['event'] in ('INITIAL','REVERSAL'):side=e['side'];weakened=False
        elif e['event']=='WEAKENING':weakened=True
        elif e['event']=='RECOVERED':weakened=False
    return {'side':side,'weakened':weakened,'opposite_count':0,'weak_count':0,'recover_count':0,
            'last_event':events[-1]['event'] if events else None}

def thesis_event(a,event,side,prior_side=None,reason=''):
    with connect() as c:
        c.execute('''INSERT INTO keke_thesis_events
          (ticker,captured_at,seconds_left,event,side,prior_side,score,ask,target,brti,reason)
          VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
          (a['ticker'],a['at'],a['seconds'],event,side,prior_side,
           a['score'],a['asks'].get(side),a['target'],a['btc'],reason))

def process_thesis(a,new_contract):
    if new_contract or state.get('thesis_ticker')!=a['ticker']:
        state['thesis_ticker']=a['ticker']
        state['thesis']=load_thesis(a['ticker'])
    t=state['thesis']
    a['thesis_side']=t['side']
    if a['source']!='BRTI' or a['target'] is None:
        return
    candidate=a['predicted'];score=a['score'];strength=abs(score)
    if t['side'] is None:
        if strength>=THESIS_START_SCORE:
            t['side']=candidate
            a['thesis_side']=candidate
            thesis_event(a,'INITIAL',candidate,reason='First qualifying directional score')
            if state['enabled']:send(format_read(a,f'{"🟢" if candidate=="UP" else "🔴"} INITIAL PICK: {candidate}'))
        elif new_contract and state['enabled']:
            send(format_read(a,'👀 ANALYZING • No initial pick yet'))
        return
    side=t['side']
    aligned=(score>0 and side=='UP') or (score<0 and side=='DOWN')
    # Sustained opposing evidence, not merely crossing the strike.
    opposite=(not aligned and strength>=THESIS_REVERSE_SCORE)
    t['opposite_count']=t['opposite_count']+1 if opposite else 0
    if t['opposite_count']>=THESIS_REVERSE_POLLS:
        previous=side;t['side']=candidate;t['weakened']=False
        t['opposite_count']=t['weak_count']=t['recover_count']=0
        a['thesis_side']=candidate
        thesis_event(a,'REVERSAL',candidate,previous,'3 consecutive strong opposite readings')
        if state['enabled']:send(format_read(a,f'🔄 PREDICTION REVERSED: {previous} → {candidate}'))
        return
    weak=(not aligned) or strength<1.5
    t['weak_count']=t['weak_count']+1 if weak else 0
    if not t['weakened'] and t['weak_count']>=THESIS_WEAKEN_POLLS:
        t['weakened']=True;t['recover_count']=0
        thesis_event(a,'WEAKENING',side,reason='2 consecutive weak/opposing readings')
        if state['enabled']:send(format_read(a,f'⚠️ {side} THESIS WEAKENING'))
    if t['weakened']:
        t['recover_count']=t['recover_count']+1 if aligned and strength>=THESIS_START_SCORE else 0
        if t['recover_count']>=THESIS_RECOVER_POLLS:
            t['weakened']=False;t['weak_count']=t['recover_count']=0
            thesis_event(a,'RECOVERED',side,reason='2 consecutive aligned readings')
            if state['enabled']:send(format_read(a,f'✅ {side} THESIS STRENGTHENING'))
    a['thesis_side']=t['side']

def finalize_previous(ticker):
    if not ticker:return
    with connect() as c:
        existing=c.execute("SELECT 1 FROM keke_thesis_events WHERE ticker=? AND event='FINAL'",(ticker,)).fetchone()
        latest=c.execute("SELECT * FROM keke_thesis_events WHERE ticker=? AND event IN ('INITIAL','REVERSAL') ORDER BY id DESC LIMIT 1",(ticker,)).fetchone()
        if existing or not latest:return
        c.execute('''INSERT INTO keke_thesis_events
            (ticker,captured_at,seconds_left,event,side,prior_side,score,ask,target,brti,reason)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
            (ticker,int(time.time()),0,'FINAL',latest['side'],None,latest['score'],
             latest['ask'],latest['target'],latest['brti'],
             'Last recorded confirmed thesis; price is from last thesis event'))

def send_last():
    with connect() as c:
        row=c.execute("SELECT * FROM keke_thesis_events WHERE event IN ('INITIAL','REVERSAL') ORDER BY id DESC LIMIT 1").fetchone()
    if row is None:send('🕒 No confirmed thesis yet.');return
    send(f'🕒 LAST THESIS: {row["side"]} ({row["event"]})\n🎯 Target: {money_text(row["target"])}\n'
         f'⏱ At {row["seconds_left"]}s remaining • {ask_text(row["ask"])}\nResult: {row["grade"]}')

def main():
    init_db()
    initialize_telegram()
    LOG.info('KeKe V5.5 Settlement Thesis starting; Stats database: %s',DB_PATH)
    run_brti_test()
    send('🧠 KeKe V5.5 settlement thesis tracker online. '
         'BRTI for target distance; Kraken for momentum. Persistent /data database. '
         'Automatic official-result grading active. No trades. /stats' )
    while True:
        started=time.monotonic()
        try:
            telegram_commands()
            if time.monotonic()-state['last_grade']>=GRADE_EVERY:
                state['last_grade']=time.monotonic()
                grade_settlements()
            market=get_market()
            if market:
                btc,candles=kraken_data()
                # Reject stale OHLC before making decisions.
                if time.time()-(candles[-1]['time']+60)>MAX_DATA_AGE:
                    raise ValueError('Stale completed Kraken candle; skipping decisions')
                try:
                    brti,brti_stamp=brti_live()
                    state['brti_error_at']=0
                except Exception as exc:
                    brti=None
                    if time.time()-state.get('brti_error_at',0)>600:
                        LOG.warning('BRTI unavailable: %s',str(exc)[:160])
                        state['brti_error_at']=time.time()
                a=assessment(market,btc,candles,brti)
                record(a)
                record_early_prediction(a)
                record_v53(a)
                new=state['ticker']!=a['ticker']
                if new:
                    finalize_previous(state['ticker'])
                    state['ticker']=a['ticker']
                    state['last_alert']=None
                    state['entry_side']=None
                process_thesis(a,new)
                state['last_read']=a
                LOG.info('%s %s BRTI %s Kraken %.2f target %s',a['ticker'],a['decision'],brti,btc,a['target'])
            else:LOG.info('No active BTC 15m contract')
        except Exception:LOG.exception('Monitor loop error; will retry')
        time.sleep(max(1,CHECK_EVERY-(time.monotonic()-started)))

if __name__=='__main__':main()
