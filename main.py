"""KeKe V5.0 — settlement-first, observation-only Kalshi BTC 15m watcher.
Requires: requests. Existing SQLite tables are preserved; new tables are additive.
"""
import os, time, json, sqlite3, re, logging
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
MAX_DATA_AGE=75
MIN_ENTRY_SECONDS=90
MAX_ENTRY_ASK=.92
MIN_NET_EDGE=.05  # conservative heuristic margin, NOT a calibrated probability edge
MIN_CUSHION=20  # USD; below this, don't issue directional ENTER
ALERT_COOLDOWN=90

session=requests.Session()
state={'ticker':None,'last_alert':None,'last_sent':0,'last_side':None,'last_read':None,
       'enabled':True,'offset':None,'entry_side':None,'entry_at':None,'last_btc':None,
       'last_btc_at':None,'last_poll':0}

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
                                'low':float(r[3]),'close':float(r[4])})
        except (IndexError,TypeError,ValueError): pass
    if len(candles)<12: raise ValueError('Not enough completed 1m candles')
    obj=http_json(KRAKEN+'/Ticker',{'pair':'XBTUSD'})
    ticker=next(iter(obj.get('result',{}).values()),None)
    if not ticker: raise ValueError('Kraken ticker unavailable')
    live=float(ticker['c'][0])
    return live,candles

def assessment(m,btc,candles):
    now=int(time.time())
    seconds=seconds_remaining(m)
    target=target_of(m)
    bids={s:quote(m,s,'bid') for s in ('UP','DOWN')}
    asks={s:quote(m,s,'ask') for s in ('UP','DOWN')}
    closes=[c['close'] for c in candles]
    # Last 1/3-minute movements use *completed* candles, live quote supplements them.
    delta1=btc-closes[-1]
    delta3=btc-closes[-3]
    last_ranges=[c['high']-c['low'] for c in candles[-12:]]
    typical_range=sum(last_ranges)/len(last_ranges)
    trend='UP' if closes[-1]>closes[-6] and closes[-3]>closes[-9] else ('DOWN' if closes[-1]<closes[-6] and closes[-3]<closes[-9] else 'MIXED')
    distance=btc-target if target is not None else None
    favored='UP' if distance is not None and distance>0 else ('DOWN' if distance is not None and distance<0 else None)
    confidence='UNCERTAIN'
    decision='WAIT'
    reasons=[]
    if target is None:
        reasons.append('Kalshi target unavailable')
    elif distance is None or abs(distance)<MIN_CUSHION:
        reasons.append('BTC too close to target for an early directional call')
    else:
        # A heuristic stress test, not an estimated probability or historical win rate.
        cushion=abs(distance)
        time_factor=max(seconds/60,0.5)
        required_move_per_min=cushion/time_factor
        observed_move=max(typical_range,abs(delta3)/3,5)
        difficulty=required_move_per_min/observed_move
        momentum_favors=(favored=='UP' and delta3>0) or (favored=='DOWN' and delta3<0)
        trend_favors=(trend==favored)
        if difficulty>=1.5 and (momentum_favors or trend_favors):
            confidence='HIGH HEURISTIC'
        elif difficulty>=.7 and (momentum_favors or trend_favors):
            confidence='MODERATE HEURISTIC'
        elif difficulty>=1.0 and abs(delta3)<typical_range:
            confidence='MODERATE HEURISTIC'
        else:
            confidence='LOW HEURISTIC'
        reasons.append(f'{favored} ahead by ${cushion:.2f}; reversal needs ~${required_move_per_min:.1f}/min')
        reasons.append(f'Observed 1m range ~${typical_range:.1f}; recent 3m change ${delta3:+.1f}')
        if not trend_favors and trend!='MIXED':
            reasons.append('Pre-contract trend opposes favored side')
        if seconds<MIN_ENTRY_SECONDS:
            reasons.append('Too close to expiration for a new entry')
        elif confidence=='LOW HEURISTIC':
            reasons.append('Cushion/reversal evidence is not strong enough')
        else:
            ask=asks[favored]
            bid=bids[favored]
            if ask is None or bid is None or ask<bid:
                reasons.append('No reliable tradable bid/ask quote')
            elif ask>=MAX_ENTRY_ASK:
                reasons.append(f'{favored} ask {ask*100:.0f}¢: limited gross payout room')
            elif ask-bid>.08:
                reasons.append('Spread too wide for an entry alert')
            else:
                # This is not a probability-vs-price edge claim; price is a hard guardrail only.
                decision=f'ENTER {favored}'
                reasons.append(f'{favored} ask {ask*100:.1f}¢ (fees not included)')
    return {'ticker':m['ticker'],'at':now,'seconds':seconds,'btc':btc,'target':target,
            'distance':distance,'favored':favored,'trend':trend,'delta1':delta1,'delta3':delta3,
            'volatility':typical_range,'confidence':confidence,'decision':decision,
            'reason':'; '.join(reasons),'bids':bids,'asks':asks}

def record(a):
    with connect() as c:
        c.execute('''INSERT INTO settlement_observations
          (captured_at,ticker,seconds_left,btc_price,target,distance,yes_bid,yes_ask,no_bid,no_ask,
           trend,momentum_1m,momentum_3m,volatility_1m,favored_side,confidence,decision,reason)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
          (a['at'],a['ticker'],a['seconds'],a['btc'],a['target'],a['distance'],
           a['bids']['UP'],a['asks']['UP'],a['bids']['DOWN'],a['asks']['DOWN'],
           a['trend'],a['delta1'],a['delta3'],a['volatility'],a['favored'],
           a['confidence'],a['decision'],a['reason']))
        if a['decision'].startswith('ENTER '):
            side=a['decision'].split()[-1]
            c.execute('''INSERT OR IGNORE INTO settlement_calls
              (ticker,created_at,seconds_left,side,ask,btc_price,target,confidence,reason)
              VALUES (?,?,?,?,?,?,?,?,?)''',
              (a['ticker'],a['at'],a['seconds'],side,a['asks'][side],a['btc'],
               a['target'],a['confidence'],a['reason']))

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
    """Phone-preview-first settlement alert. All figures are reference estimates."""
    mm, ss = divmod(max(0, int(a['seconds'])), 60)
    decision = a['decision']
    icon = '🟢' if decision == 'ENTER UP' else '🔴' if decision == 'ENTER DOWN' else '🟡'
    distance = a.get('distance')
    if distance is None:
        position = 'Target unavailable'
    else:
        position = f'BTC ${abs(distance):,.0f} {"above" if distance >= 0 else "below"} target'
    trend = {'UP':'↑','DOWN':'↓'}.get(a.get('trend'), '↔')
    reason = a.get('reason') or ''
    if decision == 'WAIT':
        if distance is not None and abs(distance) < 25:
            reason = 'Too close to target'
        elif not reason:
            reason = 'No clear settlement edge'
    first = f'{icon} {decision} — {reason}' if decision == 'WAIT' else f'{icon} {decision}'
    return (f'{first}\n'
            f'⏱ {mm}:{ss:02d} | {position}\n'
            f'Trend {trend} {a.get("trend", "?")} | UP {ask_text(a["asks"]["UP"])} • DOWN {ask_text(a["asks"]["DOWN"])}\n'
            '⚠️ Kraken proxy • Not a guaranteed settlement')

def run_brti_test():
    """Read-only entitlement test. Does not alter market decisions or place orders."""
    import io
    from contextlib import redirect_stdout
    try:
        import test_brti
        output=io.StringIO()
        with redirect_stdout(output):
            result=test_brti.main()
        lines=output.getvalue().splitlines()
        status=next((x for x in lines if x.startswith('HTTP status:')), '')
        if result == 0 and 'ACCESS SUCCESS' in lines:
            msg='🟢 BRTI ACCESS SUCCESS\nKalshi index endpoint responded. KeKe still uses Kraken until V5.1 integration.'
        else:
            issue=next((x.split(':')[0] for x in lines if x.startswith(('SETUP ERROR:', 'KEY ERROR:', 'ACCESS FAILED:', 'REQUEST ERROR:'))), 'ACCESS NOT CONFIRMED')
            msg=f'🔴 BRTI TEST: {issue}\n{status}\nKeKe still uses Kraken.'
    except ModuleNotFoundError as exc:
        name=exc.name or 'unknown'
        msg=f'🟠 BRTI TEST BLOCKED: missing Python package {name}.\nKeKe remains online; no BRTI access confirmed.'
    except Exception as exc:
        msg=f'🔴 BRTI TEST ERROR: {type(exc).__name__}\nKeKe remains online; no BRTI access confirmed.'
        LOG.exception('BRTI connectivity test failed')
    LOG.info('BRTI test finished: %s', msg.replace('\n',' | '))
    send(msg)

def send_stats():
    with connect() as c:
        n=c.execute('SELECT COUNT(*) FROM settlement_observations').fetchone()[0]
        calls=c.execute('SELECT COUNT(*) FROM settlement_calls').fetchone()[0]
        contracts=c.execute('SELECT COUNT(DISTINCT ticker) FROM settlement_observations').fetchone()[0]
        verified=c.execute('SELECT COUNT(*) FROM settlement_results WHERE verified=1').fetchone()[0]
    send(f'📊 SETTLEMENT RESEARCH\nContracts observed: {contracts}\nSnapshots: {n}\nENTER calls: {calls}\nVerified outcomes: {verified}\n\nNo accuracy or win rate is claimed without verified outcomes.')

def send_last():
    with connect() as c:
        row=c.execute('SELECT * FROM settlement_calls ORDER BY id DESC LIMIT 1').fetchone()
    if row is None: send('🕒 No settlement ENTER calls recorded yet.');return
    send(f'🕒 LAST SETTLEMENT CALL\n{row["side"]} @ {ask_text(row["ask"])}\n'
         f'Contract: {row["ticker"]}\nConfidence: {row["confidence"]}\n'
         f'Outcome: {row["result"]} (not independently verified)')

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

def maybe_alert(a,new_contract):
    if not state['enabled']:return
    now=time.time()
    action=a['decision']
    if new_contract:
        send(format_read(a,'🆕 NEW BTC 15-MIN CONTRACT — EARLY READ'))
        state['last_alert']=action
        state['last_sent']=now
        return
    # Send meaningful changes promptly; don't spam repeated WAIT/ENTER calls.
    if action!=state['last_alert']:
        if action.startswith('ENTER ') or (state['last_alert'] or '').startswith('ENTER '):
            send(format_read(a,'🚨 SETTLEMENT THESIS UPDATE'))
            state['last_sent']=now
        elif now-state['last_sent']>=ALERT_COOLDOWN:
            send(format_read(a,'🔄 SETTLEMENT READ CHANGED'))
            state['last_sent']=now
        state['last_alert']=action

def main():
    init_db()
    initialize_telegram()
    LOG.info('KeKe V5.0 Settlement-Only starting; Stats database: %s',DB_PATH)
    run_brti_test()
    send('🧠 KeKe V5.0 SETTLEMENT-ONLY online. Scalp alerts removed. '
         'Using pre-contract candles for early analysis; recording to persistent /data database. '
         'No automatic trades. /status /stats /last')
    while True:
        started=time.monotonic()
        try:
            telegram_commands()
            market=get_market()
            if market:
                btc,candles=kraken_data()
                # Reject stale OHLC before making decisions.
                if time.time()-(candles[-1]['time']+60)>MAX_DATA_AGE:
                    raise ValueError('Stale completed Kraken candle; skipping decisions')
                a=assessment(market,btc,candles)
                record(a)
                new=state['ticker']!=a['ticker']
                if new:
                    state['ticker']=a['ticker']
                    state['last_alert']=None
                    state['entry_side']=None
                state['last_read']=a
                maybe_alert(a,new)
                LOG.info('%s %s BTC %.2f target %s',a['ticker'],a['decision'],btc,a['target'])
            else:LOG.info('No active BTC 15m contract')
        except Exception:LOG.exception('Monitor loop error; will retry')
        time.sleep(max(1,CHECK_EVERY-(time.monotonic()-started)))

if __name__=='__main__':main()
