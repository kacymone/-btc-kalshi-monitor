"""KeKe AI Lab: independent, read-only live analyst and approval-gated research.
Requires requests. Never writes prediction settings, executes AI output, or places orders.
"""
import os, json, time, sqlite3, threading, queue, logging, math
import requests

LOG=logging.getLogger('keke.ai')
API='https://api.openai.com/v1/responses'
MODEL=os.getenv('KEKE_AI_MODEL','gpt-4.1-mini')
INTERVAL=max(30,int(os.getenv('KEKE_AI_INTERVAL','60')))
RESEARCH_INTERVAL=max(3600,int(os.getenv('KEKE_AI_RESEARCH_INTERVAL','3600')))
_tasks=queue.Queue(maxsize=2)
_started=False
_last_enqueued={}
_db=None
_send=None

def _conn():
    c=sqlite3.connect(_db,timeout=10)
    c.row_factory=sqlite3.Row
    return c

def _setup():
    with _conn() as c:
        c.execute('''CREATE TABLE IF NOT EXISTS keke_ai_assessments (
            id INTEGER PRIMARY KEY, ticker TEXT, observed_at INTEGER, seconds_left INTEGER,
            official TEXT, shadow TEXT, ai_side TEXT, ai_reason TEXT,
            ai_risk TEXT, model TEXT, snapshot_json TEXT, ai_json TEXT,
            result TEXT DEFAULT 'PENDING')''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_ai_assess_ticker ON keke_ai_assessments(ticker,observed_at)')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_ai_research (
            id INTEGER PRIMARY KEY, created_at INTEGER, sample_size INTEGER,
            train_size INTEGER, test_size INTEGER, report_json TEXT,
            status TEXT DEFAULT 'PROPOSAL_ONLY')''')
        c.execute('''CREATE TABLE IF NOT EXISTS keke_ai_preferences (key TEXT PRIMARY KEY, value TEXT)''')

def _get(key,default='0'):
    with _conn() as c:
        row=c.execute('SELECT value FROM keke_ai_preferences WHERE key=?',(key,)).fetchone()
    return row['value'] if row else default

def _set(key,value):
    with _conn() as c:c.execute('INSERT OR REPLACE INTO keke_ai_preferences VALUES (?,?)',(key,str(value)))

def _call(system,data,max_tokens=380):
    key=os.getenv('OPENAI_API_KEY','').strip()
    if not key:
        raise RuntimeError('OPENAI_API_KEY is not configured')
    # Responses API uses input + text.format, not Chat Completions messages/response_format.
    # Do not pass temperature: not all Responses models accept it.
    payload={
        'model':MODEL,
        'instructions':system + ' Respond with a single valid JSON object only.',
        'input':[{'role':'user','content':[{'type':'input_text','text':json.dumps(data,separators=(',',':'),allow_nan=False)}]}],
        'text':{'format':{'type':'json_object'}},
        'max_output_tokens':max(600,max_tokens),
        'store':False
    }
    response=requests.post(API,headers={
        'Authorization':'Bearer '+key,
        'Content-Type':'application/json'
    },json=payload,timeout=35)
    if response.status_code >= 400:
        # Do not log raw API messages, request bodies, or credentials.
        try:
            err=response.json().get('error') or {}
            LOG.error('OpenAI Responses HTTP %s type=%s code=%s param=%s',
                response.status_code,
                str(err.get('type','unknown'))[:80],
                str(err.get('code','unknown'))[:80],
                str(err.get('param','none'))[:80])
            message=str(err.get('message',''))
            # Error text is needed to identify input validation failures.
            # Redact the exact key and recognizable API-key tokens before logging.
            import re
            message=message.replace(key,'[REDACTED]')
            message=re.sub(r'sk-[A-Za-z0-9_-]{8,}', '[REDACTED]', message)
            LOG.error('OpenAI validation detail: %s',message[:500])
        except (ValueError,AttributeError,TypeError):
            LOG.error('OpenAI Responses HTTP %s (non-JSON response)',response.status_code)
    response.raise_for_status()
    body=response.json()
    if body.get('status') not in (None,'completed'):
        raise RuntimeError('OpenAI response incomplete: '+str(body.get('status'))[:60])
    # In the raw Responses REST API, output is a list of message/reasoning items.
    # Reasoning items are not text. Only parse output_text parts from assistant messages.
    parts=[]
    for item in body.get('output',[]):
        if item.get('type')!='message' or item.get('role')!='assistant':
            continue
        for content in item.get('content',[]):
            if content.get('type')=='output_text' and isinstance(content.get('text'),str):
                parts.append(content['text'])
    if not parts:
        raise RuntimeError('OpenAI response had no assistant output_text')
    result=json.loads(''.join(parts))
    if not isinstance(result,dict):
        raise ValueError('OpenAI returned JSON that is not an object')
    return result

def _analyst(snapshot):
    prompt=('You are an independent research analyst for a BTC 15-minute settlement forecast. '
      'Only use the supplied BRTI snapshot, never invent candles, probabilities, or settlement. '
      'Official KeKe and shadow calls are hypotheses, not authority. '
      'Assess the probability DIRECTION qualitatively, not numerical confidence. '
      'Respect current target distance, time remaining, volatility, market regime, wick rejections and false breaks. '
      'If evidence is weak or missing use WAIT. No trading instructions, position or exit advice. '
      'Return JSON keys side (UP/DOWN/WAIT), risk (LOW/MEDIUM/HIGH/UNKNOWN), reason (max 240 chars), '
      'disagreement (max 140 chars), invalidation (max 140 chars).')
    result=_call(prompt,snapshot)
    side=str(result.get('side','WAIT')).upper()
    if side not in ('UP','DOWN','WAIT'):side='WAIT'
    risk=str(result.get('risk','UNKNOWN')).upper()
    if risk not in ('LOW','MEDIUM','HIGH','UNKNOWN'):risk='UNKNOWN'
    result['side']=side;result['risk']=risk
    for k in ('reason','disagreement','invalidation'):result[k]=str(result.get(k,''))[:260]
    return result

def _store_analyst(snapshot,result):
    with _conn() as c:
        c.execute('''INSERT INTO keke_ai_assessments
        (ticker,observed_at,seconds_left,official,shadow,ai_side,ai_reason,ai_risk,model,snapshot_json,ai_json)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)''',(
        snapshot['ticker'],snapshot['observed_at'],snapshot['seconds_left'],snapshot.get('official'),
        snapshot.get('shadow'),result['side'],result['reason'],result['risk'],MODEL,
        json.dumps(snapshot),json.dumps(result)))

def _message(snapshot,result):
    return (f"🧠 KEKE AI • INDEPENDENT RESEARCH\n"
      f"Contract: {snapshot['ticker']}\n"
      f"Official: {snapshot.get('official') or 'WAIT'} | Shadow: {snapshot.get('shadow') or 'WAIT'} | AI: {result['side']}\n"
      f"Time left: {snapshot['seconds_left']//60}:{snapshot['seconds_left']%60:02d} | Risk: {result['risk']}\n"
      f"Why: {result['reason']}\n"
      f"Disagreement: {result['disagreement']}\n"
      f"Watch for: {result['invalidation']}\n"
      '🧪 Research only • no trade or official forecast change')

def _research():
    # Use exactly one representative AI snapshot per ticker to avoid pseudo-replication.
    # Grade only official settlement_results. No leaked future candle inputs.
    with _conn() as c:
        rows=c.execute('''SELECT a.* ,r.settled_side AS outcome FROM keke_ai_assessments a
           JOIN settlement_results r ON r.ticker=a.ticker AND r.verified=1
           WHERE a.id=(SELECT MIN(b.id) FROM keke_ai_assessments b WHERE b.ticker=a.ticker)
           ORDER BY a.observed_at''').fetchall()
    samples=[]
    for row in rows:
        outcome=str(row['outcome'] or '').upper()
        if outcome not in ('UP','DOWN'):continue
        snap=json.loads(row['snapshot_json'])
        if snap.get('shadow_score') is None:continue
        samples.append((row,snap,outcome))
    n=len(samples)
    if n<30:return {'status':'INSUFFICIENT','contracts':n,'required':30}
    split=max(1,int(n*.7))
    train=samples[:split];test=samples[split:]
    if len(test)<9:return {'status':'INSUFFICIENT_TEST','contracts':n}
    # A narrow, predeclared parameter family, tested chronologically; not an LLM-invented score.
    def pred(snap,offset,threshold):
        score=float(snap['shadow_score'])+offset
        return 'WAIT' if abs(score)<threshold else ('UP' if score>0 else 'DOWN')
    def metrics(rows,offset,threshold):
        calls=[(pred(s,offset,threshold),outcome) for _,s,outcome in rows]
        issued=[(p,y) for p,y in calls if p!='WAIT']
        return {'n':len(calls),'coverage':round(len(issued)/len(calls),4),
                'accuracy':round(sum(p==y for p,y in issued)/len(issued),4) if issued else None,
                'wins':sum(p==y for p,y in issued),'calls':len(issued)}
    baseline=metrics(test,0,.45)
    candidates=[]
    for offset in (-.20,-.10,0,.10,.20):
        for threshold in (.35,.45,.55,.65):
            m=metrics(train,offset,threshold)
            if m['coverage']>=.60 and m['calls']>=12:
                candidates.append((m['accuracy'],m['coverage'],offset,threshold))
    if not candidates:return {'status':'NO_TRAIN_CANDIDATE','contracts':n}
    # Choose on train only, then evaluate once on held-out chronological test.
    candidates.sort(key=lambda v:(v[0],v[1],-abs(v[2])),reverse=True)
    _,_,offset,threshold=candidates[0]
    trial=metrics(test,offset,threshold)
    ai_calls=[(row['ai_side'],outcome) for row,_,outcome in test if row['ai_side'] in ('UP','DOWN')]
    ai_metrics={'calls':len(ai_calls),'accuracy':round(sum(p==y for p,y in ai_calls)/len(ai_calls),4) if ai_calls else None}
    report={'status':'TESTED_NOT_APPROVED','contracts':n,'train':len(train),'test':len(test),
      'baseline_shadow_test':baseline,'candidate_shadow_test':trial,'ai_test':ai_metrics,
      'candidate':{'score_offset':offset,'call_threshold':threshold},
      'warning':'Exploratory chronological holdout; repeated hourly searches can overfit. Requires a new untouched forward-validation period and explicit owner approval.'}
    # AI developer writes a review/patch plan only. Never execute generated text or edit live code.
    try:
        review=_call('You are a cautious quantitative software reviewer. Return JSON keys diagnosis, proposed_patch_plan, caveats. '
          'Use ONLY the metrics given. Explain if improvement is unproven; do not claim significance. '
          'Do not propose deployment or trading. Keep each field under 300 chars.',report,420)
        report['ai_review']=review
    except Exception as e:report['ai_review_error']=type(e).__name__
    with _conn() as c:
        c.execute('''INSERT INTO keke_ai_research(created_at,sample_size,train_size,test_size,report_json)
        VALUES (?,?,?,?,?)''',(int(time.time()),n,len(train),len(test),json.dumps(report)))
    return report

def _worker():
    while True:
        job,payload=_tasks.get()
        try:
            if job=='analyst':
                result=_analyst(payload)
                _store_analyst(payload,result)
                if _get('live_enabled')=='1':_send(_message(payload,result))
            elif job=='research':
                result=_research()
                if result.get('status')=='TESTED_NOT_APPROVED' and _get('research_notifications')=='1':
                    _send('🧪 KeKe AI Research: new approval-gated test report available. /improvements to review. No changes deployed.')
        except Exception as e:
            LOG.exception('AI %s failed: %s',job,type(e).__name__)
            if job=='analyst' and _get('live_enabled')=='1':
                _send('⚠️ AI analyst unavailable; KeKe official predictions remain unchanged. Check Railway logs/API key.')
        finally:_tasks.task_done()

def start(db_path,send_fn):
    global _started,_db,_send
    if _started:return
    _db=db_path;_send=send_fn;_setup()
    threading.Thread(target=_worker,name='keke-ai-lab',daemon=True).start()
    _started=True

def queue_live(a,shadow):
    if not _started or _get('live_enabled')!='1':return
    now=int(time.time());ticker=a.get('ticker')
    if not ticker or now-_last_enqueued.get(ticker,0)<INTERVAL:return
    if not shadow:return
    snap={'ticker':ticker,'observed_at':now,'seconds_left':int(a['seconds']),
      'price':round(float(a['btc']),2),'target':round(float(a['target']),2),
      'distance':round(float(a['distance']),2),'volatility':a.get('volatility'),
      'momentum_3m':a.get('delta3'),'official':a.get('v56_side') or a.get('favored'),
      'shadow':shadow['side'],'shadow_score':shadow['score'],
      'regime':shadow['regime'],'support':shadow['support'],'resistance':shadow['resistance'],
      'target_position':round(shadow['position'],3),'target_crossings_10':shadow['crossings'],
      'closes_above_10':shadow['above'],'upper_wick_ratio':round(shadow['upper'],3),
      'lower_wick_ratio':round(shadow['lower'],3),'upper_rejections_6':shadow['upper_count'],
      'lower_rejections_6':shadow['lower_count']}
    try:_tasks.put_nowait(('analyst',snap));_last_enqueued[ticker]=now
    except queue.Full:pass

def schedule_research():
    if not _started or _get('research_enabled')!='1':return
    now=int(time.time())
    if now-int(_get('last_research_enqueue','0'))<RESEARCH_INTERVAL:return
    try:_tasks.put_nowait(('research',None));_set('last_research_enqueue',now)
    except queue.Full:pass

def command(cmd):
    if cmd=='/aion':
        if not os.getenv('OPENAI_API_KEY'):
            return '⚠️ Add OPENAI_API_KEY in Railway first. AI is off; official KeKe unaffected.'
        _set('live_enabled','1');return '🧠 AI live analyst ON • independent assessments about every 60 seconds when BRTI candles are available. Official calls unchanged.'
    if cmd=='/aioff':_set('live_enabled','0');return '🧠 AI live analyst OFF. Official KeKe unchanged.'
    if cmd=='/researchon':
        if not os.getenv('OPENAI_API_KEY'):return '⚠️ Add OPENAI_API_KEY in Railway first.'
        _set('research_enabled','1');_set('research_notifications','1');return '🧪 AI research ON • hourly checks, minimum 30 graded contracts. No automatic changes.'
    if cmd=='/researchoff':_set('research_enabled','0');return '🧪 Automatic research checks OFF.'
    if cmd=='/ai':
        with _conn() as c:r=c.execute('SELECT * FROM keke_ai_assessments ORDER BY id DESC LIMIT 1').fetchone()
        if not r:return '🧠 No AI assessments yet. /aion enables live analysis.'
        snap=json.loads(r['snapshot_json']);result=json.loads(r['ai_json'])
        return _message(snap,result)+'\nRecorded '+time.strftime('%Y-%m-%d %H:%M UTC',time.gmtime(r['observed_at']))
    if cmd in ('/improvements','/compare'):
        with _conn() as c:r=c.execute('SELECT * FROM keke_ai_research ORDER BY id DESC LIMIT 1').fetchone()
        if not r:return '🧪 No tested research proposals yet. /researchon enables hourly evaluation after at least 30 verified settled contracts with AI snapshots.'
        d=json.loads(r['report_json']);b=d['baseline_shadow_test'];t=d['candidate_shadow_test']
        return (f"🧪 AI DEVELOPER • PROPOSAL #{r['id']} (NOT DEPLOYED)\n"
          f"Graded contracts: {d['contracts']} | chronological test: {d['test']}\n"
          f"Baseline: {b['wins']}/{b['calls']} correct; coverage {b['coverage']:.0%}\n"
          f"Candidate: {t['wins']}/{t['calls']} correct; coverage {t['coverage']:.0%}\n"
          f"Proposed score offset {d['candidate']['score_offset']:+.2f}, threshold {d['candidate']['call_threshold']:.2f}\n"
          f"AI assessment test accuracy: {d['ai_test']['accuracy']} over {d['ai_test']['calls']} calls\n"
          f"Review: {d.get('ai_review',{}).get('diagnosis','Unavailable')}\n"
          '🔒 Owner approval required; no production modifications. Exploratory results, not validated edge.')
    if cmd=='/auditai':
        if not os.getenv('OPENAI_API_KEY'):return '⚠️ OPENAI_API_KEY missing.'
        try:_tasks.put_nowait(('research',None));return '🧪 Research test queued. Check /improvements after processing.'
        except queue.Full:return '⏳ AI worker busy; retry shortly.'
    return None
