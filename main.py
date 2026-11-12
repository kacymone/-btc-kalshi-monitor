
def settlement_signal(a):
    if a['source']!='BRTI' or a['distance'] is None:
        return None
    seconds=max(30,a['seconds'])
    # Completed Kraken candle high-low range approximates 1m variability.
    # The floor prevents tiny measured ranges from creating false certainty.
    range1=max(12.0,float(a['volatility'] or 0))
    scale=max(18.0,range1*math.sqrt(seconds/60))
    # A small, capped directional drift component; target distance dominates.
    momentum=a.get('delta3') or 0
    drift=max(-0.35,min(0.35,0.18*momentum/scale))
    return a['distance']/scale + drift

def load_v56(ticker):
    with connect() as c:
        rows=c.execute("SELECT * FROM keke_v56_forecasts WHERE ticker=? ORDER BY id",(ticker,)).fetchall()
    side=rows[-1]['side'] if rows else None
    return {'side':side,'opposite':0}

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
        if a['seconds']>=90 and abs(signal)>=V56_INITIAL_SIGNAL:
            t['side']=candidate
            a['v56_side']=candidate
            record_v56(a,'INITIAL',candidate,reason='First qualifying settlement signal')
            if state['enabled']:send(format_read(a,f'{"🟢" if candidate=="UP" else "🔴"} INITIAL PICK: {candidate}'))
        return
    if candidate==t['side'] or abs(signal)<V56_REVERSAL_SIGNAL:
        t['opposite']=0
        return
    t['opposite']+=1
    if abs(signal)>=V56_FAST_REVERSAL_SIGNAL or t['opposite']>=V56_REVERSAL_CONFIRM:
        old=t['side'];t['side']=candidate;t['opposite']=0
        a['v56_side']=candidate
        record_v56(a,'REVERSAL',candidate,old,'Strong opposite settlement signal')
        if state['enabled']:send(format_read(a,f'🔄 PREDICTION REVERSED: {old} → {candidate}'))

def send_last():
    with connect() as c:
        row=c.execute("SELECT * FROM keke_v56_forecasts WHERE event IN ('INITIAL','REVERSAL') ORDER BY id DESC LIMIT 1").fetchone()
    if row is None:send('🕒 No confirmed thesis yet.');return
    send(f'🕒 LAST THESIS: {row["side"]} ({row["event"]})\n🎯 Target: {money_text(row["target"])}\n'
         f'⏱ At {row["seconds_left"]}s remaining • {ask_text(row["ask"])}\nResult: {row["grade"]}')

def main():
    init_db()
    initialize_telegram()
    LOG.info('KeKe V5.6 Settlement Forecast starting; Stats database: %s',DB_PATH)
    run_brti_test()
    send('🧠 KeKe V5.6 settlement forecast alerts online. '
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
                    state['ticker']=a['ticker']
                    state['last_alert']=None
                    state['entry_side']=None
                process_v56(a,new)
                state['last_read']=a
                LOG.info('%s %s BRTI %s Kraken %.2f target %s',a['ticker'],a['decision'],brti,btc,a['target'])
            else:LOG.info('No active BTC 15m contract')
        except Exception:LOG.exception('Monitor loop error; will retry')
        time.sleep(max(1,CHECK_EVERY-(time.monotonic()-started)))

if __name__=='__main__':main()
