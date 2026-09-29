#!/usr/bin/env python3
"""Offline Nexus 2.0 release self-test. Makes no network or Telegram calls."""
import asyncio
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

HERE=Path(__file__).resolve().parent
SCANNER=HERE/'scanner.py'
spec=importlib.util.spec_from_file_location('nexus2_selftest',SCANNER)
nx=importlib.util.module_from_spec(spec);sys.modules['nexus2_selftest']=nx;spec.loader.exec_module(nx)

def old_db_migration():
    fd,path=tempfile.mkstemp(suffix='.db');os.close(fd);os.unlink(path)
    c=sqlite3.connect(path)
    c.execute('CREATE TABLE alerts(id INTEGER PRIMARY KEY AUTOINCREMENT,symbol TEXT,category TEXT,direction TEXT,timeframe TEXT,score INTEGER,alert_price REAL,ts REAL,telegram_message_id INTEGER,features TEXT,archetype TEXT,calls TEXT,source TEXT,mfe_scalp REAL,mfe_medium REAL,mfe_sleep REAL,mfe_hold REAL)')
    c.execute('INSERT INTO alerts(symbol,category,direction,timeframe,score,alert_price,ts,features,archetype,calls,source,mfe_scalp,mfe_medium,mfe_sleep,mfe_hold) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',('XUSDT','dump','down','5m',60,1,time.time(),json.dumps({'impulse_raw':1}),'balanced','{}','live',.7,1.3,1.9,3.1))
    c.commit();c.close();nx.DB_PATH=path;nx.db_init()
    cols={r[1] for r in sqlite3.connect(path).execute('pragma table_info(alerts)')}
    assert {'delivered_ts','base_message','outcome_schema','entry_price_10s','feature_schema','episode_id'} <= cols
    assert nx.migrate_legacy_outcomes_batch()==4
    os.remove(path)

async def delivery_survives_db_failure():
    sent=[]
    async def fake_send(session,text):sent.append(text);return 101
    async def fake_context(*a,**k):return [],{'quote_vol_24h':1e7,'low_vol':0,'long_liq_usd':0,'short_liq_usd':0}
    async def fail(*a,**k):raise RuntimeError('expected db failure')
    nx.send_msg=fake_send;nx.build_context=fake_context;nx.persist_alert_retry=fail
    nx.check_breakout_buffered=lambda *a,**k:None;nx.get_pct_change=lambda *a,**k:0.0;nx.ENABLE_REVERSAL_ENGINE=False
    nx.DB_PATH='/tmp/nexus_selftest_missing/x.db'
    await nx.send_timeframe_alert(None,'XUSDT','down','5m',-3.3,1.0,1e7,'x',time.time())
    assert sent

async def immediate_short_hit():
    fd,path=tempfile.mkstemp(suffix='.db');os.close(fd);os.unlink(path);nx.DB_PATH=path;nx.db_init()
    calls={b:{'display':d,'display_call':'SHORT','actionable':True} for b,d,*_ in nx.BANDS}
    delivered=time.time()-20
    aid=nx.db_insert_alert_after_delivery('SUSDT','dump','down','5m',70,1.0,delivered,delivered,222,{},'balanced',calls,0,'BASE')
    nx.db_set_entry(aid,1.0);nx.latest_price['SUSDT']=0.992
    async def fake_edit(*a,**k):return True
    async def no_kl(*a,**k):return []
    nx.edit_msg=fake_edit;nx._fetch_klines_1m=no_kl
    t=asyncio.create_task(nx.track_alert(None,aid));await asyncio.sleep(.2);t.cancel()
    try:await t
    except BaseException:pass
    out=nx.db_get_outcomes(aid)
    assert out['now']['hit']==1 and out['now']['best_market_move_pct']<0
    os.remove(path)

async def main():
    old_db_migration();await delivery_survives_db_failure();await immediate_short_hit()
    print('NEXUS_SELFTEST_OK')

if __name__=='__main__':asyncio.run(main())
