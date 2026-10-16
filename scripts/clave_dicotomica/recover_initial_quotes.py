"""Recupera apertura AH/O/U y precios nativos HK desde Nowgoal, con límite de ritmo.

No modifica la base de la aplicación. Guarda un caché separado y reanudable.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from datetime import datetime, timezone
import json
from pathlib import Path
import random
import sqlite3
import sys
import threading
import time

import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT))
from modules import estudio_scraper as es
from scripts.clave_dicotomica.audit_forced_initial import date, number


def recover(database, output, target=2200, max_requests=3500, rate=4):
    output.parent.mkdir(parents=True,exist_ok=True)
    cached = json.loads(output.read_text(encoding='utf-8')) if output.exists() else {'quotes':{},'failures':{}}
    quoted = cached['quotes']; failures = cached.get('failures') or {}
    with sqlite3.connect('file:'+database.as_posix()+'?mode=ro',uri=True) as con:
        candidates = [(str(mid),date(day)) for mid,day in con.execute("SELECT match_id,match_date FROM matches WHERE state='historical' AND score IS NOT NULL")]
    candidates = [(mid,day) for mid,day in candidates if day and day.isoformat()<'2026-10-12' and mid not in quoted and mid not in failures]
    random.Random(20261012).shuffle(candidates)
    iterator = iter(candidates[:max_requests])
    local = threading.local(); limiter = threading.Lock(); next_request = [0.]
    class LimitedSession:
        def __init__(self):
            self.inner = requests.Session()
            self.inner.headers.update({'User-Agent':'Mozilla/5.0','Referer':es.BASE_URL_OF})
        def get(self,*args,**kwargs):
            with limiter:
                delay = max(0,next_request[0]-time.monotonic())
                if delay:time.sleep(delay)
                next_request[0] = time.monotonic()+1/rate
            kwargs['timeout'] = (5,12)
            return self.inner.get(*args,**kwargs)
    def session():
        if not hasattr(local,'session'):local.session=LimitedSession()
        return local.session
    es.get_requests_session_of = session
    def worker(mid,day):
        quote = es.fetch_odds_from_ajax(mid)
        if not quote:return mid,None
        ah = es.parse_ah_to_number_of(quote.get('ah_linea_raw'))
        ou = es.parse_ah_to_number_of(quote.get('goals_linea_raw'))
        keys = ('ah_home_cuota','ah_away_cuota','goals_over_cuota','goals_under_cuota')
        raw_prices = [number(quote.get(k)) for k in keys]
        if ah is None or ou is None or not 0 < ou <= 30 or abs(ah)>20 or not all(p is not None and 0<p<10 for p in raw_prices):
            return mid,None
        return mid,{'date':day.isoformat(), 'source_url':f'{es.BASE_URL_OF}/Ajax/SoccerAjax/?type=1&id={mid}',
            'captured_at':datetime.now(timezone.utc).isoformat(), 'main_match_odds':{
                'ah_linea':str(ah),'goals_linea':str(ou),'odds_stage':'initial','price_format':'hongkong',
                'bookmaker_id':quote.get('bookmaker_id'),**{k:quote[k] for k in keys}}}
    def save():
        payload = {'source':'Nowgoal SoccerAjax opening fields 3/4/5 and 11/12/13; native HK prices',
            'generated_at':datetime.now(timezone.utc).isoformat(),'target':target,'quotes':quoted,'failures':failures}
        temporary = output.with_suffix('.tmp')
        temporary.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding='utf-8')
        temporary.replace(output)
    attempts = 0
    print(f'[START] {len(quoted)} cached quotes; {len(candidates)} eligible matches',flush=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        pending = {}
        for _ in range(4):
            item=next(iterator,None)
            if item:pending[pool.submit(worker,*item)]=item[0]
        while pending and len(quoted)<target:
            done,_=wait(pending,return_when=FIRST_COMPLETED)
            for future in done:
                mid=pending.pop(future);attempts+=1
                try:
                    _, quote=future.result()
                    if quote:quoted[mid]=quote
                    else:failures[mid]='initial_prices_unavailable'
                except Exception as exc:failures[mid]=type(exc).__name__
                if attempts%50==0:
                    save();print(f'[PROGRESS] requests={attempts}, valid={len(quoted)}, unavailable={len(failures)}',flush=True)
                if len(quoted)<target:
                    item=next(iterator,None)
                    if item:pending[pool.submit(worker,*item)]=item[0]
        for future in pending:future.cancel()
    save()
    print(f'[DONE] {len(quoted)} verified opening quotes; {output}',flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--database',default=str(ROOT/'data/app_data.db'))
    parser.add_argument('--output',default=str(ROOT/'informes/2026-10-12_auditoria_metodo/initial_quotes.json'))
    parser.add_argument('--target',type=int,default=2200)
    parser.add_argument('--max-requests',type=int,default=3500)
    args=parser.parse_args()
    recover(Path(args.database).resolve(),Path(args.output).resolve(),args.target,args.max_requests)
