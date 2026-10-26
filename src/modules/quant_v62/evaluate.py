"""Paper evaluation of frozen snapshots at their captured quotes.

JSONL: one {snapshot: {...}, result: {...}} per event. No threshold search.
Use --config to evaluate a separately versioned challenger on untouched dates.
"""
import argparse,json,random
from collections import Counter
from datetime import datetime
from pathlib import Path
from contracts import Snapshot,Config
from engine import analyze
from postmatch import attach
from audit_log import digest


def evaluate(records, config=None, bootstrap=2000, seed=62):
    cfg=Config.model_validate(config or {})
    events=[];ids=set();counts=Counter();cluster=[];covered=[]
    for rec in sorted(records,key=lambda r:datetime.fromisoformat(r['snapshot']['decision_at'].replace('Z','+00:00'))):
        s=Snapshot.model_validate(rec['snapshot'])
        if s.event_id in ids: raise ValueError('Evento duplicado en evaluación')
        ids.add(s.event_id)
        a=analyze(s,cfg);p=dict(phase='prematch',synthetic=s.synthetic,snapshot=s.model_dump(mode='json'),analysis=a,evidence_refs=a['evidence_refs'])
        post=attach(p,rec['result']);profits=[]
        for market,settlement in post['settlements'].items():
            counts[market+':'+settlement['status']]+=1
            if settlement['profit_per_unit'] is not None:
                profits.append(settlement['profit_per_unit']);covered.append(s.event_id+':'+market)
        if profits:cluster.append((sum(profits),len(profits)))
        events.append({'event_id':s.event_id,'decision_at':s.decision_at.isoformat(),
            'snapshot_sha256':a['input_sha256'],'b':a['b'],'g':a['g'],'policies':a['policy_trace'],
            'picks':{'ah':a['ah_pick'],'ou':a['ou_pick']},'settlements':post['settlements']})
    profit=sum(x[0] for x in cluster);units=sum(x[1] for x in cluster)
    interval=None
    if len(cluster)>=2 and bootstrap>0:
        rng=random.Random(seed);samples=[]
        for _ in range(bootstrap):
            draw=[rng.choice(cluster) for _ in cluster]
            samples.append(sum(x[0] for x in draw)/sum(x[1] for x in draw))
        samples.sort();interval=[samples[int(.025*(bootstrap-1))],samples[int(.975*(bootstrap-1))]]
    return {'model_version':cfg.version,'config_sha256':digest(cfg.model_dump()),'n_events':len(events),
            'counts':dict(counts),'quoted_picks':units,'quoted_pick_ids':covered,'paper_profit_units':profit,
            'paper_roi':profit/units if units else None,'event_bootstrap_95':interval,
            'bootstrap_draws':bootstrap,'seed':seed,'synthetic':any(r['snapshot'].get('synthetic',False) for r in records),
            'notes':['Rendimiento hipotético a cuotas capturadas, sin acreditar ejecución.',
                     'Remuestreo agrupa AH y O/U por partido; no corrige dependencia entre jornadas/equipos.',
                     'Intervalos descriptivos; comparar candidatos en mismos eventos y cuotas disponibles.'],
            'events':events}


def main():
    p=argparse.ArgumentParser();p.add_argument('--jsonl',required=True);p.add_argument('--out',required=True);p.add_argument('--config');a=p.parse_args()
    records=[json.loads(x) for x in Path(a.jsonl).read_text().splitlines() if x.strip()]
    cfg=json.loads(Path(a.config).read_text()) if a.config else None
    out=evaluate(records,cfg);Path(a.out).parent.mkdir(parents=True,exist_ok=True)
    Path(a.out).write_text(json.dumps(out,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in out.items() if k!='events'},ensure_ascii=False,indent=2))


if __name__=='__main__':main()
