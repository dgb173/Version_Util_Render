"""python run_engine.py --input examples/dominio.json --out outputs/dominio.json"""
import argparse
import json
from pathlib import Path
from engine import analyze
from contracts import Snapshot, Config, Review
from audit_log import append_record, digest


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--input',required=True)
    p.add_argument('--out',required=True)
    p.add_argument('--config')
    p.add_argument('--ledger',help='SQLite append-only; reinsertar el mismo hash da error')
    a=p.parse_args()
    data=Snapshot.model_validate_json(Path(a.input).read_text())
    cfg=Config.model_validate_json(Path(a.config).read_text()) if a.config else Config()
    result=analyze(data,cfg)
    packet={'phase':'prematch','synthetic':data.synthetic,'snapshot':data.model_dump(mode='json'),
            'analysis':result,'evidence_refs':result['evidence_refs']}
    target=Path(a.out);target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(packet,ensure_ascii=False,indent=2))
    if a.ledger: append_record(a.ledger,packet)
    print(json.dumps({'sha256':digest(packet),'b':[x['bit'] for x in result['b']],
                      'g':[x['bit'] for x in result['g']], 'ah':result['ah_pick'],
                      'ou':result['ou_pick']},ensure_ascii=False,indent=2))


if __name__=='__main__': main()
