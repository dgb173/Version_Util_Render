"""Append feedback to a frozen prematch packet; never recompute old picks."""
import argparse,json
from pathlib import Path
from datetime import datetime
from pydantic import Field, model_validator
from contracts import Strict
from audit_log import digest,append_record
from market import settle


class Result(Strict):
    event_id: str
    home_goals: int = Field(ge=0,strict=True)
    away_goals: int = Field(ge=0,strict=True)
    period: str = 'FT_90'
    status: str = 'confirmed'
    available_at: datetime
    source_ref: str

    @model_validator(mode='after')
    def valid(self):
        if self.period!='FT_90' or self.status!='confirmed':
            raise ValueError('Solo FT90 confirmado; void/abandono requiere reglas del operador')
        if self.available_at.tzinfo is None: raise ValueError('Falta zona horaria')
        return self


def attach(packet,result):
    r=result if isinstance(result,Result) else Result.model_validate(result)
    s=packet['snapshot']
    if packet['phase']!='prematch' or r.event_id!=s['event_id']:
        raise ValueError('Paquete/evento no válido')
    if r.available_at <= datetime.fromisoformat(s['kickoff']):
        raise ValueError('Resultado disponible antes del partido')
    payments={}
    for name in ('ah_pick','ou_pick'):
        pick=packet['analysis'][name]
        # Grade unquoted recommendations, but never fabricate monetary profit.
        q=pick['quote']
        payment=settle(pick['market'],pick['line_q'],r.home_goals,r.away_goals,q['odds'] if q else 2)
        if not q: payment['profit_per_unit']=None
        payments[name]=payment
    return {'phase':'postmatch','synthetic':packet.get('synthetic',False),
            'prematch_sha256':digest(packet),'prematch_packet':packet,
            'result':r.model_dump(mode='json'),'settlements':payments,
            'evidence_refs':sorted(set(packet['evidence_refs']+[r.source_ref]))}


def main():
    p=argparse.ArgumentParser();p.add_argument('--packet',required=True);p.add_argument('--result',required=True)
    p.add_argument('--out',required=True);p.add_argument('--ledger');a=p.parse_args()
    out=attach(json.loads(Path(a.packet).read_text()),json.loads(Path(a.result).read_text()))
    Path(a.out).parent.mkdir(parents=True,exist_ok=True)
    Path(a.out).write_text(json.dumps(out,ensure_ascii=False,indent=2))
    if a.ledger: append_record(a.ledger,out)
    print(json.dumps(out['settlements'],indent=2))


if __name__=='__main__': main()
