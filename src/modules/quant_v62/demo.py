"""Reproducible invented examples. No historical result is claimed as verified."""
from datetime import datetime,timedelta,timezone
from pathlib import Path
import copy,json
from contracts import Snapshot,Config,Review
from engine import analyze
from review_gemini import make_request


def fixture(kind='dominio'):
    now=datetime(2026,10,26,12,tzinfo=timezone.utc)
    def stamp(days): return (now-timedelta(days=days)).isoformat()
    history=[]
    scores_f=[(2,1),(3,1),(2,2),(2,1),(3,1),(1,0),(2,1)]
    scores_d=[(1,2),(1,3),(2,2),(1,2),(1,1),(1,2),(1,1)]
    for team,venue,scores in [('A','home',scores_f),('B','away',scores_d)]:
        for i,days in enumerate([2,5,8,11,14,40,50]):
            f=team=='A';gf,ga=scores[i]
            history.append(dict(match_id=f'{team}-{i}',team_id=team,opponent_id=f'R{i}',competition_id='DEMO',
                venue=venue,kickoff=stamp(days),available_at=stamp(days-.2),source_ref=f'DEMO:{team}-{i}',
                raw_home_ah=1.25 if f else .5,ou_line=2.5,gf=gf,ga=ga,shot_goals_for=gf,shot_goals_against=ga,
                shots_for=16 if f else 11,shots_against=9 if f else 15,
                sot_for=(7 if i<5 else 6) if f else (4 if i<5 else 3),sot_against=3 if f else 6,
                ap_for=65 if f else 45,ap_against=40 if f else 65,attacks_for=100 if f else 80,
                attacks_against=75 if f else 100,corners_for=6 if f else 3,corners_against=3 if f else 6))
    history[5].update(opponent_id='B',raw_home_ah=.5,raw_home_ah_close=.75)
    def similar(prefix,side):
        return dict(n=4,cover_rate=.75 if side=='home' else .25,losses=1 if side=='home' else 3,
                    gf=2.25 if side=='home' else 1.25,ga=1.25 if side=='home' else 2.0,
                    sample_ids=[f'{prefix}-{side}-{i}' for i in range(4)])
    blocks=[dict(home=similar(p,'home'),away=similar(p,'away'),correlation='home',available_at=stamp(.1),source_ref='DEMO:'+p) for p in ('CURRENT','HIST')]
    snap=dict(event_id='SYNTHETIC-'+kind,home_id='A',away_id='B',competition_id='DEMO',decision_at=now.isoformat(),
        kickoff=(now+timedelta(hours=6)).isoformat(),raw_home_ah=.75,ou_line=2.75,rank_home=3,rank_away=8,
        record_home=dict(wins=8,draws=2,losses=3),record_away=dict(wins=3,draws=3,losses=7),
        synthetic=True,provider='SYNTHETIC',provider_definitions='90 min; tiros totales incluyen bloqueados; goles de tiro excluyen autogoles',
        history=history,similar_current=blocks[0],similar_h2h=blocks[1],
        references=[dict(match_id=f'REF{i}',total_goals=t,kickoff=stamp(i+3),available_at=stamp(i+2.8),source_ref=f'DEMO:REF{i}') for i,t in enumerate([3,3,4,2,4,3,3])],
        league_sot_per_team=4.5,league_conversion=.30,baseline_available_at=stamp(1),baseline_source_ref='DEMO:LEAGUE',
        quotes=[dict(market='AH_HOME',line_q=-2,odds=1.72,available_at=stamp(.01),bookmaker='FICTICIO',source_ref='DEMO:QUOTE_AH'),
                dict(market='OVER',line_q=10,odds=1.83,available_at=stamp(.01),bookmaker='FICTICIO',source_ref='DEMO:QUOTE_OU')])
    if kind=='cerrojo':
        snap.update(ou_line=3,rank_home=6,rank_away=4)
        for r in snap['history']:
            f=r['team_id']=='A'
            r.update(gf=1 if f else 0,ga=0 if f else 1,shot_goals_for=1 if f else 0,shot_goals_against=0 if f else 1,
                     shots_for=11 if f else 6,sot_for=6 if f else 2,sot_against=1 if f else 4,ap_for=55 if f else 25,ou_line=3)
        for b in (snap['similar_current'],snap['similar_h2h']):
            for side in ('home','away'): b[side].update(gf=1.0,ga=.8)
        for r in snap['references']:r['total_goals']=1
        snap['quotes']=[dict(market='UNDER',line_q=12,odds=1.81,available_at=stamp(.01),bookmaker='FICTICIO',source_ref='DEMO:QUOTE_UNDER')]
    elif kind=='datos_parciales':
        snap.update(similar_current=None,similar_h2h=None,references=[],quotes=[])
        for r in snap['history']:r.update(ap_for=None,ap_against=None)
    elif kind=='linea_larga':
        snap.update(raw_home_ah=1.5,quotes=[])
        for r in snap['history']:
            r['raw_home_ah']=1.5
        for r in snap['history'][:2]: r.update(gf=2,ga=2,shot_goals_for=2,shot_goals_against=2)
    return snap


def main():
    Path('outputs').mkdir(exist_ok=True);Path('examples').mkdir(exist_ok=True)
    summaries=[]
    for kind in ('dominio','cerrojo','datos_parciales','linea_larga'):
        s=Snapshot.model_validate(fixture(kind));out=analyze(s)
        Path(f'examples/{kind}.json').write_text(s.model_dump_json(indent=2))
        packet={'phase':'prematch','synthetic':True,'snapshot':s.model_dump(mode='json'),'analysis':out,'evidence_refs':out['evidence_refs']}
        Path(f'outputs/{kind}.json').write_text(json.dumps(packet,ensure_ascii=False,indent=2))
        summaries.append({'case':kind,'b':[g['bit'] for g in out['b']],'b_available':[int(g['available']) for g in out['b']],
            'g':[g['bit'] for g in out['g']],'g_available':[int(g['available']) for g in out['g']],
            'ah':out['ah_pick'],'ou':out['ou_pick'],'policies':out['policy_trace']})
        if kind=='dominio': Path('outputs/gemini_request_dry_run.json').write_text(json.dumps(make_request(packet),ensure_ascii=False,indent=2))
    for name,cls in [('snapshot',Snapshot),('config',Config),('review',Review)]:
        Path(f'examples/{name}.schema.json').write_text(json.dumps(cls.model_json_schema(),ensure_ascii=False,indent=2))
    Path('examples/config.json').write_text(Config().model_dump_json(indent=2))
    Path('examples/result_demo.json').write_text(json.dumps(dict(event_id='SYNTHETIC-dominio',home_goals=2,away_goals=1,available_at='2026-10-26T20:10:00Z',source_ref='DEMO:RESULT'),indent=2))
    Path('outputs/demo_summary.json').write_text(json.dumps(summaries,ensure_ascii=False,indent=2))
    print(json.dumps(summaries,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
