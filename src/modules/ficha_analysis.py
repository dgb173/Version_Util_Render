"""Analyse the selected ficha, without replacing its rows with a fixed window."""
from collections import Counter
from .universal_analysis import analyze, event, view, day, number, norm, counts, side


def distribution(rows, key, result_key, target):
    grouped = {}
    for r in rows:
        value = r.get(key)
        if value is not None:
            grouped.setdefault(value, []).append(r)
    known = sum(len(v) for v in grouped.values())
    return [{'line': k, 'n': len(v), 'share': round(100*len(v)/known, 1),
             'frequent': len(v) >= 3 and len(v)/known >= .25,
             'same_current': target is not None and abs(k-target) < 1e-8,
             'settlement': counts(r[result_key] for r in v),
             'wins': sum(r['margin']>0 for r in v), 'draws': sum(r['margin']==0 for r in v)}
            for k,v in sorted(grouped.items())]


def ficha_analysis(p):
    base = analyze(p)
    cutoff = day(base['date'])
    profiles = {}
    for role in ('home', 'away'):
        team = base[role]
        target = base['line'] if base['favorite_side']==role else -base['line'] if base['line'] is not None else None
        seen = set(); rows = []
        for raw in (p.get('ficha_rows') or {}).get(role, []):
            e = event(raw, cutoff, str(p.get('match_id') or ''))
            if not e: continue
            k=(e['date'],norm(e['home']),norm(e['away']))
            if k in seen: continue
            seen.add(k)
            v = view(e, team, target, base['ou'])
            if v: rows.append(v)
        rows.sort(key=lambda r:r['date'],reverse=True)
        joint = [r for r in rows if r['actual_ah'] is not None and r['over_actual'] is not None]
        profiles[role] = {'team':team,'rows':rows,'n':len(rows),
            'wins':sum(r['margin']>0 for r in rows),'draws':sum(r['margin']==0 for r in rows),
            'losses':sum(r['margin']<0 for r in rows),
            'ah':counts(r['actual_ah'] for r in rows), 'target_ah':counts(r['target_ah'] for r in rows),
            'over':counts(r['over_actual'] for r in rows),'target_over':counts(r['over_target'] for r in rows),
            'ah_lines':distribution(rows,'own_line','actual_ah',target),
            'ou_lines':distribution(rows,'ou','over_actual',base['ou']),
            'joint':{'n':len(joint),'cover_over':sum(r['actual_ah']>0 and r['over_actual']>0 for r in joint),
                     'cover_under':sum(r['actual_ah']>0 and r['over_actual']<0 for r in joint),
                     'fail_over':sum(r['actual_ah']<0 and r['over_actual']>0 for r in joint),
                     'fail_under':sum(r['actual_ah']<0 and r['over_actual']<0 for r in joint),
                     'pushes':sum(r['actual_ah']==0 or r['over_actual']==0 for r in joint)}}
    common=[]
    for h in profiles['home']['rows']:
        for a in profiles['away']['rows']:
            if norm(h['opponent']) != norm(a['opponent']): continue
            delta = h['residual']-a['residual'] if h['residual'] is not None and a['residual'] is not None else None
            common.append({'opponent':h['opponent'],'home':h,'away':a,'margin_delta':h['margin']-a['margin'],
                           'residual_delta':delta,'comparison':'MEJORA LOCAL' if delta is not None and delta>0 else 'MEJORA VISITANTE' if delta is not None and delta<0 else 'IGUALA' if delta==0 else 'AH NO DISPONIBLE'})
    evolution=[]
    for role, prof in profiles.items():
        for r in prof['rows']:
            ref=(p.get('row_references') or {}).get(r['id']) or {}
            mov=ref.get('movement') or {}
            used=set()
            for kind in ('general','stadium'):
                raw=mov.get(kind)
                e=event(raw,day(r['date']),r['id']) if raw else None
                if not e or {norm(e['home']),norm(e['away'])}!={norm(r['home']),norm(r['away'])}: continue
                k=(e['date'],norm(e['home']),norm(e['away']))
                if k in used: continue
                used.add(k)
                before=view(e,prof['team'],r['own_line'],r['ou'])
                if before is None: continue
                delta=r['residual']-before['residual'] if r['residual'] is not None and before['residual'] is not None else None
                evolution.append({'role':role,'row':r,'before':before,'kind':kind,
                    'margin_delta':r['margin']-before['margin'],'residual_delta':delta,
                    'line_change':r['own_line']-before['own_line'] if r['own_line'] is not None and before['own_line'] is not None else None,
                    'verdict':'MEJORA' if delta is not None and delta>0 else 'EMPEORA' if delta is not None and delta<0 else 'IGUALA' if delta==0 else 'SIN AH COMPARABLE'})
    # Row Col3: rebuild that historical fixture's pre-match links with its own cutoff.
    col3=[]
    for role,prof in profiles.items():
        for r in prof['rows']:
            ref=(p.get('row_references') or {}).get(r['id']) or {}
            data=ref.get('col3') or {}
            comp=data.get('comparison') or {}
            names=data.get('match') or {}
            anchored={norm(names.get('home_name')),norm(names.get('away_name'))}=={norm(base['home']),norm(base['away'])}
            packet={'match_id':p.get('match_id') if anchored else r['id'],'date':base['date'] if anchored else r['date'],
                    'home_name':base['home'] if anchored else r['home'],'away_name':base['away'] if anchored else r['away'],
                    'handicap':p.get('handicap') if anchored else r['ah'],'goal_line':base['ou'] if anchored else r['ou'],'h2h_col3':data.get('h2h_col3'),
                    'last_home_match':comp.get('last_home_match'),'last_away_match':comp.get('last_away_match')}
            c=analyze(packet)['col3'] if data else None
            col3.append({'role':role,'row':r,'comparison':c,'scope':'Anclada al partido actual' if anchored else 'Previa al antecedente','error':ref.get('error')})
    return {'base':base,'profiles':profiles,'common_rivals':common,'evolution':evolution,'row_col3':col3,
            'scope':p.get('ficha_scope') or [], 'rules':'Se analizan todas las filas seleccionadas de esta ficha. Línea frecuente: al menos 3 casos y 25% de las líneas conocidas de la muestra. Cruces por rival común son descriptivos, no una correlación estadística ni casos independientes.'}
