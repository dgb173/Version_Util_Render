"""Twelve binary gates, explicit policies, and a shot-based tie-breaker.

No Poisson model, no fitted probabilities, no automatic betting.
Thresholds are operational hypotheses from the user's specification.
"""
from statistics import mean
try:
    from .contracts import Snapshot, Config
    from .market import quarters, strength_q, settle
    from .audit_log import digest
except ImportError:
    from contracts import Snapshot, Config
    from market import quarters, strength_q, settle
    from audit_log import digest



def ratio(a, b):
    return a / b if b else None


def profile(rows, cfg, p0):
    fields = ('gf','ga','shots_for','shots_against','sot_for','sot_against',
              'attacks_for','attacks_against','ap_for','ap_against','corners_for','corners_against')
    out = {'n':len(rows), 'ids':[r.match_id for r in rows], 'counts':{}}
    for key in fields:
        vals = [getattr(r,key) for r in rows if getattr(r,key) is not None]
        out[key] = mean(vals) if vals else None
        out['counts'][key] = len(vals)
    out['clean_sheets'] = sum(r.ga == 0 for r in rows)
    out['draws'] = sum(r.gf == r.ga for r in rows)
    out['btts'] = ratio(sum(r.gf > 0 and r.ga > 0 for r in rows), len(rows))
    out['scoring_rate'] = ratio(sum(r.gf > 0 for r in rows), len(rows))
    for side in ('for','against'):
        valid = [r for r in rows if getattr(r,'sot_'+side) is not None and getattr(r,'shot_goals_'+side) is not None]
        sot = sum(getattr(r,'sot_'+side) for r in valid)
        goals = sum(getattr(r,'shot_goals_'+side) for r in valid)
        out['conversion_'+side] = ratio(goals, sot)
        out['conversion_'+side+'_counts'] = {'matches':len(valid),'sot':sot,'goals':goals}
        out['conversion_'+side+'_shrunk'] = ((goals+cfg.conversion_prior_shots*p0)/(sot+cfg.conversion_prior_shots)) if valid and sot else None
    matched = [r for r in rows if r.sot_for is not None and r.sot_against is not None]
    out['sot_share'] = ratio(sum(r.sot_for for r in matched), sum(r.sot_for+r.sot_against for r in matched))
    out['net_sot'] = mean([r.sot_for-r.sot_against for r in matched]) if matched else None
    matched = [r for r in rows if r.sot_for is not None and r.shots_for is not None]
    out['accuracy'] = ratio(sum(r.sot_for for r in matched),sum(r.shots_for for r in matched))
    return out


def usable(p, *fields, n=3):
    return p['n'] >= n and all(p.get(k) is not None and p['counts'].get(k, p['n']) >= n for k in fields)


def select_history(s, team, cfg):
    venue = 'home' if team == s.home_id else 'away'
    current_strength = strength_q(s.raw_home_ah, venue)
    rows, excluded = [], []
    for r in sorted((r for r in s.history if r.team_id == team), key=lambda r:r.kickoff, reverse=True):
        age = (s.decision_at-r.kickoff).total_seconds()/86400
        reason = None
        if age > cfg.history_days: reason = 'outside_history_window'
        elif r.competition_id != s.competition_id: reason = 'competition'
        elif s.senior_match and r.opponent_squad != 'senior': reason = 'reserve_or_u20'
        elif cfg.exclude_red_cards and r.red_card_context: reason = 'red_card_context'
        elif r.raw_home_ah is None: reason = 'missing_role_line'
        elif abs(strength_q(r.raw_home_ah,r.venue)-current_strength) > cfg.role_tolerance_q: reason = 'role_shift'
        if reason: excluded.append({'match_id':r.match_id,'reason':reason})
        else: rows.append(r)
    active = [r for r in rows if (s.decision_at-r.kickoff).total_seconds()/86400 <= cfg.active_days][:cfg.active_n]
    ids = {r.match_id for r in active}
    prior = [r for r in rows if r.match_id not in ids]
    return rows, active, prior, excluded


def blend(active, prior, cfg):
    keys = ('sot_for','sot_against','conversion_for_shrunk','conversion_against_shrunk')
    values, weights = {}, {}
    for k in keys:
        a,b = active.get(k),prior.get(k)
        if a is not None and b is not None:
            values[k] = cfg.active_weight*a + (1-cfg.active_weight)*b
            weights[k] = cfg.active_weight
        elif a is not None: values[k],weights[k] = a,1.0
        elif b is not None: values[k],weights[k] = b,0.0
        else: values[k],weights[k] = None,None
    return {'values':values,'actual_active_weights':weights}


def shot_projection(f, d, baseline):
    out = {}
    for label,a,b in [('favorite',f,d),('underdog',d,f)]:
        vals = [a['sot_for'],b['sot_against'],a['conversion_for_shrunk'],b['conversion_against_shrunk']]
        if any(x is None for x in vals): return None
        shots = a['sot_for'] * b['sot_against'] / baseline
        conv = (a['conversion_for_shrunk'] + b['conversion_against_shrunk']) / 2
        out[label] = {'sot':shots, 'conversion':conv, 'goals_proxy':shots*conv}
    out['total_proxy'] = out['favorite']['goals_proxy'] + out['underdog']['goals_proxy']
    out['margin_proxy'] = out['favorite']['goals_proxy'] - out['underdog']['goals_proxy']
    out['kind'] = 'rule_of_three_proxy_not_probability_or_xG'
    return out


def gate(name, condition, available, reason, values):
    return {'id':name,'bit':int(bool(condition)) if available else 0,
            'available':bool(available),'reason':reason,'values':values}


def score(gates):
    valid = sum(g['available'] for g in gates)
    votes = sum(g['bit'] for g in gates if g['available'])
    return {'votes':votes,'valid':valid,'of_six':6*votes/valid if valid else None,
            'coverage':valid/6,'is_probability':False}


def analyze(data, config=None):
    s = data if isinstance(data,Snapshot) else Snapshot.model_validate(data)
    cfg = config if isinstance(config,Config) else Config.model_validate(config or {})
    raw_q, total_q = quarters(s.raw_home_ah),quarters(s.ou_line)
    mag = abs(raw_q)
    fav_side = 'home' if raw_q >= 0 else 'away'
    dog_side = 'away' if fav_side == 'home' else 'home'
    fav = s.home_id if fav_side == 'home' else s.away_id
    dog = s.away_id if fav_side == 'home' else s.home_id
    profiles, selected, exclusions = {},{},{}
    for label,team in [('favorite',fav),('underdog',dog)]:
        eligible,active,prior,excluded = select_history(s,team,cfg)
        pa,pb = profile(active,cfg,s.league_conversion),profile(prior,cfg,s.league_conversion)
        profiles[label] = {'active':pa,'prior':pb,'blend':blend(pa,pb,cfg)}
        venue = 'home' if team == s.home_id else 'away'
        # Exact chronological windows, independent of the comparable-role sample.
        real = sorted((r for r in s.history if r.team_id == team), key=lambda r:r.kickoff, reverse=True)
        condition = [r for r in real if r.venue == venue]
        profiles[label].update(last_five=profile(real[:5],cfg,s.league_conversion),
                               last_five_same_venue=profile(condition[:5],cfg,s.league_conversion),
                               last_same_venue=profile(condition[:1],cfg,s.league_conversion),
                               venue=venue)
        selected[label] = {'all':eligible,'active':active,'prior':prior}
        exclusions[label] = excluded
    fa,da = profiles['favorite']['active'],profiles['underdog']['active']
    projection = shot_projection(profiles['favorite']['blend']['values'],profiles['underdog']['blend']['values'],s.league_sot_per_team)
    if not (selected['favorite']['all'] and selected['underdog']['all']):
        raise ValueError('Falta al menos un partido elegible por equipo; revise competición, rol y fechas')

    blocks = []
    for b in (s.similar_current,s.similar_h2h):
        blocks.append((getattr(b,fav_side),getattr(b,dog_side)) if b else (None,None))
    (f1,d1),(f2,d2) = blocks
    simok = lambda x,n=cfg.min_similar_n: x is not None and x.n >= n and x.cover_rate is not None
    b1ok = simok(f1) and simok(d1)
    dog_structure = b1ok and (d1.cover_rate >= .60 or (d1.n >= 3 and d1.losses == 0))
    b1 = gate('b1',b1ok and not dog_structure and f1.cover_rate >= .55 and d1.cover_rate <= .50,b1ok,
              'Prioridad al apoyo estructural del no favorito; en otro caso exigir 55%/50%.',
              {'favorite':f1.model_dump() if f1 else None,'underdog':d1.model_dump() if d1 else None})
    rf = s.rank_home if fav_side == 'home' else s.rank_away
    rd = s.rank_away if fav_side == 'home' else s.rank_home
    inverted = rf is not None and rd is not None and rf > rd and mag >= 3
    histok = simok(f2) and simok(d2)
    corr = s.similar_h2h.correlation if s.similar_h2h else 'unknown'
    b2ok = inverted or (histok and corr != 'unknown')
    b2 = gate('b2',inverted or (histok and corr == fav_side),b2ok,
              'Correlación histórica favorable o favorito invertido con 3+ cuartos.',{'correlation':corr,'inverted':inverted})

    # Market triangulation uses unfiltered roles: role shifts are exactly what it measures.
    common_rows = {}
    for team in (fav,dog):
        byopp = {}
        for r in sorted((x for x in s.history if x.team_id == team),key=lambda x:x.kickoff,reverse=True):
            if (r.raw_home_ah is not None and r.competition_id == s.competition_id and
                (s.decision_at-r.kickoff).total_seconds()/86400 <= cfg.history_days and
                (not s.senior_match or r.opponent_squad == 'senior')):
                byopp.setdefault(r.opponent_id,r)
        common_rows[team] = byopp
    triangulation = []
    for opp in sorted(set(common_rows[fav]) & set(common_rows[dog])):
        a,b = common_rows[fav][opp],common_rows[dog][opp]
        neutral = lambda r: strength_q(r.raw_home_ah,r.venue)-cfg.home_advantage_q*(1 if r.venue=='home' else -1)
        delta = neutral(a)-neutral(b)+cfg.home_advantage_q*(1 if fav_side=='home' else -1)
        triangulation.append({'kind':'common_opponent','opponent':opp,'favorite_id':a.match_id,'underdog_id':b.match_id,
                              'delta_q':delta,'goal_margin_difference':(a.gf-a.ga)-(b.gf-b.ga)})
    # Col3 Espejo: F-R1, R1-R2, D-R2. Keep only the latest bridge per ordered pair.
    used_bridges=set()
    for br in sorted(s.bridges,key=lambda x:x.kickoff,reverse=True):
        if br.competition_id!=s.competition_id or (s.decision_at-br.kickoff).days>cfg.history_days: continue
        for r1,r2,sign in [(br.home_id,br.away_id,1),(br.away_id,br.home_id,-1)]:
            if (r1,r2) in used_bridges or r1 not in common_rows[fav] or r2 not in common_rows[dog]: continue
            a,b=common_rows[fav][r1],common_rows[dog][r2]
            neutral=lambda r:strength_q(r.raw_home_ah,r.venue)-cfg.home_advantage_q*(1 if r.venue=='home' else -1)
            middle=sign*(quarters(br.raw_home_ah)-cfg.home_advantage_q)
            delta_bridge=neutral(a)+middle-neutral(b)+cfg.home_advantage_q*(1 if fav_side=='home' else -1)
            performance=(a.gf-a.ga)+sign*(br.home_goals-br.away_goals)-(b.gf-b.ga)
            triangulation.append({'kind':'mirror_path','opponent':r1+' / '+r2,'favorite_id':a.match_id,
                'underdog_id':b.match_id,'bridge_id':br.match_id,'delta_q':delta_bridge,'goal_margin_difference':performance})
            used_bridges.add((r1,r2))
    delta = mean([x['delta_q'] for x in triangulation]) if triangulation else None
    perf = mean([x['goal_margin_difference'] for x in triangulation]) if triangulation else None
    b3 = gate('b3',delta is not None and delta >= mag and perf > 0,bool(triangulation),
              'Brecha de mercado ajustada por localía y mejor diferencia de goles en cruces.',{'delta_q':delta,'required_q':mag,'performance_difference':perf,'pairs':triangulation})
    h2hs = sorted([r for r in s.history if r.team_id==fav and r.opponent_id==dog and r.raw_home_ah is not None
                   and r.competition_id==s.competition_id],key=lambda x:x.kickoff,reverse=True)
    previous_condition = [r for r in selected['favorite']['all'] if r.venue==fav_side]
    prev = previous_condition[0] if previous_condition else None
    h2h = h2hs[0] if h2hs else None
    def covered(r):
        # Target-team perspective: pass target as pseudo-home, with its canonical line.
        return settle('AH_HOME',-strength_q(r.raw_home_ah,r.venue),r.gf,r.ga)['profit_per_unit']
    b4 = gate('b4',h2h is not None and prev is not None and h2h.gf > h2h.ga and covered(h2h)>0 and covered(prev)>=0,h2h is not None and prev is not None,
              'Último H2H ganado y cubierto; el previo en la misma condición no perdió la liquidación.',
              {'h2h':h2h.match_id if h2h else None,'previous':prev.match_id if prev else None,
               'h2h_raw_open':h2h.raw_home_ah if h2h else None,'h2h_raw_close':h2h.raw_home_ah_close if h2h else None,
               'h2h_age_days':(s.decision_at-h2h.kickoff).total_seconds()/86400 if h2h else None})
    previous_active = prev
    presa = previous_active is not None and previous_active.sot_for is not None and previous_active.ap_for is not None and previous_active.sot_for>=10 and previous_active.ap_for>=55 and previous_active.gf<=1
    b5ok = presa or (usable(fa,'sot_for','ap_for',n=cfg.min_active_n) and usable(da,'sot_for','ap_for',n=cfg.min_active_n))
    dominance = b5ok and not presa and fa['sot_for']-da['sot_for'] >= cfg.sot_advantage and fa['ap_for']-da['ap_for'] >= cfg.ap_advantage
    venue_f = profiles['favorite']['last_same_venue']
    venue_d = profiles['underdog']['last_same_venue']
    venue_available = all(usable(p,'sot_for','sot_against','ap_for','ap_against',n=1) for p in (venue_f,venue_d))
    venue_conflict = venue_available and (venue_f['sot_for']-venue_f['sot_against'] < venue_d['sot_for']-venue_d['sot_against']
                                          and venue_f['ap_for']-venue_f['ap_against'] < venue_d['ap_for']-venue_d['ap_against'])
    b5 = gate('b5',presa or (dominance and not venue_conflict),b5ok,
              'SOT +1.5 y AP +10; contrastar el último partido en la misma condición. Presa en esa condición.',
              {'sot_f':fa['sot_for'],'sot_d':da['sot_for'],'ap_f':fa['ap_for'],'ap_d':da['ap_for'],'presa':presa,
               'last_venue_available':venue_available,'last_venue_conflict':venue_conflict,
               'last_venue_favorite':venue_f,'last_venue_underdog':venue_d})
    samevenue = [r for r in selected['favorite']['all'] if r.venue==fav_side][:10]
    drawrate = ratio(sum(r.gf==r.ga for r in samevenue),len(samevenue))
    wins = [r.gf-r.ga for r in selected['favorite']['all'] if r.gf>r.ga]
    onegoal = len(wins)>=2 and sum(x==1 for x in wins)/len(wins)>=.5
    friction = (len(samevenue)>=3 and drawrate>=.4) or (fa['n']>=3 and fa['draws']>=2) or (mag>=5 and onegoal)
    b6ok = fa['n']>=cfg.min_active_n
    b6 = gate('b6',not friction,b6ok,'Veto por empates frecuentes o margen de un gol ante exigencia larga.',
              {'draw_rate_condition':drawrate,'active_draws':fa['draws'],'one_goal_majority':onegoal})
    bg = [b1,b2,b3,b4,b5,b6]; bs = score(bg)

    locks = {label: p['n']==5 and p['clean_sheets']>=4 and usable(p,'sot_against',n=5) and p['sot_against']<=2
             for label,p in [('favorite',fa),('underdog',da)]}
    lock = any(locks.values())
    dog_condition = [r for r in selected['underdog']['all'] if r.venue==dog_side][:10]
    dog_scoring = ratio(sum(r.gf>0 for r in dog_condition),len(dog_condition))
    projected_fav_win = projection is not None and projection['margin_proxy']>0
    g1ok = lock or (len(dog_condition)>=3 and projection is not None)
    floor = g1ok and not lock and projected_fav_win and dog_scoring is not None and dog_scoring>=.75 and total_q<=12
    g1 = gate('g1',floor,g1ok,'Escenario 1+2=3 con favorito proyectado; cerrojo tiene prioridad.',
              {'dog_scoring_rate':dog_scoring,'n_condition':len(dog_condition),'projected_fav_win':projected_fav_win,'locks':locks})
    refs = {r.match_id:r.total_goals for r in s.references+s.bridges}
    mq = 4*mean(refs.values()) if refs else None
    highref = ratio(sum(x>=3 for x in refs.values()),len(refs))
    g2 = gate('g2',mq is not None and mq>=total_q,bool(refs),'Media de referencias únicas contra la línea.',{'mean_q':mq,'n':len(refs),'share_3plus':highref})
    samples = [x for pair in blocks for x in pair if x is not None and x.n>0 and x.gf is not None and x.ga is not None]
    sample_ids = [i for x in samples for i in x.sample_ids]
    overlap = len(sample_ids)!=len(set(sample_ids))
    sq = 4*sum(x.n*(x.gf+x.ga) for x in samples)/sum(x.n for x in samples) if samples and not overlap else None
    lines = {}
    for team in (fav,dog):
        recent_lines=sorted([r for r in s.history if r.team_id==team and r.ou_line is not None and r.competition_id==s.competition_id
                    and (s.decision_at-r.kickoff).total_seconds()/86400 <= cfg.history_days],key=lambda r:r.kickoff,reverse=True)[:10]
        lines.update({r.match_id:r.ou_line for r in recent_lines})
    historical_q = 4*mean(lines.values()) if lines else None
    shock_q = total_q-historical_q if historical_q is not None else None
    recent4 = {label:any(r.gf+r.ga>=4 and (s.decision_at-r.kickoff).total_seconds()/86400<=15 for r in selected[label]['active']) for label in ('favorite','underdog')}
    shock = shock_q is not None and shock_q>=2 and all(recent4.values())
    shock_known = shock_q is not None and (shock_q < 2 or all(p['n']>=cfg.min_active_n for p in (fa,da)))
    g3_positive = (sq is not None and sq>=total_q) or shock
    g3 = gate('g3',g3_positive,g3_positive or (sq is not None and shock_known),
              'Fusión sin doble conteo o desplazamiento de línea respaldado por goles recientes.',{'fused_q':sq,'overlap_blocks':overlap,'historical_q':historical_q,'line_history_n':len(lines),'shock_q':shock_q,'recent4':recent4})
    bilateral = all(usable(p,'gf','ga',n=cfg.min_active_n) and p['gf']>=1.8 and p['ga']>=1.8 and p['btts']>=.6 for p in (fa,da))
    # Positive delta only: a negative delta does not show that the favorite dominates.
    unilateral = delta is not None and delta>=total_q
    g4 = gate('g4',unilateral or bilateral,unilateral or bilateral or (delta is not None and all(p['n']>=cfg.min_active_n for p in (fa,da))),
              'Ruptura unilateral alineada con favorito o bilateral en ambos equipos.',{'unilateral':unilateral,'bilateral':bilateral,'delta_q':delta})
    apok = all(usable(p,'ap_for',n=cfg.min_active_n) for p in (fa,da))
    aps = fa['ap_for']+da['ap_for'] if apok else None
    g5 = gate('g5',aps is not None and aps>=cfg.ap_total,apok,'Suma AP a favor, una vez por equipo.',{'combined_ap':aps,'threshold':cfg.ap_total})
    shotsok = all(usable(p,'shots_for',n=cfg.min_active_n) for p in (fa,da))
    convok = all(p['conversion_for'] is not None and p['conversion_for_counts']['matches']>=cfg.min_active_n for p in (fa,da))
    sh = fa['shots_for']+da['shots_for'] if shotsok else None
    lethal = convok and min(fa['conversion_for'],da['conversion_for'])>=cfg.conversion_high
    g6_positive = (sh is not None and sh>=cfg.shots_total) or lethal
    g6 = gate('g6',g6_positive,g6_positive or (shotsok and convok),
              '22 tiros combinados o conversión de ambos >=35%.',{'shots':sh,'conversion_f':fa['conversion_for'],'conversion_d':da['conversion_for']})
    gg = [g1,g2,g3,g4,g5,g6]; gs = score(gg)

    def base_ah():
        if bs['of_six'] is not None and bs['of_six']!=3: return 'favorite' if bs['of_six']>3 else 'underdog'
        if projection: return 'favorite' if projection['margin_proxy']>=mag/4 else 'underdog'
        if delta is not None: return 'favorite' if delta>=mag else 'underdog'
        return 'underdog'  # explicit residual tie policy, never evidence of value
    ah = base_ah()
    if gs['of_six'] is not None and gs['of_six']!=3: ou = 'OVER' if gs['of_six']>3 else 'UNDER'
    elif projection: ou = 'OVER' if projection['total_proxy']>=s.ou_line else 'UNDER'
    elif mq is not None: ou = 'OVER' if mq>=total_q else 'UNDER'
    else: ou = 'UNDER'
    initial = {'ah':ah,'ou':ou}
    policies = []
    double = all(simok(f,3) and simok(d,3) and d.cover_rate>=.6 and f.cover_rate<=.4 and d.losses==0 for f,d in blocks)
    if double: ah='underdog'; policies.append('AH_DOUBLE_CORRELATION')
    def team_fused(idx):
        vals = [pair[idx] for pair in blocks if pair[idx] is not None and pair[idx].n and pair[idx].gf is not None]
        ids = [i for x in vals for i in x.sample_ids]
        if not vals or len(ids)!=len(set(ids)): return None
        return sum(x.n*x.gf for x in vals)/sum(x.n for x in vals)
    fg,dg = team_fused(0),team_fused(1)
    cross = (gs['of_six'] is not None and gs['of_six']>=5 and fg is not None and fg>=2 and dg is not None and dg<=1.35
             and simok(d2,3) and d2.cover_rate==0 and all(p['btts'] is not None and p['n']>=3 and p['btts']>=.8 for p in (fa,da)))
    if cross: ah='favorite'; policies.append('AH_OVER_CROSSOVER')
    anti_refs = mq is not None and highref>=.8 and mq>=total_q+3
    if floor or anti_refs or shock:
        ou='OVER'
        policies += [name for cond,name in [(floor,'OU_FLOOR_SCENARIO'),(anti_refs,'OU_REFERENCES'),(shock,'OU_LINE_SHIFT')] if cond]
    # Explicit resolution of a conflict not specified in the original rules.
    if lock:
        if ou=='OVER': policies.append('CONFLICT_LOCK_OVERRIDES_OVER')
        ou='UNDER'; policies.append('OU_ACTIVE_LOCK')
    long_draw = mag>=6 and fa['n']>=3 and fa['draws']>=2
    if long_draw: ah='underdog'; policies.append('AH_LONG_LINE_DRAW_VETO')

    side = fav_side if ah=='favorite' else dog_side
    ah_q = -mag if ah=='favorite' else mag
    ah_friction = bs['of_six'] is None or bs['of_six']<=4 or (mag==3 and h2h is not None and h2h.gf-h2h.ga==1)
    if ah=='favorite' and ah_friction and mag>0: ah_q += 1
    ou_q = total_q
    if ou=='OVER' and (gs['of_six'] is None or gs['of_six']<6):
        ou_q = max(2,total_q-(2 if total_q==12 else 1))
    def pick(market,q):
        quotes = [x for x in s.quotes if x.market==market and x.line_q==q]
        quoted = max(quotes,key=lambda x:x.odds) if quotes else None
        return {'market':market,'line_q':q,'line':q/4,'quote_status':'QUOTED' if quoted else 'TARGET_UNQUOTED',
                'quote':quoted.model_dump(mode='json') if quoted else None,'expected_value':None,
                'value_status':'NOT_ESTIMATED_FROM_BINARY_VOTES','automatic_execution':False}
    patterns=[]
    if inverted: patterns.append('FAVORITO_INVERTIDO')
    if presa: patterns.append('AVERSION_PERDEDOR_PRESA')
    for label,p in [('favorite',fa),('underdog',da)]:
        if p['conversion_for'] is not None and p['conversion_for']>=cfg.bubble_conversion and p['sot_for'] is not None and p['sot_for']<=cfg.bubble_max_sot:
            patterns.append('BURBUJA_EFICIENCIA_'+label.upper())
    if friction: patterns.append('FRICCION_EMPATE_MARGEN')
    if shock: patterns.append('DISLOCACION_ALCISTA')
    if lock: patterns.append('CERROJO_ACTIVO')
    if any(any(x['reason'] in ('reserve_or_u20','role_shift') for x in v) for v in exclusions.values()): patterns.append('CAMBIO_ROL_FILIALES')
    if rf is not None and rd is not None and rd-rf>=5 and mag<=2 and da['net_sot'] is not None and da['net_sot']>0:
        patterns.append('CEBO_TABLA_DISCREPANCIA')
    evidence = sorted(set([r.source_ref for r in s.history]+[r.source_ref for r in s.references+s.bridges]+[s.baseline_source_ref]+[q.source_ref for q in s.quotes]+
                          [b.source_ref for b in (s.similar_current,s.similar_h2h) if b]))
    result = {'model_version':cfg.version,'event_id':s.event_id,'synthetic':s.synthetic,
              'input_sha256':digest(s.model_dump(mode='json')),'config':cfg.model_dump(),
              'favorite_side':fav_side,'favorite_id':fav,'underdog_id':dog,'pickem_anchor':raw_q==0,
              'raw_home_ah_q':raw_q,'canonical_home_line_q':-raw_q,'total_q':total_q,
              'profiles':profiles,'exclusions':exclusions,'shot_projection':projection,
              'b':bg,'g':gg,'score_ah':bs,'score_ou':gs,'initial_direction':initial,
              'policy_trace':policies,'patterns':patterns,
              'ah_pick':pick('AH_'+side.upper(),ah_q),'ou_pick':pick(ou,ou_q),
              'evidence_refs':evidence,
              'notes':['Los patrones no identifican la intención de la casa.',
                       'Las medias en unidades de cuarto pueden ser fraccionarias.',
                       '65/35 pondera la proyección de forma; no equivale al 65% de seis votos.',
                       'Un cero disponible es condición no cumplida, no prueba de valor del lado contrario.']}
    if overlap: result['notes'].append('Fusión de bloques anulada por solapamiento de IDs; se requiere desduplicación de filas.')
    return result
