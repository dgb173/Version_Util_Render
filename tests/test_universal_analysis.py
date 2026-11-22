import copy
import sys
from pathlib import Path

import pytest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from modules.universal_analysis import analyze, settlement, number


def row(i, home='H', away='X', score='2:0', ah=1, **extra):
    return dict(match_id=str(i), date=f'2026-10-{i:02d}', home_team=home,
                away_team=away, score=score, ahLine=ah, ouLine=2.75, league_id_hist='L', **extra)


def payload():
    return dict(match_id='target', start_time='2026-11-01T12:00:00+01:00', home_name='H', away_name='A',
                handicap=1, goal_line=2.75, league_id='L',
                recent_home_matches=[row(i) for i in range(1,6)],
                recent_away_matches=[row(i+5, 'Z', 'A', '3:0') for i in range(1,6)])


@pytest.mark.parametrize('margin,line,expected', [(1,-1,0),(1,-.75,.5),(1,-1.25,-.5),(0,-.25,-.5),
                                                (0,.25,.5),(3,-2.75,.5),(2,-2.25,-.5),(1,-.5,1)])
def test_quarter_stake_settlement(margin,line,expected):
    assert settlement(margin,line)==expected
    assert settlement(-margin,-line)==-expected


def test_line_zero_is_not_missing_and_never_assumes_away_favorite():
    p=payload();p['handicap']=0
    a=analyze(p)
    assert a['favorite'] is None
    assert a['category']=='DATOS_INSUFICIENTES'
    assert number('0')==0 and number('') is None
    assert number('NaN') is None


def test_away_favorite_orientation_and_original_line():
    p=payload();p['handicap']=-.75
    p['recent_away_matches']=[row(i+5,'X','A','0:1',-.75) for i in range(1,6)]
    a=analyze(p)
    assert a['favorite']=='A'
    r=a['profiles']['away']['rows'][0]
    assert r['own_line']==-.75 and r['margin']==1 and r['actual_ah']==.5 and r['target_ah']==.5


def test_target_result_and_stats_cannot_change_pre_match_features():
    p=payload();a=analyze(p)
    p.update(final_score='0:9', stats_rows=[dict(label='Tiros a puerta',home=0,away=25)])
    assert analyze(p)==a


def test_future_undated_same_day_and_duplicate_history_excluded():
    p=payload();old=copy.deepcopy(p['recent_home_matches'][0])
    p['recent_home_matches_all']=[old,dict(old,date='2026-11-02',match_id='future'),dict(old,date='2026-11-01',match_id='today'),dict(old,date='',match_id='unknown')]
    a=analyze(p)
    assert a['profiles']['home']['available_n']==5
    p['recent_home_matches'].append(row(12,'H Reserves'))
    assert analyze(p)['profiles']['home']['available_n']==5


def test_h2h_inversion_preserves_favorite_and_deduplicates_stadium():
    p=payload()
    e=row(12,'A','H','0:1',-.5)
    p['h2h_general']={'all_matches':[e]}
    p['h2h_stadium']={'all_matches':[copy.deepcopy(e)]}
    a=analyze(p);r=a['h2h'][0]
    assert len(a['h2h'])==1
    assert r['wdl']=='V' and r['actual_ah']==1 and r['target_ah']==0
    assert r['requirement_change']==.5
    assert a['h2h_stadium'] is None


def test_col3_mirror_uses_opponents_not_arbitrary_sign():
    p=payload()
    p['last_home_match']=row(12,'H','X','2:0',1)
    p['last_away_match']=row(13,'Y','A','1:0',.5)
    p['h2h_col3']=dict(status='found',date='2026-10-02',match_id='bridge',h2h_home_team_name='X',h2h_away_team_name='Y',goles_home=1,goles_away=0,handicap=.25)
    c=analyze(p)['col3']
    assert c['mirror']=='Y' and c['orientation']=='INVERSA'
    assert c['row']['own_line']==.25 and c['row']['margin']==-1
    assert c['residual_delta']==1.75
    p['h2h_col3']['h2h_away_team_name']='Other'
    assert analyze(p)['col3']['status']=='ND'


def test_missing_line_is_unknown_not_zero():
    p=payload()
    for e in p['recent_home_matches']:e['ahLine']='N/A'
    a=analyze(p)
    assert a['profiles']['home']['actual']['n']==0
    assert a['profiles']['home']['target']['n']==5
    assert a['category']=='DATOS_INSUFICIENTES'


def test_dossier_profiles_are_distinct_without_using_final_results():
    cases=[([0,0,0,1,2],[0,-1,-3,-2,1]),([-2,0,0,-1,0],[-3,0,-1,1,-1]),
           ([3,3,5,1,1],[0,1,1,0,1]),([-2,0,-3,-1,1],[2,-1,0,-1,1])]
    margins=[]
    for f,d in cases:
        p=payload()
        p['recent_home_matches']=[row(i+1,score=f'{max(v,0)}:{max(-v,0)}') for i,v in enumerate(f)]
        p['recent_away_matches']=[row(i+6,'X','A',f'{max(-v,0)}:{max(v,0)}') for i,v in enumerate(d)]
        a=analyze(p)
        margins.append((a['profiles']['home']['target']['win'],a['profiles']['home']['target']['push']))
    assert margins==[(1,1),(0,0),(3,2),(0,1)]


def test_api_missing_bulk_and_same_result():
    from flask import Flask
    from modules.quant_api import create_blueprint
    p=payload(); app=Flask(__name__)
    app.register_blueprint(create_blueprint(lambda mid: p if mid=='target' else None))
    client=app.test_client()
    a=client.get('/api/quant/universal/target').get_json()['analysis']
    assert a['favorite']=='H'
    assert client.get('/api/quant/universal/missing').status_code==404
    assert client.post('/api/quant/universal-bulk',json={'match_ids':[]}).status_code==400
    bulk=client.post('/api/quant/universal-bulk',json={'match_ids':['target','missing']}).get_json()['results']
    assert bulk['target']['fingerprint']==a['fingerprint']
    assert 'error' in bulk['missing']
