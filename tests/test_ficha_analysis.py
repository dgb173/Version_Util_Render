import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from modules.ficha_analysis import ficha_analysis


def fixture():
    rows=[{'match_id':str(i),'date':f'2026-10-{i:02d}','home':'H','away':'X',
           'score':'2:1' if i<=6 else '0:0','ahLine':.75,'ouLine':2.75} for i in range(1,11)]
    return dict(match_id='target',date='2026-11-01',home_name='H',away_name='A',handicap=1,goal_line=2.75,
                ficha_rows={'home':rows,'away':[]})


def test_all_selected_rows_and_frequent_lines_not_fixed_five():
    a=ficha_analysis(fixture());p=a['profiles']['home']
    assert p['n']==10 and p['wins']==6 and p['draws']==4
    assert p['ah_lines'][0]['n']==10 and p['ah_lines'][0]['frequent']
    assert p['ah']['half_win']==6 and p['ah']['loss']==4
    assert p['target_ah']['push']==6
    assert p['over']['half_win']==6
    assert p['joint']['cover_over']==6


def test_filtered_rows_are_the_only_sample_even_if_cached_history_exists():
    p=fixture();p['recent_home_matches_all']=list(p['ficha_rows']['home'])
    p['ficha_rows']['home']=p['ficha_rows']['home'][:2]
    a=ficha_analysis(p)['profiles']['home']
    assert a['n']==2 and not a['ah_lines'][0]['frequent']


def test_common_rival_orientation_and_adjusted_comparison():
    p=fixture();p['ficha_rows']['home']=p['ficha_rows']['home'][:1]
    p['ficha_rows']['away']=[{'date':'2026-10-02','home':'X','away':'A','score':'0:1','ahLine':-.25,'ouLine':2.5}]
    r=ficha_analysis(p)['common_rivals'][0]
    assert r['home']['margin']==r['away']['margin']==1
    assert r['residual_delta']==-.5
    assert r['comparison']=='MEJORA VISITANTE'


def test_historical_rematch_inverted_and_future_excluded():
    p=fixture();p['ficha_rows']['home']=p['ficha_rows']['home'][:1]
    p['row_references']={'1':{'movement':{'general':{'date':'2026-09-01','home_team':'X','away_team':'H','score':'1:0','ah':-.5}}}}
    r=ficha_analysis(p)['evolution'][0]
    assert r['margin_delta']==2 and r['residual_delta']==1.75 and r['verdict']=='MEJORA'
    p['row_references']['1']['movement']['general']['date']='2026-10-02'
    assert ficha_analysis(p)['evolution']==[]


def test_api_ficha_does_not_require_target_saved_in_database():
    from flask import Flask
    from modules.quant_api import create_blueprint
    app=Flask(__name__);app.register_blueprint(create_blueprint(lambda _:None))
    client=app.test_client()
    response=client.post('/api/quant/ficha-analysis',json=fixture())
    assert response.status_code==200
    assert response.get_json()['analysis']['profiles']['home']['n']==10
    assert client.post('/api/quant/ficha-analysis',json={}).status_code==400
