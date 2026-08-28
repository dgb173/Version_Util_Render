"""Regression: mobile match-card selectors must not restyle nested tables."""
from pathlib import Path
import re
import json
import subprocess

from bs4 import BeautifulSoup
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('template', ['precacheo.html', 'explorer.html'])
def test_match_card_selectors_do_not_reach_nested_stats(template):
    source = (ROOT / 'src/templates' / template).read_text(encoding='utf-8')
    css = re.search(r'<style>(.*?)</style>', source, re.S).group(1)
    css = re.sub(r'/\*.*?\*/', '', css, flags=re.S)
    cells = ''.join('<td>value</td>' for _ in range(14))
    dom = BeautifulSoup(f'''
        <div class="main-content"><div class="table-responsive">
        <table class="excel-table"><thead><tr><th>Matches</th></tr></thead>
        <tbody><tr data-match-id="1">{cells}</tr>
        <tr class="stats-collapse-row is-open"><td colspan="14">
        <table class="match-stats-table"><thead><tr><th>Local</th></tr></thead>
        <tbody><tr>{cells}</tr></tbody></table>
        </td></tr></tbody></table></div></div>''', 'html.parser')
    nested = dom.select_one('.match-stats-table')
    checked = 0
    for block in re.findall(r'([^{}]+)\{', css):
        for selector in block.split(','):
            selector = selector.strip()
            if '.excel-table' not in selector or not re.search(r'\b(?:tbody|thead)\b', selector):
                continue
            # Test the structural target without hover/active or generated labels.
            selector = re.sub(r'::(?:before|after)|:(?:hover|active)', '', selector)
            selected = dom.select(selector)
            assert not any(el is nested or nested in el.parents for el in selected), selector
            checked += 1
    assert checked > 20, 'Expected to exercise the actual mobile card rules'


def comparison_eval(code):
    helper = ROOT / 'src/static/js/context_comparison_cards.js'
    result = subprocess.run(
        ['node', '-e', f'const cards = require({json.dumps(str(helper))});\n' + code],
        capture_output=True, text=True, encoding='utf-8', check=True,
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize('score,expected', [('5:1', 'MEJORA'), ('1:5', 'IGUALA'), ('?:?', 'N/D')])
def test_col3_verdict_uses_mirror_side_and_valid_results(score, expected):
    result = comparison_eval('''
        const dataset = {parentHome:'Hellenic AC', parentAway:'Garuda FC', currentAh:'-0.5'};
        const data = {h2h_col3:{status:'found',h2h_home_team_name:'Casuarina FC',h2h_away_team_name:'Port Darwin FC',
            goles_home:SCORE.split(':')[0],goles_away:SCORE.split(':')[1],handicap:0},
            comparison:{last_home_match:{home:'Hellenic AC',away:'Port Darwin FC',score:'0:3'},
            last_away_match:{home:'Casuarina FC',away:'Garuda FC',score:'0:2'}}};
        console.log(JSON.stringify(cards.col3Model(dataset,data)));
    '''.replace('SCORE', json.dumps(score)))
    assert result['status']['code'] == expected
    assert result['orientation'] == 'Directa'
    # AH zero treats the visitor as favorite, just like the rest of this app.
    if score == '5:1':
        assert result['cover'] == 'No cubrió'


def test_current_col3_has_its_own_score_and_missing_data_is_not_similar():
    result = comparison_eval('''
        const dataset = {parentHome:'Local',parentAway:'Visitante',currentAh:'1'};
        const current = {h2h_col3:{status:'found',h2h_home_team_name:'Actual A',h2h_away_team_name:'Actual B',goles_home:2,goles_away:2,handicap:0},currentAh:1};
        const button = {dataset, closest:()=>({dataset:{comparisonReference:JSON.stringify(current)}})};
        const html = cards.buildCol3(button,{h2h_col3:{status:'found',h2h_home_team_name:'Fila A',h2h_away_team_name:'Fila B',goles_home:1,goles_away:5,handicap:-0.5}});
        console.log(JSON.stringify({html,empty:cards.buildCol3({dataset,closest:()=>null},{})}));
    ''')
    dom = BeautifulSoup(result['html'], 'html.parser')
    boxes = dom.select('.comparison-box')
    assert [box.select_one('.comparison-score').text for box in boxes] == ['1:5', '2:2']
    assert 'Col3 actual' in boxes[1].text
    assert all(box['data-performance'] == 'N/D' for box in boxes)
    actions = dom.select_one('.comparison-head-actions')
    assert actions.select_one('.comparison-type') is not None
    assert actions.select_one('button.comparison-close') is not None
    assert 'Col3 actual no disponible' in result['empty']


@pytest.mark.parametrize('score,expected', [('2:6', 'MEJORA'), ('1:0', 'IGUALA')])
def test_movement_compares_same_team_when_home_away_reversed(score, expected):
    result = comparison_eval('''
        console.log(JSON.stringify(cards.movementModel(
            {rowHome:'Local',rowAway:'Visitante',rowScore:'0:1',rowAh:'-0.5'},
            {home_team:'Visitante',away_team:'Local',score:SCORE,ah:1,cover:false}
        )));
    '''.replace('SCORE', json.dumps(score)))
    assert result['status']['code'] == expected
    assert result['home'] == 'Visitante'
    assert result['score'] == score
    assert result['movement'] == '↓ Baja · AH 1 → -0.5'


def test_comparison_does_not_treat_missing_team_or_score_as_a_draw():
    result = comparison_eval('''
        console.log(JSON.stringify(cards.movementModel(
            {rowHome:'Local',rowAway:'Visitante',rowScore:'?:?',rowAh:'0'},
            {score:'0:0',ah:0}
        )));
    ''')
    assert result['status']['code'] == 'N/D'


def test_red_verdict_and_untrusted_names_are_rendered_safely():
    result = comparison_eval('''
        const dataset = {parentHome:'Local',parentAway:'Visitante',currentAh:'1'};
        const data={h2h_col3:{status:'found',h2h_home_team_name:'Espejo',h2h_away_team_name:'<img src=x onerror=alert(1)>',goles_home:2,goles_away:0,handicap:1},
            comparison:{last_home_match:{home:'Local',away:'Rival',score:'0:1'},last_away_match:{home:'Visitante',away:'Espejo',score:'0:2'}}};
        const model=cards.col3Model(dataset,data);
        const html=cards.buildCol3({dataset,closest:()=>null},data);
        console.log(JSON.stringify({model,html}));
    ''')
    assert result['model']['status']['code'] == 'EMPEORA'
    dom = BeautifulSoup(result['html'], 'html.parser')
    assert dom.select_one('.comparison-box.is-worse') is not None
    assert dom.select_one('img') is None
    assert '<img src=x onerror=alert(1)>' in dom.text


def test_current_reference_load_is_shared_within_panel_but_not_across_matches():
    result = comparison_eval('''
        const requests=[];
        global.fetch=async (url,options)=>{
            const id=JSON.parse(options.body).match_id;
            requests.push(id);
            return {ok:true,json:async()=>({status:'success',h2h_col3:{status:'found',match_id:id}})};
        };
        const panelA={dataset:{comparisonReference:JSON.stringify({currentAh:0})}};
        const panelB={dataset:{comparisonReference:JSON.stringify({currentAh:1})}};
        const a={dataset:{parentMatchId:'123'},closest:()=>panelA};
        const b={dataset:{parentMatchId:'456'},closest:()=>panelB};
        Promise.all([cards.ensureReference(a),cards.ensureReference(a),cards.ensureReference(b)]).then(()=>{
            console.log(JSON.stringify({requests,a:JSON.parse(panelA.dataset.comparisonReference),b:JSON.parse(panelB.dataset.comparisonReference)}));
        });
    ''')
    assert result['requests'] == ['123', '456']
    assert result['a']['h2h_col3']['match_id'] == '123'
    assert result['b']['h2h_col3']['match_id'] == '456'
    assert result['a']['currentAh'] == 0
