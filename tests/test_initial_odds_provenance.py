from bs4 import BeautifulSoup
from src.modules import estudio_scraper as es
import json
from pathlib import Path


def _ajax_session(initial_ah='0.5'):
    values = ['0'] * 14
    values[3:6] = ['0.91', initial_ah, '0.95']
    values[8] = '2.5'
    values[11:14] = ['0.93', '2.25', '0.97']
    class Response:
        status_code = 200
        def json(self):
            return {'ErrCode':0,'Data':'8*' + ','.join(values)}
    class Session:
        def get(self, *args, **kwargs):
            return Response()
    return Session()


def test_ajax_uses_initial_ah_even_when_later_line_is_available(monkeypatch):
    monkeypatch.setattr(es,'get_requests_session_of',_ajax_session)
    result = es.fetch_odds_from_ajax('123')
    assert result['ah_linea_raw'] == '0.5'
    assert result['ah_home_cuota'] == '0.91'
    assert result['goals_linea_raw'] == '2.25'


def test_ajax_never_replaces_missing_initial_with_later_ah(monkeypatch):
    monkeypatch.setattr(es,'get_requests_session_of',lambda:_ajax_session(''))
    assert es.fetch_odds_from_ajax('123') is None


def test_initial_extraction_preserves_ajax_prices(monkeypatch):
    monkeypatch.setattr(es,'get_requests_session_of',_ajax_session)
    monkeypatch.setattr(es,'PLAYWRIGHT_AVAILABLE',False)
    monkeypatch.setattr(es,'fetch_odds_from_bf_data',lambda mid: (_ for _ in ()).throw(AssertionError('Feed no acredita inicial')))
    result = es.extract_bet365_initial_odds_of(None,'123')
    assert result['goals_over_cuota'] == '0.93'
    assert result['ah_away_cuota'] == '0.95'
    assert result['odds_stage'] == 'initial'
    assert result['price_format'] == 'hongkong'
    assert result['captured_at'].endswith('Z')


def test_html_extraction_selects_initial_row_and_keeps_raw_prices():
    cells = ''.join('<td>'+v+'</td>' for v in ['Bet365','Initial','1.03','1.5','0.85','1','2','3','0.91','2.75','0.99'])
    soup = BeautifulSoup('<table><tr id="tr_o_1_8" name="earlyOdds">'+cells+'</tr></table>','html.parser')
    result = es.extract_bet365_initial_odds_of(soup)
    assert result['ah_linea_raw'] == '1.5'
    assert result['goals_linea_raw'] == '2.75'
    assert result['ah_home_cuota'] == '1.03'
    assert result['price_format'] == 'unverified'


def test_real_nowgoal_payload_distinguishes_initial_and_later_lines(monkeypatch):
    payload = json.loads((Path(__file__).parent/'fixtures/nowgoal_ajax_initial_3074606.json').read_text(encoding='utf-8'))
    class Response:
        status_code = 200
        def json(self):return payload
    class Session:
        def get(self,*args,**kwargs):return Response()
    monkeypatch.setattr(es,'get_requests_session_of',Session)
    result = es.fetch_odds_from_ajax('3074606')
    assert es.parse_ah_to_number_of(result['ah_linea_raw']) == .25
    assert result['bookmaker_id'] == '8'
    assert result['ah_away_cuota'] == '0.75'
    assert result['price_format'] == 'hongkong'
