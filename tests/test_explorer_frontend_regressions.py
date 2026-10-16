from pathlib import Path


TEMPLATE = Path(__file__).resolve().parents[1] / "src" / "templates" / "explorer.html"


def test_league_normalization_is_declared_before_download_card_rows_use_it():
    html = TEMPLATE.read_text(encoding="utf-8")
    build_rows = html[html.index("const buildRows ="):html.index("const isSameHa =", html.index("const buildRows ="))]
    assert build_rows.index("const normCurrent = normLeagueId(currentLeagueId)") < build_rows.index("const defaultLeagueActive")
    assert build_rows.index("const sameLeague") < build_rows.index("if (!defaultLeagueActive || sameLeague)")


def test_explorer_auto_searches_on_load():
    html = TEMPLATE.read_text(encoding="utf-8")
    assert "triggerSearch();" in html



def test_multiselect_initialization_happens_after_ou_helpers():
    html = TEMPLATE.read_text(encoding="utf-8")
    helper_pos = html.index("const syncOuFilterInput")
    ready_pos = html.index("if (document.readyState !== 'loading')", helper_pos)
    assert ready_pos > helper_pos
