"""Regression: mobile match-card selectors must not restyle nested tables."""
from pathlib import Path
import re

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
