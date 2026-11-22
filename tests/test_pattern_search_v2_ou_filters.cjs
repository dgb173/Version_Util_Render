const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const jsCode = fs.readFileSync(
  path.join(__dirname, '..', 'src', 'static', 'js', 'pattern_search_v2.js'),
  'utf8'
);
const explorerHtml = fs.readFileSync(
  path.join(__dirname, '..', 'src', 'templates', 'explorer.html'),
  'utf8'
);

// Verify template contains exclusive pv2-only-filter O/U controls and never sends them to normal explorer search
for (const id of [
  'filter-pv2-ft-ou-res',
  'filter-pv2-prev-home-ou-mov',
  'filter-pv2-prev-home-ou-res',
  'filter-pv2-prev-away-ou-mov',
  'filter-pv2-prev-away-ou-res',
  'filter-pv2-h2h-stadium-ou-mov',
  'filter-pv2-h2h-stadium-ou-res',
  'filter-pv2-h2h-general-ou-mov',
  'filter-pv2-h2h-general-ou-res',
]) {
  assert.ok(explorerHtml.includes(`id="${id}"`), `Falta control ${id} en explorer.html`);
  assert.ok(
    !explorerHtml.slice(explorerHtml.indexOf('function triggerSearch()'), explorerHtml.indexOf('function renderTableWithPagination()')).includes(id),
    `${id} no debe enviarse en el Explorador normal`
  );
}
assert.ok(explorerHtml.includes('data-pv2-ou-h2h-slot='), 'Faltan slots O/U en celdas H2H');

// Minimal DOM harness for pattern_search_v2.js
const elements = new Map();
function makeClassList(initial = []) {
  const set = new Set(initial);
  return {
    add: (...cls) => cls.forEach(c => set.add(c)),
    remove: (...cls) => cls.forEach(c => set.delete(c)),
    toggle: (c, force) => {
      if (force === undefined) {
        if (set.has(c)) { set.delete(c); return false; }
        set.add(c); return true;
      }
      if (force) set.add(c); else set.delete(c);
      return Boolean(force);
    },
    contains: c => set.has(c),
    _set: set,
  };
}
function makeSelect(id) {
  const matchOpt = { textContent: '= Actual', disabled: false };
  const el = {
    id,
    value: '',
    classList: makeClassList(),
    querySelector: (sel) => (sel === 'option[value="MATCH_TARGET"]' ? matchOpt : null),
    _matchOpt: matchOpt,
  };
  elements.set(id, el);
  return el;
}

const filterIds = [
  'filter-pv2-ft-ou-res',
  'filter-pv2-prev-home-ou-mov',
  'filter-pv2-prev-home-ou-res',
  'filter-pv2-prev-away-ou-mov',
  'filter-pv2-prev-away-ou-res',
  'filter-pv2-h2h-stadium-ou-mov',
  'filter-pv2-h2h-stadium-ou-res',
  'filter-pv2-h2h-general-ou-mov',
  'filter-pv2-h2h-general-ou-res',
];
filterIds.forEach(makeSelect);

const wsEl = {
  id: 'pv2-explorer-workspace',
  innerHTML: '',
  classList: makeClassList(['d-none']),
  querySelector: () => null,
  querySelectorAll: () => [],
};
elements.set('pv2-explorer-workspace', wsEl);

const docObj = {
  readyState: 'complete',
  body: { classList: makeClassList(), appendChild: () => {} },
  title: '',
  getElementById: (id) => elements.get(id) || null,
  querySelector: () => null,
  querySelectorAll: () => [],
  addEventListener: () => {},
  createElement: () => ({ classList: makeClassList(), addEventListener: () => {}, querySelector: () => ({ addEventListener: () => {} }) }),
};

const card1 = {
  match_id: '101',
  candidate: { home: 'H1', away: 'A1', league: 'L1', score: '2:1', ah_real: 1.5, ou_line: '3.0' },
  prev_home: { ou: '2.5', ou_delta: 0.5, ou_mov_dir: 'SUBE', ou_movement: '2.5 → 3', ou_res: 'OVER', score: '3:1' },
  prev_away: { ou: '3.5', ou_delta: -0.5, ou_mov_dir: 'BAJA', ou_movement: '3.5 → 3', ou_res: 'UNDER', score: '1:0' },
  h2h_stadium: { date: '2025-05-01', score: '1:0', ou: '2.5', ou_delta: 0.5, ou_mov_dir: 'SUBE', ou_movement: '2.5 → 3', ou_res: 'UNDER' },
  h2h_general: { date: '2025-11-10', score: '2:2', ou: '3.0', ou_delta: 0, ou_mov_dir: 'IGUAL', ou_movement: '3 → 3', ou_res: 'OVER' },
  _pv2: {
    rank: 1,
    similarity_pct: 82,
    market_pct: 80,
    fav_cover: 'NO_COVER',
    wdl_1x2: '1',
    ou_res: 'PUSH',
    total_goals: 3,
    ficha_hydrated: true,
    home_tags: [{ code: 'FORTIN', label: 'Fortín Casa', tone: 'good' }],
    away_tags: [{ code: 'MAL_VIS', label: 'Mal Visitante', tone: 'warn' }],
    matchup_tags: [{ code: 'DOBLE', label: 'Doble Fuerza Fav', tone: 'strong' }],
    pillars: { core_pct: 80, ou_pct: 85, h2h_pct: 75, ind_pct: 70, col3_pct: 60 },
  },
};

const card2 = {
  match_id: '102',
  candidate: { home: 'H2', away: 'A2', league: 'L1', score: '0:0', ah_real: 0.5, ou_line: '2.5' },
  prev_home: { ou: '3.0', ou_delta: -0.5, ou_mov_dir: 'BAJA', ou_movement: '3 → 2.5', ou_res: 'UNDER', score: '0:1' },
  prev_away: { ou: '2.5', ou_delta: 0, ou_mov_dir: 'IGUAL', ou_movement: '2.5 → 2.5', ou_res: 'OVER', score: '2:1' },
  h2h_stadium: { date: '2025-02-01', score: '2:1', movement: '0.5 -> 0.5' }, // sin ou propio -> fallback vs 2.5 => OVER
  h2h_general: { date: '2025-02-01', score: '2:1', movement: '0.5 -> 0.5' }, // mismo partido que stadium!
  _pv2: {
    rank: 2,
    similarity_pct: 71,
    market_pct: 70,
    fav_cover: 'NO_COVER',
    wdl_1x2: 'X',
    ou_res: 'UNDER',
    total_goals: 0,
    ficha_hydrated: false,
    pillars: { core_pct: 70, ou_pct: 55, h2h_pct: 60, ind_pct: 60, col3_pct: 50 },
  },
};

const mockApiPayload = {
  status: 'success',
  elapsed_ms: 12,
  total_scanned: 100,
  total_candidates_scanned: 10,
  target: {
    match_id: '999',
    home: 'Daegu FC',
    away: 'Gimhae City',
    league: 'K League 2',
    ah: 1.5,
    ou: 3.25,
    fav_side: 'HOME',
    home_stats: { all: {}, venue: {} },
    away_stats: { all: {}, venue: {} },
    home_tags: [{ code: 'FORTIN', label: 'Fortín Casa', tone: 'good' }],
    away_tags: [{ code: 'MAL_VIS', label: 'Mal Visitante', tone: 'warn' }],
    matchup_tags: [{ code: 'DOBLE', label: 'Doble Fuerza Fav', tone: 'strong' }],
    headline_reason: 'Texto de etiqueta que no debe mostrarse',
  },
  target_ou_summary: {
    ou: 3.25,
    prev_home: { ou: 2.75, ou_delta: 0.5, ou_mov_dir: 'SUBE', ou_res: 'OVER' },
    prev_away: { ou: 2.5, ou_delta: 0.75, ou_mov_dir: 'SUBE', ou_res: 'OVER' },
    h2h_stadium: { ou: 2.75, ou_delta: 0.5, ou_mov_dir: 'SUBE', ou_res: 'UNDER' },
    h2h_general: null,
    text: 'O/U 3.25 · Prev Loc: 2.75→3.25 (+0.5, O)',
  },
  verdict: { signal: 'SEÑAL', tone: 'neutral', summary: 'test', confidence: 'BAJA' },
  ou_verdict: { signal: 'ZONA NEUTRA O/U', tone: 'neutral', confidence: 'BAJA', target_ou_mov_text: 'O/U 3.25' },
  similarity_ladder: [{ label: 'Top 5', n: 2, fav_cover_pct: 0, dog_cover_pct: 100, push_pct: 0, over_pct: 0, avg_goals: 1.5 }],
  tag_cohorts: [{ code: 'FORTIN', label: 'Fortín Casa', tone: 'good', n: 2, fav_cover_pct: 50, dog_cover_pct: 50, over_pct: 50 }],
  quadrant_matrix: {},
  anti_trap_checklist: [],
  explorer_results: [card1, card2],
};

const winObj = { clientContextCache: new Map(), Map };
const context = vm.createContext({
  window: winObj,
  document: docObj,
  Map,
  Set,
  Number,
  String,
  Boolean,
  Array,
  Object,
  Math,
  encodeURIComponent,
  fetch: async (url) => ({
    ok: true,
    status: 200,
    json: async () => (url === '/api/pattern_search_v2'
      ? mockApiPayload
      : { status: 'success', updated: {}, sqlite_count: 0, downloaded_count: 0, needs_download_ids: [], failed_ids: [] }),
  }),
});
winObj.window = winObj;

vm.runInContext(jsCode, context);
const pv2 = winObj.PatternSearchV2;
assert.ok(pv2, 'PatternSearchV2 debe exportarse');

// 1) Inactive state: zero interference
assert.equal(pv2.isExplorerV2Active(), false);
assert.equal(pv2.matchesActiveExplorerFilter(card2), true);
assert.equal(pv2.renderExplorerOuBadgeHtml(card1, 'h2h_stadium'), '');
assert.equal(pv2.renderExplorerRowTeamTagsHtml(card1, 'home'), '');

(async () => {
  let applyCount = 0;
  await pv2.initExplorerPatternV2Page('999', {
    setResults: () => {},
    applyFilters: () => { applyCount += 1; pv2.syncExplorerV2Stats([card1, card2].filter(c => pv2.matchesActiveExplorerFilter(c))); },
    showLoading: () => {},
    clearToNormalSearch: () => {},
  });

  assert.equal(pv2.isExplorerV2Active(), true);
  assert.equal(docObj.body.classList.contains('pv2-explorer-active'), true);

  // Target dynamic labels on MATCH_TARGET option
  assert.ok(elements.get('filter-pv2-prev-home-ou-mov')._matchOpt.textContent.includes('Subió +0.5'));
  assert.equal(elements.get('filter-pv2-h2h-general-ou-mov')._matchOpt.disabled, true);

  // 2) Zero tags everywhere in V2
  assert.equal(pv2.renderExplorerRowTeamTagsHtml(card1, 'home'), '');
  assert.equal(pv2.renderExplorerRowTeamTagsHtml(card1, 'matchup'), '');
  for (const forbidden of [
    'pv2-tags-toggle-btn',
    'Etiquetas:',
    'Sin etiqueta extrema',
    'Cruce estándar',
    'Etiquetas activas tras descargar fichas',
    'Fortín Casa',
    'Mal Visitante',
    'Doble Fuerza Fav',
    'TAG:',
  ]) {
    assert.ok(!wsEl.innerHTML.includes(forbidden), `No debe aparecer "${forbidden}" en Patrón 2.0`);
  }

  // Drawer Escalera without Cohortes por Etiqueta
  const drawerBtn = { getAttribute: () => 'ladder' };
  // Simulate toggling drawer by checking DOM output via public methods? Let's test via renderExplorerWorkspace trigger on filter change or direct check in file:
  assert.ok(!jsCode.includes('Cohortes por Etiqueta'), 'No debe existir Cohortes por Etiqueta en pattern_search_v2.js');

  // 3) H2H & Prev O/U Badges
  const phBadge = pv2.renderExplorerOuBadgeHtml(card1, 'prev_home');
  assert.ok(phBadge.includes('O/U 2.5 → 3 (+0.5) · O ✓'), `Badge Prev Home inesperado: ${phBadge}`);
  const stBadge = pv2.renderExplorerOuBadgeHtml(card1, 'h2h_stadium');
  assert.ok(stBadge.includes('O/U 2.5 → 3 (+0.5) · U ✓'), `Badge H2H Estadio inesperado: ${stBadge}`);
  const genBadge = pv2.renderExplorerOuBadgeHtml(card1, 'h2h_general');
  assert.ok(genBadge.includes('O/U 3 → 3 (0) · O'), `Badge H2H General inesperado: ${genBadge}`);
  // Fallback H2H badge (no own line, has curr_ou + score)
  const stFallback = pv2.renderExplorerOuBadgeHtml(card2, 'h2h_stadium');
  assert.ok(stFallback.includes('pv2-ou-badge--fallback') && stFallback.includes('vs 2.5 · O'), `Fallback badge inesperado: ${stFallback}`);
  // Same as stadium -> empty
  assert.equal(pv2.renderExplorerOuBadgeHtml(card2, 'h2h_general'), '');

  // 4) Independent O/U filters (Prev Local, Prev Away, H2H Stadium, H2H General, FT)
  // Prev Home BAJA
  elements.get('filter-pv2-prev-home-ou-mov').value = 'BAJA';
  pv2.onOuFilterChange();
  assert.equal(pv2.matchesActiveExplorerFilter(card1), false);
  assert.equal(pv2.matchesActiveExplorerFilter(card2), true);
  assert.ok(wsEl.innerHTML.includes('pv2-ou-active-bar') && wsEl.innerHTML.includes('Prev Loc Mov O/U: BAJA'));
  assert.ok(applyCount >= 1);

  // Clear all O/U
  pv2.clearOuFilters();
  assert.equal(pv2.matchesActiveExplorerFilter(card1), true);
  assert.equal(pv2.matchesActiveExplorerFilter(card2), true);

  // Prev Away UNDER
  elements.get('filter-pv2-prev-away-ou-res').value = 'UNDER';
  pv2.onOuFilterChange();
  assert.equal(pv2.matchesActiveExplorerFilter(card1), true);
  assert.equal(pv2.matchesActiveExplorerFilter(card2), false);
  pv2.clearOuFilters();

  // H2H Stadium SUBE_05 + UNDER -> card1 matches, card2 lacks delta -> fails
  elements.get('filter-pv2-h2h-stadium-ou-mov').value = 'SUBE_05';
  elements.get('filter-pv2-h2h-stadium-ou-res').value = 'UNDER';
  pv2.onOuFilterChange();
  assert.equal(pv2.matchesActiveExplorerFilter(card1), true);
  assert.equal(pv2.matchesActiveExplorerFilter(card2), false);
  pv2.clearOuFilters();

  // H2H General IGUAL + OVER -> card1 matches; card2 isSameAsStadium -> excluded
  elements.get('filter-pv2-h2h-general-ou-mov').value = 'IGUAL';
  elements.get('filter-pv2-h2h-general-ou-res').value = 'OVER';
  pv2.onOuFilterChange();
  assert.equal(pv2.matchesActiveExplorerFilter(card1), true);
  assert.equal(pv2.matchesActiveExplorerFilter(card2), false);
  pv2.clearOuFilters();

  // FT O/U PUSH -> card1 matches, card2 does not
  elements.get('filter-pv2-ft-ou-res').value = 'PUSH';
  pv2.onOuFilterChange();
  assert.equal(pv2.matchesActiveExplorerFilter(card1), true);
  assert.equal(pv2.matchesActiveExplorerFilter(card2), false);

  // Deactivate cleans body class and filters
  pv2.deactivateExplorerV2();
  assert.equal(pv2.isExplorerV2Active(), false);
  assert.equal(docObj.body.classList.contains('pv2-explorer-active'), false);
  assert.equal(elements.get('filter-pv2-ft-ou-res').value, '');

  console.log('ALL_PATTERN_V2_OU_FILTER_TESTS_PASSED');
})().catch(err => {
  console.error(err);
  process.exit(1);
});
