const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const jsCode = fs.readFileSync(
  path.join(__dirname, '..', 'src', 'static', 'js', 'ficha_completa.js'),
  'utf8'
);
const htmlCode = fs.readFileSync(
  path.join(__dirname, '..', 'src', 'templates', 'ficha_completa.html'),
  'utf8'
);
const cssCode = fs.readFileSync(
  path.join(__dirname, '..', 'src', 'static', 'css', 'ficha_completa.css'),
  'utf8'
);

// 1. Verificar estructura de template y separacion estricta de colores en CSS
assert.ok(!htmlCode.includes('id="fc-btn-tags"'), 'No debe existir boton Etiquetas en Ficha Completa');
assert.ok(!htmlCode.includes('id="fc-tags-slot"'), 'No debe existir fc-tags-slot en Ficha Completa');
assert.ok(!jsCode.includes('handleTagsToggle'), 'handleTagsToggle debe haberse eliminado de ficha_completa.js');
assert.ok(htmlCode.includes('data-tab="h2h"'), 'Falta pestana H2H Anos Ant.');
assert.ok(htmlCode.includes('id="fc-tab-h2h-count"'), 'Falta contador #fc-tab-h2h-count');
assert.ok(htmlCode.includes('id="fc-inspector-clear-all"'), 'Falta boton #fc-inspector-clear-all');
assert.ok(htmlCode.indexOf('data-tab="teams"') < htmlCode.indexOf('data-tab="common"'), 'Comparar Equipos debe ir antes de Rivales Comunes');

assert.ok(cssCode.includes('.fc-ou-pill.is-OVER') && cssCode.includes('#115e59'), 'OVER debe usar verde azulado (Teal)');
assert.ok(cssCode.includes('.fc-ou-pill.is-UNDER') && cssCode.includes('#5b21b6'), 'UNDER debe usar violeta (no rojo)');

// 2. Ejecutar ficha_completa.js en VM aislada
const windowObj = {
  FICHA_COMPLETA_MATCH_ID: '3114552',
};
const sandbox = {
  window: windowObj,
  document: {
    getElementById: () => null,
    querySelectorAll: () => [],
    addEventListener: () => {},
  },
  localStorage: {
    getItem: () => null,
    setItem: () => {},
  },
  fetch: async () => ({ ok: true, json: async () => ({}) }),
  console,
  setTimeout,
  clearTimeout,
};
sandbox.globalThis = sandbox;
windowObj.window = windowObj;

vm.runInNewContext(jsCode, sandbox);
const H = windowObj.FichaCompletaHelpers;
assert.ok(H, 'window.FichaCompletaHelpers debe exponerse');

// 3. Test formatAhPerspective
{
  const hFav = H.formatAhPerspective('0.5', true, 'team');
  assert.equal(hFav.role, 'FAV');
  assert.equal(hFav.label, 'FAV -0.5');
  assert.ok(hFav.html.includes('is-FAV'));

  const aDog = H.formatAhPerspective('0.5', false, 'team');
  assert.equal(aDog.role, 'DOG');
  assert.equal(aDog.label, 'DOG +0.5');
  assert.ok(aDog.html.includes('NG 0.5'));

  const aFav = H.formatAhPerspective('-0.25', false, 'team');
  assert.equal(aFav.role, 'FAV');
  assert.equal(aFav.label, 'FAV -0.25');
  assert.ok(aFav.html.includes('is-FAV'));

  const pk = H.formatAhPerspective('0', true, 'team');
  assert.equal(pk.role, 'PK');
  assert.equal(pk.label, 'PK 0');

  const ngMode = H.formatAhPerspective('-0.25', false, 'nowgoal');
  assert.equal(ngMode.label, 'NG -0.25');
  assert.ok(ngMode.html.includes('FAV -0.25'));
}

// 4. Test formatOrientedScore
{
  const sHome = H.formatOrientedScore('3:1', true, 'W', 0, 1);
  assert.ok(sHome.includes('is-W') && sHome.includes('>V<'));
  assert.ok(sHome.includes('fc-score-own">3</span>'));
  assert.ok(sHome.includes('fc-score-opp">1</span>'));
  assert.ok(sHome.includes('fc-red-card'));

  const sAway = H.formatOrientedScore('1:2', false, 'W');
  assert.ok(sAway.includes('fc-score-own">2</span>'));
  assert.ok(sAway.includes('fc-score-opp">1</span>'));
}

// 5. Test formatMovBadge
{
  const up = H.formatMovBadge('0.25 → 0.5', 0.25, 'SUBE');
  assert.ok(up.includes('is-SUBE') && up.includes('▲'));

  const down = H.formatMovBadge('1 → 0.5', -0.5, '');
  assert.ok(down.includes('is-BAJA') && down.includes('▼'));

  const eq = H.formatMovBadge('0.5 → 0.5', 0, 'IGUAL');
  assert.ok(eq.includes('is-IGUAL') && eq.includes('='));
}

// 6. Test filterRowsByPerspectiveAndOutcome
{
  const rows = [
    { rival: 'A', is_home: true, ah: '0.5', team_ah: 0.5, team_role: 'FAV', ah_cover: 'COVER', ou_res: 'OVER', is_common_all: true, is_similar_ah: true },
    { rival: 'B', is_home: false, ah: '0.25', team_ah: -0.25, team_role: 'DOG', ah_cover: 'NO_COVER', ou_res: 'UNDER', is_common_all: false, is_similar_ah: false },
    { rival: 'C', is_home: true, ah: '1', team_ah: 1, team_role: 'FAV', ah_cover: 'NO_COVER', ou_res: 'UNDER', is_common_all: true, is_similar_ah: false },
  ];
  assert.equal(H.filterRowsByPerspectiveAndOutcome(rows, { roleFilter: 'FAV' }).length, 2);
  assert.equal(H.filterRowsByPerspectiveAndOutcome(rows, { roleFilter: 'DOG' }).length, 1);
  assert.equal(H.filterRowsByPerspectiveAndOutcome(rows, { outcomeFilter: 'COVER' }).length, 1);
  assert.equal(H.filterRowsByPerspectiveAndOutcome(rows, { outcomeFilter: 'NO_COVER' }).length, 2);
  assert.equal(H.filterRowsByPerspectiveAndOutcome(rows, { outcomeFilter: 'OVER' }).length, 1);
  assert.equal(H.filterRowsByPerspectiveAndOutcome(rows, { outcomeFilter: 'UNDER' }).length, 2);
}

// 7. Test findCommonBetweenSides, renderTabTeams & renderTabH2hHistory
{
  const matchesA = [
    { match_id: '1', date: '2026-11-16', league_id: '2872', venue: 'home', is_home: true, home_team: 'Gamba Osaka', away_team: 'Shimizu S-Pulse', rival: 'Shimizu S-Pulse', score: '4:0', wdl: 'W', ah: '2', team_ah: 2, team_role: 'FAV', ou: '3.5', ou_res: 'OVER', goals_for: 4, goals_against: 0, ah_eval: { code: 'COVER' } },
    { match_id: '2', date: '2026-11-01', league_id: '2872', venue: 'away', is_home: false, home_team: 'FC Tokyo', away_team: 'Gamba Osaka', rival: 'FC Tokyo', score: '2:2', wdl: 'D', ah: '0.25', team_ah: -0.25, team_role: 'DOG', ou: '3.25', ou_res: 'OVER', goals_for: 2, goals_against: 2, ah_eval: { code: 'COVER' } },
  ];
  const matchesB = [
    { match_id: '3', date: '2026-11-10', league_id: '2872', venue: 'away', is_home: false, home_team: 'Shimizu S-Pulse', away_team: 'Tokyo Verdy', rival: 'Shimizu S-Pulse', score: '2:1', wdl: 'L', ah: '0.25', team_ah: -0.25, team_role: 'DOG', ou: '3.25', ou_res: 'UNDER', goals_for: 1, goals_against: 2, ah_eval: { code: 'NO_COVER' } },
  ];
  const common = H.findCommonBetweenSides(matchesA, matchesB);
  assert.equal(common.pairs.length, 1);
  assert.equal(common.pairs[0].rival, 'Shimizu S-Pulse');

  H.setStateForTest({
    ahViewMode: 'team',
    data: {
      match_id: '3114552',
      target: {
        match_id: '3114552',
        home: 'Gamba Osaka',
        away: 'Tokyo Verdy',
        date: '2026-11-22',
        ah: '0.5',
        home_target_ah: 0.5,
        ou: '3.25',
        score: '?:?',
      },
      selectable_teams: [
        { name: 'Gamba Osaka', role: 'target_home', count: 2 },
        { name: 'Tokyo Verdy', role: 'target_away', count: 1 },
        { name: 'Shimizu S-Pulse', role: 'common_rival', count: 2 },
      ],
      common_rivals: [{ rival: 'Shimizu S-Pulse' }],
      h2h_history: [
        {
          match_id: '601',
          date: '2025-10-10',
          short_date: '10/10',
          year: '2025',
          league: 'J-League',
          home_team: 'Gamba Osaka',
          away_team: 'Tokyo Verdy',
          is_same_stadium: true,
          score: '2:1',
          wdl_local: 'W',
          ah: '0.25',
          home_persp_ah: 0.25,
          ah_delta_vs_today: 0.25,
          ah_mov_dir_vs_today: 'SUBE',
          ah_movement_vs_today: '0.25 → 0.5',
          ah_delta_vs_prev: 0.5,
          cover_own: 'COVER',
          cover_today: 'COVER',
          ou: '2.5',
          ou_delta_vs_today: 0.75,
          ou_mov_dir_vs_today: 'SUBE',
          ou_movement_vs_today: '2.5 → 3.25',
          ou_res: 'OVER',
          ou_res_vs_today: 'UNDER',
        },
        {
          match_id: '600',
          date: '2024-05-03',
          short_date: '03/05',
          year: '2024',
          league: 'J-League',
          home_team: 'Tokyo Verdy',
          away_team: 'Gamba Osaka',
          is_same_stadium: false,
          score: '1:0',
          wdl_local: 'L',
          ah: '0.25',
          home_persp_ah: -0.25,
          ah_delta_vs_today: 0.75,
          ah_mov_dir_vs_today: 'SUBE',
          ah_movement_vs_today: '-0.25 → 0.5',
          ah_delta_vs_prev: null,
          cover_own: 'NO_COVER',
          cover_today: 'NO_COVER',
          ou: '2.75',
          ou_delta_vs_today: 0.5,
          ou_mov_dir_vs_today: 'SUBE',
          ou_movement_vs_today: '2.75 → 3.25',
          ou_res: 'UNDER',
          ou_res_vs_today: 'UNDER',
        },
      ],
    },
    comparator: {
      A: { teamName: 'Gamba Osaka', venue: 'all', data: { matches: matchesA }, loading: false, error: null },
      B: { teamName: 'Tokyo Verdy', venue: 'all', data: { matches: matchesB }, loading: false, error: null },
      roleFilter: 'all',
      onlyCommonAB: false,
      searchTeam: '',
      showMirror: true,
      customCol3: null,
      customCol3Loading: false,
    },
  });

  const teamsHtml = H.renderTabTeams();
  assert.ok(teamsHtml.includes('fc-vs-card'), 'Debe renderizar tarjeta VS');
  assert.ok(teamsHtml.includes('Espejo Directo: Frente a los Mismos Rivales'), 'Debe renderizar espejo A vs B');
  assert.ok(teamsHtml.includes('fc-ou-pill is-OVER') && teamsHtml.includes('fc-ou-pill is-UNDER'), 'O/U en comparador no debe ir fijo en is-SIN_DATO');
  assert.ok(teamsHtml.includes('→A') && teamsHtml.includes('→B'), 'Deben existir micro-botones ->A y ->B');

  const h2hHtml = H.renderTabH2hHistory();
  assert.ok(h2hHtml.includes('fc-h2h-hero'), 'Debe renderizar tarjetas hero H2H');
  assert.ok(h2hHtml.includes('2025') && h2hHtml.includes('2024'), 'Debe mostrar anos historicos');
  assert.ok(h2hHtml.includes('Mismo Estadio') && h2hHtml.includes('Invertido'), 'Debe distinguir Mismo Estadio vs Invertido');
  assert.ok(h2hHtml.includes('0.25 → 0.5') && h2hHtml.includes('is-SUBE'), 'Debe renderizar movimientos AH vs hoy');

  const movHtml = H.renderMovSlotHtml({
    stadium: { date: '2025-10-10', score: '2:1', ah: '0.25', ou: '2.5', movement: '0.25 → 0.5', movement_dir: 'SUBE', delta_ah: 0.25, ou_movement: '2.5 → 3.25', ou_dir: 'SUBE', delta_ou: 0.75, cover: true },
    history: [{ year: '2025', same_stadium: true, score: '2:1', ah: '0.25', delta_ah: 0.25, ou: '2.5' }],
  }, { homeTeam: 'Gamba Osaka', awayTeam: 'Tokyo Verdy' });
  assert.ok(movHtml.includes('fc-mov-slot-box') && movHtml.includes('Historial H2H año a año'));
}

console.log('OK: test_ficha_completa_ui.cjs superado al 100%');
