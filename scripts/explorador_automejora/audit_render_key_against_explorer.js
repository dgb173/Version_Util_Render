#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const TRAINING_FILES = [
  'data_ah_0.json',
  'data_ah_0.5.json',
  'data_ah_1.5.json',
  'data_ah_2_plus.json',
  'data_minus_ah_0.5.json',
  'data_minus_ah_1.5.json',
  'data_minus_ah_2_plus.json',
];

function parseArgs(argv) {
  const args = {
    projectRoot: '.',
    outputJson: 'data/auditoria_render_explorador.json',
    outputMd: 'AUDITORIA_RENDER_EXPLORADOR.md',
  };
  for (let i = 2; i < argv.length; i++) {
    const key = argv[i];
    const val = argv[i + 1];
    if (key === '--project-root') { args.projectRoot = val; i++; }
    else if (key === '--output-json') { args.outputJson = val; i++; }
    else if (key === '--output-md') { args.outputMd = val; i++; }
  }
  return args;
}

function loadEngine(projectRoot) {
  const htmlPath = path.join(projectRoot, 'src', 'templates', 'precacheo.html');
  const html = fs.readFileSync(htmlPath, 'utf8');
  const start = html.indexOf('function parseH2HMovement');
  const end = html.indexOf('function renderTable(matches)');
  if (start < 0 || end < 0 || end <= start) {
    throw new Error('No se pudo extraer el bloque de funciones del render.');
  }
  const sandbox = { console, Math, Number, String, Array, Object, Set, parseInt, parseFloat, isNaN, Date };
  vm.createContext(sandbox);
  vm.runInContext(html.slice(start, end), sandbox);
  if (typeof sandbox.computeBookieSystemPick !== 'function') {
    throw new Error('computeBookieSystemPick no quedo disponible.');
  }
  return sandbox;
}

function parseScore(score) {
  const text = String(score || '').replace(' - ', ':').replace('-', ':').trim();
  if (!text || text.includes('?')) return null;
  const parts = text.split(':').map(v => Number.parseInt(v, 10));
  if (parts.length !== 2 || !Number.isFinite(parts[0]) || !Number.isFinite(parts[1])) return null;
  return { home: parts[0], away: parts[1], total: parts[0] + parts[1] };
}

function num(value) {
  if (value === null || value === undefined) return null;
  const text = String(value).replace(',', '.').trim();
  if (!text || text === '-' || text.includes('?')) return null;
  const n = Number.parseFloat(text);
  return Number.isFinite(n) ? n : null;
}

function ahFamily(ah) {
  const mag = Math.abs(ah ?? 0);
  if (mag < 0.01) return 'AH_0';
  if (mag <= 0.25) return 'AH_0_25';
  if (mag <= 0.75) return 'AH_0_5_0_75';
  if (mag <= 1.25) return 'AH_1_1_25';
  if (mag <= 1.75) return 'AH_1_5_1_75';
  return 'AH_2_PLUS';
}

function ouFamily(ou) {
  if (ou === null || ou === undefined) return 'OU_UNKNOWN';
  if (ou <= 2.25) return 'OU_LOW';
  if (ou <= 2.75) return 'OU_MID';
  if (ou <= 3.5) return 'OU_HIGH';
  return 'OU_EXTREME';
}

function richerScore(row) {
  return ['market_analysis_data', 'h2h_general', 'h2h_stadium', 'h2h_col3', 'comparativas_indirectas', 'last_home_match', 'last_away_match']
    .reduce((sum, key) => sum + (row && row[key] ? 1 : 0), 0);
}

function loadRows(projectRoot) {
  const byId = new Map();
  const audit = {};
  for (const file of TRAINING_FILES) {
    const full = path.join(projectRoot, 'data', file);
    if (!fs.existsSync(full)) continue;
    const raw = JSON.parse(fs.readFileSync(full, 'utf8'));
    const rows = Array.isArray(raw) ? raw : (raw.matches || raw.data || []);
    audit[file] = rows.length;
    rows.forEach((row, idx) => {
      if (!row || typeof row !== 'object') return;
      if (!parseScore(row.final_score || row.score)) return;
      const ah = num(row.main_match_odds?.ah_linea ?? row.handicap);
      if (ah === null) return;
      const id = String(row.match_id || row.id || `${file}:${idx}`);
      const enriched = { ...row, _source_file: file };
      const old = byId.get(id);
      if (!old || richerScore(enriched) > richerScore(old)) byId.set(id, enriched);
    });
  }
  return { rows: [...byId.values()], audit };
}

function sideOutcome(row, pick) {
  if (!pick.sidePick) return null;
  const score = parseScore(row.final_score || row.score);
  const ah = num(row.main_match_odds?.ah_linea ?? row.handicap);
  if (!score || ah === null) return null;
  if (pick.sideKind === 'FAVORITE') {
    const favMargin = ah > 0 ? score.home - score.away : score.away - score.home;
    const diff = favMargin - Math.abs(ah);
    if (diff >= 0.25) return 'HIT';
    if (diff <= -0.25) return 'MISS';
    return 'PUSH';
  }
  if (pick.sideKind === 'DOG') {
    const dogMargin = ah > 0 ? score.away - score.home : score.home - score.away;
    const diff = dogMargin + Math.abs(ah);
    if (diff >= 0.25) return 'HIT';
    if (diff <= -0.25) return 'MISS';
    return 'PUSH';
  }
  return null;
}

function goalsOutcome(row, pick) {
  if (!pick.goalsPick) return null;
  const score = parseScore(row.final_score || row.score);
  const ou = num(row.main_match_odds?.goals_linea ?? row.goals_line);
  if (!score || ou === null) return null;
  const diff = score.total - ou;
  if (Math.abs(diff) < 0.25) return 'PUSH';
  if (pick.goalsPick === 'OVER') return diff >= 0.25 ? 'HIT' : 'MISS';
  if (pick.goalsPick === 'UNDER') return diff <= -0.25 ? 'HIT' : 'MISS';
  return null;
}

function emptyStats() {
  return { bets: 0, hit: 0, miss: 0, push: 0 };
}

function addOutcome(stats, outcome) {
  if (!outcome) return;
  stats.bets += 1;
  if (outcome === 'HIT') stats.hit += 1;
  else if (outcome === 'MISS') stats.miss += 1;
  else if (outcome === 'PUSH') stats.push += 1;
}

function rate(stats) {
  const settled = stats.hit + stats.miss;
  return {
    ...stats,
    settled,
    hit_rate: settled ? Math.round((10000 * stats.hit / settled)) / 100 : null,
  };
}

function addCluster(map, key, sideOut, goalsOut) {
  if (!key) return;
  if (!map[key]) map[key] = { side: emptyStats(), goals: emptyStats() };
  addOutcome(map[key].side, sideOut);
  addOutcome(map[key].goals, goalsOut);
}

function topClusters(clusterMap, market, direction = 'weak') {
  return Object.entries(clusterMap)
    .map(([key, val]) => ({ key, ...rate(val[market]) }))
    .filter(row => row.settled >= 20)
    .sort((a, b) => {
      if (direction === 'weak') return (a.hit_rate ?? 101) - (b.hit_rate ?? 101) || b.settled - a.settled;
      return (b.hit_rate ?? -1) - (a.hit_rate ?? -1) || b.settled - a.settled;
    })
    .slice(0, 20);
}

function example(row, pick, sideOut, goalsOut) {
  return {
    match_id: row.match_id,
    date: row.match_date,
    source_file: row._source_file,
    match: `${row.home_name} vs ${row.away_name}`,
    score: row.final_score || row.score,
    ah: row.main_match_odds?.ah_linea ?? row.handicap,
    ou: row.main_match_odds?.goals_linea ?? row.goals_line,
    side_pick: pick.sidePick || null,
    side_kind: pick.sideKind,
    side_outcome: sideOut || 'NO_BET',
    goals_pick: pick.goalsPick || null,
    goals_outcome: goalsOut || 'NO_BET',
    confidence: pick.confidence,
    reasons: pick.reasons || [],
    risk: pick.risk || [],
    sideDiff: pick.sideDiff,
    goalsDiff: pick.goalsDiff,
  };
}

function writeMarkdown(payload, outputPath) {
  const lines = [];
  lines.push('# Auditoria masiva del render contra Explorador');
  lines.push('');
  lines.push(`Generado: ${payload.generated_at}`);
  lines.push('');
  lines.push('## Resumen');
  lines.push('');
  for (const [key, value] of Object.entries(payload.summary)) {
    lines.push(`- ${key}: ${typeof value === 'object' ? JSON.stringify(value) : value}`);
  }
  lines.push('');
  lines.push('## AH por familia');
  lines.push('');
  for (const row of payload.by_ah_family) {
    lines.push(`- ${row.key}: ${row.hit}/${row.settled} = ${row.hit_rate}% (push ${row.push}, bets ${row.bets})`);
  }
  lines.push('');
  lines.push('## O/U por familia');
  lines.push('');
  for (const row of payload.by_ou_family) {
    lines.push(`- ${row.key}: ${row.hit}/${row.settled} = ${row.hit_rate}% (push ${row.push}, bets ${row.bets})`);
  }
  lines.push('');
  lines.push('## Clusters debiles AH');
  lines.push('');
  for (const row of payload.weak_side_clusters) {
    lines.push(`- ${row.key}: ${row.hit}/${row.settled} = ${row.hit_rate}% (bets ${row.bets})`);
  }
  lines.push('');
  lines.push('## Clusters fuertes AH');
  lines.push('');
  for (const row of payload.strong_side_clusters) {
    lines.push(`- ${row.key}: ${row.hit}/${row.settled} = ${row.hit_rate}% (bets ${row.bets})`);
  }
  lines.push('');
  lines.push('## Clusters debiles O/U');
  lines.push('');
  for (const row of payload.weak_goals_clusters) {
    lines.push(`- ${row.key}: ${row.hit}/${row.settled} = ${row.hit_rate}% (bets ${row.bets})`);
  }
  lines.push('');
  lines.push('## Clusters fuertes O/U');
  lines.push('');
  for (const row of payload.strong_goals_clusters) {
    lines.push(`- ${row.key}: ${row.hit}/${row.settled} = ${row.hit_rate}% (bets ${row.bets})`);
  }
  lines.push('');
  lines.push('## Ejemplos de fallos AH');
  lines.push('');
  for (const ex of payload.side_miss_examples.slice(0, 25)) {
    lines.push(`- ${ex.match_id} ${ex.match} ${ex.score} | AH ${ex.ah} | pick ${ex.side_pick} | ${ex.reasons.slice(0, 3).join(' / ')} | riesgos ${ex.risk.slice(0, 3).join(' / ')}`);
  }
  lines.push('');
  lines.push('## Ejemplos de fallos O/U');
  lines.push('');
  for (const ex of payload.goals_miss_examples.slice(0, 25)) {
    lines.push(`- ${ex.match_id} ${ex.match} ${ex.score} | OU ${ex.ou} | pick ${ex.goals_pick} | ${ex.reasons.slice(0, 3).join(' / ')} | riesgos ${ex.risk.slice(0, 3).join(' / ')}`);
  }
  fs.writeFileSync(outputPath, lines.join('\n') + '\n', 'utf8');
}

function main() {
  const args = parseArgs(process.argv);
  const projectRoot = path.resolve(args.projectRoot);
  const engine = loadEngine(projectRoot);
  const { rows, audit } = loadRows(projectRoot);

  const summary = {
    rows: rows.length,
    errors: 0,
    no_bet_both: 0,
    side: emptyStats(),
    goals: emptyStats(),
    side_picks: 0,
    goals_picks: 0,
  };
  const clusters = {};
  const byAh = {};
  const byOu = {};
  const sideMissExamples = [];
  const goalsMissExamples = [];
  const sideHitExamples = [];
  const goalsHitExamples = [];

  for (const row of rows) {
    try {
      const ah = num(row.main_match_odds?.ah_linea ?? row.handicap);
      const ou = num(row.main_match_odds?.goals_linea ?? row.goals_line);
      const pick = engine.computeBookieSystemPick(row, row, row.main_match_odds?.ah_linea ?? row.handicap, {});
      const sideOut = sideOutcome(row, pick);
      const goalsOut = goalsOutcome(row, pick);
      if (pick.sidePick) summary.side_picks += 1;
      if (pick.goalsPick) summary.goals_picks += 1;
      if (!pick.sidePick && !pick.goalsPick) summary.no_bet_both += 1;
      addOutcome(summary.side, sideOut);
      addOutcome(summary.goals, goalsOut);
      const ahKey = ahFamily(ah);
      const ouKey = ouFamily(ou);
      if (!byAh[ahKey]) byAh[ahKey] = emptyStats();
      if (!byOu[ouKey]) byOu[ouKey] = emptyStats();
      addOutcome(byAh[ahKey], sideOut);
      addOutcome(byOu[ouKey], goalsOut);
      addCluster(clusters, `CONF=${pick.confidence}`, sideOut, goalsOut);
      addCluster(clusters, `SIDE=${pick.sideKind || 'NO_BET'}`, sideOut, goalsOut);
      addCluster(clusters, `GOALS=${pick.goalsPick || 'NO_BET'}`, sideOut, goalsOut);
      addCluster(clusters, `AH=${ahKey}`, sideOut, goalsOut);
      addCluster(clusters, `OU=${ouKey}`, sideOut, goalsOut);
      for (const label of [...(pick.reasons || []), ...(pick.risk || [])]) {
        addCluster(clusters, `TAG=${label}`, sideOut, goalsOut);
      }
      const ex = example(row, pick, sideOut, goalsOut);
      if (sideOut === 'MISS' && sideMissExamples.length < 100) sideMissExamples.push(ex);
      if (goalsOut === 'MISS' && goalsMissExamples.length < 100) goalsMissExamples.push(ex);
      if (sideOut === 'HIT' && sideHitExamples.length < 50) sideHitExamples.push(ex);
      if (goalsOut === 'HIT' && goalsHitExamples.length < 50) goalsHitExamples.push(ex);
    } catch (err) {
      summary.errors += 1;
    }
  }

  const payload = {
    generated_at: new Date().toISOString(),
    source_files: audit,
    summary: {
      ...summary,
      side: rate(summary.side),
      goals: rate(summary.goals),
    },
    by_ah_family: Object.entries(byAh).map(([key, val]) => ({ key, ...rate(val) })).sort((a, b) => a.key.localeCompare(b.key)),
    by_ou_family: Object.entries(byOu).map(([key, val]) => ({ key, ...rate(val) })).sort((a, b) => a.key.localeCompare(b.key)),
    weak_side_clusters: topClusters(clusters, 'side', 'weak'),
    strong_side_clusters: topClusters(clusters, 'side', 'strong'),
    weak_goals_clusters: topClusters(clusters, 'goals', 'weak'),
    strong_goals_clusters: topClusters(clusters, 'goals', 'strong'),
    side_miss_examples: sideMissExamples,
    goals_miss_examples: goalsMissExamples,
    side_hit_examples: sideHitExamples,
    goals_hit_examples: goalsHitExamples,
  };

  const outJson = path.resolve(projectRoot, args.outputJson);
  fs.mkdirSync(path.dirname(outJson), { recursive: true });
  fs.writeFileSync(outJson, JSON.stringify(payload, null, 2), 'utf8');
  const outMd = path.resolve(projectRoot, args.outputMd);
  writeMarkdown(payload, outMd);
  console.log(`[OK] JSON: ${outJson}`);
  console.log(`[OK] MD: ${outMd}`);
  console.log(`[INFO] rows=${payload.summary.rows} side=${payload.summary.side.hit}/${payload.summary.side.settled} (${payload.summary.side.hit_rate}%) goals=${payload.summary.goals.hit}/${payload.summary.goals.settled} (${payload.summary.goals.hit_rate}%) no_bet=${payload.summary.no_bet_both}`);
}

main();
