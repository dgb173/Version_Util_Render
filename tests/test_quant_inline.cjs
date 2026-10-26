const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const rows = [{dataset: {}}, {dataset: {}}];
const slots = rows.map(row => ({dataset: {quantMatchId: '123'}, isConnected: true,
    innerHTML: '', closest: () => row}));
let observer;
let filterCalls = 0;
let fetches = 0;
const events = {};
const context = {console, setTimeout, Date, Map, encodeURIComponent, decodeURIComponent,
    document: {
        body: {},
        querySelectorAll: () => slots.filter(s => !s.dataset.quantLoaded),
        querySelector: () => slots.find(s => !s.dataset.quantLoaded),
        addEventListener: (name, fn) => { events[name] = fn; }
    },
    MutationObserver: class { constructor(fn) { observer = fn; } observe() {} },
    applyFiltersVisually: () => { filterCalls++; },
    fetch: async (url, options) => {
        fetches++;
        assert.equal(url, '/api/quant/bulk');
        assert.deepEqual(JSON.parse(options.body).match_ids, ['123']);
        return {ok: true, json: async () => ({success: true, predictions: {'123': {
            selections: {AH: {market: 'AH', side: 'home', line: .25, prob_profit: .65},
                OU: {market: 'OU', side: 'over', line: -2.75, prob_profit: .55}},
            favorite_side: 'away', risk: 'ALTO', match: {home: 'Home', away: 'Away'},
            telemetry: {used_in_prediction: true}, alerts: []
        }}})};
    }
};
context.window = context;
vm.createContext(context);
vm.runInContext(fs.readFileSync('src/static/js/quant_inline.js', 'utf8'), context);
(async () => {
    await new Promise(resolve => setTimeout(resolve, 120));
    assert.equal(fetches, 1);
    assert.match(slots[0].innerHTML, /AH: Home \+0.25/);
    assert.match(slots[0].innerHTML, /OU: Over 2.75/);
    assert.equal(rows[0].dataset.hasAhPick, '1');
    assert.equal(rows[0].dataset.pickSideKind, 'DOG');
    assert.equal(rows[0].dataset.pickGoals, 'OVER');
    assert.equal(rows[0].dataset.pickConfidence, 'BAJA');
    observer();
    await new Promise(resolve => setTimeout(resolve, 100));
    assert.equal(filterCalls, 1); // No observer/filter render loop after hydration.
    assert.equal(fetches, 1);
    console.log('Existing Pick column: hydration, duplicate IDs, filters and observer stability passed');
})().catch(err => { console.error(err); process.exitCode = 1; });
