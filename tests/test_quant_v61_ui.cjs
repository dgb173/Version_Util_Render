// Exercise the actual modal controller without a network or a running server.
const fs = require('node:fs');
const vm = require('node:vm');
const assert = require('node:assert/strict');
let modal;
let fail = false;
const alerts = [];
const document = {
    getElementById: () => modal,
    querySelector: () => null,
    createElement: () => ({innerHTML: ''}),
    body: {appendChild: el => { modal = el; }}
};
const calls = [];
const context = {
    document, console, setTimeout, alert: value => alerts.push(value),
    fetch: async (url, options) => {
        calls.push([url, options]);
        if (url.startsWith('/api/export_prompt/')) return {json: async () => ({success: true, prompt: 'Datos'})};
        if (fail) return {ok: false, json: async () => ({success: false, error: 'Sin datos'})};
        return {ok: true, json: async () => ({success: true,
            match: {home: '<img src=x onerror=alert(1)>', away: 'Away', league: 'L'},
            prediction: {status: 'CANDIDATO_SIMULACION', alerts: ['Research'], markets: [
                {market: 'OU', side: 'over', line: -2.75, fair_odds: 1.8, odds_decimal: 1.9, ev: .04, prob_profit: .5}],
                selections: {OU: {market: 'OU', side: 'over', line: -2.75, prob_profit: .5, ev: -.02, risk: 'ALTO'}},
                picks: {}, probabilities_1x2: {home: .4, draw: .3, away: .3}}
        })};
    }
};
context.window = context;
vm.createContext(context);
vm.runInContext(fs.readFileSync('src/static/js/motor_cuantitativo_v4.js', 'utf8'), context);
(async () => {
    await context.openMotorV4Modal(null, '123');
    assert.match(modal.innerHTML, /v6.1 RESEARCH/);
    assert.match(modal.innerHTML, /Over 2.75/);
    assert.match(modal.innerHTML, /PICK GOLES/);
    assert.match(modal.innerHTML, /sin confirmar/);
    assert.match(modal.innerHTML, /&lt;img/);
    assert.doesNotMatch(modal.innerHTML, /<img|Trampa Detectada|APUESTAS RECOMENDADAS/);
    assert.equal(calls[1][1].method, 'POST');
    assert.match(context._lastMotorV4Prompt, /RESEARCH/);
    modal = undefined;
    fail = true;
    await context.openMotorV4Modal(null, 'missing');
    assert.equal(modal, undefined); // Backend failure must never revive binary picks.
    assert.match(alerts[0], /Sin datos/);
    console.log('Quant modal: server integration, escaping, lines and failure path passed');
})().catch(err => { console.error(err); process.exitCode = 1; });
