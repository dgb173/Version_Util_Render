/* Shared, compact Col3 / movement cards. Results keep their home-away order. */
const ContextComparisonCards = (() => {
    const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const number = value => value === null || value === undefined || String(value).trim() === '' ? null : (Number.isFinite(Number(value)) ? Number(value) : null);
    const same = (a, b) => {
        a = String(a || '').trim().toLowerCase(); b = String(b || '').trim().toLowerCase();
        return !!a && !!b && (a === b || a.includes(b) || b.includes(a));
    };
    const scoreParts = value => {
        const m = String(value || '').match(/^\s*(\d+)\s*[:-]\s*(\d+)\s*$/);
        return m ? [Number(m[1]), Number(m[2])] : null;
    };
    const rank = (score, home) => {
        const p = scoreParts(score);
        if (!p || home === null) return null;
        const delta = home ? p[0] - p[1] : p[1] - p[0];
        return delta > 0 ? 2 : delta < 0 ? 0 : 1;
    };
    const verdict = (actual, previous) => actual === null || previous === null
        ? {state:'is-unknown', label:'SIN COMPARACIÓN', code:'N/D'}
        : actual > previous ? {state:'is-better', label:'MEJORA', code:'MEJORA'}
        : actual < previous ? {state:'is-worse', label:'EMPEORA', code:'EMPEORA'}
        : {state:'is-equal', label:'SIMILAR', code:'IGUALA'};
    const previousMatch = row => ({
        home: row?.home_team ?? row?.home ?? '', away: row?.away_team ?? row?.away ?? '',
        score: row?.score ?? row?.score_raw ?? '', date: row?.date ?? '',
    });
    const compactCol3 = col3 => {
        if (!col3 || (col3.status && col3.status !== 'found')) return null;
        const fields = ['status','h2h_home_team_name','h2h_away_team_name','goles_home','goles_away','handicap','date','match_id','matchIndex'];
        return Object.fromEntries(fields.filter(k => col3[k] !== undefined).map(k => [k, col3[k]]));
    };
    function reference(pc, home, away, ah) {
        const current = pc?.pre_match_context?.current || pc?.pre_match_context || {};
        return {
            h2h_col3: compactCol3(pc?.h2h_col3 || pc?.h2h_col3_general),
            match: {home_name:home, away_name:away}, currentAh:ah,
            comparison: {
                last_home_match: previousMatch(pc?.last_home_match || pc?.last_general_home || current.home_matches?.[0]),
                last_away_match: previousMatch(pc?.last_away_match || pc?.last_general_away || current.away_matches?.[0]),
            },
        };
    }
    function readReference(button) {
        try { return JSON.parse(button.closest('[data-comparison-reference]')?.dataset.comparisonReference || 'null'); }
        catch (_) { return null; }
    }
    const referenceRequests = new WeakMap();
    function ensureReference(button) {
        const panel = button.closest('[data-comparison-reference]');
        const ref = readReference(button);
        if (!panel || ref?.h2h_col3?.status === 'found') return Promise.resolve(ref);
        if (referenceRequests.has(panel)) return referenceRequests.get(panel);
        const id = String(button.dataset.parentMatchId || '').replace(/\D/g, '');
        if (!id) return Promise.resolve(ref);
        const pending = (async () => {
            try {
                const response = await fetch('/api/precacheo_h2h_col3', {
                    method:'POST', headers:{'Content-Type':'application/json'},
                    body:JSON.stringify({match_id:id}),
                });
                const data = await response.json();
                if (!response.ok || data.status !== 'success') return ref;
                const current = {...data, currentAh:ref?.currentAh ?? button.dataset.currentAh};
                // Each panel owns its reference; loading another match cannot replace it.
                panel.dataset.comparisonReference = JSON.stringify(current);
                return current;
            } catch (_) { return ref; }
        })();
        referenceRequests.set(panel, pending);
        return pending;
    }
    const sideOf = (row, team) => same(row.home, team) ? true : same(row.away, team) ? false : null;
    function col3Model(dataset, data) {
        const col3 = data?.h2h_col3;
        if (!col3 || (col3.status && col3.status !== 'found') || !col3.h2h_home_team_name || !col3.h2h_away_team_name) return null;
        const home = col3.h2h_home_team_name, away = col3.h2h_away_team_name;
        const parentHome = dataset.parentHome || data?.match?.home_name || '';
        const parentAway = dataset.parentAway || data?.match?.away_name || '';
        const ah = number(dataset.currentAh);
        const favHome = ah === null ? null : ah > 0;
        const lastHome = previousMatch(data?.comparison?.last_home_match);
        const lastAway = previousMatch(data?.comparison?.last_away_match);
        const own = favHome ? lastHome : lastAway;
        const other = favHome ? lastAway : lastHome;
        const ownTeam = favHome ? parentHome : parentAway;
        const otherSide = sideOf(other, favHome ? parentAway : parentHome);
        const mirror = otherSide === true ? other.away : otherSide === false ? other.home : '';
        const mirrorSide = same(home, mirror) ? true : same(away, mirror) ? false : null;
        const score = `${col3.goles_home ?? '?'}:${col3.goles_away ?? '?'}`;
        const status = verdict(favHome === null ? null : rank(own.score, sideOf(own, ownTeam)), rank(score, mirrorSide));
        const orientation = favHome === null || mirrorSide === null ? 'N/D' : favHome === mirrorSide ? 'Directa' : 'Inversa';
        const rivalOf = (row, team) => sideOf(row, team) === true ? row.away : sideOf(row, team) === false ? row.home : '';
        const homeRival = rivalOf(lastHome, parentHome), awayRival = rivalOf(lastAway, parentAway);
        const role = team => same(team, homeRival) ? `Rival de ${parentHome}` : same(team, awayRival) ? `Rival de ${parentAway}` : 'Rival';
        const colAh = number(col3.handicap), goals = scoreParts(score);
        let cover = '';
        if (colAh !== null && goals) {
            const margin = colAh > 0 ? goals[0] - goals[1] : goals[1] - goals[0];
            cover = margin > Math.abs(colAh) ? 'Cubrió' : margin < Math.abs(colAh) ? 'No cubrió' : 'Push';
        }
        const normalized = value => {
            const absolute = Math.abs(value), decimal = absolute % 1;
            return decimal >= .2 && decimal <= .8 ? Math.floor(absolute) + .5 : Math.round(absolute);
        };
        let movement = '';
        if (ah !== null && colAh !== null) {
            const delta = normalized(ah) - normalized(colAh);
            movement = `${Math.abs(delta) < .01 ? '= Igual' : delta > 0 ? '↑ Sube' : '↓ Baja'} · AH Col3 ${col3.handicap} → actual ${dataset.currentAh}`;
        }
        return {home, away, score, ah:col3.handicap ?? '—', date:col3.date || 'Fecha N/D',
            id:col3.match_id || col3.matchIndex || '', status, orientation, cover, movement,
            homeRole:role(home), awayRole:role(away),
            homeTone:same(home, homeRival) ? 'home-rival' : 'away-rival',
            awayTone:same(away, homeRival) ? 'home-rival' : 'away-rival'};
    }
    function header(title, badge = '') {
        return `<div class="pre-col3-card-head comparison-head"><span class="pre-col3-status">${esc(title)}</span>
            <div class="comparison-head-actions">${badge ? `<span class="pre-col3-badge comparison-type">${esc(badge)}</span>` : ''}
            <button type="button" class="comparison-close" onclick="closeDetailSlot(this.closest('.pre-context-detail-slot'), event)" aria-label="Ocultar ${esc(title)}">× Ocultar</button></div></div>`;
    }
    function team(name, tone = '') {
        const size = String(name).length > 22 ? '.52rem' : String(name).length > 16 ? '.56rem' : '.62rem';
        return `<span class="comparison-team ${tone}" style="--team-size:${size}" title="${esc(name)}">${esc(name || 'N/D')}</span>`;
    }
    function matchLine(model) {
        return `<div class="comparison-match-line">${team(model.home, model.homeTone)}<b class="comparison-score">${esc(model.score || '?:?')}</b>${team(model.away, model.awayTone)}<span class="comparison-ah"><small>AH</small> ${esc(model.ah ?? '—')}</span></div>`;
    }
    function statsButton(model) {
        const id = String(model.id || '').replace(/\D/g, '');
        if (!id) return '';
        return `<div class="pre-col3-stats-wrap" onclick="event.stopPropagation();"><button type="button" class="pre-card-stats-toggle"
            data-col3-home="${esc(model.home)}" data-col3-away="${esc(model.away)}"
            onclick="toggleCardStats(this, event, '${id}')">▥ Estadísticas</button><div class="pre-card-stats-container" style="display:none;"></div></div>`;
    }
    function box(model, title, extra = '', referenceBox = false) {
        const summary = `${title}: ${model.date} | ${model.home} ${model.score} ${model.away} | AH ${model.ah} | ${model.status.label} | ${model.orientation || ''}`;
        return `<section class="comparison-box ${model.status.state}${referenceBox ? ' comparison-reference' : ''}" data-performance="${model.status.code}" data-comparison-export="${esc(summary)}">
            <div class="comparison-box-label"><b>${esc(title)}</b><span class="comparison-verdict">${esc(model.status.label)}</span></div>
            <div class="comparison-meta"><time>${esc(model.date)}</time>${model.orientation ? `<span>${esc(model.orientation)}</span>` : ''}${model.cover ? `<span>${esc(model.cover)}</span>` : ''}</div>
            ${matchLine(model)}
            ${model.homeRole ? `<div class="comparison-roles"><span title="${esc(model.homeRole)}">${esc(model.homeRole)}</span><span title="${esc(model.awayRole)}">${esc(model.awayRole)}</span></div>` : ''}
            ${extra}${statsButton(model)}</section>`;
    }
    function buildCol3(button, data) {
        const selected = col3Model(button.dataset, data);
        const ref = readReference(button);
        const current = ref ? col3Model({...button.dataset, currentAh:ref.currentAh}, ref) : null;
        return `<div class="context-comparison-card" data-performance="${selected?.status.code || 'N/D'}">${header('COL3', selected?.orientation || 'N/D')}
            ${selected ? box(selected, 'Col3 de esta fila', selected.movement ? `<div class="comparison-movement-line">${esc(selected.movement)}</div>` : '') : '<div class="comparison-empty">Sin Col3 para esta fila</div>'}
            ${current ? box(current, 'Col3 actual', '', true) : '<div class="comparison-empty comparison-reference">Col3 actual no disponible</div>'}
            <div class="comparison-key">Color: resultado del favorito frente al rival espejo (V/E/D).</div></div>`;
    }
    function movementModel(dataset, item) {
        const row = {home:dataset.rowHome || '', away:dataset.rowAway || '', score:dataset.rowScore || '?:?'};
        const rowAh = number(dataset.rowAh);
        const favorite = rowAh === null ? '' : rowAh > 0 ? row.home : row.away;
        const home = item.home_team || '', away = item.away_team || '';
        const past = {home, away, score:item.score || '?:?'};
        const status = verdict(rank(row.score, sideOf(row, favorite)), rank(past.score, sideOf(past, favorite)));
        const previousAh = number(item.ah);
        let movement = 'Movimiento AH: N/D';
        if (rowAh !== null && previousAh !== null) {
            const difference = Math.abs(rowAh) - Math.abs(previousAh);
            movement = `${Math.abs(difference) < .01 ? '= Igual' : difference > 0 ? '↑ Sube' : '↓ Baja'} · AH ${item.ah} → ${dataset.rowAh}`;
        }
        const cover = item.cover === true || ['CUBIERTO','WIN'].includes(item.cover) ? 'Cubrió' : item.cover === false || ['NO_CUBIERTO','NO_WIN'].includes(item.cover) ? 'No cubrió' : item.cover === 'PUSH' ? 'Push' : '';
        return {home, away, score:past.score, ah:item.ah ?? '—', date:item.date || 'Fecha N/D', id:item.match_id || '', status, cover, movement};
    }
    function buildMov(button, data) {
        const dataset = button.dataset;
        const sections = [['Mismo estadio', data?.stadium], ['Estadio diferente', data?.general]].filter(([, item]) => item);
        const content = sections.map(([label, item]) => {
            const historical = movementModel(dataset, item);
            // Keep names in their actual home-away positions; never swap only a score.
            const actual = {home:dataset.rowHome, away:dataset.rowAway, score:dataset.rowScore || '?:?', ah:dataset.rowAh || '—', date:dataset.rowDate || 'Fecha N/D', status:historical.status};
            return `<div class="comparison-movement" data-performance="${historical.status.code}">
                ${box({...historical, status:{state:'is-equal',label:'ANTECEDENTE',code:'N/D'}}, label)}
                ${box(actual, 'Partido de esta fila', `<div class="comparison-movement-line">${esc(historical.movement)}</div>`, true)}</div>`;
        }).join('');
        return `<div class="context-comparison-card">${header('MOV · H2H', 'Comparativa')}${content || '<div class="comparison-empty">Sin H2H previo entre ellos</div>'}<div class="comparison-key">Color: V/E/D del favorito de esta fila frente al antecedente. AH: antes → fila.</div></div>`;
    }
    return {reference, ensureReference, col3Model, movementModel, buildCol3, buildMov};
})();
if (typeof module !== 'undefined' && module.exports) module.exports = ContextComparisonCards;
