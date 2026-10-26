/** Hydrate the existing Pick column with current server forecasts. */
(() => {
    const esc = v => String(v ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#039;');
    let busy = false;
    let scheduled = false;
    const cache = new Map();
    const pct = v => Number.isFinite(v) ? (100 * v).toFixed(0) + '%' : 'N/D';

    function render(slot, id, result) {
        if (!slot.isConnected) return;
        const selections = result.selections || {};
        const names = {home: result.match?.home, away: result.match?.away, over: 'Over', under: 'Under'};
        const lines = Object.values(selections).map(p => {
            const line = p.market === 'OU' ? Math.abs(p.line) : (p.line > 0 ? '+' + p.line : p.line);
            return `<div><strong>${esc(p.market)}: ${esc(names[p.side])} ${esc(line)}</strong>
                <span class="small">${pct(p.prob_profit)}</span></div>`;
        }).join('');
        const reason = (result.alerts || []).join(' ');
        slot.innerHTML = `<div class="clave-dicotomica-v7" style="border:1px solid #64748b;border-radius:8px;padding:6px;width:100%;font-size:.68rem" title="${esc(reason)}">
            <div style="font-size:.55rem;font-weight:800">MODELO v6.1 + STATS</div>
            ${lines || `<span>${esc(result.alerts?.[0] || 'No hay líneas válidas')}</span>`}
            ${lines ? `<div class="small">Riesgo ${esc(result.risk)} · Stats ${result.telemetry?.used_in_prediction ? 'incluidas' : 'sin efecto'}</div>` : ''}
            <button type="button" class="btn btn-outline-primary btn-sm py-0 mt-1" style="font-size:.6rem" data-quant-open="${encodeURIComponent(id)}">Ver fundamento</button>
        </div>`;
        const row = slot.closest('tr[data-match-id]');
        if (row) {
            row.dataset.hasPick = Object.keys(selections).length ? '1' : '0';
            row.dataset.hasAhPick = selections.AH ? '1' : '0';
            row.dataset.hasOuPick = selections.OU ? '1' : '0';
            row.dataset.pickSideKind = selections.AH && result.favorite_side ?
                (selections.AH.side === result.favorite_side ? 'FAVORITE' : 'DOG') : '';
            row.dataset.pickGoals = selections.OU ? selections.OU.side.toUpperCase() : '';
            row.dataset.pickConfidence = result.risk === 'MEDIO' ? 'MEDIA' : 'BAJA';
        }
    }

    async function hydrate() {
        if (busy) return;
        busy = true;
        try {
            const slots = [...document.querySelectorAll('.quant-inline-forecast:not([data-quant-loaded])')];
            if (!slots.length) return;
            const pending = new Map();
            for (const slot of slots) {
                slot.dataset.quantLoaded = '1';
                const id = decodeURIComponent(slot.dataset.quantMatchId);
                const saved = cache.get(id);
                if (saved && Date.now() - saved.at < 60000 && (!saved.result.kickoff_at || new Date(saved.result.kickoff_at) > new Date())) {
                    render(slot, id, saved.result);
                } else {
                    if (!pending.has(id)) pending.set(id, []);
                    pending.get(id).push(slot);
                }
            }
            const ids = [...pending.keys()];
            for (let i = 0; i < ids.length; i += 50) {
                const batch = ids.slice(i, i + 50);
                try {
                    const response = await fetch('/api/quant/bulk', {method: 'POST', headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({match_ids: batch})});
                    const body = await response.json();
                    if (!response.ok || !body.success) throw new Error(body.error || 'Modelo no disponible');
                    for (const id of batch) {
                        const result = body.predictions[id];
                        if (!result) throw new Error('Respuesta incompleta');
                        cache.set(id, {at: Date.now(), result});
                        for (const slot of pending.get(id)) render(slot, id, result);
                    }
                } catch (error) {
                    for (const id of batch) for (const slot of pending.get(id)) {
                        slot.textContent = 'No se pudo calcular el pick. Abre Pronóstico para reintentar.';
                    }
                    console.warn('Pronósticos de la columna Pick:', error.message);
                }
            }
            if (typeof window.applyFiltersVisually === 'function') window.applyFiltersVisually();
        } finally {
            busy = false;
            if (document.querySelector('.quant-inline-forecast:not([data-quant-loaded])')) schedule();
        }
    }
    function schedule() {
        if (scheduled) return;
        scheduled = true;
        setTimeout(() => { scheduled = false; hydrate(); }, 50);
    }
    document.addEventListener('click', event => {
        const button = event.target.closest('[data-quant-open]');
        if (button) window.openMotorV4Modal(button, decodeURIComponent(button.dataset.quantOpen));
    });
    new MutationObserver(schedule).observe(document.body, {childList: true, subtree: true});
    schedule();
})();
