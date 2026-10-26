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

        // Soporte v6.2 prioritario
        if (result.ah_pick || result.ou_pick || result.status === 'OK') {
            const ah = result.ah_pick;
            const ou = result.ou_pick;
            const scoreAh = result.score_ah || {};
            const scoreOu = result.score_ou || {};
            const patterns = result.patterns || [];
            const context = result.recent_context || {};
            const statValue = value => Number.isFinite(value) ? value.toFixed(1) : 'N/D';
            const contextHtml = Object.entries(context).map(([side, windows]) => {
                const rows = [['last_five', 'Últimos 5'], ['last_five_same_venue', side === 'home' ? '5 en casa' : '5 fuera'],
                              ['last_same_venue', side === 'home' ? 'Último en casa' : 'Último fuera']];
                return `<strong>${side === 'home' ? 'Local' : 'Visitante'}</strong>` + rows.map(([key, label]) => {
                    const p = windows[key] || {};
                    return `<div>${label} (${p.n || 0}): ` + ['shots', 'sot', 'attacks', 'ap'].map((s, i) =>
                        `${['Tiros', 'Puerta', 'Ataques', 'Peligrosos'][i]} ${statValue(p[s + '_for'])}/${statValue(p[s + '_against'])} [${p.counts?.[s + '_for'] || 0}]`).join(' · ') + '</div>';
                }).join('');
            }).join('');

            let ahLabel = 'N/D';
            if (ah && ah.market) {
                const sideName = ah.market === 'AH_HOME' ? 'Loc' : 'Vis';
                const lineVal = ah.line !== undefined ? (ah.line > 0 ? '+' + ah.line : ah.line) : '';
                ahLabel = `${sideName} ${lineVal}`;
            }

            let ouLabel = 'N/D';
            if (ou && ou.market) {
                ouLabel = `${ou.market === 'OVER' ? 'Over' : 'Under'} ${ou.line ?? ''}`;
            }

            const ahScoreText = scoreAh.of_six !== undefined ? Number(scoreAh.of_six).toFixed(1) + '/6' : '';
            const ouScoreText = scoreOu.of_six !== undefined ? Number(scoreOu.of_six).toFixed(1) + '/6' : '';

            const patternBadge = patterns.length ? `<span class="badge bg-warning text-dark py-0" style="font-size:.55rem">${esc(patterns[0])}</span>` : '';

            slot.innerHTML = `<div class="clave-dicotomica-v7" style="border:1px solid #7c3aed;border-radius:8px;padding:6px;width:100%;font-size:.68rem;background:rgba(124,58,237,0.06)">
                <div style="font-size:.56rem;font-weight:800;color:#7c3aed;display:flex;justify-content:space-between;align-items:center;">
                    <span>🧠 MOTOR v6.2</span>
                    ${patternBadge}
                </div>
                <div style="margin:2px 0;line-height:1.2;">
                    <div style="color:#2563eb;font-weight:700;">AH: ${esc(ahLabel)} <span style="color:#64748b;font-size:.58rem">(${esc(ahScoreText)})</span></div>
                    <div style="color:#059669;font-weight:700;">Tot: ${esc(ouLabel)} <span style="color:#64748b;font-size:.58rem">(${esc(ouScoreText)})</span></div>
                </div>
                <button type="button" class="btn btn-outline-primary btn-sm py-0 mt-1 w-100" style="font-size:.6rem;font-weight:700;" data-quant-open="${encodeURIComponent(id)}">Ver 12 compuertas</button>
                <details style="margin-top:4px"><summary>Stats recientes · casa/fuera</summary><div>A favor / en contra · [partidos con dato]</div>${contextHtml}</details>
            </div>`;

            const row = slot.closest('tr[data-match-id]');
            if (row) {
                row.dataset.hasPick = (ah || ou) ? '1' : '0';
                row.dataset.hasAhPick = ah ? '1' : '0';
                row.dataset.hasOuPick = ou ? '1' : '0';
                row.dataset.pickSideKind = ah?.market === 'AH_HOME' ? 'HOME' : 'AWAY';
                row.dataset.pickGoals = ou?.market || '';
            }
            return;
        }

        // Fallback si no tiene picks
        slot.innerHTML = `<div class="clave-dicotomica-v7" style="border:1px solid #64748b;border-radius:8px;padding:6px;width:100%;font-size:.68rem">
            <div style="font-size:.55rem;font-weight:800;color:#64748b;">MOTOR v6.2</div>
            <span class="text-muted small">${esc(result.message || 'Analiza el partido')}</span>
        </div>`;
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
