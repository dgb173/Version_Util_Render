(() => {
    if (window.__leagueTableModalLoaded) return;
    window.__leagueTableModalLoaded = true;

    let tableData = null;
    let activeView = 'total';
    let currentQuery = null;

    const esc = value => String(value ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');

    const numberOrNull = value => {
        if (value === null || value === undefined || String(value).trim() === '') return null;
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : null;
    };

    const signed = value => {
        const numeric = numberOrNull(value);
        if (numeric !== null) return numeric > 0 ? `+${numeric}` : String(numeric);
        return esc(value || '-');
    };

    const decimal = (value, digits = 2) => {
        const numeric = numberOrNull(value);
        return numeric === null ? '-' : numeric.toFixed(digits).replace(/\.00$/, '');
    };

    const normalizeTeamName = value => String(value || '')
        .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
        .toLowerCase()
        .replace(/\((w|women|f)\)/g, ' ')
        .replace(/\b(women|woman|femenino|femenina|ladies|football club|fc)\b/g, ' ')
        .replace(/[^a-z0-9]+/g, ' ')
        .trim();

    const nameSimilarity = (requested, row) => {
        const target = normalizeTeamName(requested);
        const candidates = [row?.team, row?.short_name].map(normalizeTeamName).filter(Boolean);
        if (!target || !candidates.length) return 0;
        let best = 0;
        candidates.forEach(candidate => {
            if (candidate === target) {
                best = Math.max(best, 100);
                return;
            }
            if (candidate.includes(target) || target.includes(candidate)) {
                best = Math.max(best, 82 - Math.abs(candidate.length - target.length) * .2);
            }
            const left = new Set(target.split(' ').filter(Boolean));
            const right = new Set(candidate.split(' ').filter(Boolean));
            const intersection = [...left].filter(token => right.has(token)).length;
            const union = new Set([...left, ...right]).size || 1;
            best = Math.max(best, (intersection / union) * 70);
        });
        return best;
    };

    const findStandingById = (view, teamId) => (
        (tableData?.views?.[view] || []).find(row => String(row.team_id) === String(teamId)) || null
    );

    const findTeamRowByName = (view, name) => {
        const rows = tableData?.views?.[view] || [];
        let bestRow = null;
        let bestScore = 0;
        rows.forEach(row => {
            const score = nameSimilarity(name, row);
            if (score > bestScore) {
                bestScore = score;
                bestRow = row;
            }
        });
        return bestScore >= 42 ? bestRow : null;
    };

    const renderFormPills = formList => {
        if (!Array.isArray(formList) || !formList.length) return '<span class="text-muted">—</span>';
        return `<div class="sofa-form-list">` +
            formList.map(item => {
                const res = String(item).toUpperCase();
                const cls = res === 'W' || res === 'V' ? 'win' : (res === 'L' ? 'loss' : 'draw');
                const label = res === 'W' || res === 'V' ? 'W' : (res === 'L' ? 'L' : 'D');
                return `<span class="sofa-form-badge ${cls}">${label}</span>`;
            }).join('') +
            `</div>`;
    };

    const fetchSeasonTable = async (seasonId) => {
        if (!currentQuery) return;
        const body = document.getElementById('leagueTableModalBody');
        if (body) {
            body.innerHTML = `
                <div class="league-loading-state">
                    <div class="spinner-border text-primary" role="status"></div>
                    <strong>Cargando temporada de SofaScore…</strong>
                </div>`;
        }
        try {
            const payload = {
                ...currentQuery,
                tournament_id: tableData?.tournament_id,
                season_id: seasonId,
            };
            const res = await fetch('/api/sofascore/league-table', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            const data = await res.json();
            if (data.available) {
                tableData = data;
                renderStandings(activeView);
            } else {
                body.innerHTML = `
                    <div class="league-empty-state">
                        <i class="fa-solid fa-triangle-exclamation text-warning"></i>
                        <strong>No hay datos para esta temporada</strong>
                    </div>`;
            }
        } catch (e) {
            console.error('Error al cambiar de temporada:', e);
        }
    };

    const renderStandings = (view = 'total') => {
        activeView = view;
        const rows = tableData?.views?.[view] || tableData?.views?.total || [];
        const body = document.getElementById('leagueTableModalBody');
        if (!body) return;

        if (!rows.length) {
            body.innerHTML = `
                <div class="league-empty-state">
                    <i class="fa-solid fa-chart-simple"></i>
                    <strong>No hay clasificación disponible para esta vista</strong>
                </div>`;
            return;
        }

        const seasons = tableData.seasons || [];
        const currentSeasonId = String(tableData.season_id || '');
        const tournamentName = tableData.tournament || 'Clasificación';

        const seasonOptionsHtml = seasons.map(s => {
            const selected = String(s.id) === currentSeasonId ? 'selected' : '';
            return `<option value="${esc(s.id)}" ${selected}>${esc(s.name || s.year)}</option>`;
        }).join('');

        let hasPromotion = false;
        let hasRelegation = false;

        let htmlRows = '';
        rows.forEach((row, index) => {
            const pos = numberOrNull(row.position) ?? (index + 1);
            let promoBarClass = '';
            const promoText = String(row.promotion || '').toLowerCase();

            if (promoText.includes('promotion') || promoText.includes('ascenso') || promoText.includes('champions') || (pos <= 2 && rows.length > 3)) {
                promoBarClass = 'promotion';
                hasPromotion = true;
            } else if (promoText.includes('relegation') || promoText.includes('descenso') || (pos >= rows.length - 1 && rows.length > 5)) {
                promoBarClass = 'relegation';
                hasRelegation = true;
            }

            const isHome = String(row.team_id) === String(tableData.home_team_id);
            const isAway = String(row.team_id) === String(tableData.away_team_id);

            let teamClasses = ['sofa-team-name'];
            let tag = '';
            if (isHome) {
                teamClasses.push('highlight-home');
                tag = '<span class="team-context-tag home">LOCAL</span>';
            } else if (isAway) {
                teamClasses.push('highlight-away');
                tag = '<span class="team-context-tag away">VISITANTE</span>';
            }

            const gls = `${esc(row.scores_for ?? 0)}:${esc(row.scores_against ?? 0)}`;

            htmlRows += `
                <tr>
                    <td class="col-promo-bar"><span class="promo-bar ${promoBarClass}"></span></td>
                    <td class="text-center"><span class="sofa-pos-badge">${pos}</span></td>
                    <td><div class="${teamClasses.join(' ')}"><span>${esc(row.team)}</span>${tag}</div></td>
                    <td class="text-center">${esc(row.matches ?? 0)}</td>
                    <td class="text-center win-stat">${esc(row.wins ?? 0)}</td>
                    <td class="text-center">${esc(row.draws ?? 0)}</td>
                    <td class="text-center loss-stat">${esc(row.losses ?? 0)}</td>
                    <td class="text-center">${signed(row.goal_difference)}</td>
                    <td class="text-center text-muted font-monospace">${gls}</td>
                    <td class="text-center">${renderFormPills(row.form)}</td>
                    <td class="text-center sofa-pts-cell">${esc(row.points ?? 0)}</td>
                </tr>`;
        });

        body.innerHTML = `
            <div class="sofa-header-bar">
                <div class="sofa-tournament-season">
                    <span class="sofa-tournament-name"><i class="fa-solid fa-trophy text-warning"></i> ${esc(tournamentName)}</span>
                    ${seasons.length > 1 ? `
                        <select class="sofa-season-select" id="sofaSeasonSelect">
                            ${seasonOptionsHtml}
                        </select>` : (tableData.season ? `<span class="badge bg-light text-dark border">${esc(tableData.season)}</span>` : '')}
                </div>
                <div class="d-flex align-items-center gap-2">
                    <div class="sofa-view-pills">
                        <button type="button" class="sofa-pill ${view === 'total' ? 'active' : ''}" data-sofa-view="total">All</button>
                        <button type="button" class="sofa-pill ${view === 'home' ? 'active' : ''}" data-sofa-view="home">Home</button>
                        <button type="button" class="sofa-pill ${view === 'away' ? 'active' : ''}" data-sofa-view="away">Away</button>
                    </div>
                    <button type="button" class="btn btn-sm btn-outline-secondary rounded-pill fw-bold" data-open-analysis><i class="fa-solid fa-chart-pie me-1"></i> Análisis AH / O-U</button>
                </div>
            </div>
            <div class="sofa-table-container">
                <table class="sofa-standings-table">
                    <thead>
                        <tr>
                            <th class="col-promo-bar"></th>
                            <th class="text-center" style="width:36px">#</th>
                            <th>Team</th>
                            <th class="text-center" style="width:40px">P</th>
                            <th class="text-center" style="width:40px">W</th>
                            <th class="text-center" style="width:40px">D</th>
                            <th class="text-center" style="width:40px">L</th>
                            <th class="text-center" style="width:50px">DIFF</th>
                            <th class="text-center" style="width:60px">GLS</th>
                            <th class="text-center" style="width:115px">Last 5</th>
                            <th class="text-center" style="width:45px">PTS</th>
                        </tr>
                    </thead>
                    <tbody>${htmlRows}</tbody>
                </table>
            </div>
            <div class="sofa-legend-bar">
                ${hasPromotion ? `<div class="sofa-legend-item"><span class="sofa-legend-box promotion"></span> Promotion</div>` : ''}
                ${hasRelegation ? `<div class="sofa-legend-item"><span class="sofa-legend-box relegation"></span> Relegation</div>` : ''}
                <div class="ms-auto text-muted" style="font-size:0.7rem">Datos: SofaScore Oficial</div>
            </div>`;

        // Eventos
        body.querySelector('#sofaSeasonSelect')?.addEventListener('change', e => {
            fetchSeasonTable(e.target.value);
        });

        body.querySelectorAll('[data-sofa-view]').forEach(btn => {
            btn.addEventListener('click', () => renderStandings(btn.dataset.sofaView));
        });

        body.querySelector('[data-open-analysis]')?.addEventListener('click', () => renderAnalysis());
    };

    const renderAnalysis = () => {
        const body = document.getElementById('leagueTableModalBody');
        if (!body || !tableData) return;

        const homeRow = findStandingById('total', tableData.home_team_id) || findTeamRowByName('total', tableData.home_name);
        const awayRow = findStandingById('total', tableData.away_team_id) || findTeamRowByName('total', tableData.away_name);
        const ou = tableData.ou || {};
        const signal = ou.signal || {};

        body.innerHTML = `
            <div class="table-view-heading">
                <div><span class="insight-eyebrow">LECTURA COMPARATIVA</span><h5>${esc(tableData.home_name)} vs ${esc(tableData.away_name)}</h5></div>
                <button type="button" class="back-to-insight" data-back-standings><i class="fa-solid fa-list-ol"></i> Ver Tabla SofaScore</button>
            </div>
            <div class="league-insight-dashboard">
                <div class="league-profile-grid">
                    <div class="league-profile-card favorite">
                        <span class="profile-kicker">LOCAL</span>
                        <h6>${esc(tableData.home_name)}</h6>
                        <span class="profile-position">#${esc(homeRow?.position ?? '-')}</span>
                        <div class="profile-metrics">
                            <span><small>PJ</small><strong>${esc(homeRow?.matches ?? '-')}</strong></span>
                            <span><small>PTS</small><strong>${esc(homeRow?.points ?? '-')}</strong></span>
                            <span><small>DG</small><strong>${signed(homeRow?.goal_difference)}</strong></span>
                        </div>
                    </div>
                    <div class="league-profile-card opponent">
                        <span class="profile-kicker">VISITANTE</span>
                        <h6>${esc(tableData.away_name)}</h6>
                        <span class="profile-position">#${esc(awayRow?.position ?? '-')}</span>
                        <div class="profile-metrics">
                            <span><small>PJ</small><strong>${esc(awayRow?.matches ?? '-')}</strong></span>
                            <span><small>PTS</small><strong>${esc(awayRow?.points ?? '-')}</strong></span>
                            <span><small>DG</small><strong>${signed(awayRow?.goal_difference)}</strong></span>
                        </div>
                    </div>
                    <div class="league-profile-card">
                        <span class="profile-kicker">TENDENCIA GOLES (O/U ${esc(ou.line ?? 2.5)})</span>
                        <h6>${esc(signal.label || 'PERFIL EQUILIBRADO')}</h6>
                        <div class="profile-metrics">
                            <span><small>% OVER</small><strong>${esc(signal.over_pct ?? '-')}%</strong></span>
                            <span><small>MUESTRA</small><strong>${esc(ou.matches_analyzed ?? 0)} part.</strong></span>
                            <span><small>LÍNEA</small><strong>${esc(ou.line ?? 2.5)}</strong></span>
                        </div>
                    </div>
                </div>
            </div>`;

        body.querySelector('[data-back-standings]')?.addEventListener('click', () => renderStandings(activeView));
    };

    const showTable = data => {
        tableData = data;
        document.getElementById('leagueTableModalTitle').textContent = data.tournament || 'Clasificación';
        document.getElementById('leagueTableModalSubtitle').textContent =
            [data.season, `${data.home_name} vs ${data.away_name}`].filter(Boolean).join(' · ');

        renderStandings('total');
        bootstrap.Modal.getOrCreateInstance(document.getElementById('leagueTableModal')).show();
    };

    const openStatusModal = (button, state, message = '') => {
        tableData = null;
        const modal = document.getElementById('leagueTableModal');
        const title = document.getElementById('leagueTableModalTitle');
        const subtitle = document.getElementById('leagueTableModalSubtitle');
        const body = document.getElementById('leagueTableModalBody');
        if (!modal || !title || !subtitle || !body) return;

        title.textContent = button.dataset.leagueName || 'Clasificación de liga';
        subtitle.textContent = [button.dataset.homeName, button.dataset.awayName]
            .filter(Boolean).join(' vs ');

        if (state === 'loading') {
            body.innerHTML = `
                <div class="league-loading-state">
                    <div class="spinner-border text-primary" role="status"></div>
                    <strong>Consultando SofaScore…</strong>
                    <span>Cargando clasificación y temporadas</span>
                </div>`;
        } else {
            body.innerHTML = `
                <div class="league-empty-state">
                    <i class="fa-solid fa-chart-simple"></i>
                    <strong>${esc(message)}</strong>
                    <span>No se mostrará una conclusión si la fuente no ofrece datos suficientes.</span>
                </div>`;
        }
        bootstrap.Modal.getOrCreateInstance(modal).show();
    };

    document.addEventListener('click', async event => {
        const button = event.target.closest('.league-table-trigger, [data-league-table-trigger]');
        if (!button) return;
        event.preventDefault();

        currentQuery = {
            home_name: button.dataset.homeName || '',
            away_name: button.dataset.awayName || '',
            league_name: button.dataset.leagueName || '',
            match_date: button.dataset.matchDate || '',
            goal_line: button.dataset.goalLine || '2.5',
            handicap: button.dataset.handicap || '0',
        };

        openStatusModal(button, 'loading');

        try {
            const response = await fetch('/api/sofascore/league-table', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(currentQuery),
            });
            const data = await response.json();
            if (data && data.available) {
                showTable(data);
            } else {
                const reasons = {
                    teams_not_resolved: 'SofaScore no ha reconocido ninguno de los dos equipos.',
                    match_not_resolved: 'SofaScore no ha podido relacionar este partido con su liga.',
                    standings_not_available: 'SofaScore no publica una tabla para esta competición.',
                    provider_unavailable: 'SofaScore no está disponible en este momento.',
                };
                openStatusModal(button, 'error', reasons[data.reason] || 'No hay clasificación disponible para esta liga.');
            }
        } catch (error) {
            openStatusModal(button, 'error', 'No se ha podido conectar con SofaScore.');
        }
    });
})();
