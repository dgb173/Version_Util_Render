(() => {
    if (window.__leagueTableModalLoaded) return;
    window.__leagueTableModalLoaded = true;

    let tableData = null;
    let activeMainTab = 'standings'; // 'standings' | 'ou'
    let activeVenue = 'total';       // 'total' | 'home' | 'away'
    let activeOuLine = '2.5';
    let currentQuery = null;

    const labels = {
        total: 'General',
        home: 'En casa',
        away: 'Fuera',
        analysis: 'Lectura AH / O-U'
    };

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

    const isMatchHomeTeam = row => {
        if (!row || !tableData) return false;
        if (tableData.home_team_id && String(row.team_id) === String(tableData.home_team_id)) return true;
        return nameSimilarity(tableData.home_name, row) >= 65;
    };

    const isMatchAwayTeam = row => {
        if (!row || !tableData) return false;
        if (tableData.away_team_id && String(row.team_id) === String(tableData.away_team_id)) return true;
        return nameSimilarity(tableData.away_name, row) >= 65;
    };

    const renderFormPills = formList => {
        if (!Array.isArray(formList) || !formList.length) return '<span class="text-muted" style="opacity:0.4">—</span>';
        return `<div class="sofa-form-list">` +
            formList.map(item => {
                const res = String(item).toUpperCase();
                const cls = res === 'W' || res === 'V' ? 'win' : (res === 'L' ? 'loss' : 'draw');
                const label = res === 'W' || res === 'V' ? 'W' : (res === 'L' ? 'L' : 'D');
                return `<span class="sofa-form-badge ${cls}" title="${res === 'W' ? 'Victoria' : (res === 'L' ? 'Derrota' : 'Empate')}">${label}</span>`;
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
                    <strong>Cargando temporada…</strong>
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
            if (data && data.available) {
                tableData = data;
                renderModalContent();
            } else {
                body.innerHTML = `
                    <div class="league-empty-state">
                        <i class="fa-solid fa-triangle-exclamation text-warning"></i>
                        <strong>No hay datos disponibles para esta temporada</strong>
                    </div>`;
            }
        } catch (e) {
            console.error('Error al cambiar temporada:', e);
        }
    };

    const renderHeaderControls = () => {
        const seasons = tableData?.seasons || [];
        const currentSeasonId = String(tableData?.season_id || '');
        const tournamentName = tableData?.tournament || 'Clasificación';
        const sourceName = tableData?.source || 'Oficial';

        const seasonOptionsHtml = seasons.map(s => {
            const selected = String(s.id) === currentSeasonId ? 'selected' : '';
            return `<option value="${esc(s.id)}" ${selected}>${esc(s.name || s.year)}</option>`;
        }).join('');

        // Líneas disponibles para O/U
        const ouTables = tableData?.ou?.tables || {};
        const availableLines = Object.keys(ouTables).length ? Object.keys(ouTables) : ['1.5', '2.5', '3.5', '4.5'];
        if (!availableLines.includes(activeOuLine)) {
            activeOuLine = String(tableData?.ou?.line || availableLines[0] || '2.5');
        }

        return `
            <div class="sofa-top-bar">
                <div class="sofa-meta-group">
                    <span class="sofa-comp-title"><i class="fa-solid fa-trophy text-warning me-1"></i>${esc(tournamentName)}</span>
                    ${seasons.length > 1 ? `
                        <select class="sofa-season-select" id="sofaSeasonSelect">
                            ${seasonOptionsHtml}
                        </select>
                    ` : (tableData?.season ? `<span class="sofa-season-pill">${esc(tableData.season)}</span>` : '')}
                    <span class="sofa-source-badge">${esc(sourceName)}</span>
                </div>

                <div class="sofa-nav-controls">
                    <!-- Switch Principal: Clasificación / Over Under -->
                    <div class="sofa-segmented-control">
                        <button type="button" class="sofa-segment-btn ${activeMainTab === 'standings' ? 'active' : ''}" data-main-tab="standings">
                            <i class="fa-solid fa-ranking-star me-1"></i>Clasificación
                        </button>
                        <button type="button" class="sofa-segment-btn ${activeMainTab === 'ou' ? 'active' : ''}" data-main-tab="ou">
                            <i class="fa-solid fa-futbol me-1"></i>Over / Under
                        </button>
                    </div>

                    <!-- Píldoras de Localización: General / En casa / Fuera -->
                    <div class="sofa-venue-pills">
                        <button type="button" class="sofa-venue-btn ${activeVenue === 'total' ? 'active' : ''}" data-venue-tab="total">General</button>
                        <button type="button" class="sofa-venue-btn ${activeVenue === 'home' ? 'active' : ''}" data-venue-tab="home">En casa</button>
                        <button type="button" class="sofa-venue-btn ${activeVenue === 'away' ? 'active' : ''}" data-venue-tab="away">Fuera</button>
                    </div>
                </div>
            </div>
            ${activeMainTab === 'ou' ? `
                <div class="sofa-ou-line-bar">
                    <span class="sofa-line-label"><i class="fa-solid fa-sliders me-1"></i>Línea de goles:</span>
                    <div class="sofa-line-selector">
                        ${availableLines.map(line => `
                            <button type="button" class="sofa-line-btn ${String(line) === String(activeOuLine) ? 'active' : ''}" data-ou-line="${esc(line)}">
                                ${esc(line)}
                            </button>
                        `).join('')}
                    </div>
                </div>
            ` : ''}
        `;
    };

    const renderStandingsTableHtml = () => {
        const rows = tableData?.views?.[activeVenue] || tableData?.views?.total || [];
        if (!rows.length) {
            return `
                <div class="league-empty-state">
                    <i class="fa-solid fa-table-list"></i>
                    <strong>No hay datos de clasificación para la vista seleccionada</strong>
                </div>`;
        }

        let hasPromotion = false;
        let hasRelegation = false;

        let htmlRows = '';
        rows.forEach((row, index) => {
            const pos = numberOrNull(row.position) ?? (index + 1);
            let promoBarClass = '';
            const promoText = String(row.promotion || '').toLowerCase();

            if (promoText.includes('promotion') || promoText.includes('ascenso') || promoText.includes('champions') || (pos <= 2 && rows.length > 4)) {
                promoBarClass = 'promotion';
                hasPromotion = true;
            } else if (promoText.includes('relegation') || promoText.includes('descenso') || (pos >= rows.length - 1 && rows.length > 5)) {
                promoBarClass = 'relegation';
                hasRelegation = true;
            }

            const isHome = isMatchHomeTeam(row);
            const isAway = isMatchAwayTeam(row);

            let rowClass = '';
            let badgeTag = '';
            if (isHome) {
                rowClass = 'row-highlight-home';
                badgeTag = '<span class="team-mini-badge home">LOCAL</span>';
            } else if (isAway) {
                rowClass = 'row-highlight-away';
                badgeTag = '<span class="team-mini-badge away">VISITANTE</span>';
            }

            const gf = esc(row.scores_for ?? 0);
            const gc = esc(row.scores_against ?? 0);
            const gls = `${gf}:${gc}`;

            htmlRows += `
                <tr class="${rowClass}">
                    <td class="col-indicator"><span class="promo-indicator ${promoBarClass}"></span></td>
                    <td class="text-center col-pos"><span class="pos-num">${pos}</span></td>
                    <td class="col-team">
                        <div class="team-cell">
                            <span class="team-title text-truncate">${esc(row.team || row.short_name)}</span>
                            ${badgeTag}
                        </div>
                    </td>
                    <td class="text-center">${esc(row.matches ?? 0)}</td>
                    <td class="text-center font-stat-win">${esc(row.wins ?? 0)}</td>
                    <td class="text-center text-muted">${esc(row.draws ?? 0)}</td>
                    <td class="text-center font-stat-loss">${esc(row.losses ?? 0)}</td>
                    <td class="text-center text-muted font-monospace">${gls}</td>
                    <td class="text-center fw-bold ${numberOrNull(row.goal_difference) > 0 ? 'text-success' : (numberOrNull(row.goal_difference) < 0 ? 'text-danger' : 'text-muted')}">${signed(row.goal_difference)}</td>
                    <td class="text-center col-pts"><strong>${esc(row.points ?? 0)}</strong></td>
                    <td class="text-center col-form">${renderFormPills(row.form)}</td>
                </tr>`;
        });

        return `
            <div class="sofa-table-scroll-area">
                <table class="sofa-clean-table">
                    <thead>
                        <tr>
                            <th class="col-indicator"></th>
                            <th class="text-center" style="width:36px">#</th>
                            <th>Equipo</th>
                            <th class="text-center" style="width:42px" title="Partidos Jugados">PJ</th>
                            <th class="text-center" style="width:38px" title="Victorias">V</th>
                            <th class="text-center" style="width:38px" title="Empates">E</th>
                            <th class="text-center" style="width:38px" title="Derrotas">D</th>
                            <th class="text-center" style="width:65px" title="Goles a favor y en contra">GF:GC</th>
                            <th class="text-center" style="width:50px" title="Diferencia de goles">DG</th>
                            <th class="text-center col-pts" style="width:52px" title="Puntos">PTS</th>
                            <th class="text-center col-form" style="width:125px" title="Últimos 5 partidos">Racha</th>
                        </tr>
                    </thead>
                    <tbody>${htmlRows}</tbody>
                </table>
            </div>
            <div class="sofa-footer-legend">
                ${hasPromotion ? `<span class="legend-item"><i class="legend-dot promotion"></i> Ascenso / Champions</span>` : ''}
                ${hasRelegation ? `<span class="legend-item"><i class="legend-dot relegation"></i> Descenso</span>` : ''}
                <span class="ms-auto text-muted small"><i class="fa-solid fa-circle-check text-success me-1"></i>Actualizado</span>
            </div>
        `;
    };

    const renderOuTableHtml = () => {
        const lineKey = String(activeOuLine);
        const ouData = tableData?.ou || {};
        const tables = ouData.tables || {};
        const selectedTable = tables[lineKey] || ouData;
        const lineViews = selectedTable.views || ouData.views || {};
        const rows = lineViews[activeVenue] || lineViews.total || [];
        const signal = selectedTable.signal || ouData.signal || {};

        // Resumen rápido del cruce
        const homeRow = rows.find(r => isMatchHomeTeam(r)) || (lineViews.total || []).find(r => isMatchHomeTeam(r));
        const awayRow = rows.find(r => isMatchAwayTeam(r)) || (lineViews.total || []).find(r => isMatchAwayTeam(r));

        let signalBadgeCls = 'bg-secondary';
        if (signal.tone === 'over') signalBadgeCls = 'bg-success';
        else if (signal.tone === 'under') signalBadgeCls = 'bg-danger';

        let htmlRows = '';
        if (rows.length) {
            rows.forEach((row, index) => {
                const isHome = isMatchHomeTeam(row);
                const isAway = isMatchAwayTeam(row);

                let rowClass = '';
                let badgeTag = '';
                if (isHome) {
                    rowClass = 'row-highlight-home';
                    badgeTag = '<span class="team-mini-badge home">LOCAL</span>';
                } else if (isAway) {
                    rowClass = 'row-highlight-away';
                    badgeTag = '<span class="team-mini-badge away">VISITANTE</span>';
                }

                const matches = numberOrNull(row.matches) || 0;
                const over = numberOrNull(row.over) || 0;
                const under = numberOrNull(row.under) || 0;
                const push = numberOrNull(row.push) || 0;
                const overPct = numberOrNull(row.over_pct) ?? (matches ? Math.round((over / matches) * 100) : 0);
                const avgGoals = decimal(row.avg_goals, 2);

                const pctColor = overPct >= 60 ? 'text-success fw-bold' : (overPct <= 40 ? 'text-danger fw-bold' : 'text-dark');

                htmlRows += `
                    <tr class="${rowClass}">
                        <td class="text-center col-pos"><span class="pos-num">${index + 1}</span></td>
                        <td class="col-team">
                            <div class="team-cell">
                                <span class="team-title text-truncate">${esc(row.team || row.short_name)}</span>
                                ${badgeTag}
                            </div>
                        </td>
                        <td class="text-center">${matches}</td>
                        <td class="text-center text-success fw-bold">${over}</td>
                        <td class="text-center text-danger fw-bold">${under}</td>
                        <td class="text-center text-muted">${push}</td>
                        <td class="text-center">
                            <span class="${pctColor}">${overPct}%</span>
                            <div class="sofa-progress-bar-wrap">
                                <div class="sofa-progress-bar" style="width:${Math.min(100, Math.max(0, overPct))}%"></div>
                            </div>
                        </td>
                        <td class="text-center font-monospace">${avgGoals}</td>
                    </tr>`;
            });
        }

        return `
            <div class="sofa-ou-summary-card">
                <div class="ou-summary-left">
                    <span class="ou-summary-label">CRUCE LÍNEA ${esc(activeOuLine)}</span>
                    <div class="ou-teams-rate">
                        <span class="ou-rate-item ${homeRow ? 'fw-bold text-primary' : ''}">
                            <b>${esc(tableData?.home_name || 'Local')}:</b> ${homeRow ? `${esc(homeRow.over_pct)}% Over (${homeRow.over}/${homeRow.matches})` : '—'}
                        </span>
                        <span class="ou-separator">vs</span>
                        <span class="ou-rate-item ${awayRow ? 'fw-bold text-danger' : ''}">
                            <b>${esc(tableData?.away_name || 'Visitante')}:</b> ${awayRow ? `${esc(awayRow.over_pct)}% Over (${awayRow.over}/${awayRow.matches})` : '—'}
                        </span>
                    </div>
                </div>
                <div class="ou-summary-right">
                    <span class="badge ${signalBadgeCls} rounded-pill px-3 py-2 fw-bold text-uppercase" style="font-size:0.75rem;">
                        ${esc(signal.label || 'PERFIL EQUILIBRADO')}
                    </span>
                </div>
            </div>

            <div class="sofa-table-scroll-area">
                ${rows.length ? `
                    <table class="sofa-clean-table">
                        <thead>
                            <tr>
                                <th class="text-center" style="width:36px">#</th>
                                <th>Equipo</th>
                                <th class="text-center" style="width:45px">PJ</th>
                                <th class="text-center text-success" style="width:50px">Over</th>
                                <th class="text-center text-danger" style="width:50px">Under</th>
                                <th class="text-center text-muted" style="width:45px">Nulo</th>
                                <th class="text-center" style="width:110px">% Over</th>
                                <th class="text-center" style="width:65px">Media</th>
                            </tr>
                        </thead>
                        <tbody>${htmlRows}</tbody>
                    </table>
                ` : `
                    <div class="league-empty-state">
                        <i class="fa-solid fa-futbol"></i>
                        <strong>No hay partidos registrados de la temporada para calcular O/U</strong>
                    </div>
                `}
            </div>
            <div class="sofa-footer-legend">
                <span class="text-muted small">Cálculo en base a partidos oficiales de la temporada actual.</span>
            </div>
        `;
    };

    const renderModalContent = () => {
        const body = document.getElementById('leagueTableModalBody');
        if (!body) return;

        const headerHtml = renderHeaderControls();
        const contentHtml = activeMainTab === 'standings' ? renderStandingsTableHtml() : renderOuTableHtml();

        body.innerHTML = headerHtml + contentHtml;

        // Limpiar contenedor exterior antiguo de pestañas si existe en la plantilla
        const oldTabs = document.getElementById('leagueTableTabs');
        if (oldTabs) oldTabs.innerHTML = '';

        // Bind events
        body.querySelector('#sofaSeasonSelect')?.addEventListener('change', e => {
            fetchSeasonTable(e.target.value);
        });

        body.querySelectorAll('[data-main-tab]').forEach(btn => {
            btn.addEventListener('click', () => {
                activeMainTab = btn.dataset.mainTab;
                renderModalContent();
            });
        });

        body.querySelectorAll('[data-venue-tab]').forEach(btn => {
            btn.addEventListener('click', () => {
                activeVenue = btn.dataset.venueTab;
                renderModalContent();
            });
        });

        body.querySelectorAll('[data-ou-line]').forEach(btn => {
            btn.addEventListener('click', () => {
                activeOuLine = btn.dataset.ouLine;
                renderModalContent();
            });
        });
    };

    // Funciones requeridas por tests o accesos directos
    const renderStandings = (venue = 'total') => {
        activeMainTab = 'standings';
        activeVenue = venue;
        renderModalContent();
    };

    const renderOuTable = (line = null) => {
        activeMainTab = 'ou';
        if (line) activeOuLine = String(line);
        renderModalContent();
    };

    const renderAnalysis = () => {
        // Redirige limpiamente a Over/Under dentro de la tabla
        activeMainTab = 'ou';
        renderModalContent();
    };

    const buildHandicapDiagnosis = (info) => {
        return {
            title: 'Lectura de tabla',
            text: 'Información directa de clasificación y estadísticas Over/Under.',
            tone: 'neutral'
        };
    };

    const showTable = data => {
        tableData = data;
        document.getElementById('leagueTableModalTitle').textContent = data.tournament || 'Clasificación';
        document.getElementById('leagueTableModalSubtitle').textContent =
            [data.season, `${data.home_name} vs ${data.away_name}`].filter(Boolean).join(' · ');

        activeMainTab = 'standings';
        activeVenue = 'total';
        if (data.ou?.line) {
            activeOuLine = String(data.ou.line);
        }

        renderModalContent();
        bootstrap.Modal.getOrCreateInstance(document.getElementById('leagueTableModal')).show();
    };

    const openStatusModal = (button, state, message = '') => {
        tableData = null;
        const modal = document.getElementById('leagueTableModal');
        const title = document.getElementById('leagueTableModalTitle');
        const subtitle = document.getElementById('leagueTableModalSubtitle');
        const body = document.getElementById('leagueTableModalBody');
        const tabs = document.getElementById('leagueTableTabs');
        if (!modal || !title || !subtitle || !body) return;

        if (tabs) tabs.innerHTML = '';
        title.textContent = button.dataset.leagueName || 'Clasificación de liga';
        subtitle.textContent = [button.dataset.homeName, button.dataset.awayName]
            .filter(Boolean).join(' vs ');

        if (state === 'loading') {
            body.innerHTML = `
                <div class="league-loading-state">
                    <div class="spinner-border text-primary" role="status"></div>
                    <strong>Cargando clasificación y Over/Under…</strong>
                    <span>Consultando datos actualizados</span>
                </div>`;
        } else {
            body.innerHTML = `
                <div class="league-empty-state">
                    <i class="fa-solid fa-chart-simple"></i>
                    <strong>${esc(message || 'No hay clasificación disponible')}</strong>
                    <span>No se ha podido recuperar una clasificación verificada en este momento.</span>
                </div>`;
        }
        bootstrap.Modal.getOrCreateInstance(modal).show();
    };

    // Desde la ficha de Pre-Cacheo, la tabla se monta dentro del partido.
    const findInlineStandingsHost = button => {
        const row = button.closest('tr');
        const detailRow = row?.nextElementSibling?.classList.contains('pre-context-detail-row') ? row.nextElementSibling : null;
        const panel = button.closest('.pre-context-panel') || detailRow?.querySelector('.pre-context-panel');
        const host = panel?.querySelector('.pre-context-moment .sofa-inline-column');
        return host ? { host, contentGrid: host.closest('.pre-context-content-grid') } : null;
    };

    const renderInlineStatus = (host, message, loading = false) => {
        host.innerHTML = `<div class="sofa-inline-status ${loading ? 'is-loading' : ''}">
            ${loading ? '<span class="spinner-border spinner-border-sm text-primary" role="status"></span>' : '<i class="fa-solid fa-chart-simple"></i>'}
            <strong>${esc(message)}</strong>
        </div>`;
    };

    const renderInlineStandings = (host, data, view = 'total', query = {}) => {
        const rows = data?.views?.[view]?.length ? data.views[view] : (data?.views?.total || []);
        const activeView = data?.views?.[view]?.length ? view : 'total';
        if (!rows.length) {
            renderInlineStatus(host, 'No hay clasificación disponible para esta liga.');
            return;
        }
        const seasons = data.seasons || [];
        const seasonOptions = seasons.length > 1
            ? `<select class="sofa-inline-season" aria-label="Temporada">${seasons.map(season => `<option value="${esc(season.id)}" ${String(season.id) === String(data.season_id || '') ? 'selected' : ''}>${esc(season.name || season.year || '')}</option>`).join('')}</select>`
            : '';
        let previousGroup = null;
        const multipleGroups = rows.some(row => String(row.group || '').trim());
        const tableRows = [...rows].sort((a, b) => String(a.group || '').localeCompare(String(b.group || ''), 'es') || (numberOrNull(a.position) ?? 9999) - (numberOrNull(b.position) ?? 9999)).map((row, index) => {
            const group = String(row.group || '').trim();
            const groupHeader = multipleGroups && group && group !== previousGroup ? `<tr class="sofa-group-row"><th colspan="8">${esc(group)}</th></tr>` : '';
            previousGroup = group;
            const isHome = row.team_id != null && data.home_team_id != null && String(row.team_id) === String(data.home_team_id);
            const isAway = row.team_id != null && data.away_team_id != null && String(row.team_id) === String(data.away_team_id);
            return `${groupHeader}<tr class="${isHome ? 'sofa-match-home' : (isAway ? 'sofa-match-away' : '')}">
                <td class="text-center">${esc(row.position ?? index + 1)}</td>
                <td class="sofa-inline-team" title="${esc(row.team)}">${esc(row.team)}${isHome ? '<small class="home">L</small>' : (isAway ? '<small class="away">V</small>' : '')}</td>
                <td class="text-center">${esc(row.matches ?? '-')}</td>
                <td class="text-center sofa-win-cell">${esc(row.wins ?? '-')}</td>
                <td class="text-center sofa-loss-cell">${esc(row.losses ?? '-')}</td>
                <td class="text-center sofa-gf-cell">${esc(row.scores_for ?? '-')}</td>
                <td class="text-center">${signed(row.goal_difference)}</td>
                <td class="text-center sofa-pts-cell">${esc(row.points ?? '-')}</td></tr>`;
        }).join('');
        host.innerHTML = `<div class="sofa-inline-head"><div class="sofa-inline-title"><i class="fa-solid fa-ranking-star me-1"></i><span title="${esc(data.tournament || query.league_name || 'Clasificación')}">${esc(data.tournament || query.league_name || 'Clasificación')}</span></div><button type="button" class="sofa-inline-close" aria-label="Ocultar clasificación" title="Ocultar clasificación">×</button></div>
            <div class="sofa-inline-controls"><div class="sofa-inline-views"><button type="button" data-inline-view="total" class="${activeView === 'total' ? 'active' : ''}">General</button>${data.views?.home?.length ? `<button type="button" data-inline-view="home" class="${activeView === 'home' ? 'active' : ''}">Local</button>` : ''}${data.views?.away?.length ? `<button type="button" data-inline-view="away" class="${activeView === 'away' ? 'active' : ''}">Fuera</button>` : ''}</div>${seasonOptions}</div>
            <div class="sofa-inline-table-scroll"><table class="sofa-inline-table"><thead><tr><th title="Posición">#</th><th>Equipo</th><th title="Partidos jugados">PJ</th><th title="Victorias">V</th><th title="Derrotas">D</th><th title="Goles a favor">GF</th><th title="Diferencia de goles">DG</th><th title="Puntos">Pts</th></tr></thead><tbody>${tableRows}</tbody></table></div>
            <div class="sofa-inline-footer">${esc(data.season || '')}${data.season && data.source ? ' · ' : ''}${esc(data.source || 'SofaScore')}${data.cached && data.fetched_at ? ` · ${esc(String(data.fetched_at).slice(0, 10))}` : ''}</div>`;

        host.querySelector('.sofa-inline-close')?.addEventListener('click', () => {
            host.classList.remove('is-open');
            host.closest('.pre-context-content-grid')?.classList.remove('has-inline-table');
            host._leagueTableTrigger?.setAttribute('aria-expanded', 'false');
            const toggle = host.closest('.pre-context-panel')?.querySelector('.pre-context-standings-btn');
            if (toggle) { toggle.setAttribute('aria-expanded', 'false'); toggle.querySelector('span').textContent = 'Ver tabla'; }
        });
        host.querySelectorAll('[data-inline-view]').forEach(control => control.addEventListener('click', () => renderInlineStandings(host, data, control.dataset.inlineView, query)));
        host.querySelector('.sofa-inline-season')?.addEventListener('change', async event => {
            renderInlineStatus(host, 'Cargando temporada…', true);
            try {
                const response = await fetch('/api/sofascore/league-table', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...query, tournament_id: data.tournament_id, season_id: event.target.value }) });
                const nextData = await response.json();
                if (!response.ok || !nextData.available) throw new Error('No hay datos para esta temporada.');
                host._leagueTableData = nextData;
                renderInlineStandings(host, nextData, activeView, query);
            } catch (error) { renderInlineStatus(host, error.message || 'No se pudo cargar la temporada.'); }
        });
    };

    const loadInlineStandings = async (button, target, query) => {
        const { host, contentGrid } = target;
        host._leagueTableTrigger = button;
        const toggle = host.closest('.pre-context-panel')?.querySelector('.pre-context-standings-btn');
        if (host.classList.contains('is-open')) {
            host.classList.remove('is-open'); contentGrid?.classList.remove('has-inline-table'); button.setAttribute('aria-expanded', 'false');
            if (toggle) { toggle.setAttribute('aria-expanded', 'false'); toggle.querySelector('span').textContent = 'Ver tabla'; }
            return;
        }
        host.classList.add('is-open'); contentGrid?.classList.add('has-inline-table'); button.setAttribute('aria-expanded', 'true');
        if (toggle) { toggle.setAttribute('aria-expanded', 'true'); toggle.querySelector('span').textContent = 'Ocultar tabla'; }
        if (host._leagueTableData) { renderInlineStandings(host, host._leagueTableData, 'total', query); return; }
        renderInlineStatus(host, 'Consultando clasificación…', true);
        try {
            const response = await fetch('/api/sofascore/league-table', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(query) });
            const data = await response.json();
            if (!response.ok || !data.available) {
                const reasons = { teams_not_resolved: 'No se han podido identificar los equipos.', match_not_resolved: 'No se ha podido relacionar el partido con la competición.', standings_not_available: 'La fuente no ofrece una clasificación verificada.', competition_has_no_standings: 'Esta competición no tiene clasificación.', provider_unavailable: 'La fuente de clasificación no está disponible ahora.' };
                throw new Error(reasons[data.reason] || 'No hay clasificación disponible para esta liga.');
            }
            host._leagueTableData = data; renderInlineStandings(host, data, 'total', query);
        } catch (error) { renderInlineStatus(host, error.message || 'No se pudo cargar la clasificación.'); }
    };

    document.addEventListener('click', async event => {
        const button = event.target.closest('.league-table-trigger, [data-league-table-trigger]');
        if (!button) return;
        event.preventDefault();

        currentQuery = {
            home_name: button.dataset.homeName || '',
            away_name: button.dataset.awayName || '',
            league_name: button.dataset.leagueName || '',
            nowgoal_league_id: button.dataset.nowgoalLeagueId || '',
            match_date: button.dataset.matchDate || '',
            goal_line: button.dataset.goalLine || '2.5',
            handicap: button.dataset.handicap || '0',
            match_id: button.closest('tr')?.dataset.matchId || button.dataset.matchId || '',
        };

        const inlineTarget = findInlineStandingsHost(button);
        if (inlineTarget) { await loadInlineStandings(button, inlineTarget, currentQuery); return; }

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
                    teams_not_resolved: 'No se reconocieron los equipos en la fuente.',
                    match_not_resolved: 'No se pudo relacionar este partido con su competición.',
                    standings_not_available: 'No se ha podido recuperar una clasificación verificada para esta competición y temporada.',
                    provider_unavailable: 'El proveedor de clasificaciones no está disponible ahora.',
                    provider_access_challenge: 'El proveedor ha bloqueado temporalmente la consulta de clasificación.',
                    provider_rate_limited: 'El proveedor ha limitado temporalmente las consultas de clasificación.',
                    competition_not_resolved: 'No se ha podido identificar con seguridad esta competición.',
                    competition_ambiguous: 'Hay varias competiciones posibles; falta confirmar cuál corresponde al partido.',
                };
                openStatusModal(button, 'error', reasons[data.reason] || 'No hay clasificación disponible para esta liga.');
            }
        } catch (error) {
            openStatusModal(button, 'error', 'No se ha podido conectar con el servicio de clasificación.');
        }
    });

    // Exponer helpers por si se necesitan
    window.__leagueTableHelpers = {
        labels,
        renderOuTable,
        renderStandings,
        renderAnalysis,
        buildHandicapDiagnosis
    };
})();
