(() => {
    if (window.__leagueTableModalLoaded) return;
    window.__leagueTableModalLoaded = true;

    let tableData = null;

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

    const setActiveTab = view => {
        const tabs = document.getElementById('leagueTableTabs');
        if (!tabs) return;
        tabs.querySelectorAll('[data-table-view]').forEach(tab => {
            tab.classList.toggle('is-active', tab.dataset.tableView === view);
        });
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

    const findStandingById = (view, teamId) => (
        (tableData?.views?.[view] || []).find(row => String(row.team_id) === String(teamId)) || null
    );

    const ppg = row => {
        const matches = numberOrNull(row?.matches);
        const points = numberOrNull(row?.points);
        return matches && points !== null ? points / matches : null;
    };

    const gdPerMatch = row => {
        const matches = numberOrNull(row?.matches);
        const difference = numberOrNull(row?.goal_difference);
        return matches && difference !== null ? difference / matches : null;
    };

    const groupSizeFor = row => {
        if (!row || !tableData?.group_count || tableData.group_count <= 1) {
            return (tableData?.views?.total || []).length;
        }
        return (tableData.views.total || []).filter(candidate => candidate.group === row.group).length;
    };

    const rankBand = (row, totalTeams) => {
        const position = numberOrNull(row?.position);
        if (position === null) return { label: 'Sin puesto', tone: 'neutral' };
        if (tableData?.group_count > 1) {
            const groupSize = groupSizeFor(row);
            if (position === 1) return { label: 'Líder de grupo', tone: 'strong' };
            if (position <= Math.max(2, Math.ceil(groupSize / 2))) {
                return { label: 'Zona alta del grupo', tone: 'medium' };
            }
            return { label: 'Zona baja del grupo', tone: 'weak' };
        }
        if (position <= 3) return { label: 'Equipo fuerte · Top 3', tone: 'strong' };
        if (position <= 10) return { label: 'Bloque Top 10', tone: 'medium' };
        if (position >= Math.max(11, totalTeams - 2)) return { label: 'Zona baja', tone: 'weak' };
        return { label: 'Mitad inferior', tone: 'weak' };
    };

    const compareStrength = (left, right, leftName, rightName) => {
        if (!left || !right) return { label: 'Comparación no disponible', tone: 'neutral' };
        const leftRank = numberOrNull(left.position);
        const rightRank = numberOrNull(right.position);
        const leftPpg = ppg(left);
        const rightPpg = ppg(right);
        const comparableRanks = tableData?.group_count <= 1 || left.group === right.group;
        if (comparableRanks && leftRank !== null && rightRank !== null && Math.abs(leftRank - rightRank) >= 2) {
            return leftRank < rightRank
                ? { label: `${leftName} es más fuerte que ${rightName}`, tone: 'strong' }
                : { label: `${leftName} es más débil que ${rightName}`, tone: 'weak' };
        }
        if (leftPpg !== null && rightPpg !== null && Math.abs(leftPpg - rightPpg) >= .2) {
            return leftPpg > rightPpg
                ? { label: `${leftName} rinde mejor que ${rightName}`, tone: 'strong' }
                : { label: `${leftName} rinde peor que ${rightName}`, tone: 'weak' };
        }
        return { label: `${leftName} y ${rightName} tienen nivel parecido`, tone: 'neutral' };
    };

    const findOuRow = (selected, view, teamId) => (
        (selected?.views?.[view] || []).find(row => String(row.team_id) === String(teamId)) || null
    );

    const findOuRowByName = (selected, view, name) => {
        const rows = selected?.views?.[view] || [];
        let best = null;
        let score = 0;
        rows.forEach(row => {
            const candidate = nameSimilarity(name, row);
            if (candidate > score) {
                score = candidate;
                best = row;
            }
        });
        return score >= 42 ? best : null;
    };

    const ouCell = row => {
        if (!row) return '<span class="metric-empty">—</span>';
        const pct = Math.max(0, Math.min(100, numberOrNull(row.over_pct) ?? 0));
        return `<div class="ou-cell-meter">
            <div><strong>${esc(decimal(pct, 1))}%</strong><small>${esc(row.over)}/${esc(row.matches)}</small></div>
            <span><i style="width:${pct}%"></i></span>
        </div>`;
    };

    const analysisContext = () => {
        const context = tableData?.ui_context || {};
        const favoriteSide = context.favorite_side;
        const favoriteId = favoriteSide === 'HOME' ? tableData?.home_team_id
            : (favoriteSide === 'AWAY' ? tableData?.away_team_id : null);
        const opponentId = favoriteSide === 'HOME' ? tableData?.away_team_id
            : (favoriteSide === 'AWAY' ? tableData?.home_team_id : null);
        const favoriteName = context.favorite_name || (favoriteSide === 'HOME' ? tableData?.home_name : tableData?.away_name) || 'Favorito';
        const opponentName = context.current_opponent || (favoriteSide === 'HOME' ? tableData?.away_name : tableData?.home_name) || 'Rival actual';
        const favoriteTotal = favoriteId ? findStandingById('total', favoriteId) : null;
        const opponentTotal = opponentId ? findStandingById('total', opponentId) : null;
        const lastRivalTotal = context.favorite_last_rival
            ? findTeamRowByName('total', context.favorite_last_rival)
            : null;
        return {
            context, favoriteSide, favoriteId, opponentId, favoriteName, opponentName,
            favoriteTotal, opponentTotal, lastRivalTotal,
            lastRivalId: lastRivalTotal?.team_id || null,
        };
    };

    const buildHandicapDiagnosis = info => {
        const { favoriteTotal, opponentTotal, favoriteName, opponentName, favoriteSide, favoriteId } = info;
        const totalTeams = (tableData?.views?.total || []).length;
        const grouped = tableData?.group_count > 1;
        const handicap = Math.abs(numberOrNull(info.context.handicap) ?? 0);
        if (!favoriteId || !favoriteTotal || !opponentTotal) {
            return { title: 'AH sin favorito o sin tabla completa', text: 'No se puede justificar el hándicap por jerarquía.', tone: 'neutral' };
        }

        const favRank = numberOrNull(favoriteTotal.position);
        const oppRank = numberOrNull(opponentTotal.position);
        let title = 'Hándicap entre niveles próximos';
        let text = `${favoriteName} y ${opponentName} no presentan una brecha grande de clasificación.`;
        let tone = 'neutral';
        if (grouped) {
            const sameGroup = favoriteTotal.group && favoriteTotal.group === opponentTotal.group;
            const favPpg = ppg(favoriteTotal);
            const oppPpg = ppg(opponentTotal);
            if (sameGroup && favRank === 1 && oppRank >= 3) {
                title = 'Líder contra zona baja de su grupo';
                text = `El AH ${decimal(handicap, 2)} sí tiene respaldo en la jerarquía del grupo.`;
                tone = 'strong';
            } else if (favPpg !== null && oppPpg !== null && favPpg >= oppPpg + .5) {
                title = 'Favorito con ventaja clara de rendimiento';
                text = `${favoriteName} suma ${decimal(favPpg)} PPG frente a ${decimal(oppPpg)} del rival. Al estar en grupos distintos, no se comparan los puestos de forma directa.`;
                tone = 'support';
            } else if (favPpg !== null && oppPpg !== null && favPpg + .2 < oppPpg) {
                title = 'El grupo no respalda el favoritismo';
                text = `${opponentName} presenta mejor PPG. La línea exige confirmación fuera de la posición nominal.`;
                tone = 'warning';
            } else {
                title = 'Cruce de grupos con nivel parecido';
                text = 'Los puestos pertenecen a grupos diferentes; la lectura se apoya en PPG y diferencia de gol, no en un Top 10 global ficticio.';
            }
        } else if (favRank <= 3 && oppRank > 10) {
            title = 'Brecha fuerte: Top 3 contra zona baja';
            text = `El AH ${decimal(handicap, 2)} se apoya en una diferencia estructural clara de tabla.`;
            tone = 'strong';
        } else if (favRank <= 10 && oppRank > 10) {
            title = 'Top 10 contra mitad inferior';
            text = `El favorito está dentro del Top 10 y el rival actual aparece ${oppRank}.º de ${totalTeams}.`;
            tone = 'support';
        } else if (oppRank <= 3) {
            title = 'AH exigente contra rival fuerte';
            text = `${opponentName} es Top 3: la línea necesita algo más que la etiqueta de favorito.`;
            tone = 'warning';
        } else if (favRank > 10) {
            title = 'Favorito situado en zona baja';
            text = `El mercado concede favoritismo a un equipo ${favRank}.º; conviene exigir confirmación estadística.`;
            tone = 'warning';
        }

        const homeRow = findStandingById('home', favoriteId);
        const awayRow = findStandingById('away', favoriteId);
        const homePpg = ppg(homeRow);
        const awayPpg = ppg(awayRow);
        const currentVenue = favoriteSide === 'HOME' ? 'casa' : 'fuera';
        const currentPpg = favoriteSide === 'HOME' ? homePpg : awayPpg;
        const otherPpg = favoriteSide === 'HOME' ? awayPpg : homePpg;
        let venueText = 'No hay muestra suficiente para comparar casa y fuera.';
        let venueTone = 'neutral';
        if (homePpg !== null && awayPpg !== null) {
            const betterVenue = homePpg > awayPpg + .2 ? 'casa'
                : (awayPpg > homePpg + .2 ? 'fuera' : 'equilibrado');
            if (betterVenue === 'equilibrado') {
                venueText = `${favoriteName} mantiene un rendimiento parecido en casa (${decimal(homePpg)}) y fuera (${decimal(awayPpg)}) PPG.`;
            } else {
                venueText = `${favoriteName} es mejor ${betterVenue === 'casa' ? 'en casa' : 'fuera'}: ${decimal(homePpg)} PPG local vs ${decimal(awayPpg)} visitante.`;
                venueTone = betterVenue === currentVenue ? 'strong' : 'warning';
            }
            if (handicap >= 1.5 && currentPpg !== null && otherPpg !== null && currentPpg + .2 < otherPpg) {
                venueText += ' La localía actual no respalda una línea tan alta.';
                venueTone = 'warning';
            }
        }
        return { title, text, tone, venueText, venueTone };
    };

    const renderAnalysis = (requestedLine = null) => {
        const body = document.getElementById('leagueTableModalBody');
        if (!body || !tableData) return;
        setActiveTab('analysis');

        const info = analysisContext();
        const totalTeams = (tableData.views?.total || []).length;
        const handicap = numberOrNull(info.context.handicap);
        const lineKey = String(requestedLine ?? tableData.ou?.activeLine ?? tableData.ou?.line ?? 2.5);
        const selectedOu = tableData.ou?.tables?.[lineKey] || tableData.ou || {};
        tableData.ou.activeLine = selectedOu.line ?? Number(lineKey);
        const line = selectedOu.line ?? lineKey;
        const diagnosis = buildHandicapDiagnosis(info);
        const favoriteBand = rankBand(info.favoriteTotal, totalTeams);
        const opponentBand = rankBand(info.opponentTotal, totalTeams);
        const lastRivalName = info.context.favorite_last_rival || 'No identificado';
        const lastBand = rankBand(info.lastRivalTotal, totalTeams);
        const lastVsCurrent = compareStrength(info.lastRivalTotal, info.opponentTotal, lastRivalName, info.opponentName);
        const lastVsFavorite = compareStrength(info.lastRivalTotal, info.favoriteTotal, lastRivalName, info.favoriteName);

        const favoriteHome = info.favoriteId ? findStandingById('home', info.favoriteId) : null;
        const favoriteAway = info.favoriteId ? findStandingById('away', info.favoriteId) : null;
        const favoriteGeneralOu = findOuRow(selectedOu, 'total', info.favoriteId);
        const favoriteHomeOu = findOuRow(selectedOu, 'home', info.favoriteId);
        const favoriteAwayOu = findOuRow(selectedOu, 'away', info.favoriteId);
        const opponentGeneralOu = findOuRow(selectedOu, 'total', info.opponentId);
        const opponentHomeOu = findOuRow(selectedOu, 'home', info.opponentId);
        const opponentAwayOu = findOuRow(selectedOu, 'away', info.opponentId);
        const lastGeneralOu = findOuRow(selectedOu, 'total', info.lastRivalId)
            || findOuRowByName(selectedOu, 'total', lastRivalName);
        const lastOuId = info.lastRivalId || lastGeneralOu?.team_id;
        const lastHomeOu = findOuRow(selectedOu, 'home', lastOuId)
            || findOuRowByName(selectedOu, 'home', lastRivalName);
        const lastAwayOu = findOuRow(selectedOu, 'away', lastOuId)
            || findOuRowByName(selectedOu, 'away', lastRivalName);

        let ouComparison = 'No hay datos suficientes del último rival para comparar la tendencia goleadora.';
        const favOuPct = numberOrNull(favoriteGeneralOu?.over_pct);
        const lastOuPct = numberOrNull(lastGeneralOu?.over_pct);
        if (favOuPct !== null && lastOuPct !== null) {
            const difference = lastOuPct - favOuPct;
            if (difference >= 12) ouComparison = `${lastRivalName} es claramente más Over ${line} que ${info.favoriteName} (${decimal(lastOuPct, 1)}% vs ${decimal(favOuPct, 1)}%).`;
            else if (difference <= -12) ouComparison = `${lastRivalName} es más Under ${line} que ${info.favoriteName} (${decimal(lastOuPct, 1)}% vs ${decimal(favOuPct, 1)}%).`;
            else ouComparison = `${lastRivalName} y ${info.favoriteName} tienen una tendencia O/U ${line} similar.`;
        }

        const unresolvedLastRival = !!info.context.favorite_last_rival && !info.lastRivalTotal;
        const differentLeague = !!info.context.last_rival_different_league;
        const lastRivalNotice = unresolvedLastRival
            ? `<div class="insight-alert warning"><i class="fa-solid fa-triangle-exclamation"></i><span><strong>${esc(lastRivalName)}</strong> no aparece en esta clasificación; no se fuerza ninguna equivalencia.</span></div>`
            : (differentLeague
                ? `<div class="insight-alert info"><i class="fa-solid fa-circle-info"></i><span>El último rival procede de otra competición. La comparación se muestra solo si SofaScore lo reconoce en esta tabla.</span></div>`
                : '');

        const profileCard = (kind, name, row, band, extra = '') => {
            const grouped = tableData.group_count > 1;
            const denominator = grouped ? groupSizeFor(row) : totalTeams;
            const groupLabel = grouped && row?.group
                ? `<span class="profile-group">${esc(row.group)}</span>` : '';
            return `
            <article class="league-profile-card ${kind}">
                <div class="profile-kicker">${kind === 'favorite' ? 'FAVORITO' : (kind === 'opponent' ? 'RIVAL ACTUAL' : 'ÚLTIMO RIVAL')}</div>
                <h6>${esc(name)}</h6>
                ${groupLabel}
                <div class="profile-position">${row ? `${esc(row.position)}<small>/${denominator}</small>` : '—'}</div>
                <span class="strength-chip ${band.tone}">${esc(band.label)}</span>
                <div class="profile-metrics">
                    <span><small>PPG</small><strong>${decimal(ppg(row))}</strong></span>
                    <span><small>DG/PJ</small><strong>${signed(decimal(gdPerMatch(row)))}</strong></span>
                    <span><small>PJ</small><strong>${esc(row?.matches ?? '-')}</strong></span>
                </div>
                ${extra}
            </article>`;
        };

        const ouProfileRow = (label, name, general, home, away, emphasis = '') => `
            <tr class="${emphasis}">
                <td><span class="ou-role">${esc(label)}</span><strong>${esc(name)}</strong></td>
                <td>${ouCell(general)}</td>
                <td>${ouCell(home)}</td>
                <td>${ouCell(away)}</td>
            </tr>`;

        body.innerHTML = `
            <div class="league-insight-dashboard">
                <section class="insight-hero">
                    <div>
                        <span class="insight-eyebrow">LECTURA ESTRUCTURAL DEL MERCADO</span>
                        <h4>${esc(diagnosis.title)}</h4>
                        <p>${esc(diagnosis.text)}</p>
                    </div>
                    <div class="ah-orb ${diagnosis.tone}">
                        <small>AH FAVORITO</small>
                        <strong>${handicap === null ? 'PICK' : decimal(Math.abs(handicap), 2)}</strong>
                        <span>${esc(info.favoriteName)}</span>
                    </div>
                </section>

                <div class="league-profile-grid">
                    ${profileCard('favorite', info.favoriteName, info.favoriteTotal, favoriteBand,
                        `<div class="venue-mini"><span>Casa <b>${decimal(ppg(favoriteHome))}</b></span><span>Fuera <b>${decimal(ppg(favoriteAway))}</b></span></div>`)}
                    ${profileCard('opponent', info.opponentName, info.opponentTotal, opponentBand)}
                    ${profileCard('last-rival', lastRivalName, info.lastRivalTotal, lastBand,
                        info.context.favorite_last_score ? `<div class="last-score">Último partido: <b>${esc(info.context.favorite_last_score)}</b></div>` : '')}
                </div>

                ${lastRivalNotice}

                <section class="comparison-ribbon">
                    <div class="comparison-item ${lastVsCurrent.tone}"><small>Último rival vs rival actual</small><strong>${esc(lastVsCurrent.label)}</strong></div>
                    <div class="comparison-item ${lastVsFavorite.tone}"><small>Último rival vs favorito</small><strong>${esc(lastVsFavorite.label)}</strong></div>
                    <div class="comparison-item ${diagnosis.venueTone}"><small>Casa / fuera del favorito</small><strong>${esc(diagnosis.venueText)}</strong></div>
                </section>

                <section class="ou-analysis-panel">
                    <div class="ou-analysis-head">
                        <div><span class="insight-eyebrow">PERFIL OVER / UNDER</span><h5>Línea ${esc(line)}</h5></div>
                        <div class="ou-line-selector compact">
                            ${Object.keys(tableData.ou?.tables || {}).map(key => `
                                <button type="button" class="${String(key) === String(line) ? 'active' : ''}" data-analysis-ou-line="${esc(key)}">${esc(key)}</button>
                            `).join('')}
                        </div>
                    </div>
                    <div class="ou-explainer">Un porcentaje Over más alto indica más partidos con goles; no significa que el equipo sea mejor.</div>
                    <div class="table-responsive">
                        <table class="ou-compare-table">
                            <thead><tr><th>Perfil</th><th>General</th><th>En casa</th><th>Fuera</th></tr></thead>
                            <tbody>
                                ${ouProfileRow('Favorito', info.favoriteName, favoriteGeneralOu, favoriteHomeOu, favoriteAwayOu, 'favorite')}
                                ${ouProfileRow('Rival actual', info.opponentName, opponentGeneralOu, opponentHomeOu, opponentAwayOu)}
                                ${ouProfileRow('Último rival', lastRivalName, lastGeneralOu, lastHomeOu, lastAwayOu, 'last-rival')}
                            </tbody>
                        </table>
                    </div>
                    <div class="ou-verdict"><i class="fa-solid fa-wave-square"></i><span>${esc(ouComparison)}</span></div>
                </section>
            </div>`;

        body.querySelectorAll('[data-analysis-ou-line]').forEach(button => {
            button.addEventListener('click', () => renderAnalysis(button.dataset.analysisOuLine));
        });
    };

    const renderTable = view => {
        const rows = tableData?.views?.[view] || [];
        const body = document.getElementById('leagueTableModalBody');
        if (!body || !rows.length) return;
        setActiveTab(view);

        let previousGroup = null;
        let htmlRows = '';
        rows.forEach(row => {
            if (row.group && row.group !== previousGroup && tableData.group_count > 1) {
                htmlRows += `<tr class="league-group-row"><td colspan="10">${esc(row.group)}</td></tr>`;
            }
            previousGroup = row.group;
            const classes = [];
            const position = numberOrNull(row.position);
            if (position !== null && position <= 3) classes.push('rank-zone-top');
            if (position !== null && position > Math.max(10, rows.length - 3)) classes.push('rank-zone-low');
            let marker = '';
            if (String(row.team_id) === String(tableData.home_team_id)) {
                classes.push('current-home-row');
                marker = '<span class="team-context-tag home">LOCAL</span>';
            }
            if (String(row.team_id) === String(tableData.away_team_id)) {
                classes.push('current-away-row');
                marker = '<span class="team-context-tag away">VISITANTE</span>';
            }
            const title = row.promotion ? ` title="${esc(row.promotion)}"` : '';
            htmlRows += `
                <tr class="${classes.join(' ')}"${title}>
                    <td class="text-center"><span class="league-position">${esc(row.position ?? '-')}</span></td>
                    <td class="team-cell"><span>${esc(row.team)}</span>${marker}</td>
                    <td class="text-center">${esc(row.matches)}</td>
                    <td class="text-center win-stat">${esc(row.wins)}</td>
                    <td class="text-center">${esc(row.draws)}</td>
                    <td class="text-center loss-stat">${esc(row.losses)}</td>
                    <td class="text-center fw-semibold">${esc(row.scores_for)}</td>
                    <td class="text-center fw-semibold">${esc(row.scores_against)}</td>
                    <td class="text-center">${signed(row.goal_difference)}</td>
                    <td class="text-center"><span class="points-pill">${esc(row.points)}</span></td>
                </tr>`;
        });

        body.innerHTML = `
            <div class="table-view-heading">
                <div><span class="insight-eyebrow">CLASIFICACIÓN</span><h5>${view === 'home' ? 'Rendimiento en casa' : (view === 'away' ? 'Rendimiento fuera' : 'Tabla general')}</h5></div>
                <button type="button" class="back-to-insight" data-back-analysis><i class="fa-solid fa-lightbulb"></i> Ver lectura</button>
            </div>
            <div class="league-standings-wrap table-responsive">
                <table class="table league-standings">
                    <thead><tr>
                        <th class="text-center">#</th><th>Equipo</th><th class="text-center">PJ</th>
                        <th class="text-center">V</th><th class="text-center">E</th><th class="text-center">D</th>
                        <th class="text-center">GF</th><th class="text-center">GC</th>
                        <th class="text-center">DG</th><th class="text-center">Pts</th>
                    </tr></thead>
                    <tbody>${htmlRows}</tbody>
                </table>
            </div>`;
        body.querySelector('[data-back-analysis]')?.addEventListener('click', () => renderAnalysis());
    };

    const renderOuTable = (requestedLine = null) => {
        const ou = tableData?.ou || {};
        const lineKey = String(requestedLine ?? ou.activeLine ?? ou.line ?? 2.5);
        const selected = ou.tables?.[lineKey] || ou;
        ou.activeLine = selected.line ?? Number(lineKey);
        const rows = selected?.views?.total || [];
        const body = document.getElementById('leagueTableModalBody');
        if (!body || !rows.length) return;
        setActiveTab('ou');

        const line = selected.line ?? ou.line ?? 2.5;
        const signal = selected.signal || {};
        const signalClass = signal.tone === 'over' ? 'over'
            : signal.tone === 'under' ? 'under' : 'neutral';
        const htmlRows = rows.map(row => {
            const classes = [];
            let marker = '';
            if (String(row.team_id) === String(tableData.home_team_id)) {
                classes.push('current-home-row');
                marker = '<span class="team-context-tag home">LOCAL</span>';
            }
            if (String(row.team_id) === String(tableData.away_team_id)) {
                classes.push('current-away-row');
                marker = '<span class="team-context-tag away">VISITANTE</span>';
            }
            const pct = Math.max(0, Math.min(100, numberOrNull(row.over_pct) ?? 0));
            return `<tr class="${classes.join(' ')}">
                <td class="text-center"><span class="league-position">${esc(row.position)}</span></td>
                <td class="team-cell"><span>${esc(row.team)}</span>${marker}</td>
                <td class="text-center">${esc(row.matches)}</td>
                <td class="text-center over-number">${esc(row.over)}</td>
                <td class="text-center under-number">${esc(row.under)}</td>
                <td class="text-center">${esc(row.push)}</td>
                <td><div class="table-ou-meter"><strong>${esc(row.over_pct)}%</strong><span><i style="width:${pct}%"></i></span></div></td>
                <td class="text-center"><span class="avg-goals-pill">${esc(row.avg_goals)}</span></td>
            </tr>`;
        }).join('');

        body.innerHTML = `
            <div class="table-view-heading ou-heading">
                <div><span class="insight-eyebrow">TABLA DE FRECUENCIA</span><h5>Más / Menos de ${esc(line)}</h5></div>
                <div class="ou-line-selector compact">
                    ${Object.keys(ou.tables || {}).map(key => `<button type="button" class="${String(key) === String(line) ? 'active' : ''}" data-ou-line="${esc(key)}">${esc(key)}</button>`).join('')}
                </div>
            </div>
            <div class="ou-signal-card ${signalClass}">
                <div><small>SEÑAL DEL CRUCE</small><strong>${esc(signal.label || 'TENDENCIA NEUTRA')}</strong></div>
                <span>Over ${esc(line)}: <b>${esc(signal.over_pct ?? '-')}%</b> · muestra contextual: ${esc(signal.sample || 0)} partidos</span>
            </div>
            <div class="league-standings-wrap table-responsive">
                <table class="table league-standings ou-table">
                    <thead><tr>
                        <th class="text-center">#</th><th>Equipo</th><th class="text-center">PJ</th>
                        <th class="text-center">Más</th><th class="text-center">Menos</th>
                        <th class="text-center">Nulo</th><th>% Más</th><th class="text-center">Media</th>
                    </tr></thead>
                    <tbody>${htmlRows}</tbody>
                </table>
            </div>`;
        body.querySelectorAll('[data-ou-line]').forEach(button => {
            button.addEventListener('click', () => renderOuTable(button.dataset.ouLine));
        });

        const flashscoreLink = document.getElementById('leagueFlashscoreLink');
        if (flashscoreLink && tableData.external_links?.flashscore) {
            flashscoreLink.href = `${tableData.external_links.flashscore}#/baT3Pnwf/mas-de_menos-de/general/${line}/`;
        }
    };

    const showTable = data => {
        tableData = data;
        tableData.group_count = new Set(
            (data.views?.total || []).map(row => row.group).filter(Boolean)
        ).size;
        document.getElementById('leagueTableModalTitle').textContent = data.tournament || 'Clasificación';
        document.getElementById('leagueTableModalSubtitle').textContent =
            [data.season, `${data.home_name} vs ${data.away_name}`].filter(Boolean).join(' · ');

        const tabs = document.getElementById('leagueTableTabs');
        const labels = { analysis: 'Lectura AH / O-U', total: 'General', home: 'En casa', away: 'Fuera' };
        tabs.innerHTML = Object.entries(labels)
            .filter(([view]) => view === 'analysis' || (data.views?.[view] || []).length)
            .map(([view, label]) => `<button type="button" class="league-tab ${view === 'analysis' ? 'is-active' : ''}" data-table-view="${view}">${label}</button>`)
            .join('') + ((data.ou?.views?.total || []).length
                ? '<button type="button" class="league-tab" data-table-view="ou">Tabla O/U</button>'
                : '');
        tabs.querySelectorAll('[data-table-view]').forEach(tab => {
            tab.addEventListener('click', () => {
                const view = tab.dataset.tableView;
                if (view === 'analysis') renderAnalysis();
                else if (view === 'ou') renderOuTable();
                else renderTable(view);
            });
        });

        const flashscoreLink = document.getElementById('leagueFlashscoreLink');
        if (flashscoreLink) {
            const target = data.external_links?.flashscore_ou;
            flashscoreLink.classList.toggle('d-none', !target);
            if (target) flashscoreLink.href = target;
        }

        renderAnalysis();
        bootstrap.Modal.getOrCreateInstance(document.getElementById('leagueTableModal')).show();
    };

    const openStatusModal = (button, state, message = '') => {
        tableData = null;
        const modal = document.getElementById('leagueTableModal');
        const title = document.getElementById('leagueTableModalTitle');
        const subtitle = document.getElementById('leagueTableModalSubtitle');
        const tabs = document.getElementById('leagueTableTabs');
        const body = document.getElementById('leagueTableModalBody');
        if (!modal || !title || !subtitle || !tabs || !body) return;

        title.textContent = button.dataset.leagueName || 'Clasificación de liga';
        subtitle.textContent = [button.dataset.homeName, button.dataset.awayName]
            .filter(Boolean).join(' vs ');
        tabs.innerHTML = '';
        if (state === 'loading') {
            body.innerHTML = `
                <div class="league-loading-state">
                    <div class="spinner-border" role="status"></div>
                    <strong>Construyendo lectura de liga…</strong>
                    <span>Clasificación, localía, hándicap y perfil O/U</span>
                </div>`;
        } else {
            body.innerHTML = `
                <div class="league-empty-state">
                    <i class="fa-solid fa-chart-column"></i>
                    <strong>${esc(message || 'No hay clasificación disponible para esta liga.')}</strong>
                    <span>No se mostrará una conclusión si la fuente no ofrece datos suficientes.</span>
                </div>`;
        }
        bootstrap.Modal.getOrCreateInstance(modal).show();
    };

    document.addEventListener('click', async event => {
        const button = event.target.closest('.league-table-trigger');
        if (!button || button.disabled) return;
        const originalHtml = button.innerHTML;
        openStatusModal(button, 'loading');
        button.disabled = true;
        button.innerHTML = button.classList.contains('league-table-trigger--compact')
            ? '<i class="fa-solid fa-spinner fa-spin"></i>'
            : '<i class="fa-solid fa-spinner fa-spin me-2"></i>Cargando lectura';
        try {
            const response = await fetch('/api/sofascore/league-table', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    home_name: button.dataset.homeName,
                    away_name: button.dataset.awayName,
                    league_name: button.dataset.leagueName,
                    match_date: button.dataset.matchDate,
                    goal_line: button.dataset.goalLine || 2.5,
                    handicap: button.dataset.handicap
                })
            });
            if (!response.ok) throw new Error('provider_error');
            const data = await response.json();
            if (!data.available || !(data.views?.total || []).length) {
                const reasons = {
                    teams_not_resolved: 'SofaScore no ha reconocido ninguno de los dos equipos.',
                    match_not_resolved: 'SofaScore no ha podido relacionar este partido con su liga.',
                    standings_not_available: 'SofaScore no publica una tabla para esta competición.',
                    provider_unavailable: 'SofaScore no está disponible en este momento.'
                };
                openStatusModal(button, 'error', reasons[data.reason] || 'No hay clasificación disponible para esta liga.');
                return;
            }
            data.ui_context = {
                handicap: button.dataset.handicap,
                favorite_side: button.dataset.favoriteSide,
                favorite_name: button.dataset.favoriteName,
                current_opponent: button.dataset.currentOpponent,
                favorite_last_rival: button.dataset.favoriteLastRival,
                favorite_last_score: button.dataset.favoriteLastScore,
                favorite_last_venue: button.dataset.favoriteLastVenue,
                last_rival_venue: button.dataset.lastRivalVenue,
                last_rival_different_league: button.dataset.lastRivalDifferentLeague === '1'
            };
            showTable(data);
        } catch (error) {
            openStatusModal(button, 'error', 'No se ha podido conectar con SofaScore.');
        } finally {
            if (button.isConnected) {
                button.disabled = false;
                button.innerHTML = originalHtml;
            }
        }
    });
})();
