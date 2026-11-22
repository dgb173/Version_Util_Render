(function () {
    'use strict';

    const MATCH_ID = String(window.FICHA_COMPLETA_MATCH_ID || '').trim();

    const state = {
        data: null,
        activeTab: 'tables',
        ahViewMode: 'team',      // 'team' | 'nowgoal'
        h2hFilter: 'all',        // 'all' | 'stadium' | 'inverted'
        filters: {
            scope: 'venue',       // 'venue' | 'all' | 'same_league'
            limit: '20',          // '5' | '10' | '20'
            onlyCommon: false,
            onlySimAh: false,
            roleFilter: 'all',    // 'all' | 'FAV' | 'DOG'
            outcomeFilter: 'all', // 'all' | 'COVER' | 'NO_COVER' | 'OVER' | 'UNDER'
            search: '',
        },
        standings: null,
        standingsView: 'total',
        inspectorTarget: 'auto',  // 'auto' | 'A' | 'B'
        inspectorNextAuto: 'A',
        inspectorSlots: {
            A: null,
            B: null,
        },
        comparator: {
            A: { teamName: '', venue: 'all', data: null, loading: false, error: null },
            B: { teamName: '', venue: 'all', data: null, loading: false, error: null },
            roleFilter: 'all',    // 'all' | 'FAV' | 'DOG'
            onlyCommonAB: false,
            searchTeam: '',
            showMirror: true,
            customCol3: null,
            customCol3Loading: false,
        },
        caches: {
            stats: new Map(),
            col3: new Map(),
            mov: new Map(),
            teamHistory: new Map(),
        },
    };

    const $ = (id) => (typeof document !== 'undefined' ? document.getElementById(id) : null);

    function esc(v) {
        return String(v == null ? '' : v)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function normTeam(name) {
        return String(name || '')
            .toLowerCase()
            .normalize('NFD')
            .replace(/[\u0300-\u036f]/g, '')
            .replace(/[^a-z0-9\s]/g, ' ')
            .replace(/\s+/g, ' ')
            .trim();
    }

    function sameTeam(a, b) {
        const na = normTeam(a);
        const nb = normTeam(b);
        if (!na || !nb) return false;
        if (na === nb) return true;
        return (na.length >= 4 && nb.length >= 4) && (na.includes(nb) || nb.includes(na));
    }

    function fmtNum(v, fallback = '-') {
        if (v === null || v === undefined || v === '' || Number.isNaN(Number(v))) return fallback;
        return String(v);
    }

    function parseAhNum(raw) {
        if (typeof raw === 'number' && Number.isFinite(raw)) return raw;
        const txt = String(raw ?? '').trim().replace(',', '.');
        if (!txt || txt === '-' || txt === 'N/A' || txt === '?') return null;
        if (txt.includes('/')) {
            const parts = txt.split('/').map((p) => Number.parseFloat(p.trim())).filter((n) => Number.isFinite(n));
            if (parts.length === 2) {
                let a = parts[0];
                let b = parts[1];
                if (txt.trim().startsWith('-') && b > 0) b = -b;
                return Math.round(((a + b) / 2) * 100) / 100;
            }
        }
        const n = Number.parseFloat(txt);
        return Number.isFinite(n) ? Math.round(n * 100) / 100 : null;
    }

    function formatNumClean(n) {
        if (n === null || n === undefined || Number.isNaN(Number(n))) return '-';
        const rounded = Math.round(Number(n) * 100) / 100;
        if (Object.is(rounded, -0)) return '0';
        return String(rounded);
    }

    function resolveRowTeamRole(row) {
        if (!row || typeof row !== 'object') return 'N/D';
        if (row.team_role === 'FAV' || row.team_role === 'DOG' || row.team_role === 'PK') {
            return row.team_role;
        }
        const tAh = (row.team_ah !== null && row.team_ah !== undefined && Number.isFinite(Number(row.team_ah)))
            ? Number(row.team_ah)
            : (() => {
                const rawN = parseAhNum(row.ah);
                if (rawN === null) return null;
                return row.is_home === false ? -rawN : rawN;
            })();
        if (tAh === null) return 'N/D';
        if (tAh > 0.01) return 'FAV';
        if (tAh < -0.01) return 'DOG';
        return 'PK';
    }

    function formatAhPerspective(rawAh, isHome = true, mode = 'team', opts = {}) {
        const hasTeamAh = opts.teamAh !== null && opts.teamAh !== undefined && Number.isFinite(Number(opts.teamAh));
        const rawNum = parseAhNum(rawAh);
        const teamGives = hasTeamAh
            ? Math.round(Number(opts.teamAh) * 100) / 100
            : (rawNum !== null ? Math.round((isHome ? rawNum : -rawNum) * 100) / 100 : null);

        if (teamGives === null) {
            return {
                role: 'ND',
                teamAh: null,
                label: '-',
                html: '<span class="fc-ah-pill is-ND">-</span>',
            };
        }

        let role = 'PK';
        let teamLabel = 'PK 0';
        if (teamGives > 0.01) {
            role = 'FAV';
            teamLabel = `FAV -${formatNumClean(Math.abs(teamGives))}`;
        } else if (teamGives < -0.01) {
            role = 'DOG';
            teamLabel = `DOG +${formatNumClean(Math.abs(teamGives))}`;
        }

        const rawDisplay = rawNum !== null ? formatNumClean(rawNum) : esc(rawAh || '-');
        const mainText = mode === 'nowgoal' ? `NG ${rawDisplay}` : teamLabel;
        const ringCls = opts.isExactToday ? ' is-exact-today' : (opts.isSimToday ? ' is-sim-today' : '');
        const subText = mode === 'nowgoal'
            ? teamLabel
            : (!isHome ? `NG ${rawDisplay}` : '');
        const titleTxt = mode === 'nowgoal'
            ? `Línea NowGoal: ${rawDisplay} · Perspectiva equipo: ${teamLabel}`
            : `${teamLabel}${!isHome ? ` (NowGoal visita: ${rawDisplay})` : ''}`;

        const html = `<div class="fc-ah-stack">
            <span class="fc-ah-pill is-${role}${ringCls}" title="${esc(titleTxt)}">${esc(mainText)}</span>
            ${subText ? `<span class="fc-ah-raw-sub">${esc(subText)}</span>` : ''}
        </div>`;

        return { role, teamAh: teamGives, label: mainText, html };
    }

    function formatOrientedScore(scoreStr, isHome = true, wdl = '', homeRed = null, awayRed = null) {
        const clean = String(scoreStr || '').trim();
        const m = clean.match(/(\d+)\s*[:\-]\s*(\d+)/);
        if (!m) {
            return `<span class="fc-score-wrap"><span class="fc-score-dot is-SIN_DATO">?</span><span class="fc-score-opp">${esc(clean || '-')}</span></span>`;
        }
        const hg = Number(m[1]);
        const ag = Number(m[2]);
        const ownG = isHome ? hg : ag;
        const oppG = isHome ? ag : hg;
        let resWdl = wdl;
        if (resWdl !== 'W' && resWdl !== 'D' && resWdl !== 'L') {
            resWdl = ownG > oppG ? 'W' : (ownG < oppG ? 'L' : 'D');
        }
        const letter = resWdl === 'W' ? 'V' : (resWdl === 'L' ? 'D' : 'E');
        const ownRed = Number(isHome ? homeRed : awayRed) || 0;
        const oppRed = Number(isHome ? awayRed : homeRed) || 0;
        const redHtml = (ownRed > 0 || oppRed > 0)
            ? `<span class="fc-red-card" title="Tarjetas rojas (Equipo: ${ownRed}, Rival: ${oppRed})">🟥${ownRed}:${oppRed}</span>`
            : '';

        return `<span class="fc-score-wrap" title="Marcador desde la perspectiva del equipo (${ownG} a favor, ${oppG} en contra)">
            <span class="fc-score-dot is-${resWdl}">${letter}</span>
            <span class="fc-score-own">${ownG}</span><span class="fc-score-sep">:</span><span class="fc-score-opp">${oppG}</span>
            ${redHtml}
        </span>`;
    }

    function formatMovBadge(movStr, deltaNum = null, dirHint = '') {
        let dir = String(dirHint || '').toUpperCase();
        if (dir !== 'SUBE' && dir !== 'BAJA' && dir !== 'IGUAL') {
            if (deltaNum !== null && deltaNum !== undefined && Number.isFinite(Number(deltaNum))) {
                const d = Number(deltaNum);
                dir = Math.abs(d) < 0.01 ? 'IGUAL' : (d > 0 ? 'SUBE' : 'BAJA');
            } else {
                const txt = String(movStr || '');
                const parts = txt.split(/→|->/).map((s) => parseAhNum(s));
                if (parts.length === 2 && parts[0] !== null && parts[1] !== null) {
                    const diff = parts[1] - parts[0];
                    dir = Math.abs(diff) < 0.01 ? 'IGUAL' : (diff > 0 ? 'SUBE' : 'BAJA');
                } else {
                    dir = 'IGUAL';
                }
            }
        }
        const arrow = dir === 'SUBE' ? '▲' : (dir === 'BAJA' ? '▼' : '=');
        const label = movStr || (deltaNum !== null && deltaNum !== undefined ? `${Number(deltaNum) > 0 ? '+' : ''}${formatNumClean(deltaNum)}` : dir);
        return `<span class="fc-mov-pill is-${dir}">${arrow} ${esc(label)}</span>`;
    }

    function buildQuickAbBtnsHtml(teamName, oppFallback = '') {
        if (!teamName || teamName === '-') return '';
        const isA = sameTeam(teamName, state.comparator?.A?.teamName);
        const isB = sameTeam(teamName, state.comparator?.B?.teamName);
        return `<span class="fc-quick-ab-btns">
            <button type="button" class="fc-ab-btn is-a ${isA ? 'is-active' : ''}" data-fc-select-team="${esc(teamName)}" data-fc-side="A" data-fc-opp="${esc(oppFallback)}" title="Fijar ${esc(teamName)} en Lado A">→A</button>
            <button type="button" class="fc-ab-btn is-b ${isB ? 'is-active' : ''}" data-fc-select-team="${esc(teamName)}" data-fc-side="B" data-fc-opp="${esc(oppFallback)}" title="Fijar ${esc(teamName)} en Lado B">→B</button>
        </span>`;
    }

    function coverLabel(code) {
        if (code === 'COVER') return '✓ CUBRE';
        if (code === 'HALF_WIN') return '½✓ CUBRE';
        if (code === 'NO_COVER') return '✕ FALLA';
        if (code === 'HALF_LOSS') return '½✕ FALLA';
        if (code === 'PUSH') return '= NULO';
        return 'N/D';
    }

    function coverCssClass(code) {
        if (code === 'COVER' || code === 'HALF_WIN' || code === 'NO_COVER' || code === 'HALF_LOSS' || code === 'PUSH') {
            return `is-${code}`;
        }
        return 'is-SIN_DATO';
    }

    function ouArrowHtml(ouStr, ouRes) {
        const cleanOu = esc(ouStr || '-');
        if (!ouRes || ouRes === 'SIN_DATO') {
            return `<span class="fc-ou-pill is-SIN_DATO">${cleanOu}</span>`;
        }
        const tag = ouRes === 'OVER' ? '↑ O' : (ouRes === 'UNDER' ? '↓ U' : '= P');
        return `<span class="fc-ou-pill is-${esc(ouRes)}">${cleanOu} ${tag}</span>`;
    }

    function calcSummaryFromRows(rows) {
        const list = Array.isArray(rows) ? rows : [];
        let w = 0, d = 0, l = 0, cw = 0, cl = 0, cp = 0, ov = 0, un = 0, op = 0;
        let gfSum = 0, gaSum = 0, scoredN = 0;
        let favN = 0, favCw = 0, favDec = 0, dogN = 0, dogCw = 0, dogDec = 0;

        list.forEach((r) => {
            if (r.wdl === 'W') w += 1;
            else if (r.wdl === 'D') d += 1;
            else if (r.wdl === 'L') l += 1;

            const covRaw = r.ah_cover || r.ah_eval?.code || 'SIN_DATO';
            const isCw = covRaw === 'COVER' || covRaw === 'HALF_WIN';
            const isCl = covRaw === 'NO_COVER' || covRaw === 'HALF_LOSS';
            if (isCw) cw += 1;
            else if (isCl) cl += 1;
            else if (covRaw === 'PUSH') cp += 1;

            if (r.ou_res === 'OVER') ov += 1;
            else if (r.ou_res === 'UNDER') un += 1;
            else if (r.ou_res === 'PUSH') op += 1;

            if (typeof r.goals_for === 'number' && typeof r.goals_against === 'number') {
                gfSum += r.goals_for;
                gaSum += r.goals_against;
                scoredN += 1;
            }

            const role = resolveRowTeamRole(r);
            if (role === 'FAV') {
                favN += 1;
                if (isCw) { favCw += 1; favDec += 1; }
                else if (isCl) favDec += 1;
            } else if (role === 'DOG') {
                dogN += 1;
                if (isCw) { dogCw += 1; dogDec += 1; }
                else if (isCl) dogDec += 1;
            }
        });
        const ahDec = cw + cl;
        const ouDec = ov + un;
        return {
            total: list.length,
            wins: w, draws: d, losses: l,
            cover_wins: cw, cover_losses: cl, cover_push: cp,
            cover_pct: ahDec ? Math.round((cw * 1000) / ahDec) / 10 : null,
            overs: ov, unders: un, ou_push: op,
            over_pct: ouDec ? Math.round((ov * 1000) / ouDec) / 10 : null,
            goals_for_avg: scoredN ? Math.round((gfSum * 100) / scoredN) / 100 : null,
            goals_against_avg: scoredN ? Math.round((gaSum * 100) / scoredN) / 100 : null,
            fav_n: favN, fav_cover_wins: favCw,
            fav_cover_pct: favDec ? Math.round((favCw * 1000) / favDec) / 10 : null,
            dog_n: dogN, dog_cover_wins: dogCw,
            dog_cover_pct: dogDec ? Math.round((dogCw * 1000) / dogDec) / 10 : null,
        };
    }

    function summaryPillsHtml(sum) {
        if (!sum) return '';
        const coverTxt = sum.cover_pct !== null && sum.cover_pct !== undefined ? `${sum.cover_pct}%` : 'N/D';
        const overTxt = sum.over_pct !== null && sum.over_pct !== undefined ? `${sum.over_pct}% O` : 'N/D O/U';
        const gfAvg = sum.goals_for_avg != null ? `GF ${sum.goals_for_avg} · GC ${sum.goals_against_avg ?? '-'}` : '';
        const favChip = sum.fav_n > 0 ? `<span class="fc-stat-chip is-indigo" title="AH cubierto como Favorito">Fav ${sum.fav_cover_wins}/${sum.fav_n}${sum.fav_cover_pct != null ? ` (${sum.fav_cover_pct}%)` : ''}</span>` : '';
        const dogChip = sum.dog_n > 0 ? `<span class="fc-stat-chip is-amber" title="AH cubierto como No Favorito">Dog ${sum.dog_cover_wins}/${sum.dog_n}${sum.dog_cover_pct != null ? ` (${sum.dog_cover_pct}%)` : ''}</span>` : '';
        return `<div class="fc-summary-pills">
            <span class="fc-stat-chip">${sum.total} PJ</span>
            <span class="fc-stat-chip is-green">V ${sum.wins}</span>
            <span class="fc-stat-chip is-amber">E ${sum.draws}</span>
            <span class="fc-stat-chip is-red">D ${sum.losses}</span>
            <span class="fc-stat-chip is-blue" title="Cobertura AH (${sum.cover_wins}C / ${sum.cover_losses}NC / ${sum.cover_push}P)">AH ${coverTxt}</span>
            ${favChip}
            ${dogChip}
            <span class="fc-stat-chip is-teal" title="Over/Under (${sum.overs}O / ${sum.unders}U)">O/U ${overTxt}</span>
            ${gfAvg ? `<span class="fc-stat-chip">${esc(gfAvg)}</span>` : ''}
        </div>`;
    }

    async function fetchJson(url, options = {}) {
        const resp = await fetch(url, options);
        const body = await resp.json().catch(() => ({}));
        if (!resp.ok) {
            throw new Error(body.error || body.message || `HTTP ${resp.status}`);
        }
        return body;
    }

    function preSeedCachesFromCard(card) {
        if (!card || typeof card !== 'object') return;
        ['prev_home', 'prev_away', 'h2h_stadium', 'h2h_general', 'h2h_col3', 'ind_local', 'ind_visitante'].forEach((k) => {
            const n = card[k];
            if (!n || typeof n !== 'object') return;
            if (Array.isArray(n.stats_rows) && n.stats_rows.length) {
                const mid = String(n.match_id || '').replace(/\D/g, '');
                const payload = {
                    status: 'success',
                    match_id: mid,
                    stats: n.stats_rows,
                    home_red: n.home_red,
                    away_red: n.away_red,
                    ou_line: n.ou || n.ou_line || '',
                    ah_line: n.ah || n.ah_line || n.handicap || '',
                };
                if (mid) state.caches.stats.set(`id:${mid}`, payload);
                const homeT = n.home_team || '';
                const awayT = n.away_team || '';
                if (homeT || awayT) {
                    state.caches.stats.set(`meta:${homeT}|${awayT}|${n.date || ''}|${n.score || ''}`, payload);
                }
            }
        });
    }

    function updateTopbar() {
        const t = state.data?.target || {};
        const titleEl = $('fc-title-teams');
        const metaEl = $('fc-title-meta');
        if (titleEl) {
            titleEl.innerHTML = `<span class="text-info">${esc(t.home || 'Local')}</span> <span class="opacity-75 mx-1">vs</span> <span class="text-warning">${esc(t.away || 'Visitante')}</span>`;
        }
        const localPersp = formatAhPerspective(t.ah, true, 'team', { teamAh: t.home_target_ah });
        if (metaEl) {
            metaEl.innerHTML = `
                <span class="fc-meta-chip"><i class="fa-solid fa-trophy me-1 text-warning"></i>${esc(t.league_name || t.league_id || '-')}</span>
                <span class="fc-meta-chip"><i class="fa-regular fa-calendar me-1"></i>${esc(t.date || '-')}</span>
                <span class="fc-meta-chip" title="AH perspectiva Local">Local ${esc(localPersp.label)} (NG ${esc(t.ah || '-')})</span>
                <span class="fc-meta-chip">O/U ${esc(t.ou || '-')}</span>
                <span class="fc-meta-chip">Score ${esc(t.score || '?:?')}</span>
                <span class="fc-meta-chip">ID ${esc(t.match_id || MATCH_ID)}</span>
                ${state.data?.hydrated_from_live ? '<span class="fc-meta-chip" style="background:rgba(16,185,129,.25);color:#a7f3d0;">Hidratado en vivo ✓</span>' : ''}
            `;
        }
        const ccBadge = $('fc-tab-common-count');
        if (ccBadge) {
            ccBadge.textContent = String((state.data?.common_rivals || []).length);
        }
        const h2hBadge = $('fc-tab-h2h-count');
        if (h2hBadge) {
            h2hBadge.textContent = String((state.data?.h2h_history || []).length);
        }
    }

    function buildRowActionButtonsHtml(opts) {
        const attrs = [
            `data-mid="${esc(opts.matchId || '')}"`,
            `data-home="${esc(opts.homeTeam || '')}"`,
            `data-away="${esc(opts.awayTeam || '')}"`,
            `data-date="${esc(opts.date || '')}"`,
            `data-score="${esc(opts.score || '')}"`,
            `data-ah="${esc(opts.ah || '')}"`,
            `data-ou="${esc(opts.ou || '')}"`,
            `data-home-red="${esc(opts.homeRed ?? '')}"`,
            `data-away-red="${esc(opts.awayRed ?? '')}"`,
            `data-role="${esc(opts.role || 'home')}"`,
            `data-slot="${esc(opts.preferredSlot || '')}"`,
            `data-home-id="${esc(opts.homeId || '')}"`,
            `data-away-id="${esc(opts.awayId || '')}"`,
        ].join(' ');

        const statsBtn = `<button type="button" class="fc-act-btn is-stats" data-fc-action="stats" ${attrs} title="Cargar estadísticas en Inspector Dual"><i class="fa-solid fa-chart-simple me-1"></i>Stats</button>`;
        if (opts.statsOnly) return `<div class="fc-row-actions mt-1">${statsBtn}</div>`;
        return `<div class="fc-row-actions">
            ${statsBtn}
            <button type="button" class="fc-act-btn is-col3" data-fc-action="col3" ${attrs} title="Cargar H2H Col3 en Inspector Dual">Col3</button>
            <button type="button" class="fc-act-btn is-mov" data-fc-action="mov" ${attrs} title="Cargar Mov. Histórico en Inspector Dual">Mov</button>
        </div>`;
    }

    function renderStandingsMini(st, isHome) {
        if (!st || typeof st !== 'object') return '';
        const rank = st.ranking ? `<div class="fw-bold text-dark">[#${esc(st.ranking)}]</div>` : '';
        const gen = st.total_pj ? `<div class="fc-row0-sub">G: ${esc(st.total_v)}-${esc(st.total_e)}-${esc(st.total_d)} (${esc(st.total_pj)}) · ${esc(st.total_gf)}:${esc(st.total_gc)}</div>` : '';
        const specPrefix = isHome ? 'L' : 'V';
        const spec = st.specific_pj ? `<div class="fc-row0-sub">${specPrefix}: ${esc(st.specific_v)}-${esc(st.specific_e)}-${esc(st.specific_d)} (${esc(st.specific_pj)})</div>` : '';
        return rank + gen + spec;
    }

    function renderPrevCell(node, side, target) {
        if (!node || (node.score == null && node.ah == null && !node.home_team && !node.away_team)) {
            return '<span class="text-muted">-</span>';
        }
        let rHome = node.home_team || '';
        let rAway = node.away_team || '';
        if ((!rHome || !rAway) && node.rival) {
            const parts = String(node.rival).split(' vs ');
            if (parts.length > 1) {
                rHome = parts[0].trim();
                rAway = parts[1].trim();
            } else if (side === 'home') {
                rHome = target.home;
                rAway = node.rival;
            } else {
                rHome = node.rival;
                rAway = target.away;
            }
        }
        const cov = node.cover_result || '';
        const ahPersp = formatAhPerspective(node.ah, side === 'home', state.ahViewMode);
        const movBadge = node.movement ? formatMovBadge(node.movement) : '';
        const covBadge = cov ? `<span class="fc-cover-badge ${coverCssClass(cov)}">${esc(coverLabel(cov))}</span>` : '';
        const ouPill = node.ou_movement
            ? `<div class="mt-1"><span class="fc-ou-pill is-${esc(node.ou_res || 'SIN_DATO')}">Goles ${esc(node.ou_movement)}${node.ou_res ? ` · ${esc(node.ou_res)}` : ''}</span></div>`
            : (node.ou ? `<div class="mt-1">${ouArrowHtml(node.ou, node.ou_res || 'SIN_DATO')}</div>` : '');

        const btn = (node.score || node.match_id) ? buildRowActionButtonsHtml({
            matchId: node.match_id,
            homeTeam: rHome,
            awayTeam: rAway,
            date: node.date,
            score: node.score,
            ah: node.ah,
            ou: node.ou || node.ou_line,
            homeRed: node.home_red,
            awayRed: node.away_red,
            preferredSlot: side === 'home' ? 'A' : 'B',
            statsOnly: true,
        }) : '';

        return `<div class="d-flex flex-column gap-1">
            ${node.date ? `<div class="fc-row0-sub">${esc(node.date)}</div>` : ''}
            <div class="d-flex justify-content-between align-items-center gap-2">
                ${ahPersp.html}
                ${formatOrientedScore(node.score, side === 'home', '', node.home_red, node.away_red)}
            </div>
            ${(movBadge || covBadge) ? `<div class="d-flex flex-wrap align-items-center gap-1">${movBadge}${covBadge}</div>` : ''}
            ${ouPill}
            <div class="fc-row0-sub text-dark">
                <button type="button" class="fc-rival-link" style="font-size:.66rem;" data-fc-select-team="${esc(rHome)}" data-fc-side="A">${esc(rHome)}</button>
                <span class="text-muted">vs</span>
                <button type="button" class="fc-rival-link" style="font-size:.66rem;" data-fc-select-team="${esc(rAway)}" data-fc-side="B">${esc(rAway)}</button>
            </div>
            ${btn}
        </div>`;
    }

    function renderH2hCell(node, target) {
        if (!node || ((!node.movement || node.movement === 'N/A') && (!node.score || node.score === '?:?'))) {
            return '<span class="text-muted">-</span>';
        }
        const btn = (node.score && node.score !== '?:?') ? buildRowActionButtonsHtml({
            matchId: node.match_id,
            homeTeam: node.home_team || target.home,
            awayTeam: node.away_team || target.away,
            date: node.date,
            score: node.score,
            ah: node.movement || node.ah,
            ou: node.ou || node.ou_line,
            homeRed: node.home_red,
            awayRed: node.away_red,
            preferredSlot: 'A',
            statsOnly: true,
        }) : '';

        const movTxt = node.movement || node.ah || '-';
        return `<div class="d-flex flex-column gap-1">
            ${node.date ? `<div class="fc-row0-sub">${esc(node.date)}</div>` : ''}
            <div>${formatMovBadge(movTxt)}</div>
            <div class="fw-bold text-primary">${esc(node.score || '-')}</div>
            ${node.ou ? ouArrowHtml(node.ou, node.ou_res || 'SIN_DATO') : ''}
            ${btn}
        </div>`;
    }

    function renderCol3Cell(card, target) {
        const h = card?.h2h_col3;
        if (!h || (h.score == null && h.ah == null)) return '<span class="text-muted">-</span>';
        const compType = h.comparison_type ? `<span class="badge ${h.comparison_type === 'directa' ? 'bg-info text-dark' : 'bg-warning text-dark'}" style="font-size:.58rem;">${esc(h.comparison_type)}</span>` : '';
        const covTxt = h.cover_status || '';
        const btn = h.score ? buildRowActionButtonsHtml({
            matchId: h.match_id,
            homeTeam: h.home_team || '',
            awayTeam: h.away_team || '',
            date: h.date,
            score: h.score,
            ah: h.ah,
            ou: h.ou || h.ou_line,
            preferredSlot: 'A',
            statsOnly: true,
        }) : '';

        return `<div class="d-flex flex-column gap-1">
            <div class="d-flex align-items-center gap-1 flex-wrap">${compType}${covTxt ? `<span class="fc-cover-badge ${coverCssClass(covTxt)}">${esc(covTxt)}</span>` : ''}</div>
            ${h.date ? `<div class="fc-row0-sub">${esc(h.date)}</div>` : ''}
            <div class="d-flex justify-content-between gap-2">
                <span class="fw-bold">${esc(h.ah ?? '-')}</span>
                <span class="fw-bold">${esc(h.score || '-')}</span>
            </div>
            <div class="fc-row0-sub text-dark">${esc(h.home_team || '-')} vs ${esc(h.away_team || '-')}</div>
            ${btn}
        </div>`;
    }

    function renderIndCell(node, defaultSideTeam, preferredSlot) {
        if (!node || (node.score == null && node.ah_line == null && node.ah == null)) {
            return '<span class="text-muted">-</span>';
        }
        const ahStr = node.ah_line ?? node.ah ?? '-';
        const loc = String(node.localia || '').toUpperCase();
        const isH = loc === 'H' || loc === 'L' || loc === 'HOME' || loc === 'LOCAL';
        const badge = node.localia ? `<span class="fc-venue-badge ${isH ? 'is-h' : 'is-v'}">${isH ? 'H' : 'A'}</span>` : '';
        const btn = node.score ? buildRowActionButtonsHtml({
            matchId: node.match_id,
            homeTeam: node.home_team || (isH ? defaultSideTeam : ''),
            awayTeam: node.away_team || (isH ? '' : defaultSideTeam),
            date: node.date,
            score: node.score,
            ah: ahStr,
            ou: node.ou || node.ou_line,
            preferredSlot,
            statsOnly: true,
        }) : '';

        return `<div class="d-flex flex-column gap-1">
            ${node.date ? `<div class="fc-row0-sub">${esc(node.date)}</div>` : ''}
            <div class="d-flex justify-content-between gap-2">
                <span class="fw-bold">${esc(ahStr)}</span>
                <span class="fw-bold text-primary">${esc(node.score || '-')}</span>
            </div>
            ${(node.home_team || node.away_team) ? `<div class="fc-row0-sub text-dark">${badge}${esc(node.home_team || '')} vs ${esc(node.away_team || '')}</div>` : badge}
            ${btn}
        </div>`;
    }

    function renderRow0() {
        const container = $('fc-row0-container');
        if (!container || !state.data) return;
        const card = state.data.row0 || {};
        const cand = card.candidate || {};
        const target = state.data.target || {};
        const homeName = cand.home || target.home || 'Local';
        const awayName = cand.away || target.away || 'Visitante';

        const st = card.h2h_stadium;
        const gen = card.h2h_general;
        const genSameAsSt = st && gen && st.date === gen.date && st.score === gen.score;

        container.innerHTML = `<table class="fc-row0-table">
            <thead>
                <tr>
                    <th>Fecha / Liga</th>
                    <th>Local</th>
                    <th>Visitante</th>
                    <th>AH / O-U</th>
                    <th>Res.</th>
                    <th>Prev. Home</th>
                    <th>Prev. Away</th>
                    <th>H2H Estadio</th>
                    <th>H2H General</th>
                    <th class="col-h2h-col3">H2H Col3</th>
                    <th>Ind. Local</th>
                    <th>Ind. Visitante</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td style="min-width:95px;">
                        <div class="fw-bold">${esc(target.date || card.date || '-')}</div>
                        <div class="fc-row0-sub">${esc(target.league_name || card.league || '-')}</div>
                        <div class="fc-row0-sub">ID ${esc(target.match_id || MATCH_ID)}</div>
                    </td>
                    <td style="min-width:135px;">
                        <button type="button" class="fc-row0-team-btn is-home" data-fc-select-team="${esc(homeName)}" data-fc-side="A" data-fc-opp="${esc(awayName)}" title="Abrir ${esc(homeName)} vs ${esc(awayName)} en Comparar Equipos">${esc(homeName)}</button>
                        ${buildQuickAbBtnsHtml(homeName, awayName)}
                        ${renderStandingsMini(cand.home_standings, true)}
                    </td>
                    <td style="min-width:135px;">
                        <button type="button" class="fc-row0-team-btn is-away" data-fc-select-team="${esc(awayName)}" data-fc-side="B" data-fc-opp="${esc(homeName)}" title="Abrir ${esc(awayName)} vs ${esc(homeName)} en Comparar Equipos">${esc(awayName)}</button>
                        ${buildQuickAbBtnsHtml(awayName, homeName)}
                        ${renderStandingsMini(cand.away_standings, false)}
                    </td>
                    <td class="text-center" style="min-width:86px;">
                        <div class="d-flex flex-column align-items-center gap-1">
                            ${formatAhPerspective(cand.ah_real ?? target.ah, true, state.ahViewMode, { teamAh: target.home_target_ah }).html}
                            <div class="fc-row0-sub fw-bold text-dark">O/U ${esc(cand.ou_line ?? target.ou ?? '-')}</div>
                        </div>
                    </td>
                    <td class="text-center" style="min-width:60px;">
                        <div class="fc-row0-score">${esc(cand.score || target.score || '?:?')}</div>
                    </td>
                    <td style="min-width:140px;">${renderPrevCell(card.prev_home, 'home', target)}</td>
                    <td style="min-width:140px;">${renderPrevCell(card.prev_away, 'away', target)}</td>
                    <td style="min-width:105px;">${renderH2hCell(st, target)}</td>
                    <td style="min-width:105px;">${genSameAsSt ? '<span class="fc-row0-sub">(= Estadio)</span>' : renderH2hCell(gen, target)}</td>
                    <td class="col-h2h-col3" style="min-width:145px;">${renderCol3Cell(card, target)}</td>
                    <td style="min-width:125px;">${renderIndCell(card.ind_local, homeName, 'A')}</td>
                    <td style="min-width:125px;">${renderIndCell(card.ind_visitante, awayName, 'B')}</td>
                </tr>
            </tbody>
        </table>`;
    }

    function filterRowsByPerspectiveAndOutcome(rows, opts = {}) {
        const f = opts || {};
        const q = normTeam(f.search || '');
        const roleFilter = f.roleFilter || 'all';
        const outcomeFilter = f.outcomeFilter || 'all';
        const limitNum = f.limit && f.limit !== '20' && f.limit !== 'all' ? Number(f.limit) : null;

        let out = (rows || []).filter((r) => {
            if (f.scope === 'same_league' && !r.same_league) return false;
            if (f.onlyCommon && !r.is_common_all && !r.has_common_rival) return false;
            if (f.onlySimAh && !r.is_similar_ah) return false;

            if (roleFilter === 'FAV' || roleFilter === 'DOG') {
                if (resolveRowTeamRole(r) !== roleFilter) return false;
            }

            if (outcomeFilter && outcomeFilter !== 'all') {
                const cov = r.ah_cover_detail || r.ah_cover || r.ah_eval?.code || 'SIN_DATO';
                const ouRes = r.ou_res || 'SIN_DATO';
                if (outcomeFilter === 'COVER' && cov !== 'COVER' && cov !== 'HALF_WIN') return false;
                if (outcomeFilter === 'NO_COVER' && cov !== 'NO_COVER' && cov !== 'HALF_LOSS') return false;
                if (outcomeFilter === 'OVER' && ouRes !== 'OVER') return false;
                if (outcomeFilter === 'UNDER' && ouRes !== 'UNDER') return false;
            }

            if (q && !normTeam(r.rival).includes(q) && !normTeam(r.league_id).includes(q)) return false;
            return true;
        });

        if (limitNum && Number.isFinite(limitNum) && limitNum > 0) {
            out = out.slice(0, limitNum);
        }
        return out;
    }

    function filterTableRows(rows) {
        return filterRowsByPerspectiveAndOutcome(rows, state.filters);
    }

    function renderHistoryRowsTbody(rows, sideRole, targetTeam, oppTeam) {
        if (!rows.length) {
            return '<tr><td colspan="8" class="text-center text-muted py-3">Sin partidos que cumplan los filtros actuales</td></tr>';
        }
        const preferredSlot = sideRole === 'home' ? 'A' : 'B';
        return rows.map((r) => {
            const commonCls = r.is_common_all ? 'fc-row-common' : '';
            const venueBadge = `<span class="fc-venue-badge ${r.is_home ? 'is-h' : 'is-v'}" title="${r.is_home ? 'Jugó de Local' : 'Jugó de Visitante'}">${r.is_home ? 'H' : 'V'}</span>`;
            const commonStar = r.is_common_all
                ? `<span class="fc-common-star" title="${r.is_common_venue ? 'Rival común en esta condición (Casa/Fuera)' : 'Rival común en historial general'}"><i class="fa-solid fa-star"></i>${r.is_common_venue ? 'Sede' : 'Común'}</span>`
                : '';
            const crossInfo = (r.other_matches_against_rival || [])[0];
            const crossSub = crossInfo
                ? `<div class="fc-row0-sub text-warning-emphasis">${esc(oppTeam)} vs él: <b>${esc(crossInfo.score)}</b> (AH ${esc(crossInfo.ah)})</div>`
                : '';

            const covCode = r.ah_cover_detail || r.ah_cover || 'SIN_DATO';
            const ahPersp = formatAhPerspective(r.ah, r.is_home, state.ahViewMode, {
                teamAh: r.team_ah,
                isExactToday: r.is_exact_ah,
                isSimToday: r.is_similar_ah,
            });

            const actHtml = buildRowActionButtonsHtml({
                matchId: r.match_id,
                homeTeam: r.home_team,
                awayTeam: r.away_team,
                date: r.date,
                score: r.score,
                ah: r.ah,
                ou: r.ou,
                homeRed: r.home_red,
                awayRed: r.away_red,
                role: sideRole,
                preferredSlot,
                homeId: r.home_id,
                awayId: r.away_id,
            });

            return `<tr class="${commonCls}" data-rival-norm="${esc(r.rival_norm || '')}">
                <td style="white-space:nowrap;" title="${esc(r.date)}">${venueBadge}<span>${esc(r.short_date || r.date)}</span></td>
                <td><span class="${r.same_league ? 'fw-bold text-primary' : 'text-muted'}">${esc(r.league_id)}</span></td>
                <td>
                    <div class="fc-rival-cell">
                        <button type="button" class="fc-rival-link" data-fc-select-rival="${esc(r.rival)}" data-fc-rival-side="${esc(sideRole)}" title="Comparar a ${esc(r.rival)} vs ${esc(oppTeam)}">${esc(r.rival)}</button>
                        ${buildQuickAbBtnsHtml(r.rival, oppTeam)}
                        ${commonStar}
                    </div>
                    ${crossSub}
                </td>
                <td class="text-center">${formatOrientedScore(r.score, r.is_home, r.wdl, r.home_red, r.away_red)}</td>
                <td class="text-center">${ahPersp.html}</td>
                <td class="text-center"><span class="fc-cover-badge ${coverCssClass(covCode)}">${esc(coverLabel(covCode))}</span></td>
                <td class="text-center">${ouArrowHtml(r.ou, r.ou_res)}</td>
                <td class="text-end">${actHtml}</td>
            </tr>`;
        }).join('');
    }

    function renderTabTables() {
        const t = state.data.target || {};
        const tables = state.data.tables || {};
        const f = state.filters;
        const useAll = f.scope === 'all' || f.scope === 'same_league';
        const rawHome = useAll ? (tables.home_all || []) : (tables.home_venue || []);
        const rawAway = useAll ? (tables.away_all || []) : (tables.away_venue || []);

        const homeRows = filterTableRows(rawHome);
        const awayRows = filterTableRows(rawAway);
        const homeSum = calcSummaryFromRows(homeRows);
        const awaySum = calcSummaryFromRows(awayRows);

        const scopeLabelHome = f.scope === 'venue' ? 'CASA' : (f.scope === 'same_league' ? 'MISMA LIGA' : 'TODOS');
        const scopeLabelAway = f.scope === 'venue' ? 'FUERA' : (f.scope === 'same_league' ? 'MISMA LIGA' : 'TODOS');

        return `
            <div class="fc-filter-bar">
                <div class="fc-filter-group">
                    <span class="fc-filter-label">Vista AH:</span>
                    <div class="fc-seg-group" title="Cambiar entre Perspectiva de cada equipo (FAV/DOG) y Línea cruda NowGoal">
                        <button type="button" class="fc-seg-btn ${state.ahViewMode === 'team' ? 'is-active' : ''}" data-fc-ah-mode="team">Perspectiva Equipo</button>
                        <button type="button" class="fc-seg-btn ${state.ahViewMode === 'nowgoal' ? 'is-active' : ''}" data-fc-ah-mode="nowgoal">Línea NowGoal</button>
                    </div>
                    <span class="fc-filter-label ms-2">Sede / Liga:</span>
                    <button type="button" class="fc-pill-btn ${f.scope === 'venue' ? 'is-active' : ''}" data-fc-filter="scope" data-val="venue">Casa vs Fuera</button>
                    <button type="button" class="fc-pill-btn ${f.scope === 'all' ? 'is-active' : ''}" data-fc-filter="scope" data-val="all">Todas las sedes</button>
                    <button type="button" class="fc-pill-btn ${f.scope === 'same_league' ? 'is-active' : ''}" data-fc-filter="scope" data-val="same_league">Misma liga</button>
                    <span class="fc-filter-label ms-2">Últimos:</span>
                    <button type="button" class="fc-pill-btn ${f.limit === '5' ? 'is-active' : ''}" data-fc-filter="limit" data-val="5">5</button>
                    <button type="button" class="fc-pill-btn ${f.limit === '10' ? 'is-active' : ''}" data-fc-filter="limit" data-val="10">10</button>
                    <button type="button" class="fc-pill-btn ${f.limit === '20' ? 'is-active' : ''}" data-fc-filter="limit" data-val="20">Todos (20)</button>
                </div>
                <div class="fc-filter-group">
                    <span class="fc-filter-label">Rol:</span>
                    <button type="button" class="fc-pill-btn ${f.roleFilter === 'all' ? 'is-active' : ''}" data-fc-filter="roleFilter" data-val="all">Todos</button>
                    <button type="button" class="fc-pill-btn ${f.roleFilter === 'FAV' ? 'is-active' : ''}" data-fc-filter="roleFilter" data-val="FAV">Fav</button>
                    <button type="button" class="fc-pill-btn ${f.roleFilter === 'DOG' ? 'is-active' : ''}" data-fc-filter="roleFilter" data-val="DOG">Dog</button>
                    <span class="fc-filter-label ms-1">Pick:</span>
                    <button type="button" class="fc-pill-btn ${f.outcomeFilter === 'all' ? 'is-active' : ''}" data-fc-filter="outcomeFilter" data-val="all">Todos</button>
                    <button type="button" class="fc-pill-btn ${f.outcomeFilter === 'COVER' ? 'is-active' : ''}" data-fc-filter="outcomeFilter" data-val="COVER">✓ Cubre</button>
                    <button type="button" class="fc-pill-btn ${f.outcomeFilter === 'NO_COVER' ? 'is-active' : ''}" data-fc-filter="outcomeFilter" data-val="NO_COVER">✕ Falla</button>
                    <button type="button" class="fc-pill-btn ${f.outcomeFilter === 'OVER' ? 'is-active' : ''}" data-fc-filter="outcomeFilter" data-val="OVER">↑ Over</button>
                    <button type="button" class="fc-pill-btn ${f.outcomeFilter === 'UNDER' ? 'is-active' : ''}" data-fc-filter="outcomeFilter" data-val="UNDER">↓ Under</button>
                    <button type="button" class="fc-pill-btn is-toggle ${f.onlyCommon ? 'is-active' : ''}" data-fc-toggle="onlyCommon"><i class="fa-solid fa-star me-1"></i>Comunes</button>
                    <button type="button" class="fc-pill-btn is-toggle ${f.onlySimAh ? 'is-active' : ''}" data-fc-toggle="onlySimAh"><i class="fa-solid fa-scale-balanced me-1"></i>AH ±0.5</button>
                    <input type="search" id="fc-search-rival" class="fc-search-input" value="${esc(f.search)}" placeholder="Filtrar rival o liga...">
                </div>
            </div>

            <div class="fc-split-grid">
                <div class="fc-panel-card is-home">
                    <div class="fc-panel-head">
                        <div class="fc-panel-team-title">
                            <button type="button" class="fc-row0-team-btn is-home" data-fc-select-team="${esc(t.home)}" data-fc-side="A" data-fc-opp="${esc(t.away)}">${esc(t.home)}</button>
                            ${buildQuickAbBtnsHtml(t.home, t.away)}
                            <span class="badge bg-primary" style="font-size:.63rem;">${scopeLabelHome}</span>
                        </div>
                        ${summaryPillsHtml(homeSum)}
                    </div>
                    <div class="fc-table-wrap">
                        <table class="fc-data-table">
                            <thead>
                                <tr><th>Fecha</th><th>Liga</th><th>Rival</th><th class="text-center">Res</th><th class="text-center">AH</th><th class="text-center">Cov AH</th><th class="text-center">O/U</th><th class="text-end">Ficha</th></tr>
                            </thead>
                            <tbody>${renderHistoryRowsTbody(homeRows, 'home', t.home, t.away)}</tbody>
                        </table>
                    </div>
                </div>

                <div class="fc-panel-card is-away">
                    <div class="fc-panel-head">
                        <div class="fc-panel-team-title">
                            <button type="button" class="fc-row0-team-btn is-away" data-fc-select-team="${esc(t.away)}" data-fc-side="B" data-fc-opp="${esc(t.home)}">${esc(t.away)}</button>
                            ${buildQuickAbBtnsHtml(t.away, t.home)}
                            <span class="badge bg-danger" style="font-size:.63rem;">${scopeLabelAway}</span>
                        </div>
                        ${summaryPillsHtml(awaySum)}
                    </div>
                    <div class="fc-table-wrap">
                        <table class="fc-data-table">
                            <thead>
                                <tr><th>Fecha</th><th>Liga</th><th>Rival</th><th class="text-center">Res</th><th class="text-center">AH</th><th class="text-center">Cov AH</th><th class="text-center">O/U</th><th class="text-end">Ficha</th></tr>
                            </thead>
                            <tbody>${renderHistoryRowsTbody(awayRows, 'away', t.away, t.home)}</tbody>
                        </table>
                    </div>
                </div>
            </div>
        `;
    }

    function renderCommonSideMiniTable(matches, sideRole, preferredSlot) {
        if (!matches || !matches.length) return '<div class="text-muted p-2">Sin partidos registrados</div>';
        return `<table class="fc-data-table">
            <thead><tr><th>Fecha</th><th>Sede</th><th class="text-center">Res</th><th class="text-center">AH</th><th class="text-center">Cov</th><th class="text-center">O/U</th><th class="text-end">Inspector</th></tr></thead>
            <tbody>
                ${matches.map((m) => {
                    const covCode = m.ah_cover_detail || m.ah_cover || 'SIN_DATO';
                    const ahPersp = formatAhPerspective(m.ah, m.is_home, state.ahViewMode, { teamAh: m.team_ah });
                    return `<tr>
                        <td>${esc(m.short_date || m.date)}</td>
                        <td><span class="fc-venue-badge ${m.is_home ? 'is-h' : 'is-v'}">${m.is_home ? 'Local' : 'Visita'}</span><span class="fc-row0-sub">L.${esc(m.league_id)}</span></td>
                        <td class="text-center">${formatOrientedScore(m.score, m.is_home, m.wdl, m.home_red, m.away_red)}</td>
                        <td class="text-center">${ahPersp.html}</td>
                        <td class="text-center"><span class="fc-cover-badge ${coverCssClass(covCode)}">${esc(coverLabel(covCode))}</span></td>
                        <td class="text-center">${ouArrowHtml(m.ou, m.ou_res)}</td>
                        <td class="text-end">${buildRowActionButtonsHtml({
                            matchId: m.match_id,
                            homeTeam: m.home_team,
                            awayTeam: m.away_team,
                            date: m.date,
                            score: m.score,
                            ah: m.ah,
                            ou: m.ou,
                            homeRed: m.home_red,
                            awayRed: m.away_red,
                            role: sideRole,
                            preferredSlot,
                            homeId: m.home_id,
                            awayId: m.away_id,
                        })}</td>
                    </tr>`;
                }).join('')}
            </tbody>
        </table>`;
    }

    function renderTabCommon() {
        const crList = state.data.common_rivals || [];
        const t = state.data.target || {};
        if (!crList.length) {
            return '<div class="fc-empty-box"><i class="fa-solid fa-circle-info me-1"></i>No se encontraron rivales comunes entre ambos equipos en sus últimos 20 partidos.</div>';
        }
        return `<div class="fc-common-grid">
            ${crList.map((cr, idx) => {
                const hSum = cr.summary?.home || {};
                const aSum = cr.summary?.away || {};
                return `<article class="fc-common-card">
                    <div class="fc-common-head">
                        <div class="d-flex align-items-center gap-2 flex-wrap">
                            <span class="badge ${cr.in_venue ? 'bg-warning text-dark' : 'bg-secondary'}">${cr.in_venue ? '★ En sede (Casa vs Fuera)' : 'Historial General'}</span>
                            <button type="button" class="fc-rival-link fs-6" data-fc-select-rival="${esc(cr.rival)}" data-fc-rival-side="home" title="Analizar historial completo de ${esc(cr.rival)} en Comparar A vs B">${esc(cr.rival)}</button>
                            ${buildQuickAbBtnsHtml(cr.rival)}
                        </div>
                        <div class="d-flex align-items-center gap-2 flex-wrap">
                            <button type="button" class="fc-act-btn is-stats" data-fc-compare-common="${idx}" title="Cargar las Stats del primer partido del Local (Slot A) vs primer partido del Visitante (Slot B) contra este rival">
                                <i class="fa-solid fa-code-compare me-1"></i>Comparar Stats A vs B
                            </button>
                            <button type="button" class="fc-act-btn is-col3" data-fc-select-team="${esc(cr.rival)}" data-fc-side="A" data-fc-opp="${esc(t.away)}">
                                Abrir ${esc(cr.rival)} en A vs B
                            </button>
                        </div>
                    </div>
                    <div class="fc-split-grid" style="padding:10px;">
                        <div class="fc-panel-card is-home">
                            <div class="fc-panel-head">
                                <span class="fw-bold text-primary">${esc(t.home)} vs ${esc(cr.rival)}</span>
                                ${summaryPillsHtml(hSum)}
                            </div>
                            <div class="fc-table-wrap">${renderCommonSideMiniTable(cr.home_matches, 'home', 'A')}</div>
                        </div>
                        <div class="fc-panel-card is-away">
                            <div class="fc-panel-head">
                                <span class="fw-bold text-danger">${esc(t.away)} vs ${esc(cr.rival)}</span>
                                ${summaryPillsHtml(aSum)}
                            </div>
                            <div class="fc-table-wrap">${renderCommonSideMiniTable(cr.away_matches, 'away', 'B')}</div>
                        </div>
                    </div>
                </article>`;
            }).join('')}
        </div>`;
    }

    function findCommonBetweenSides(matchesA, matchesB) {
        const mapA = new Map();
        const mapB = new Map();
        (matchesA || []).forEach((m) => {
            const rn = normTeam(m.rival);
            if (!rn) return;
            if (!mapA.has(rn)) mapA.set(rn, []);
            mapA.get(rn).push(m);
        });
        (matchesB || []).forEach((m) => {
            const rn = normTeam(m.rival);
            if (!rn) return;
            if (!mapB.has(rn)) mapB.set(rn, []);
            mapB.get(rn).push(m);
        });
        const commonSet = new Set();
        const pairs = [];
        mapA.forEach((listA, rn) => {
            if (mapB.has(rn)) {
                commonSet.add(rn);
                const listB = mapB.get(rn);
                const dA = String(listA[0]?.date || '');
                const dB = String(listB[0]?.date || '');
                pairs.push({
                    rival: listA[0]?.rival || listB[0]?.rival || rn,
                    rivalNorm: rn,
                    matchesA: listA,
                    matchesB: listB,
                    latestDate: dA > dB ? dA : dB,
                });
            }
        });
        pairs.sort((x, y) => String(y.latestDate || '').localeCompare(String(x.latestDate || '')));
        return { commonSet, pairs };
    }

    function getComparatorFilteredMatches(sideKey, commonSet) {
        const slot = state.comparator[sideKey];
        if (!slot?.data) return [];
        const raw = slot.data.matches || [];
        return raw.filter((m) => {
            if (slot.venue !== 'all' && m.venue !== slot.venue) return false;
            if (state.comparator.roleFilter === 'FAV' || state.comparator.roleFilter === 'DOG') {
                if (resolveRowTeamRole(m) !== state.comparator.roleFilter) return false;
            }
            if (state.comparator.onlyCommonAB && commonSet && !commonSet.has(normTeam(m.rival))) {
                return false;
            }
            return true;
        });
    }

    function renderVsComparisonCard(nameA, sumA, nameB, sumB, commonCount) {
        if (!nameA || !nameB || !sumA || !sumB) return '';
        const pctWins = (s) => (s.total > 0 ? Math.round((s.wins * 1000) / s.total) / 10 : null);
        const rows = [
            { label: '% Victorias', a: pctWins(sumA), b: pctWins(sumB), unit: '%' },
            { label: '% AH Cubierto', a: sumA.cover_pct, b: sumB.cover_pct, unit: '%' },
            { label: '% AH Favorito', a: sumA.fav_cover_pct, b: sumB.fav_cover_pct, unit: '%' },
            { label: '% AH No Fav', a: sumA.dog_cover_pct, b: sumB.dog_cover_pct, unit: '%' },
            { label: '% Over Goles', a: sumA.over_pct, b: sumB.over_pct, unit: '%' },
            { label: 'Goles Favor / PJ', a: sumA.goals_for_avg, b: sumB.goals_for_avg, unit: '' },
            { label: 'Goles Contra / PJ', a: sumA.goals_against_avg, b: sumB.goals_against_avg, unit: '' },
        ];

        const barsHtml = rows.map((row) => {
            const na = (row.a !== null && row.a !== undefined && Number.isFinite(Number(row.a))) ? Number(row.a) : null;
            const nb = (row.b !== null && row.b !== undefined && Number.isFinite(Number(row.b))) ? Number(row.b) : null;
            let pA = 50;
            let pB = 50;
            if (na !== null && nb !== null && (na + nb) > 0) {
                pA = Math.round((na * 100) / (na + nb));
                pB = 100 - pA;
            } else if (na !== null && nb === null) {
                pA = 100; pB = 0;
            } else if (nb !== null && na === null) {
                pA = 0; pB = 100;
            }
            const txtA = na !== null ? `${na}${row.unit}` : 'N/D';
            const txtB = nb !== null ? `${nb}${row.unit}` : 'N/D';
            return `<div class="fc-vs-row">
                <div class="fc-vs-val is-a">${esc(txtA)}</div>
                <div class="fc-vs-bar-mid">
                    <div class="fc-vs-bar-label">${esc(row.label)}</div>
                    <div class="fc-vs-bar-track">
                        <div class="fc-vs-bar-a" style="width:${pA}%;"></div>
                        <div class="fc-vs-bar-b" style="width:${pB}%;"></div>
                    </div>
                </div>
                <div class="fc-vs-val is-b">${esc(txtB)}</div>
            </div>`;
        }).join('');

        return `<section class="fc-vs-card">
            <div class="fc-vs-header">
                <div class="fc-vs-side-name is-a"><span class="badge bg-primary">A</span><span>${esc(nameA)}</span><span class="badge bg-light text-dark border">${sumA.total} PJ</span></div>
                <div class="fc-vs-center-pill">VS · ${commonCount} Rivales Comunes A∩B</div>
                <div class="fc-vs-side-name is-b"><span class="badge bg-light text-dark border">${sumB.total} PJ</span><span>${esc(nameB)}</span><span class="badge bg-danger">B</span></div>
            </div>
            <div class="fc-vs-metrics">${barsHtml}</div>
        </section>`;
    }

    function renderMirrorCommonTable(aName, bName, pairs) {
        if (!pairs || !pairs.length) return '';
        const formatMiniSide = (m) => {
            if (!m) return '<span class="text-muted">-</span>';
            const ahP = formatAhPerspective(m.ah, m.is_home, state.ahViewMode, { teamAh: m.team_ah });
            const covCode = m.ah_eval?.code || m.ah_cover || 'SIN_DATO';
            return `<div class="d-flex flex-wrap align-items-center gap-1">
                <span class="fc-venue-badge ${m.is_home ? 'is-h' : 'is-v'}">${m.is_home ? 'H' : 'V'}</span>
                <span class="fc-row0-sub">${esc(m.date || '-')}</span>
                ${formatOrientedScore(m.score, m.is_home, m.wdl)}
                ${ahP.html}
                <span class="fc-cover-badge ${coverCssClass(covCode)}">${esc(coverLabel(covCode))}</span>
                ${ouArrowHtml(m.ou, m.ou_res)}
            </div>`;
        };

        return `<section class="fc-mirror-card">
            <div class="fc-mirror-head">
                <span class="fw-bold small text-dark"><i class="fa-solid fa-star text-warning me-1"></i>Espejo Directo: Frente a los Mismos Rivales (${esc(aName)} vs ${esc(bName)} · ${pairs.length})</span>
                <span class="text-muted" style="font-size:.69rem;">Compara en una sola línea cómo rindió cada uno ante el mismo rival</span>
            </div>
            <div class="fc-table-wrap">
                <table class="fc-data-table fc-mirror-table">
                    <thead>
                        <tr>
                            <th>${esc(aName)} (Lado A) vs Rival</th>
                            <th class="text-center">Rival Común</th>
                            <th>${esc(bName)} (Lado B) vs Rival</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${pairs.slice(0, 14).map((p) => `<tr data-rival-norm="${esc(p.rivalNorm)}">
                            <td>${formatMiniSide(p.matchesA[0])}${p.matchesA.length > 1 ? `<div class="fc-row0-sub mt-1">+${p.matchesA.length - 1} cruce(s) previo(s)</div>` : ''}</td>
                            <td class="text-center">
                                <div class="fw-bold text-dark">${esc(p.rival)}</div>
                                ${buildQuickAbBtnsHtml(p.rival)}
                            </td>
                            <td>${formatMiniSide(p.matchesB[0])}${p.matchesB.length > 1 ? `<div class="fc-row0-sub mt-1">+${p.matchesB.length - 1} cruce(s) previo(s)</div>` : ''}</td>
                        </tr>`).join('')}
                    </tbody>
                </table>
            </div>
        </section>`;
    }

    function renderComparatorSide(sideKey, commonSet = new Set()) {
        const slot = state.comparator[sideKey];
        const t = state.data.target || {};
        const teams = state.data.selectable_teams || [];
        const borderCls = sideKey === 'A' ? 'is-home' : 'is-away';
        const badgeCls = sideKey === 'A' ? 'bg-primary' : 'bg-danger';

        const optionsHtml = teams.map((item) => {
            const selected = sameTeam(item.name, slot.teamName) ? 'selected' : '';
            const roleTxt = item.role === 'target_home' ? ' [Local]' : (item.role === 'target_away' ? ' [Visita]' : (item.role === 'common_rival' ? ' ★ Común' : ''));
            return `<option value="${esc(item.name)}" ${selected}>${esc(item.name)}${roleTxt} (${item.count})</option>`;
        }).join('');

        let bodyHtml = '<div class="fc-empty-box">Selecciona un equipo arriba para cargar su historial reciente.</div>';
        if (slot.loading) {
            bodyHtml = '<div class="fc-loading-inline"><i class="fa-solid fa-spinner fa-spin me-2"></i>Cargando historial de ' + esc(slot.teamName) + '...</div>';
        } else if (slot.error) {
            bodyHtml = `<div class="alert alert-danger m-2 p-2 small mb-0">${esc(slot.error)}</div>`;
        } else if (slot.data) {
            const filtered = getComparatorFilteredMatches(sideKey, commonSet);
            const otherSlot = state.comparator[sideKey === 'A' ? 'B' : 'A'];
            const otherRivals = new Set((otherSlot.data?.matches || []).map((x) => normTeam(x.rival)));
            const stObj = calcSummaryFromRows(filtered);
            const stSummary = `<div class="p-2 border-bottom bg-light">${summaryPillsHtml(stObj)}</div>`;

            bodyHtml = `${stSummary}<div class="fc-table-wrap"><table class="fc-data-table">
                <thead><tr><th>Fecha</th><th>Liga</th><th>Rival</th><th class="text-center">Res</th><th class="text-center">AH</th><th class="text-center">Cov</th><th class="text-center">O/U</th><th class="text-end">Inspector</th></tr></thead>
                <tbody>
                    ${filtered.length ? filtered.map((m) => {
                        const rNorm = normTeam(m.rival);
                        const isSharedAB = m.has_common_rival || (rNorm && (commonSet.has(rNorm) || otherRivals.has(rNorm)));
                        const covCode = m.ah_eval?.code || 'SIN_DATO';
                        const ahPersp = formatAhPerspective(m.ah, m.is_home, state.ahViewMode, { teamAh: m.team_ah });
                        const compTxt = m.comparative_match
                            ? `<div class="fc-row0-sub text-warning-emphasis">Oponente vs él: <b>${esc(m.comparative_match.score || '-')}</b> (${esc(m.comparative_match.ah || '-')})</div>`
                            : '';
                        return `<tr class="${isSharedAB ? 'fc-row-common' : ''}" data-rival-norm="${esc(rNorm)}">
                            <td style="white-space:nowrap;"><span class="fc-venue-badge ${m.is_home ? 'is-h' : 'is-v'}">${m.is_home ? 'H' : 'V'}</span>${esc(m.date)}</td>
                            <td>${esc(m.league_id || '-')}</td>
                            <td>
                                <div class="fc-rival-cell">
                                    <button type="button" class="fc-rival-link" data-fc-select-team="${esc(m.rival)}" data-fc-side="${sideKey === 'A' ? 'B' : 'A'}" title="Cargar ${esc(m.rival)} en Lado ${sideKey === 'A' ? 'B' : 'A'}">${esc(m.rival)}</button>
                                    ${buildQuickAbBtnsHtml(m.rival, slot.teamName)}
                                    ${isSharedAB ? '<span class="fc-common-star"><i class="fa-solid fa-star"></i>Común</span>' : ''}
                                </div>
                                ${compTxt}
                            </td>
                            <td class="text-center">${formatOrientedScore(m.score, m.is_home, m.wdl)}</td>
                            <td class="text-center">${ahPersp.html}</td>
                            <td class="text-center"><span class="fc-cover-badge ${coverCssClass(covCode)}">${esc(coverLabel(covCode))}</span></td>
                            <td class="text-center">${ouArrowHtml(m.ou, m.ou_res || 'SIN_DATO')}</td>
                            <td class="text-end">${buildRowActionButtonsHtml({
                                matchId: m.match_id,
                                homeTeam: m.home_team,
                                awayTeam: m.away_team,
                                date: m.date,
                                score: m.score,
                                ah: m.ah,
                                ou: m.ou,
                                role: sideKey === 'A' ? 'home' : 'away',
                                preferredSlot: sideKey,
                            })}</td>
                        </tr>`;
                    }).join('') : '<tr><td colspan="8" class="text-center text-muted py-3">Sin partidos con los filtros actuales</td></tr>'}
                </tbody>
            </table></div>`;
        }

        return `<div class="fc-panel-card ${borderCls}">
            <div class="fc-panel-head">
                <div class="fc-comp-select-row">
                    <span class="badge ${badgeCls}">LADO ${sideKey}</span>
                    <select class="form-select form-select-sm" style="width:auto; min-width:210px; font-weight:700;" data-fc-comp-select="${sideKey}">
                        <option value="">-- Elegir equipo --</option>
                        ${optionsHtml}
                    </select>
                </div>
                <div class="fc-filter-group">
                    <button type="button" class="fc-pill-btn ${slot.venue === 'all' ? 'is-active' : ''}" data-fc-comp-venue="${sideKey}" data-val="all">Todos</button>
                    <button type="button" class="fc-pill-btn ${slot.venue === 'home' ? 'is-active' : ''}" data-fc-comp-venue="${sideKey}" data-val="home">Casa</button>
                    <button type="button" class="fc-pill-btn ${slot.venue === 'away' ? 'is-active' : ''}" data-fc-comp-venue="${sideKey}" data-val="away">Fuera</button>
                </div>
            </div>
            ${slot.teamName ? `<div class="px-3 py-1 bg-white border-bottom small text-muted">Mostrando: <strong class="text-dark">${esc(slot.teamName)}</strong> (vs ${esc(state.comparator[sideKey === 'A' ? 'B' : 'A'].teamName || t.away)})</div>` : ''}
            ${bodyHtml}
        </div>`;
    }

    function renderTabTeams() {
        const aName = state.comparator.A.teamName;
        const bName = state.comparator.B.teamName;
        const customCol3 = state.comparator.customCol3;
        const teams = state.data.selectable_teams || [];
        const searchQ = normTeam(state.comparator.searchTeam || '');
        const matchingTeams = teams.filter((it) => !searchQ || normTeam(it.name).includes(searchQ)).slice(0, 14);

        const matchesA = state.comparator.A.data?.matches || [];
        const matchesB = state.comparator.B.data?.matches || [];
        const { commonSet, pairs } = (state.comparator.A.data && state.comparator.B.data)
            ? findCommonBetweenSides(matchesA, matchesB)
            : { commonSet: new Set(), pairs: [] };

        const filteredForSummaryA = getComparatorFilteredMatches('A', commonSet);
        const filteredForSummaryB = getComparatorFilteredMatches('B', commonSet);
        const sumA = state.comparator.A.data ? calcSummaryFromRows(filteredForSummaryA) : null;
        const sumB = state.comparator.B.data ? calcSummaryFromRows(filteredForSummaryB) : null;

        let col3Banner = '';
        if (state.comparator.customCol3Loading) {
            col3Banner = '<div class="fc-loading-inline mb-2"><i class="fa-solid fa-spinner fa-spin me-2"></i>Buscando H2H Col3 entre ' + esc(aName) + ' y ' + esc(bName) + '...</div>';
        } else if (customCol3) {
            const h = customCol3.h2h_col3 || {};
            if (h.status === 'found') {
                col3Banner = `<div class="alert alert-light border border-primary mb-2 py-2 px-3 small d-flex flex-wrap align-items-center justify-content-between gap-2">
                    <div>
                        <span class="badge bg-primary me-2">H2H Col3 (${esc(aName)} ↔ ${esc(bName)})</span>
                        <b>${esc(h.h2h_home_team_name)}</b> <span class="badge bg-dark mx-1">${esc(h.goles_home)}:${esc(h.goles_away)}</span> <b>${esc(h.h2h_away_team_name)}</b>
                        <span class="text-muted ms-2">AH ${esc(h.handicap || '-')} · ${esc(h.date || '')}</span>
                    </div>
                    ${buildRowActionButtonsHtml({
                        matchId: h.match_id || '',
                        homeTeam: h.h2h_home_team_name || aName,
                        awayTeam: h.h2h_away_team_name || bName,
                        date: h.date || '',
                        score: `${h.goles_home}:${h.goles_away}`,
                        ah: h.handicap || '',
                        ou: '',
                        preferredSlot: 'A',
                        statsOnly: true,
                    })}
                </div>`;
            } else {
                col3Banner = `<div class="alert alert-secondary mb-2 py-2 px-3 small">${esc(h.resultado || `No se encontró H2H directo entre ${aName} y ${bName}`)}</div>`;
            }
        }

        const quickBarHtml = `<div class="fc-quick-team-bar">
            <span class="fc-quick-team-label"><i class="fa-solid fa-bolt text-warning me-1"></i>Acceso rápido:</span>
            <input type="search" id="fc-comp-search-team" class="fc-search-input" style="min-width:155px;" value="${esc(state.comparator.searchTeam || '')}" placeholder="Buscar equipo A/B...">
            ${matchingTeams.map((it) => {
                const roleCls = it.role === 'target_home' ? 'is-target-home' : (it.role === 'target_away' ? 'is-target-away' : (it.role === 'common_rival' ? 'is-common' : ''));
                const icon = it.role === 'common_rival' ? '★ ' : '';
                return `<span class="fc-qteam-pill ${roleCls}">${icon}${esc(it.name)}${buildQuickAbBtnsHtml(it.name)}</span>`;
            }).join('')}
        </div>`;

        return `
            ${quickBarHtml}
            <div class="fc-comp-toolbar">
                <div class="fc-filter-group">
                    <button type="button" class="fc-pill-btn" id="fc-comp-reset-targets"><i class="fa-solid fa-rotate-left me-1"></i>Local vs Visitante</button>
                    <button type="button" class="fc-pill-btn" id="fc-comp-swap"><i class="fa-solid fa-right-left me-1"></i>A ↔ B</button>
                    <div class="fc-seg-group ms-1">
                        <button type="button" class="fc-seg-btn ${state.ahViewMode === 'team' ? 'is-active' : ''}" data-fc-ah-mode="team">Perspectiva</button>
                        <button type="button" class="fc-seg-btn ${state.ahViewMode === 'nowgoal' ? 'is-active' : ''}" data-fc-ah-mode="nowgoal">NowGoal</button>
                    </div>
                    <span class="fc-filter-label ms-1">Rol:</span>
                    <button type="button" class="fc-pill-btn ${state.comparator.roleFilter === 'all' ? 'is-active' : ''}" data-fc-comp-role="all">Todos</button>
                    <button type="button" class="fc-pill-btn ${state.comparator.roleFilter === 'FAV' ? 'is-active' : ''}" data-fc-comp-role="FAV">Fav</button>
                    <button type="button" class="fc-pill-btn ${state.comparator.roleFilter === 'DOG' ? 'is-active' : ''}" data-fc-comp-role="DOG">Dog</button>
                    <button type="button" class="fc-pill-btn is-toggle ${state.comparator.onlyCommonAB ? 'is-active' : ''}" data-fc-comp-toggle="onlyCommonAB"><i class="fa-solid fa-star me-1"></i>Solo Comunes (${pairs.length})</button>
                    <button type="button" class="fc-pill-btn is-toggle ${state.comparator.showMirror ? 'is-active' : ''}" data-fc-comp-toggle="showMirror"><i class="fa-solid fa-table-list me-1"></i>Espejo A∩B</button>
                </div>
                <div class="fc-filter-group">
                    <button type="button" class="fc-act-btn is-col3 py-1 px-2" id="fc-comp-find-col3" ${(!aName || !bName) ? 'disabled' : ''}>
                        <i class="fa-solid fa-magnifying-glass me-1"></i>H2H Col3 (${esc(aName || 'A')} vs ${esc(bName || 'B')})
                    </button>
                </div>
            </div>
            ${col3Banner}
            ${renderVsComparisonCard(aName, sumA, bName, sumB, pairs.length)}
            ${(state.comparator.showMirror && pairs.length) ? renderMirrorCommonTable(aName, bName, pairs) : ''}
            <div class="fc-split-grid">
                ${renderComparatorSide('A', commonSet)}
                ${renderComparatorSide('B', commonSet)}
            </div>
        `;
    }

    function renderSimilarLineBlock(title, subtitle, homeObj, awayObj, target) {
        return `<div class="fc-panel-card mb-3">
            <div class="fc-panel-head">
                <div>
                    <span class="fw-bold fs-6">${esc(title)}</span>
                    <span class="text-muted small ms-2">${esc(subtitle)}</span>
                </div>
            </div>
            <div class="fc-split-grid" style="padding:10px;">
                <div class="fc-panel-card is-home">
                    <div class="fc-panel-head">
                        <span class="fw-bold text-primary">${esc(target.home)} <span class="badge bg-primary">Línea Obj: ${esc(homeObj.target_line ?? '-')}</span></span>
                        ${summaryPillsHtml(homeObj.summary)}
                    </div>
                    <div class="fc-table-wrap">
                        <table class="fc-data-table">
                            <thead><tr><th>Fecha</th><th>Liga</th><th>Rival</th><th class="text-center">Res</th><th class="text-center">AH</th><th class="text-center">Cov AH</th><th class="text-center">O/U</th><th class="text-end">Ficha</th></tr></thead>
                            <tbody>${renderHistoryRowsTbody(homeObj.rows || [], 'home', target.home, target.away)}</tbody>
                        </table>
                    </div>
                </div>
                <div class="fc-panel-card is-away">
                    <div class="fc-panel-head">
                        <span class="fw-bold text-danger">${esc(target.away)} <span class="badge bg-danger">Línea Obj: ${esc(awayObj.target_line ?? '-')}</span></span>
                        ${summaryPillsHtml(awayObj.summary)}
                    </div>
                    <div class="fc-table-wrap">
                        <table class="fc-data-table">
                            <thead><tr><th>Fecha</th><th>Liga</th><th>Rival</th><th class="text-center">Res</th><th class="text-center">AH</th><th class="text-center">Cov AH</th><th class="text-center">O/U</th><th class="text-end">Ficha</th></tr></thead>
                            <tbody>${renderHistoryRowsTbody(awayObj.rows || [], 'away', target.away, target.home)}</tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>`;
    }

    function renderTabLines() {
        const sim = state.data.similar_lines || {};
        const t = state.data.target || {};
        return `
            ${renderSimilarLineBlock(
                `Handicap Asiático Similar (±${sim.ah_threshold ?? 0.5})`,
                'Comparación desde la perspectiva de cada equipo en sus últimos 20 partidos',
                sim.home_ah || {},
                sim.away_ah || {},
                t
            )}
            ${renderSimilarLineBlock(
                `Línea de Goles O/U Similar (±${sim.ou_threshold ?? 0.25})`,
                'Rendimiento Over / Under alrededor de la línea actual de goles',
                sim.home_ou || {},
                sim.away_ou || {},
                t
            )}
        `;
    }

    function renderTabH2hHistory() {
        const allH2h = state.data.h2h_history || [];
        const t = state.data.target || {};
        if (!allH2h.length) {
            return '<div class="fc-empty-box"><i class="fa-solid fa-clock-rotate-left me-1"></i>No hay enfrentamientos H2H previos registrados para este cruce.</div>';
        }

        const stadList = allH2h.filter((r) => r.is_same_stadium);
        const invList = allH2h.filter((r) => !r.is_same_stadium);
        const lastStad = stadList[0] || null;
        const lastAny = allH2h[0] || null;

        const todayPersp = formatAhPerspective(t.ah, true, state.ahViewMode, { teamAh: t.home_target_ah });

        const renderHeroHistoryCard = (entry, title, cls) => {
            if (!entry) {
                return `<div class="fc-h2h-card ${cls}">
                    <div class="fc-h2h-card-kicker">${esc(title)}</div>
                    <div class="text-muted small">Sin registro previo en esta condición</div>
                </div>`;
            }
            const ahP = formatAhPerspective(entry.ah, entry.is_same_stadium, state.ahViewMode, { teamAh: entry.home_persp_ah });
            const movAhHtml = entry.ah_movement_vs_today
                ? formatMovBadge(entry.ah_movement_vs_today, entry.ah_delta_vs_today, entry.ah_mov_dir_vs_today)
                : '';
            const movOuHtml = entry.ou_movement_vs_today
                ? formatMovBadge(`O/U ${entry.ou_movement_vs_today}`, entry.ou_delta_vs_today, entry.ou_mov_dir_vs_today)
                : '';
            return `<div class="fc-h2h-card ${cls}">
                <div class="fc-h2h-card-kicker">${esc(title)}</div>
                <div class="fc-h2h-card-main">
                    ${entry.year ? `<span class="fc-year-badge">${esc(entry.year)}</span>` : ''}
                    <span>${esc(entry.short_date || entry.date || '-')}</span>
                    ${formatOrientedScore(entry.score, entry.is_same_stadium, entry.wdl_local)}
                    ${ahP.html}
                </div>
                <div class="d-flex flex-wrap align-items-center gap-1 mt-1">
                    <span class="fc-stadium-pill ${entry.is_same_stadium ? 'is-same' : 'is-inv'}">${entry.is_same_stadium ? 'Mismo Estadio' : 'Estadio Invertido'}</span>
                    ${movAhHtml}
                    <span class="fc-cover-badge ${coverCssClass(entry.cover_own)}">Prop: ${esc(coverLabel(entry.cover_own))}</span>
                    <span class="fc-cover-badge ${coverCssClass(entry.cover_today)}">Hoy: ${esc(coverLabel(entry.cover_today))}</span>
                    ${ouArrowHtml(entry.ou, entry.ou_res)}
                    ${movOuHtml}
                </div>
            </div>`;
        };

        const heroHtml = `<div class="fc-h2h-hero">
            <div class="fc-h2h-card is-today">
                <div class="fc-h2h-card-kicker">HOY · PARTIDO ACTUAL (${esc(t.date || '-')})</div>
                <div class="fc-h2h-card-main">
                    <span>${esc(t.home || 'Local')} vs ${esc(t.away || 'Visitante')}</span>
                </div>
                <div class="d-flex flex-wrap align-items-center gap-2 mt-1">
                    ${todayPersp.html}
                    <span class="fc-ou-pill is-SIN_DATO">O/U ${esc(t.ou || '-')}</span>
                    <span class="badge bg-dark">${esc(t.score || '?:?')}</span>
                </div>
            </div>
            ${renderHeroHistoryCard(lastStad, 'ÚLTIMO EN ESTE ESTADIO', 'is-stadium')}
            ${renderHeroHistoryCard(lastAny, 'ÚLTIMO EN GENERAL (CRUCE DIRECTO)', 'is-general')}
        </div>`;

        const hFilter = state.h2hFilter || 'all';
        const rows = allH2h.filter((r) => {
            if (hFilter === 'stadium') return !!r.is_same_stadium;
            if (hFilter === 'inverted') return !r.is_same_stadium;
            return true;
        });

        const fmtDeltaPrev = (val) => {
            if (val === null || val === undefined || Number.isNaN(Number(val))) return '';
            const n = Number(val);
            if (Math.abs(n) < 0.01) return '<div class="fc-row0-sub">= vs anterior</div>';
            return `<div class="fc-row0-sub fw-bold ${n > 0 ? 'text-primary' : 'text-warning-emphasis'}">${n > 0 ? '▲ +' : '▼ '}${formatNumClean(n)} vs ant.</div>`;
        };

        return `${heroHtml}
            <div class="fc-filter-bar mb-2">
                <div class="fc-filter-group">
                    <span class="fc-filter-label">Estadio H2H:</span>
                    <button type="button" class="fc-pill-btn ${hFilter === 'all' ? 'is-active' : ''}" data-fc-h2h-filter="all">Todos (${allH2h.length})</button>
                    <button type="button" class="fc-pill-btn ${hFilter === 'stadium' ? 'is-active' : ''}" data-fc-h2h-filter="stadium">Mismo Estadio (${stadList.length})</button>
                    <button type="button" class="fc-pill-btn ${hFilter === 'inverted' ? 'is-active' : ''}" data-fc-h2h-filter="inverted">Invertido (${invList.length})</button>
                </div>
                <div class="fc-filter-group">
                    <span class="fc-filter-label">Vista AH:</span>
                    <div class="fc-seg-group">
                        <button type="button" class="fc-seg-btn ${state.ahViewMode === 'team' ? 'is-active' : ''}" data-fc-ah-mode="team">Perspectiva Local</button>
                        <button type="button" class="fc-seg-btn ${state.ahViewMode === 'nowgoal' ? 'is-active' : ''}" data-fc-ah-mode="nowgoal">Línea NowGoal</button>
                    </div>
                </div>
            </div>
            <div class="fc-panel-card">
                <div class="fc-panel-head">
                    <span class="fw-bold"><i class="fa-solid fa-clock-rotate-left text-primary me-1"></i>Evolución Histórica Año a Año (${esc(t.home)} en perspectiva fija)</span>
                    <span class="text-muted small">Movimientos siempre medidos respecto a la línea actual del Local (${esc(todayPersp.label)})</span>
                </div>
                <div class="fc-table-wrap">
                    <table class="fc-data-table">
                        <thead>
                            <tr>
                                <th>Año / Fecha</th>
                                <th>Estadio</th>
                                <th>Cruce Histórico</th>
                                <th class="text-center">Res (Local)</th>
                                <th class="text-center">AH Histórico</th>
                                <th class="text-center">Mov AH → Hoy</th>
                                <th class="text-center">Cov Propia / Hoy</th>
                                <th class="text-center">O/U · Mov → Hoy</th>
                                <th class="text-end">Ficha</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${rows.map((r) => {
                                const ahP = formatAhPerspective(r.ah, r.is_same_stadium, state.ahViewMode, { teamAh: r.home_persp_ah });
                                const movAhBadge = r.ah_movement_vs_today
                                    ? formatMovBadge(r.ah_movement_vs_today, r.ah_delta_vs_today, r.ah_mov_dir_vs_today)
                                    : '<span class="text-muted">-</span>';
                                const movOuBadge = r.ou_movement_vs_today
                                    ? formatMovBadge(r.ou_movement_vs_today, r.ou_delta_vs_today, r.ou_mov_dir_vs_today)
                                    : '';
                                return `<tr>
                                    <td style="white-space:nowrap;">
                                        ${r.year ? `<span class="fc-year-badge me-1">${esc(r.year)}</span>` : ''}
                                        <span>${esc(r.short_date || r.date || '-')}</span>
                                    </td>
                                    <td><span class="fc-stadium-pill ${r.is_same_stadium ? 'is-same' : 'is-inv'}">${r.is_same_stadium ? 'Mismo Estadio' : 'Invertido'}</span></td>
                                    <td>
                                        <span class="${r.is_same_stadium ? 'fw-bold text-primary' : 'text-dark'}">${esc(r.home_team)}</span>
                                        <span class="text-muted mx-1">vs</span>
                                        <span class="${r.is_same_stadium ? 'text-danger fw-bold' : 'text-dark'}">${esc(r.away_team)}</span>
                                        ${r.league ? `<div class="fc-row0-sub">${esc(r.league)}</div>` : ''}
                                    </td>
                                    <td class="text-center">${formatOrientedScore(r.score, r.is_same_stadium, r.wdl_local)}</td>
                                    <td class="text-center">${ahP.html}</td>
                                    <td class="text-center">${movAhBadge}${fmtDeltaPrev(r.ah_delta_vs_prev)}</td>
                                    <td class="text-center">
                                        <div class="d-flex flex-column align-items-center gap-1">
                                            <span class="fc-cover-badge ${coverCssClass(r.cover_own)}">Prop: ${esc(coverLabel(r.cover_own))}</span>
                                            <span class="fc-cover-badge ${coverCssClass(r.cover_today)}">Hoy: ${esc(coverLabel(r.cover_today))}</span>
                                        </div>
                                    </td>
                                    <td class="text-center">
                                        <div class="d-flex flex-column align-items-center gap-1">
                                            ${ouArrowHtml(r.ou, r.ou_res)}
                                            ${movOuBadge}
                                        </div>
                                    </td>
                                    <td class="text-end">${buildRowActionButtonsHtml({
                                        matchId: r.match_id,
                                        homeTeam: r.home_team,
                                        awayTeam: r.away_team,
                                        date: r.date,
                                        score: r.score,
                                        ah: r.ah,
                                        ou: r.ou,
                                        preferredSlot: 'A',
                                        statsOnly: true,
                                    })}</td>
                                </tr>`;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
            </div>`;
    }

    function renderTabStandings() {
        const st = state.standings;
        if (!st) {
            return '<div class="fc-loading-card"><div class="spinner-border text-primary spinner-border-sm me-2"></div>Cargando clasificación oficial de la liga...</div>';
        }
        if (st.loading) {
            return '<div class="fc-loading-card"><div class="spinner-border text-primary spinner-border-sm me-2"></div>Consultando tabla de clasificación...</div>';
        }
        if (st.error) {
            return `<div class="fc-empty-box text-danger"><i class="fa-solid fa-triangle-exclamation me-1"></i>${esc(st.error)}</div>`;
        }
        if (!st.available) {
            return '<div class="fc-empty-box">Clasificación no disponible para esta competición o temporada.</div>';
        }

        const view = state.standingsView;
        const rows = (st.views && st.views[view]) ? st.views[view] : (st.views?.total || []);
        const commonSet = new Set((state.data.common_rivals || []).map((c) => normTeam(c.rival)));
        const t = state.data.target || {};

        return `<div class="fc-panel-card">
            <div class="fc-panel-head">
                <div class="d-flex align-items-center gap-2 flex-wrap">
                    <span class="fw-bold"><i class="fa-solid fa-trophy text-warning me-1"></i>${esc(st.tournament || t.league_name || 'Liga')}</span>
                    ${st.season ? `<span class="badge bg-light text-dark border">${esc(st.season)}</span>` : ''}
                </div>
                <div class="fc-filter-group">
                    <button type="button" class="fc-pill-btn ${view === 'total' ? 'is-active' : ''}" data-fc-standings-view="total">General</button>
                    ${st.views?.home?.length ? `<button type="button" class="fc-pill-btn ${view === 'home' ? 'is-active' : ''}" data-fc-standings-view="home">Local</button>` : ''}
                    ${st.views?.away?.length ? `<button type="button" class="fc-pill-btn ${view === 'away' ? 'is-active' : ''}" data-fc-standings-view="away">Visitante</button>` : ''}
                </div>
            </div>
            <div class="fc-table-wrap">
                <table class="fc-standings-table">
                    <thead>
                        <tr>
                            <th class="text-center">#</th>
                            <th>Equipo</th>
                            <th class="text-center">PJ</th>
                            <th class="text-center">V</th>
                            <th class="text-center">E</th>
                            <th class="text-center">D</th>
                            <th class="text-center">DG</th>
                            <th class="text-center">Goles</th>
                            <th class="text-center">Forma</th>
                            <th class="text-center">Pts</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${rows.map((r) => {
                            const isH = (r.team_id != null && st.home_team_id != null && String(r.team_id) === String(st.home_team_id)) || sameTeam(r.team, t.home);
                            const isA = (r.team_id != null && st.away_team_id != null && String(r.team_id) === String(st.away_team_id)) || sameTeam(r.team, t.away);
                            const isCom = !isH && !isA && commonSet.has(normTeam(r.team));
                            const rowCls = isH ? 'is-target-home' : (isA ? 'is-target-away' : (isCom ? 'is-common-row' : ''));
                            const tag = isH ? '<span class="badge bg-primary ms-1">LOCAL</span>'
                                : (isA ? '<span class="badge bg-danger ms-1">VISITA</span>'
                                : (isCom ? '<span class="fc-common-star">★ Común</span>' : ''));
                            const formPills = Array.isArray(r.form)
                                ? r.form.slice(-5).map((ch) => `<span class="fc-form-pill is-${esc(ch)}">${esc(ch)}</span>`).join('')
                                : '';
                            return `<tr class="${rowCls}">
                                <td class="text-center fw-bold">${esc(r.position ?? '-')}</td>
                                <td>
                                    <div class="fc-rival-cell">
                                        <button type="button" class="fc-rival-link" data-fc-select-team="${esc(r.team)}" data-fc-side="${isA ? 'B' : 'A'}" title="Cargar ${esc(r.team)} en Comparar Equipos">${esc(r.team)}</button>
                                        ${buildQuickAbBtnsHtml(r.team)}
                                        ${tag}
                                    </div>
                                </td>
                                <td class="text-center">${esc(r.matches ?? 0)}</td>
                                <td class="text-center text-success fw-bold">${esc(r.wins ?? 0)}</td>
                                <td class="text-center">${esc(r.draws ?? 0)}</td>
                                <td class="text-center text-danger fw-bold">${esc(r.losses ?? 0)}</td>
                                <td class="text-center">${Number(r.goal_difference) > 0 ? '+' : ''}${esc(r.goal_difference ?? 0)}</td>
                                <td class="text-center font-monospace">${esc(r.scores_for ?? 0)}:${esc(r.scores_against ?? 0)}</td>
                                <td class="text-center">${formPills || '-'}</td>
                                <td class="text-center fw-bold">${esc(r.points ?? 0)}</td>
                            </tr>`;
                        }).join('')}
                    </tbody>
                </table>
            </div>
        </div>`;
    }

    function renderActiveTab() {
        const container = $('fc-tab-content');
        if (!container || !state.data) return;
        let html = '';
        if (state.activeTab === 'tables') html = renderTabTables();
        else if (state.activeTab === 'teams') html = renderTabTeams();
        else if (state.activeTab === 'common') html = renderTabCommon();
        else if (state.activeTab === 'h2h') html = renderTabH2hHistory();
        else if (state.activeTab === 'lines') html = renderTabLines();
        else if (state.activeTab === 'standings') html = renderTabStandings();
        container.innerHTML = html;

        if (typeof document !== 'undefined') {
            document.querySelectorAll('#fc-tabs-nav .fc-tab-btn').forEach((btn) => {
                btn.classList.toggle('is-active', btn.dataset.tab === state.activeTab);
            });
        }
    }

    async function ensureStandingsLoaded() {
        if (state.standings && (state.standings.loading || state.standings.available !== undefined)) {
            return;
        }
        const t = state.data?.target || {};
        state.standings = { loading: true };
        renderActiveTab();
        try {
            const resp = await fetchJson('/api/sofascore/league-table', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    home_name: t.home || '',
                    away_name: t.away || '',
                    league_name: t.league_name || '',
                    match_date: t.date || '',
                    goal_line: t.ou || '',
                    match_id: t.match_id || MATCH_ID,
                    nowgoal_league_id: t.league_id || '',
                }),
            });
            state.standings = resp;
        } catch (err) {
            state.standings = { error: err.message || 'Error cargando clasificación' };
        }
        if (state.activeTab === 'standings') renderActiveTab();
    }

    async function loadTeamComparatorSlot(sideKey, teamName, opponentName) {
        const cleanTeam = String(teamName || '').trim();
        if (!cleanTeam) return;
        const slot = state.comparator[sideKey];
        const t = state.data?.target || {};
        const opp = opponentName || state.comparator[sideKey === 'A' ? 'B' : 'A'].teamName || (sideKey === 'A' ? t.away : t.home) || '';

        slot.teamName = cleanTeam;
        slot.error = null;
        state.comparator.customCol3 = null;

        const cacheKey = `${normTeam(cleanTeam)}|${normTeam(opp)}`;
        if (state.caches.teamHistory.has(cacheKey)) {
            slot.data = state.caches.teamHistory.get(cacheKey);
            slot.loading = false;
            if (state.activeTab === 'teams') renderActiveTab();
            return;
        }

        slot.loading = true;
        if (state.activeTab === 'teams') renderActiveTab();
        try {
            const body = await fetchJson('/api/team_recent_history', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    team_name: cleanTeam,
                    match_id: '',
                    parent_match_id: t.match_id || MATCH_ID,
                    league_id: t.league_id || '',
                    opponent_name: opp,
                }),
            });
            slot.data = body;
            state.caches.teamHistory.set(cacheKey, body);
        } catch (err) {
            slot.error = err.message || 'No se pudo cargar el historial del equipo';
        } finally {
            slot.loading = false;
            if (state.activeTab === 'teams') renderActiveTab();
        }
    }

    function openTeamInComparator(teamName, sideKey, oppName) {
        state.activeTab = 'teams';
        const target = state.data?.target || {};
        const otherKey = sideKey === 'A' ? 'B' : 'A';
        const resolvedOpp = oppName || state.comparator[otherKey].teamName || (sideKey === 'A' ? target.away : target.home);

        if (resolvedOpp && !state.comparator[otherKey].data && !state.comparator[otherKey].loading) {
            loadTeamComparatorSlot(otherKey, resolvedOpp, teamName);
        }
        loadTeamComparatorSlot(sideKey, teamName, resolvedOpp);
        renderActiveTab();
        $('fc-tab-content')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    function resolveInspectorSlot(preferred) {
        if (state.inspectorTarget === 'A' || state.inspectorTarget === 'B') {
            return state.inspectorTarget;
        }
        if (preferred === 'A' || preferred === 'B') {
            return preferred;
        }
        const chosen = state.inspectorNextAuto;
        state.inspectorNextAuto = chosen === 'A' ? 'B' : 'A';
        return chosen;
    }

    function renderStatsSlotHtml(resp, meta) {
        const rows = Array.isArray(resp.stats) ? resp.stats : [];
        const header = `<div class="d-flex justify-content-between align-items-center mb-2 pb-1 border-bottom">
            <div>
                <div class="fw-bold text-dark">${esc(meta.homeTeam || '-')} <span class="text-primary">${esc(meta.score || '-')}</span> ${esc(meta.awayTeam || '-')}</div>
                <div class="fc-row0-sub">${esc(meta.date || '-')} · AH ${esc(resp.ah_line || meta.ah || '-')} · O/U ${esc(resp.ou_line || meta.ou || '-')}</div>
            </div>
            <span class="badge bg-success">STATS</span>
        </div>`;

        if (!rows.length) {
            return `${header}<div class="text-muted text-center py-2">Sin desglose estadístico disponible para este partido.</div>`;
        }
        const bars = rows.map((st) => {
            const lh = Number.parseFloat(String(st.home ?? '').replace(/[^\d.-]/g, ''));
            const la = Number.parseFloat(String(st.away ?? '').replace(/[^\d.-]/g, ''));
            let pH = 50, pA = 50;
            if (Number.isFinite(lh) && Number.isFinite(la) && (lh + la) > 0) {
                pH = Math.round((lh * 100) / (lh + la));
                pA = 100 - pH;
            }
            return `<div class="fc-stat-row">
                <div class="fc-stat-val-left">${esc(st.home ?? '-')}</div>
                <div class="fc-stat-center">
                    <div class="fc-stat-label">${esc(st.label || '')}</div>
                    <div class="fc-stat-bars">
                        <div class="fc-stat-bar-left" style="width:${pH}%;"></div>
                        <div class="fc-stat-bar-right" style="width:${pA}%;"></div>
                    </div>
                </div>
                <div class="fc-stat-val-right">${esc(st.away ?? '-')}</div>
            </div>`;
        }).join('');
        return header + bars;
    }

    function renderCol3SlotHtml(resp, meta) {
        const h = resp.h2h_col3 || {};
        if (h.status !== 'found') {
            return `<div class="text-muted py-2">${esc(h.resultado || `Sin H2H Col3 para ${meta.homeTeam} vs ${meta.awayTeam}`)}</div>`;
        }
        const statsPreview = Array.isArray(h.stats_rows) && h.stats_rows.length
            ? `<div class="mt-2 pt-2 border-top">${renderStatsSlotHtml({ stats: h.stats_rows, ah_line: h.handicap }, {
                homeTeam: h.h2h_home_team_name,
                awayTeam: h.h2h_away_team_name,
                score: `${h.goles_home}:${h.goles_away}`,
                date: h.date,
                ah: h.handicap,
            })}</div>`
            : '';

        return `<div class="d-flex justify-content-between align-items-center mb-2 pb-1 border-bottom">
            <div>
                <div class="fw-bold">${esc(h.h2h_home_team_name)} <span class="badge bg-dark">${esc(h.goles_home)}:${esc(h.goles_away)}</span> ${esc(h.h2h_away_team_name)}</div>
                <div class="fc-row0-sub">${esc(h.date || '-')} · AH ${esc(h.handicap || '-')} · Fuente: ${esc(h.source || 'NowGoal')}</div>
            </div>
            <span class="badge bg-purple text-white" style="background:#6d28d9;">COL3</span>
        </div>${statsPreview}`;
    }

    function renderMovSlotHtml(resp, meta) {
        const parts = [];
        ['stadium', 'general'].forEach((k) => {
            const node = resp[k];
            if (!node) return;
            const title = k === 'stadium' ? 'H2H Mismo Estadio' : 'H2H General';
            const movBadge = (node.movement || node.delta_ah != null)
                ? formatMovBadge(node.movement || node.ah || '-', node.delta_ah, node.movement_dir)
                : `<span class="fw-bold">${esc(node.ah || '-')}</span>`;
            const ouMovBadge = node.ou_movement
                ? formatMovBadge(`O/U ${node.ou_movement}`, node.delta_ou, node.ou_dir)
                : (node.ou ? `<span class="fc-ou-pill is-SIN_DATO">O/U ${esc(node.ou)}</span>` : '');
            const covCls = coverCssClass(node.cover === true ? 'COVER' : (node.cover === false ? 'NO_COVER' : String(node.cover || 'SIN_DATO')));
            parts.push(`<div class="fc-mov-card-block">
                <div class="d-flex justify-content-between align-items-center fw-bold small mb-1">
                    <span>${title}${node.same_stadium === false ? ' <span class="fc-stadium-pill is-inv">Invertido</span>' : ''}</span>
                    <span>${esc(node.date || '-')}</span>
                </div>
                <div class="d-flex justify-content-between align-items-center mb-1">
                    <span>${esc(node.home_team || meta.homeTeam)} vs ${esc(node.away_team || meta.awayTeam)}</span>
                    <span class="badge bg-primary">${esc(node.score || '-')}</span>
                </div>
                <div class="d-flex flex-wrap align-items-center gap-1">
                    ${movBadge}
                    ${node.cover != null ? `<span class="fc-cover-badge ${covCls}">Cov: ${esc(String(node.cover))}</span>` : ''}
                    ${ouMovBadge}
                </div>
            </div>`);
        });

        const hist = Array.isArray(resp.history) ? resp.history : [];
        if (hist.length) {
            parts.push(`<div class="fc-mov-history-mini">
                <div class="fw-bold small text-muted mb-1">Historial H2H año a año (${hist.length}):</div>
                ${hist.map((h) => `<div class="fc-mov-mini-row">
                    <span>
                        ${h.year ? `<span class="fc-year-badge me-1">${esc(h.year)}</span>` : ''}
                        <span class="fc-stadium-pill ${h.same_stadium ? 'is-same' : 'is-inv'}">${h.same_stadium ? 'Est.' : 'Inv.'}</span>
                        <b class="ms-1">${esc(h.score || '-')}</b>
                    </span>
                    <span class="d-flex align-items-center gap-1">
                        <span>AH ${esc(h.ah || '-')}</span>
                        ${h.delta_ah != null ? formatMovBadge(`${h.delta_ah > 0 ? '+' : ''}${formatNumClean(h.delta_ah)}`, h.delta_ah) : ''}
                        ${h.ou && h.ou !== '-' ? `<span class="fc-ou-pill is-SIN_DATO">O/U ${esc(h.ou)}</span>` : ''}
                    </span>
                </div>`).join('')}
            </div>`);
        }

        return parts.length
            ? `<div class="fc-mov-slot-box">${parts.join('')}</div>`
            : '<div class="text-muted py-2">Sin histórico directo previo para este cruce.</div>';
    }

    function updateInspectorDockUi() {
        const dock = $('fc-inspector-dock');
        if (dock) {
            dock.classList.remove('is-collapsed');
            dock.classList.toggle('is-empty', !state.inspectorSlots.A && !state.inspectorSlots.B);
        }
        ['A', 'B'].forEach((sKey) => {
            const slotObj = state.inspectorSlots[sKey];
            const labelEl = $(`fc-slot-${sKey.toLowerCase()}-label`);
            const contentEl = $(`fc-slot-${sKey.toLowerCase()}-content`);
            if (!labelEl || !contentEl) return;
            if (!slotObj) {
                labelEl.textContent = 'Vacío';
                contentEl.innerHTML = `<div class="fc-slot-empty">${sKey === 'A' ? 'Selecciona Stats, Col3 o Mov de cualquier partido.' : 'Carga una segunda ficha para comparar estadísticas lado a lado.'}</div>`;
                return;
            }
            labelEl.textContent = slotObj.title || '';
            contentEl.innerHTML = slotObj.html || '';
        });
    }

    async function loadActionIntoInspector(action, meta, explicitSlot) {
        const slotKey = explicitSlot || resolveInspectorSlot(meta.preferredSlot);
        const t = state.data?.target || {};
        state.inspectorSlots[slotKey] = {
            title: `${action.toUpperCase()} · ${meta.homeTeam} vs ${meta.awayTeam}`,
            meta,
            html: '<div class="fc-loading-inline"><i class="fa-solid fa-spinner fa-spin me-2"></i>Cargando ficha...</div>',
        };
        updateInspectorDockUi();
        $('fc-inspector-dock')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });

        try {
            let html = '';
            if (action === 'stats') {
                const mid = String(meta.matchId || '').replace(/\D/g, '');
                const cKeyId = mid ? `id:${mid}` : '';
                const cKeyMeta = `meta:${meta.homeTeam}|${meta.awayTeam}|${meta.date}|${meta.score}`;
                let res = (cKeyId && state.caches.stats.get(cKeyId)) || state.caches.stats.get(cKeyMeta);
                if (!res) {
                    res = await fetchJson('/api/match_stats', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            match_id: mid,
                            parent_match_id: t.match_id || MATCH_ID,
                            row_home: meta.homeTeam,
                            row_away: meta.awayTeam,
                            row_date: meta.date,
                            row_score: meta.score,
                            row_ah: meta.ah,
                            row_ou: meta.ou,
                            row_home_red: meta.homeRed,
                            row_away_red: meta.awayRed,
                        }),
                    });
                    if (cKeyId) state.caches.stats.set(cKeyId, res);
                    state.caches.stats.set(cKeyMeta, res);
                }
                html = renderStatsSlotHtml(res, meta);
            } else if (action === 'col3') {
                const cKey = `col3:${meta.matchId}|${meta.homeTeam}|${meta.awayTeam}|${meta.role}`;
                let res = state.caches.col3.get(cKey);
                if (!res) {
                    res = await fetchJson('/api/precacheo_h2h_col3', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            match_id: meta.matchId,
                            parent_match_id: t.match_id || MATCH_ID,
                            row_home: meta.homeTeam,
                            row_away: meta.awayTeam,
                            row_date: meta.date,
                            row_role: meta.role || 'home',
                            parent_home: t.home,
                            parent_away: t.away,
                            parent_current_ah: t.ah,
                        }),
                    });
                    state.caches.col3.set(cKey, res);
                }
                html = renderCol3SlotHtml(res, meta);
            } else if (action === 'mov') {
                const cKey = `mov:${meta.matchId}|${meta.homeTeam}|${meta.awayTeam}|${meta.date}|${meta.ou || ''}`;
                let res = state.caches.mov.get(cKey);
                if (!res) {
                    res = await fetchJson('/api/precacheo_h2h_mov_historico', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            match_id: meta.matchId,
                            parent_match_id: t.match_id || MATCH_ID,
                            row_home: meta.homeTeam,
                            row_away: meta.awayTeam,
                            row_ah: meta.ah,
                            row_ou: meta.ou,
                            row_score: meta.score,
                            row_date: meta.date,
                        }),
                    });
                    state.caches.mov.set(cKey, res);
                }
                html = renderMovSlotHtml(res, meta);
            }
            state.inspectorSlots[slotKey] = {
                title: `${action.toUpperCase()} · ${meta.homeTeam} vs ${meta.awayTeam}`,
                meta,
                html,
            };
        } catch (err) {
            state.inspectorSlots[slotKey] = {
                title: `Error · ${meta.homeTeam} vs ${meta.awayTeam}`,
                meta,
                html: `<div class="alert alert-danger m-0 p-2 small">${esc(err.message || 'Error al cargar la ficha')}</div>`,
            };
        }
        updateInspectorDockUi();
    }

    async function loadMainPayload(forceRefresh = false) {
        const tabContent = $('fc-tab-content');
        if (forceRefresh && tabContent) {
            tabContent.innerHTML = '<div class="fc-loading-card"><div class="spinner-border text-primary spinner-border-sm me-2"></div>Actualizando historial previo en vivo...</div>';
        }
        try {
            const url = `/api/ficha_completa/${encodeURIComponent(MATCH_ID)}${forceRefresh ? '?refresh=1' : ''}`;
            const data = await fetchJson(url);
            state.data = data;
            preSeedCachesFromCard(data.row0);
            updateTopbar();
            renderRow0();

            // Preseleccionar Local y Visitante en el comparador (carga bajo demanda cuando entre en Tab 3)
            state.comparator.A.teamName = data.target?.home || '';
            state.comparator.B.teamName = data.target?.away || '';
            state.comparator.A.data = null;
            state.comparator.B.data = null;

            renderActiveTab();
        } catch (err) {
            const msg = `<div class="fc-empty-box text-danger"><i class="fa-solid fa-circle-xmark me-2"></i>Error cargando Ficha Completa (${esc(err.message)})</div>`;
            if ($('fc-row0-container')) $('fc-row0-container').innerHTML = msg;
            if (tabContent) tabContent.innerHTML = msg;
        }
    }

    function applyRow0CollapsedState(collapsed) {
        const wrap = $('fc-row0-wrap');
        const btn = $('fc-row0-toggle');
        if (!wrap || !btn) return;
        wrap.classList.toggle('is-collapsed', !!collapsed);
        btn.innerHTML = collapsed
            ? '<i class="fa-solid fa-chevron-down me-1"></i><span>Mostrar Fila 0</span>'
            : '<i class="fa-solid fa-chevron-up me-1"></i><span>Plegar Fila 0</span>';
    }

    function wireEvents() {
        $('fc-row0-toggle')?.addEventListener('click', () => {
            const wrap = $('fc-row0-wrap');
            if (!wrap) return;
            const collapsed = !wrap.classList.contains('is-collapsed');
            applyRow0CollapsedState(collapsed);
            try {
                if (typeof localStorage !== 'undefined') {
                    localStorage.setItem('fc_row0_collapsed', collapsed ? '1' : '0');
                }
            } catch (_) {}
        });

        $('fc-btn-refresh')?.addEventListener('click', () => loadMainPayload(true));

        $('fc-tabs-nav')?.addEventListener('click', (ev) => {
            const btn = ev.target.closest('.fc-tab-btn');
            if (!btn) return;
            const tab = btn.dataset.tab;
            if (!tab) return;
            state.activeTab = tab;
            if (tab === 'teams') {
                const t = state.data?.target || {};
                if (!state.comparator.A.data && !state.comparator.A.loading && (state.comparator.A.teamName || t.home)) {
                    loadTeamComparatorSlot('A', state.comparator.A.teamName || t.home, state.comparator.B.teamName || t.away);
                }
                if (!state.comparator.B.data && !state.comparator.B.loading && (state.comparator.B.teamName || t.away)) {
                    loadTeamComparatorSlot('B', state.comparator.B.teamName || t.away, state.comparator.A.teamName || t.home);
                }
            } else if (tab === 'standings') {
                ensureStandingsLoaded();
            }
            renderActiveTab();
        });

        // Delegated clicks across Fila 0 and Active Tab content
        document.addEventListener('click', async (ev) => {
            const actBtn = ev.target.closest('[data-fc-action]');
            if (actBtn) {
                const d = actBtn.dataset;
                loadActionIntoInspector(d.fcAction, {
                    matchId: d.mid,
                    homeTeam: d.home,
                    awayTeam: d.away,
                    date: d.date,
                    score: d.score,
                    ah: d.ah,
                    ou: d.ou,
                    homeRed: d.homeRed,
                    awayRed: d.awayRed,
                    role: d.role,
                    preferredSlot: d.slot,
                });
                return;
            }

            const commonCompareBtn = ev.target.closest('[data-fc-compare-common]');
            if (commonCompareBtn) {
                const idx = Number(commonCompareBtn.dataset.fcCompareCommon);
                const cr = (state.data?.common_rivals || [])[idx];
                if (!cr) return;
                const hm = cr.home_matches?.[0];
                const am = cr.away_matches?.[0];
                if (hm) {
                    loadActionIntoInspector('stats', {
                        matchId: hm.match_id,
                        homeTeam: hm.home_team,
                        awayTeam: hm.away_team,
                        date: hm.date,
                        score: hm.score,
                        ah: hm.ah,
                        ou: hm.ou,
                        role: 'home',
                    }, 'A');
                }
                if (am) {
                    loadActionIntoInspector('stats', {
                        matchId: am.match_id,
                        homeTeam: am.home_team,
                        awayTeam: am.away_team,
                        date: am.date,
                        score: am.score,
                        ah: am.ah,
                        ou: am.ou,
                        role: 'away',
                    }, 'B');
                }
                return;
            }

            const selTeamBtn = ev.target.closest('[data-fc-select-team]');
            if (selTeamBtn) {
                openTeamInComparator(
                    selTeamBtn.dataset.fcSelectTeam,
                    selTeamBtn.dataset.fcSide || 'A',
                    selTeamBtn.dataset.fcOpp || ''
                );
                return;
            }

            const selRivalBtn = ev.target.closest('[data-fc-select-rival]');
            if (selRivalBtn) {
                const t = state.data?.target || {};
                const fromSide = selRivalBtn.dataset.fcRivalSide || 'home';
                const rivalName = selRivalBtn.dataset.fcSelectRival;
                if (fromSide === 'home') {
                    openTeamInComparator(rivalName, 'A', t.away);
                } else {
                    openTeamInComparator(rivalName, 'B', t.home);
                }
                return;
            }

            const filterBtn = ev.target.closest('[data-fc-filter]');
            if (filterBtn) {
                state.filters[filterBtn.dataset.fcFilter] = filterBtn.dataset.val;
                renderActiveTab();
                return;
            }

            const toggleBtn = ev.target.closest('[data-fc-toggle]');
            if (toggleBtn) {
                const key = toggleBtn.dataset.fcToggle;
                state.filters[key] = !state.filters[key];
                renderActiveTab();
                return;
            }

            const compVenueBtn = ev.target.closest('[data-fc-comp-venue]');
            if (compVenueBtn) {
                const sKey = compVenueBtn.dataset.fcCompVenue;
                state.comparator[sKey].venue = compVenueBtn.dataset.val;
                renderActiveTab();
                return;
            }

            const ahModeBtn = ev.target.closest('[data-fc-ah-mode]');
            if (ahModeBtn) {
                state.ahViewMode = ahModeBtn.dataset.fcAhMode === 'nowgoal' ? 'nowgoal' : 'team';
                renderRow0();
                renderActiveTab();
                return;
            }

            const h2hFilterBtn = ev.target.closest('[data-fc-h2h-filter]');
            if (h2hFilterBtn) {
                state.h2hFilter = h2hFilterBtn.dataset.fcH2hFilter || 'all';
                renderActiveTab();
                return;
            }

            const compRoleBtn = ev.target.closest('[data-fc-comp-role]');
            if (compRoleBtn) {
                state.comparator.roleFilter = compRoleBtn.dataset.fcCompRole || 'all';
                renderActiveTab();
                return;
            }

            const compToggleBtn = ev.target.closest('[data-fc-comp-toggle]');
            if (compToggleBtn) {
                const key = compToggleBtn.dataset.fcCompToggle;
                if (key) {
                    state.comparator[key] = !state.comparator[key];
                    renderActiveTab();
                }
                return;
            }

            const stViewBtn = ev.target.closest('[data-fc-standings-view]');
            if (stViewBtn) {
                state.standingsView = stViewBtn.dataset.fcStandingsView;
                renderActiveTab();
                return;
            }

            if (ev.target.closest('#fc-comp-reset-targets')) {
                const t = state.data?.target || {};
                loadTeamComparatorSlot('A', t.home, t.away);
                loadTeamComparatorSlot('B', t.away, t.home);
                return;
            }

            if (ev.target.closest('#fc-comp-swap')) {
                const tmpA = { ...state.comparator.A };
                state.comparator.A = { ...state.comparator.B };
                state.comparator.B = tmpA;
                state.comparator.customCol3 = null;
                renderActiveTab();
                return;
            }

            if (ev.target.closest('#fc-comp-find-col3')) {
                const aName = state.comparator.A.teamName;
                const bName = state.comparator.B.teamName;
                const t = state.data?.target || {};
                if (!aName || !bName) return;
                state.comparator.customCol3Loading = true;
                renderActiveTab();
                try {
                    const res = await fetchJson('/api/custom_rival_compare', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            parent_match_id: t.match_id || MATCH_ID,
                            rival_a_name: aName,
                            rival_b_name: bName,
                            league_id: t.league_id || '',
                            parent_home: t.home || '',
                            parent_away: t.away || '',
                            current_ah: t.ah || '',
                        }),
                    });
                    state.comparator.customCol3 = res;
                } catch (err) {
                    state.comparator.customCol3 = { h2h_col3: { status: 'not_found', resultado: err.message } };
                } finally {
                    state.comparator.customCol3Loading = false;
                    renderActiveTab();
                }
                return;
            }
        });

        // Search input & team dropdowns
        document.addEventListener('input', (ev) => {
            if (ev.target && ev.target.id === 'fc-search-rival') {
                const caretPos = ev.target.selectionStart;
                state.filters.search = ev.target.value;
                renderActiveTab();
                const restored = $('fc-search-rival');
                if (restored) {
                    restored.focus();
                    if (typeof caretPos === 'number') restored.setSelectionRange(caretPos, caretPos);
                }
            } else if (ev.target && ev.target.id === 'fc-comp-search-team') {
                const caretPos = ev.target.selectionStart;
                state.comparator.searchTeam = ev.target.value;
                renderActiveTab();
                const restored = $('fc-comp-search-team');
                if (restored) {
                    restored.focus();
                    if (typeof caretPos === 'number') restored.setSelectionRange(caretPos, caretPos);
                }
            }
        });

        document.addEventListener('change', (ev) => {
            const sel = ev.target.closest('[data-fc-comp-select]');
            if (sel) {
                const sKey = sel.dataset.fcCompSelect;
                const opp = state.comparator[sKey === 'A' ? 'B' : 'A'].teamName;
                loadTeamComparatorSlot(sKey, sel.value, opp);
            }
        });

        // Synchronized hover highlight for common rivals across both tables
        document.addEventListener('mouseover', (ev) => {
            const tr = ev.target.closest('tr[data-rival-norm]');
            if (!tr || !tr.dataset.rivalNorm) return;
            const norm = tr.dataset.rivalNorm;
            document.querySelectorAll('tr[data-rival-norm]').forEach((row) => {
                row.classList.toggle('fc-row-common-hover', row.dataset.rivalNorm === norm);
            });
        });

        document.addEventListener('mouseout', (ev) => {
            const tr = ev.target.closest('tr[data-rival-norm]');
            if (!tr) return;
            if (ev.relatedTarget && tr.contains(ev.relatedTarget)) return;
            document.querySelectorAll('tr.fc-row-common-hover').forEach((row) => {
                row.classList.remove('fc-row-common-hover');
            });
        });

        // Inspector Dock controls
        document.querySelectorAll('[data-slot-target]').forEach((btn) => {
            btn.addEventListener('click', () => {
                state.inspectorTarget = btn.dataset.slotTarget;
                document.querySelectorAll('[data-slot-target]').forEach((b) => {
                    b.classList.toggle('is-active', b.dataset.slotTarget === state.inspectorTarget);
                });
            });
        });

        $('fc-slot-a-clear')?.addEventListener('click', () => {
            state.inspectorSlots.A = null;
            updateInspectorDockUi();
        });
        $('fc-slot-b-clear')?.addEventListener('click', () => {
            state.inspectorSlots.B = null;
            updateInspectorDockUi();
        });
        $('fc-inspector-clear-all')?.addEventListener('click', () => {
            state.inspectorSlots.A = null;
            state.inspectorSlots.B = null;
            updateInspectorDockUi();
        });
        $('fc-inspector-collapse')?.addEventListener('click', () => {
            $('fc-inspector-dock')?.classList.toggle('is-collapsed');
        });
        $('fc-inspector-col3-ab')?.addEventListener('click', async () => {
            const slotA = state.inspectorSlots.A?.meta;
            const slotB = state.inspectorSlots.B?.meta;
            const t = state.data?.target || {};
            const pickTeam = (meta, fallbackTeam) => {
                if (!meta) return fallbackTeam;
                if (sameTeam(meta.homeTeam, t.home) || sameTeam(meta.homeTeam, t.away)) {
                    return meta.awayTeam || fallbackTeam;
                }
                if (sameTeam(meta.awayTeam, t.home) || sameTeam(meta.awayTeam, t.away)) {
                    return meta.homeTeam || fallbackTeam;
                }
                return meta.homeTeam || fallbackTeam;
            };
            const rivalA = pickTeam(slotA, t.home);
            const rivalB = pickTeam(slotB, t.away);
            if (!rivalA || !rivalB) return;

            state.inspectorSlots.A = {
                title: `COL3 · ${rivalA} vs ${rivalB}`,
                meta: { homeTeam: rivalA, awayTeam: rivalB },
                html: '<div class="fc-loading-inline"><i class="fa-solid fa-spinner fa-spin me-2"></i>Buscando H2H Col3 entre ambos...</div>',
            };
            updateInspectorDockUi();
            try {
                const res = await fetchJson('/api/custom_rival_compare', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        parent_match_id: t.match_id || MATCH_ID,
                        rival_a_name: rivalA,
                        rival_b_name: rivalB,
                        league_id: t.league_id || '',
                        parent_home: t.home || '',
                        parent_away: t.away || '',
                        current_ah: t.ah || '',
                    }),
                });
                state.inspectorSlots.A = {
                    title: `COL3 · ${rivalA} vs ${rivalB}`,
                    meta: { homeTeam: rivalA, awayTeam: rivalB },
                    html: renderCol3SlotHtml(res, { homeTeam: rivalA, awayTeam: rivalB }),
                };
            } catch (err) {
                state.inspectorSlots.A = {
                    title: `Error COL3 · ${rivalA} vs ${rivalB}`,
                    meta: { homeTeam: rivalA, awayTeam: rivalB },
                    html: `<div class="alert alert-danger m-0 p-2 small">${esc(err.message)}</div>`,
                };
            }
            updateInspectorDockUi();
        });
    }

    if (typeof window !== 'undefined') {
        window.FichaCompletaHelpers = {
            state,
            parseAhNum,
            formatNumClean,
            resolveRowTeamRole,
            formatAhPerspective,
            formatOrientedScore,
            formatMovBadge,
            findCommonBetweenSides,
            filterRowsByPerspectiveAndOutcome,
            calcSummaryFromRows,
            renderTabH2hHistory,
            renderTabTeams,
            renderMovSlotHtml,
            setStateForTest(patch) {
                if (!patch || typeof patch !== 'object') return;
                Object.keys(patch).forEach((k) => {
                    if (k === 'comparator' && patch.comparator) {
                        Object.assign(state.comparator, patch.comparator);
                    } else if (k === 'filters' && patch.filters) {
                        Object.assign(state.filters, patch.filters);
                    } else {
                        state[k] = patch[k];
                    }
                });
            },
        };
    }

    if (typeof document !== 'undefined') {
        document.addEventListener('DOMContentLoaded', () => {
            wireEvents();
            try {
                if (typeof localStorage !== 'undefined' && localStorage.getItem('fc_row0_collapsed') === '1') {
                    applyRow0CollapsedState(true);
                }
            } catch (_) {}
            loadMainPayload(false);
        });
    }
})();
