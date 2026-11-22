/**
 * BÚSQUEDA PATRÓN 2.0 & ETIQUETADO DE EQUIPOS
 * Módulo completamente aislado (window.PatternSearchV2).
 * No altera el buscador actual (searchPatterns / explore_matches / totalResults).
 */
(function () {
    'use strict';

    const State = {
        modalPayload: null,
        activeTab: 'summary',     // 'summary' | 'matrix' | 'matches' | 'antitrap'
        quickFilter: 'ALL',       // 'ALL' | 'SIM75' | 'SIM65' | 'SAME_CROSS' | 'EXACT_AH' | 'TAG:<code>'
        explorerTagMap: {},       // match_id -> profile
        explorerTagSummary: [],
        explorerActiveTag: 'ALL',
        fichaTagCache: {},        // match_id -> profile
        explorerV2: {
            active: false,
            payload: null,
            bridge: null,
            activeFilter: 'ALL',  // 'ALL' | 'TOP5' | 'TOP10' | 'TOP20' | 'TIER1' | 'OU_HIT' | 'OU_MOV_MATCH' | 'H2H_HIT' | 'IND_HIT' | 'COL3_HIT' | 'FICHA_HIT' | 'WITH_FICHA' | 'SIM75' | 'SIM65' | 'EXACT_AH' | 'QUAD:<key>'
            openDrawer: 'none',   // 'none' | 'ladder' | 'matrix' | 'antitrap'
            visibleCards: null,
            ouFilters: {
                ft_ou_res: '',
                prev_home_ou_mov: '',
                prev_home_ou_res: '',
                prev_away_ou_mov: '',
                prev_away_ou_res: '',
                h2h_stadium_ou_mov: '',
                h2h_stadium_ou_res: '',
                h2h_general_ou_mov: '',
                h2h_general_ou_res: '',
            },
            hydratedCount: 0,
            pendingResortCount: 0,
            bgRunning: false,
            bgPaused: false,
            bgRunToken: 0,
            bgTargetTotal: 20,
            bgChunkSize: 4,
            bgNeedsDownloadQueue: [],
            deepBusy: false,
            deepLimit: 15,
            deepProgress: { done: 0, total: 0, downloaded: 0, sqlite: 0, failed: 0, lastMsg: '' },
        },
    };

    const OU_FILTER_DOM_MAP = [
        { key: 'ft_ou_res', id: 'filter-pv2-ft-ou-res', label: 'FT O/U' },
        { key: 'prev_home_ou_mov', id: 'filter-pv2-prev-home-ou-mov', label: 'Prev Loc Mov O/U' },
        { key: 'prev_home_ou_res', id: 'filter-pv2-prev-home-ou-res', label: 'Prev Loc Res O/U' },
        { key: 'prev_away_ou_mov', id: 'filter-pv2-prev-away-ou-mov', label: 'Prev Vis Mov O/U' },
        { key: 'prev_away_ou_res', id: 'filter-pv2-prev-away-ou-res', label: 'Prev Vis Res O/U' },
        { key: 'h2h_stadium_ou_mov', id: 'filter-pv2-h2h-stadium-ou-mov', label: 'H2H Est Mov O/U' },
        { key: 'h2h_stadium_ou_res', id: 'filter-pv2-h2h-stadium-ou-res', label: 'H2H Est Res O/U' },
        { key: 'h2h_general_ou_mov', id: 'filter-pv2-h2h-general-ou-mov', label: 'H2H Gen Mov O/U' },
        { key: 'h2h_general_ou_res', id: 'filter-pv2-h2h-general-ou-res', label: 'H2H Gen Res O/U' },
    ];

    function emptyOuFilters() {
        return {
            ft_ou_res: '',
            prev_home_ou_mov: '',
            prev_home_ou_res: '',
            prev_away_ou_mov: '',
            prev_away_ou_res: '',
            h2h_stadium_ou_mov: '',
            h2h_stadium_ou_res: '',
            h2h_general_ou_mov: '',
            h2h_general_ou_res: '',
        };
    }

    function esc(val) {
        return String(val ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function fmtAh(val) {
        if (val === null || val === undefined || val === '') return '-';
        const n = Number(val);
        if (!Number.isFinite(n)) return esc(val);
        if (Math.abs(n) < 0.01) return '0';
        return String(Number(n.toFixed(2)));
    }

    function renderTagPill(tag, compact) {
        if (!tag) return '';
        const cls = `pv2-tag pv2-tag-${esc(tag.tone || 'neutral')}${compact ? ' pv2-tag--sm' : ''}`;
        return `<span class="${cls}" title="${esc(tag.reason || '')}">${esc(tag.label)}</span>`;
    }

    function getInlineMatchPayload(matchId) {
        const id = String(matchId || '').trim();
        if (!id) return null;
        try {
            if (typeof precacheoData !== 'undefined' && precacheoData && precacheoData[id]) {
                return precacheoData[id];
            }
        } catch (_) {}
        if (window.precacheoData && window.precacheoData[id]) {
            return window.precacheoData[id];
        }
        if (window._preMatchContextMap && window._preMatchContextMap[id]) {
            return window._preMatchContextMap[id];
        }
        return null;
    }

    function formatTeamSummaryLine(statsObj, labelPrefix) {
        const a = statsObj?.all || {};
        const v = statsObj?.venue || {};
        const parts = [];
        if (a.n) {
            parts.push(`Gen ${a.n}p: <strong>${a.w}V-${a.d}E-${a.l}D</strong> (${a.win_pct ?? 0}% V)`);
        }
        if (v.n) {
            parts.push(`${labelPrefix} ${v.n}p: <strong>${v.w}V-${v.d}E-${v.l}D</strong>`);
        }
        if (a.ah_n) {
            parts.push(`Cubre AH: <strong>${a.ah_cover_pct ?? 0}%</strong> (${a.ah_n}p)`);
        }
        return parts.join(' · ') || 'Sin historial reciente detallado';
    }

    // =========================================================================
    // 1. MODAL PATRÓN 2.0
    // =========================================================================
    function ensureModalDom() {
        let overlay = document.getElementById('pv2-modal-overlay');
        if (overlay) return overlay;

        overlay = document.createElement('div');
        overlay.id = 'pv2-modal-overlay';
        overlay.className = 'pv2-modal-overlay d-none';
        overlay.innerHTML = `
            <div class="pv2-modal-dialog" role="dialog" aria-modal="true" aria-labelledby="pv2-modal-title">
                <div class="pv2-modal-header">
                    <div>
                        <h5 class="mb-0 fw-bold" id="pv2-modal-title" style="font-size: 1rem;">
                            <i class="fa-solid fa-wand-magic-sparkles me-2 text-warning"></i>Búsqueda Patrón 2.0 · Máxima Similitud
                        </h5>
                        <div id="pv2-modal-subtitle" class="small opacity-75 mt-1" style="font-size: 0.74rem;"></div>
                    </div>
                    <div class="d-flex align-items-center gap-2">
                        <span id="pv2-modal-timing" class="badge bg-light text-dark" style="font-size: 0.7rem;"></span>
                        <button type="button" class="btn btn-light btn-sm fw-bold" id="pv2-modal-close-btn">
                            <i class="fa-solid fa-xmark me-1"></i>Cerrar
                        </button>
                    </div>
                </div>
                <div class="pv2-modal-body" id="pv2-modal-body"></div>
            </div>
        `;
        document.body.appendChild(overlay);

        overlay.addEventListener('click', function (ev) {
            if (ev.target === overlay) closeModal();
        });
        overlay.querySelector('#pv2-modal-close-btn').addEventListener('click', closeModal);
        document.addEventListener('keydown', function (ev) {
            if (ev.key === 'Escape' && !overlay.classList.contains('d-none')) {
                closeModal();
            }
        });
        return overlay;
    }

    function closeModal() {
        const overlay = document.getElementById('pv2-modal-overlay');
        if (overlay) overlay.classList.add('d-none');
    }

    function aggregateMatchesClient(list) {
        const n = list.length;
        if (!n) {
            return {
                n: 0, fav_cover_pct: 0, dog_cover_pct: 0, push_pct: 0,
                home_pct: 0, draw_pct: 0, away_pct: 0,
                over_pct: 0, under_pct: 0, avg_goals: 0
            };
        }
        let favEq = 0, dogEq = 0, pushEq = 0;
        let hCnt = 0, dCnt = 0, aCnt = 0;
        let overCnt = 0, underCnt = 0, ouN = 0, gSum = 0;
        list.forEach(item => {
            const r = (item && item._pv2) ? item._pv2 : (item || {});
            if (r.fav_cover === 'COVER') favEq += 1;
            else if (r.fav_cover === 'HALF_WIN') { favEq += 0.5; pushEq += 0.5; }
            else if (r.fav_cover === 'PUSH') pushEq += 1;
            else if (r.fav_cover === 'HALF_LOSS') { dogEq += 0.5; pushEq += 0.5; }
            else if (r.fav_cover === 'NO_COVER') dogEq += 1;

            if (r.wdl_1x2 === '1') hCnt += 1;
            else if (r.wdl_1x2 === 'X') dCnt += 1;
            else if (r.wdl_1x2 === '2') aCnt += 1;

            if (Number.isFinite(r.total_goals)) gSum += r.total_goals;
            if (r.ou_res === 'OVER') { overCnt += 1; ouN += 1; }
            else if (r.ou_res === 'UNDER') { underCnt += 1; ouN += 1; }
        });
        return {
            n,
            fav_cover_pct: Number(((favEq / n) * 100).toFixed(1)),
            dog_cover_pct: Number(((dogEq / n) * 100).toFixed(1)),
            push_pct: Number(((pushEq / n) * 100).toFixed(1)),
            home_pct: Number(((hCnt / n) * 100).toFixed(1)),
            draw_pct: Number(((dCnt / n) * 100).toFixed(1)),
            away_pct: Number(((aCnt / n) * 100).toFixed(1)),
            over_pct: ouN ? Number(((overCnt / ouN) * 100).toFixed(1)) : 0,
            under_pct: ouN ? Number(((underCnt / ouN) * 100).toFixed(1)) : 0,
            avg_goals: Number((gSum / n).toFixed(2)),
        };
    }

    function matchesPatternFilter(rowOrCard, filterKey, targetCrossCodes) {
        const f = filterKey || 'ALL';
        if (f === 'ALL') return true;
        const r = (rowOrCard && rowOrCard._pv2) ? rowOrCard._pv2 : rowOrCard;
        if (!r) return false;

        if (f === 'TOP5') return (r.rank || 999) <= 5;
        if (f === 'TOP10') return (r.rank || 999) <= 10;
        if (f === 'TOP20') return (r.rank || 999) <= 20;
        if (f === 'TIER1') return Number(r.pattern_tier) === 1;
        if (f === 'OU_HIT') return Boolean(r.ou_match);
        if (f === 'OU_MOV_MATCH') return Boolean(r.ou_mov_match);
        if (f === 'H2H_HIT') return Boolean(r.h2h_match);
        if (f === 'IND_HIT') return Boolean(r.ind_match);
        if (f === 'COL3_HIT') return Boolean(r.col3_match);
        if (f === 'FICHA_HIT') return Boolean(r.ficha_hydrated && r.ficha_match);
        if (f === 'WITH_FICHA') return Boolean(r.ficha_hydrated || r.has_full_ficha);
        if (f === 'SIM75') return (r.similarity_pct || 0) >= 75;
        if (f === 'SIM65') return (r.similarity_pct || 0) >= 65;
        if (f === 'EXACT_AH') return !!r.exact_ah;
        if (f === 'SAME_CROSS') {
            if (!targetCrossCodes || !targetCrossCodes.length) return true;
            const rowCodes = new Set((r.matchup_tags || []).map(t => t.code));
            return targetCrossCodes.every(c => rowCodes.has(c));
        }
        if (f.startsWith('QUAD:')) {
            return r.quadrant === f.slice(5);
        }
        if (f.startsWith('TAG:')) {
            const code = f.slice(4);
            if (code === '__SAME_CROSS__') {
                if (!targetCrossCodes || !targetCrossCodes.length) return true;
                const rowCodes = new Set((r.matchup_tags || []).map(t => t.code));
                return targetCrossCodes.every(c => rowCodes.has(c));
            }
            const allCodes = [
                ...(r.matchup_tags || []),
                ...(r.home_tags || []),
                ...(r.away_tags || []),
            ].map(t => t.code);
            return allCodes.includes(code);
        }
        return true;
    }

    function getFilteredMatches() {
        const data = State.modalPayload;
        if (!data || !Array.isArray(data.matches)) return [];
        const f = State.quickFilter || 'ALL';
        return data.matches.filter(row => matchesPatternFilter(row, f));
    }

    function parseScoreGoals(scoreStr) {
        const raw = String(scoreStr || '').trim().replace('-', ':');
        const m = raw.match(/^(\d+)\s*:\s*(\d+)$/);
        if (!m) return null;
        const home = Number(m[1]);
        const away = Number(m[2]);
        if (!Number.isFinite(home) || !Number.isFinite(away)) return null;
        return { home, away, total: home + away };
    }

    function parseNumClean(val) {
        if (val === null || val === undefined || val === '' || val === '-' || val === 'N/A') return null;
        const n = Number(String(val).trim().replace(',', '.'));
        return Number.isFinite(n) ? n : null;
    }

    function isSameH2HMatch(item) {
        const st = item && item.h2h_stadium;
        const gen = item && item.h2h_general;
        return Boolean(st && gen && st.date && st.date === gen.date && st.score && st.score === gen.score);
    }

    function extractNodeOuInfo(item, side) {
        const currOu = parseNumClean(item?.candidate?.ou_line ?? item?.candidate?.ou);
        const node = item && item[side];
        if (!node || typeof node !== 'object') {
            return { ou: null, currOu, delta: null, dir: null, res: null, refSource: null, movement: '' };
        }
        let nodeOu = parseNumClean(node.ou ?? node.ou_line ?? node.over_under_line_raw);
        if (nodeOu === null && typeof node.ou_movement === 'string' && node.ou_movement.includes('→')) {
            nodeOu = parseNumClean(node.ou_movement.split('→')[0]);
        }
        let delta = (typeof node.ou_delta === 'number' && !Number.isNaN(node.ou_delta))
            ? Number(node.ou_delta)
            : null;
        if (delta === null && nodeOu !== null && currOu !== null) {
            delta = Number((currOu - nodeOu).toFixed(2));
        }
        let dir = node.ou_mov_dir || null;
        if (!dir && delta !== null) {
            dir = Math.abs(delta) <= 0.05 ? 'IGUAL' : (delta > 0 ? 'SUBE' : 'BAJA');
        }
        let res = node.ou_res || null;
        if (!res) {
            const sc = parseScoreGoals(node.score);
            const ouRef = nodeOu !== null ? nodeOu : currOu;
            if (sc && ouRef !== null && ouRef > 0) {
                res = sc.total > ouRef + 0.01 ? 'OVER' : (sc.total < ouRef - 0.01 ? 'UNDER' : 'PUSH');
            }
        }
        const refSource = node.ou_ref_source || (nodeOu !== null ? 'own' : (res ? 'curr' : null));
        return {
            ou: nodeOu,
            currOu,
            delta,
            dir,
            res,
            refSource,
            movement: node.ou_movement || '',
        };
    }

    function matchesOuNodeFilter(item, side, movFilter, resFilter, targetOuSummary) {
        const movF = String(movFilter || '').trim();
        const resF = String(resFilter || '').trim();
        if (!movF && !resF) return true;
        if (side === 'h2h_general' && isSameH2HMatch(item)) return false;

        const info = extractNodeOuInfo(item, side);
        if (movF) {
            if (movF === 'MATCH_TARGET') {
                const tNode = targetOuSummary && targetOuSummary[side];
                const tDir = tNode && tNode.ou_mov_dir ? String(tNode.ou_mov_dir) : null;
                if (!tDir || info.dir !== tDir) return false;
            } else if (movF === 'SUBE') {
                if (info.dir !== 'SUBE') return false;
            } else if (movF === 'SUBE_05') {
                if (typeof info.delta !== 'number' || info.delta < 0.49) return false;
            } else if (movF === 'BAJA') {
                if (info.dir !== 'BAJA') return false;
            } else if (movF === 'BAJA_05') {
                if (typeof info.delta !== 'number' || info.delta > -0.49) return false;
            } else if (movF === 'IGUAL') {
                if (info.dir !== 'IGUAL') return false;
            }
        }
        if (resF) {
            if (info.res !== resF) return false;
        }
        return true;
    }

    function matchesActiveOuColumnFilters(item, ouFilters, targetOuSummary) {
        if (!ouFilters || typeof ouFilters !== 'object') return true;
        if (ouFilters.ft_ou_res) {
            let ftRes = item && item._pv2 && item._pv2.ou_res ? String(item._pv2.ou_res) : '';
            if (!ftRes) {
                const sc = parseScoreGoals(item?.candidate?.score);
                const cOu = parseNumClean(item?.candidate?.ou_line ?? item?.candidate?.ou ?? targetOuSummary?.ou);
                if (sc && cOu !== null && cOu > 0) {
                    ftRes = sc.total > cOu + 0.01 ? 'OVER' : (sc.total < cOu - 0.01 ? 'UNDER' : 'PUSH');
                }
            }
            if (ftRes !== ouFilters.ft_ou_res) return false;
        }

        const colChecks = [
            ['prev_home', 'prev_home_ou_mov', 'prev_home_ou_res'],
            ['prev_away', 'prev_away_ou_mov', 'prev_away_ou_res'],
            ['h2h_stadium', 'h2h_stadium_ou_mov', 'h2h_stadium_ou_res'],
            ['h2h_general', 'h2h_general_ou_mov', 'h2h_general_ou_res'],
        ];
        for (const [side, movK, resK] of colChecks) {
            if (!matchesOuNodeFilter(item, side, ouFilters[movK], ouFilters[resK], targetOuSummary)) {
                return false;
            }
        }
        return true;
    }

    function hasActiveOuColumnFilters(ouFilters) {
        const src = ouFilters || State.explorerV2.ouFilters || {};
        return OU_FILTER_DOM_MAP.some(f => Boolean(src[f.key]));
    }

    function renderTargetCard(target, opts = {}) {
        const ouMovText = opts.ouMovText || '';
        const cr = target.common_rivals || {};

        return `
            <div class="pv2-target-card">
                <div class="pv2-target-grid">
                    <div>
                        <div class="fw-bold text-primary" style="font-size:0.84rem;">
                            🏠 ${esc(target.home)} ${target.home_rank ? `<span class="badge bg-light text-dark border">#${esc(target.home_rank)}</span>` : ''}
                        </div>
                        <div class="pv2-ficha-metrics mt-1">${formatTeamSummaryLine(target.home_stats, 'Casa')}</div>
                    </div>
                    <div class="text-center border-start border-end px-2">
                        <div class="d-flex justify-content-center align-items-center gap-2 flex-wrap mb-1">
                            <span class="badge bg-dark">AH ${fmtAh(target.ah)}</span>
                            ${target.ou ? `<span class="badge bg-secondary">O/U ${fmtAh(target.ou)}</span>` : ''}
                            <span class="badge bg-light text-dark border">Fav: ${target.fav_side === 'HOME' ? 'Local' : (target.fav_side === 'AWAY' ? 'Visitante' : 'Parejo')}</span>
                        </div>
                        ${ouMovText ? `<div class="small fw-bold text-primary mb-1" style="font-size:0.68rem;">${esc(ouMovText)}</div>` : ''}
                        ${cr.count >= 2 ? `<div class="small text-muted mt-1" style="font-size:0.66rem;">Rivales comunes (${cr.count}): Local ${cr.home_pts} pts (${cr.home_gd >= 0 ? '+' : ''}${cr.home_gd}) vs Vis ${cr.away_pts} pts (${cr.away_gd >= 0 ? '+' : ''}${cr.away_gd})</div>` : ''}
                    </div>
                    <div>
                        <div class="fw-bold text-danger" style="font-size:0.84rem;">
                            ✈️ ${esc(target.away)} ${target.away_rank ? `<span class="badge bg-light text-dark border">#${esc(target.away_rank)}</span>` : ''}
                        </div>
                        <div class="pv2-ficha-metrics mt-1">${formatTeamSummaryLine(target.away_stats, 'Fuera')}</div>
                    </div>
                </div>
            </div>
        `;
    }

    function renderVerdictBanner(data, filteredList) {
        const usingFilter = State.quickFilter !== 'ALL';
        const v = usingFilter ? null : data.verdict;
        const stats = usingFilter ? aggregateMatchesClient(filteredList) : data.verdict;
        const favP = stats.fav_cover_pct || 0;
        const pushP = stats.push_pct || 0;
        const dogP = stats.dog_cover_pct || 0;

        const title = usingFilter
            ? `Subgrupo Filtrado (N=${stats.n})`
            : (v?.signal || 'DIAGNÓSTICO');
        const subtitle = usingFilter
            ? `Fav cubre ${favP}% · Rival/Dog cubre ${dogP}% · Push ${pushP}% · Over ${stats.over_pct}%`
            : (v?.summary || '');
        const tone = usingFilter
            ? (favP >= 62 ? 'good' : (dogP >= 62 ? 'warn' : 'neutral'))
            : (v?.tone || 'neutral');

        return `
            <div class="pv2-verdict-box tone-${esc(tone)}">
                <div>
                    <div class="d-flex align-items-center gap-2 flex-wrap">
                        <span class="fw-bold" style="font-size:0.95rem;">${esc(title)}</span>
                        ${!usingFilter && v?.confidence ? `<span class="badge bg-dark" style="font-size:0.66rem;">Confianza ${esc(v.confidence)}</span>` : ''}
                        <span class="badge bg-light text-dark border" style="font-size:0.66rem;">Muestra: ${stats.n} similares</span>
                    </div>
                    <div class="text-muted mt-1" style="font-size:0.76rem;">${esc(subtitle)}</div>
                    <div class="pv2-kpi-row">
                        <span class="pv2-kpi-pill text-success">Fav Cubre: ${favP}%</span>
                        <span class="pv2-kpi-pill text-danger">Rival Cubre: ${dogP}%</span>
                        <span class="pv2-kpi-pill text-warning">Push: ${pushP}%</span>
                        <span class="pv2-kpi-pill">1X2: ${stats.home_pct}% / ${stats.draw_pct}% / ${stats.away_pct}%</span>
                        <span class="pv2-kpi-pill">O/U: ${stats.over_pct}% Over · ${stats.avg_goals} g/p</span>
                    </div>
                </div>
                <div>
                    <div class="d-flex justify-content-between small fw-bold mb-1" style="font-size:0.69rem;">
                        <span class="text-success">Fav ${favP}%</span>
                        <span class="text-warning">Push ${pushP}%</span>
                        <span class="text-danger">Rival ${dogP}%</span>
                    </div>
                    <div class="pv2-prob-bar" title="Distribución AH Fav / Push / Rival">
                        <div class="pv2-prob-seg-fav" style="width:${favP}%"></div>
                        <div class="pv2-prob-seg-push" style="width:${pushP}%"></div>
                        <div class="pv2-prob-seg-dog" style="width:${dogP}%"></div>
                    </div>
                    <div class="text-end text-muted mt-1" style="font-size:0.66rem;">
                        Escaneados ${esc(data.total_candidates_scanned || 0)} partidos del bucket → Top ${esc(data.shortlist_size || 0)} evaluados con perfil completo
                    </div>
                </div>
            </div>
        `;
    }

    function renderTabSummary(data) {
        const ladderRows = (data.similarity_ladder || []).map(row => `
            <tr>
                <td class="fw-bold">${esc(row.label)}</td>
                <td class="text-center fw-bold">${row.n}</td>
                <td class="text-center fw-bold ${row.fav_cover_pct >= 60 ? 'text-success' : ''}">${row.fav_cover_pct}%</td>
                <td class="text-center fw-bold ${row.dog_cover_pct >= 60 ? 'text-danger' : ''}">${row.dog_cover_pct}%</td>
                <td class="text-center">${row.push_pct}%</td>
                <td class="text-center">${row.over_pct}%</td>
                <td class="text-center">${row.avg_goals}</td>
            </tr>
        `).join('');

        return `
            <div class="pv2-panel">
                <div class="pv2-panel-title">
                    <span><i class="fa-solid fa-stairs me-1 text-primary"></i>Escalera de Convergencia (por Similitud)</span>
                    <span class="small text-muted fw-normal">De más estricto a muestra base</span>
                </div>
                <table class="pv2-table">
                    <thead>
                        <tr><th>Escalón</th><th class="text-center">N</th><th class="text-center">Fav%</th><th class="text-center">Rival%</th><th class="text-center">Push</th><th class="text-center">Over%</th><th class="text-center">Goles</th></tr>
                    </thead>
                    <tbody>${ladderRows}</tbody>
                </table>
            </div>
        `;
    }

    function renderTabMatrix(data) {
        const q = data.quadrant_matrix || {};
        const cellHtml = (title, stats) => {
            const s = stats || { n: 0, fav_cover_pct: 0, dog_cover_pct: 0, over_pct: 0 };
            let heat = 'heat-even';
            if (s.n >= 3) {
                if (s.fav_cover_pct >= 60) heat = 'heat-fav';
                else if (s.dog_cover_pct >= 60) heat = 'heat-dog';
            }
            return `
                <div class="pv2-matrix-cell ${heat}">
                    <div class="d-flex justify-content-between align-items-center mb-1">
                        <strong style="font-size:0.75rem;">${esc(title)}</strong>
                        <span class="badge bg-dark">N=${s.n}</span>
                    </div>
                    <div style="font-size:0.76rem;">
                        Fav: <strong class="text-success">${s.fav_cover_pct}%</strong> ·
                        Rival: <strong class="text-danger">${s.dog_cover_pct}%</strong> ·
                        Push: <strong>${s.push_pct ?? 0}%</strong>
                    </div>
                    <div class="small text-muted mt-1" style="font-size:0.68rem;">
                        1X2: ${s.home_pct ?? 0}% / ${s.draw_pct ?? 0}% / ${s.away_pct ?? 0}% · Over ${s.over_pct}% (${s.avg_goals ?? 0} g/p)
                    </div>
                </div>
            `;
        };

        const cr = data.target?.common_rivals || {};
        const crSamples = (cr.samples || []).map(s => `
            <tr>
                <td class="fw-bold">${esc(s.rival)}</td>
                <td class="text-center">${esc(s.home_score)} <span class="text-muted">(AH ${esc(s.home_ah)})</span></td>
                <td class="text-center">${esc(s.away_score)} <span class="text-muted">(AH ${esc(s.away_ah)})</span></td>
            </tr>
        `).join('');

        return `
            <div class="pv2-panel mb-3">
                <div class="pv2-panel-title">
                    <span><i class="fa-solid fa-table-cells-large me-1 text-primary"></i>Matriz 2×2: Similitud de Mercado × Perfil de Equipos</span>
                    <span class="small text-muted fw-normal">Verde ≥60% Fav · Rojo ≥60% Rival</span>
                </div>
                <div class="pv2-matrix-grid">
                    <div></div>
                    <div class="pv2-matrix-hdr">Mercado Alto (≥65%)</div>
                    <div class="pv2-matrix-hdr">Mercado Parcial (&lt;65%)</div>
                    <div class="pv2-matrix-hdr">Perfil Equipos Alto (≥65%)</div>
                    ${cellHtml('Zona Oro: Mismo Mercado + Mismo Perfil', q.high_prof_high_mkt)}
                    ${cellHtml('Mismo Perfil (Mercado Parcial)', q.high_prof_part_mkt)}
                    <div class="pv2-matrix-hdr">Otro Perfil (&lt;65%)</div>
                    ${cellHtml('Mismo Mercado (Otro Perfil)', q.other_prof_high_mkt)}
                    ${cellHtml('Similitud Baja', q.other_prof_part_mkt)}
                </div>
            </div>
            ${cr.count ? `
            <div class="pv2-panel">
                <div class="pv2-panel-title">
                    <span><i class="fa-solid fa-scale-balanced me-1 text-primary"></i>Rivales Comunes del Partido Actual (${cr.count})</span>
                    <span class="small text-muted">Local: ${cr.home_pts} pts (${cr.home_gd >= 0 ? '+' : ''}${cr.home_gd}) · Visitante: ${cr.away_pts} pts (${cr.away_gd >= 0 ? '+' : ''}${cr.away_gd})</span>
                </div>
                <table class="pv2-table">
                    <thead><tr><th>Rival Común</th><th class="text-center">Resultado ${esc(data.target?.home)}</th><th class="text-center">Resultado ${esc(data.target?.away)}</th></tr></thead>
                    <tbody>${crSamples}</tbody>
                </table>
            </div>` : ''}
        `;
    }

    function renderTabMatches(filteredList) {
        if (!filteredList.length) {
            return '<div class="pv2-panel text-center text-muted py-4">No hay partidos similares con el filtro seleccionado.</div>';
        }
        const rows = filteredList.map(r => {
            const covBadge = r.fav_cover === 'COVER' || r.fav_cover === 'HALF_WIN'
                ? `<span class="badge bg-success">${r.fav_cover === 'HALF_WIN' ? '1/2 Fav' : 'Fav Cubre'}</span>`
                : (r.fav_cover === 'PUSH'
                    ? '<span class="badge bg-warning text-dark">Push</span>'
                    : '<span class="badge bg-danger">Rival Cubre</span>');
            const ouBadge = r.ou_res === 'OVER'
                ? '<span class="badge bg-info text-dark">Over</span>'
                : (r.ou_res === 'UNDER' ? '<span class="badge bg-secondary">Under</span>' : '');
            const reasonChips = (r.matched_reasons || []).slice(0, 5)
                .map(txt => `<span class="pv2-tag pv2-tag-neutral pv2-tag--sm">${esc(txt)}</span>`).join(' ');
            const divChips = (r.divergences || [])
                .map(txt => `<span class="pv2-tag pv2-tag-warn pv2-tag--sm">⚠ ${esc(txt)}</span>`).join(' ');

            return `
                <tr>
                    <td style="white-space:nowrap;">
                        <div class="fw-bold">${esc(r.date)}</div>
                        <div class="text-muted text-truncate" style="max-width:110px; font-size:0.64rem;" title="${esc(r.league)}">${esc(r.league)}</div>
                    </td>
                    <td>
                        <div class="fw-bold text-primary">${esc(r.home)}</div>
                        <div class="fw-bold text-danger">${esc(r.away)}</div>
                    </td>
                    <td class="text-center fw-bold">
                        <div>${fmtAh(r.ah)}</div>
                        ${r.ou ? `<div class="text-muted" style="font-size:0.64rem;">O/U ${fmtAh(r.ou)}</div>` : ''}
                    </td>
                    <td class="text-center fw-bold" style="font-size:0.84rem;">
                        <div>${esc(r.score)}</div>
                        <div class="text-muted" style="font-size:0.65rem;">${esc(r.wdl_1x2)}</div>
                    </td>
                    <td>
                        <div class="fw-bold">${r.similarity_pct}%</div>
                        <div class="pv2-sim-bar-wrap"><div class="pv2-sim-bar-fill" style="width:${r.similarity_pct}%"></div></div>
                        <div class="text-muted" style="font-size:0.62rem;">Mkt ${r.market_pct}% · Perf ${r.profile_pct}%</div>
                    </td>
                    <td>
                        <div class="d-flex flex-wrap gap-1">${reasonChips} ${divChips}</div>
                    </td>
                    <td class="text-center">
                        <div class="d-flex flex-column align-items-center gap-1">${covBadge} ${ouBadge}</div>
                    </td>
                    <td class="text-center">
                        <a href="/estudio/${encodeURIComponent(r.match_id)}" target="_blank" rel="noopener" class="btn btn-sm btn-outline-primary py-0 px-1" title="Abrir Estudio completo">
                            <i class="fa-solid fa-up-right-from-square"></i>
                        </a>
                    </td>
                </tr>
            `;
        }).join('');

        return `
            <div class="pv2-panel" style="overflow-x:auto;">
                <table class="pv2-table">
                    <thead>
                        <tr>
                            <th>Fecha / Liga</th>
                            <th>Partido</th>
                            <th class="text-center">AH / O/U</th>
                            <th class="text-center">FT</th>
                            <th>Similitud</th>
                            <th>Coincidencias y Divergencias</th>
                            <th class="text-center">Res. AH</th>
                            <th class="text-center">Estudio</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;
    }

    function renderTabAntiTrap(data) {
        const list = data.anti_trap_checklist || [];
        if (!list.length) {
            return '<div class="pv2-panel text-muted text-center py-4">No se detectaron alertas ni trampas relevantes.</div>';
        }
        const iconMap = {
            pro: 'fa-circle-check text-success',
            alert: 'fa-triangle-exclamation text-warning',
            danger: 'fa-shield-halved text-danger',
            info: 'fa-circle-info text-primary',
        };
        return `
            <div class="pv2-panel">
                <div class="pv2-panel-title">
                    <span><i class="fa-solid fa-radar me-1 text-primary"></i>Checklist Rápido Anti-Trampa</span>
                </div>
                ${list.map(item => `
                    <div class="pv2-check-item kind-${esc(item.kind || 'info')}">
                        <i class="fa-solid ${iconMap[item.kind] || iconMap.info} mt-1"></i>
                        <div>
                            <div class="fw-bold" style="font-size:0.78rem;">${esc(item.title)}</div>
                            <div class="text-muted" style="font-size:0.73rem;">${esc(item.detail)}</div>
                        </div>
                    </div>
                `).join('')}
            </div>
        `;
    }

    function renderModalContent() {
        const data = State.modalPayload;
        const body = document.getElementById('pv2-modal-body');
        if (!data || !body) return;

        const filteredList = getFilteredMatches();
        const f = State.quickFilter;

        const filterButtons = [
            { id: 'ALL', label: `Todos (${(data.matches || []).length})` },
            { id: 'SIM75', label: 'Similitud ≥ 75%' },
            { id: 'SIM65', label: 'Similitud ≥ 65%' },
            { id: 'EXACT_AH', label: 'AH Exacto' },
        ];

        const tabs = [
            { id: 'summary', label: '1. Resumen & Escalera' },
            { id: 'matrix', label: '2. Matriz Mercado × Equipo' },
            { id: 'matches', label: `3. Top Similares (${filteredList.length})` },
            { id: 'antitrap', label: '4. Anti-Trampa' },
        ];

        let tabHtml = '';
        if (State.activeTab === 'summary') tabHtml = renderTabSummary(data);
        else if (State.activeTab === 'matrix') tabHtml = renderTabMatrix(data);
        else if (State.activeTab === 'matches') tabHtml = renderTabMatches(filteredList);
        else tabHtml = renderTabAntiTrap(data);

        body.innerHTML = `
            ${renderTargetCard(data.target)}
            ${renderVerdictBanner(data, filteredList)}
            <div class="pv2-controls-row">
                <div class="pv2-tabs">
                    ${tabs.map(t => `<button type="button" class="pv2-tab-btn ${State.activeTab === t.id ? 'is-active' : ''}" data-pv2-tab="${t.id}">${esc(t.label)}</button>`).join('')}
                </div>
                <div class="d-flex flex-wrap gap-1 align-items-center">
                    <span class="small fw-bold text-muted me-1" style="font-size:0.7rem;">Filtro rápido:</span>
                    ${filterButtons.map(b => `<button type="button" class="pv2-filter-chip ${f === b.id ? 'is-active' : ''}" data-pv2-filter="${esc(b.id)}">${esc(b.label)}</button>`).join('')}
                </div>
            </div>
            ${tabHtml}
        `;

        body.querySelectorAll('[data-pv2-tab]').forEach(btn => {
            btn.addEventListener('click', () => {
                State.activeTab = btn.getAttribute('data-pv2-tab');
                renderModalContent();
            });
        });
        body.querySelectorAll('[data-pv2-filter]').forEach(el => {
            el.addEventListener('click', () => {
                const next = el.getAttribute('data-pv2-filter');
                if (next.startsWith('TAG:') && State.quickFilter === next) {
                    State.quickFilter = 'ALL';
                } else {
                    State.quickFilter = next;
                }
                renderModalContent();
            });
        });
    }

    async function openModal(matchId, btnEl) {
        const overlay = ensureModalDom();
        const body = document.getElementById('pv2-modal-body');
        const subtitle = document.getElementById('pv2-modal-subtitle');
        const timing = document.getElementById('pv2-modal-timing');

        State.activeTab = 'summary';
        State.quickFilter = 'ALL';
        overlay.classList.remove('d-none');

        const inline = getInlineMatchPayload(matchId);
        const hName = inline?.home_name || inline?.home_team || '';
        const aName = inline?.away_name || inline?.away_team || '';
        subtitle.textContent = hName && aName ? `${hName} vs ${aName} (ID ${matchId})` : `Partido ID ${matchId}`;
        timing.textContent = 'Buscando...';
        body.innerHTML = `
            <div class="text-center py-5">
                <div class="spinner-border text-primary" role="status"></div>
                <div class="fw-bold mt-2">Calculando Patrón 2.0 (Mercado + Histórico + O/U)...</div>
                <div class="small text-muted">Evaluando similitudes estructurales y rachas recientes en paralelo</div>
            </div>
        `;

        const origHtml = btnEl ? btnEl.innerHTML : null;
        if (btnEl) btnEl.disabled = true;

        try {
            const resp = await fetch('/api/pattern_search_v2', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    match_id: String(matchId),
                    match_data: inline,
                    top_n: 60,
                }),
            });
            const data = await resp.json();
            if (!resp.ok || data.status !== 'success') {
                throw new Error(data.error || `Error HTTP ${resp.status}`);
            }
            State.modalPayload = data;
            subtitle.textContent = `${data.target?.home || ''} vs ${data.target?.away || ''} · ${data.target?.league || ''}`;
            timing.textContent = `${data.elapsed_ms} ms`;
            renderModalContent();
        } catch (err) {
            timing.textContent = 'Error';
            body.innerHTML = `<div class="alert alert-danger mb-0">No se pudo ejecutar Patrón 2.0: ${esc(err.message)}</div>`;
        } finally {
            if (btnEl) {
                btnEl.disabled = false;
                if (origHtml !== null) btnEl.innerHTML = origHtml;
            }
        }
    }

    // =========================================================================
    // 2. ETIQUETADO EN EL EXPLORADOR (BOTÓN SUPERIOR + FILA INDIVIDUAL)
    // =========================================================================
    function injectRowTags(tr, prof) {
        if (!tr || !prof) return;
        const cells = tr.querySelectorAll('td');
        if (cells.length < 5) return;

        const homeCell = cells[2];
        const awayCell = cells[3];
        const ahCell = cells[4];

        [homeCell, awayCell, ahCell].forEach(c => {
            c.querySelectorAll('.pv2-row-tags').forEach(node => node.remove());
        });

        const hTags = prof.home_tags || [];
        if (hTags.length) {
            const wrap = document.createElement('div');
            wrap.className = 'pv2-row-tags';
            wrap.innerHTML = hTags.map(t => renderTagPill(t, true)).join('');
            homeCell.appendChild(wrap);
        }

        const aTags = prof.away_tags || [];
        if (aTags.length) {
            const wrap = document.createElement('div');
            wrap.className = 'pv2-row-tags';
            wrap.innerHTML = aTags.map(t => renderTagPill(t, true)).join('');
            awayCell.appendChild(wrap);
        }

        const mTags = prof.matchup_tags || [];
        if (mTags.length) {
            const wrap = document.createElement('div');
            wrap.className = 'pv2-row-tags justify-content-center';
            wrap.innerHTML = mTags.slice(0, 2).map(t => renderTagPill(t, true)).join('');
            ahCell.appendChild(wrap);
        }
    }

    function computeFilteredExplorerStats(tagCode) {
        const rows = Array.from(document.querySelectorAll('#table-body tr[data-match_id]'));
        let total = 0, favCov = 0, dogCov = 0, push = 0;
        rows.forEach(tr => {
            if (tr.style.display === 'none') return;
            total += 1;
            const mid = tr.dataset.match_id;
            const prof = State.explorerTagMap[mid];
            if (!prof) return;
            const ftRes = tr.dataset.ft_res || '';
            if (ftRes === 'COVER' || ftRes === 'HALF_WIN') favCov += 1;
            else if (ftRes === 'NO_COVER' || ftRes === 'HALF_LOSS') dogCov += 1;
            else if (ftRes === 'PUSH') push += 1;
        });
        const settled = favCov + dogCov + push;
        return {
            total,
            favPct: settled ? ((favCov / settled) * 100).toFixed(1) : '0.0',
            dogPct: settled ? ((dogCov / settled) * 100).toFixed(1) : '0.0',
            pushPct: settled ? ((push / settled) * 100).toFixed(1) : '0.0',
            tagCode,
        };
    }

    function renderExplorerTagsBar() {
        const bar = document.getElementById('explorer-team-tags-bar');
        if (!bar) return;
        bar.classList.remove('d-none');

        const summary = State.explorerTagSummary || [];
        const active = State.explorerActiveTag || 'ALL';
        const stats = computeFilteredExplorerStats(active);

        const chipsHtml = [
            `<button type="button" class="pv2-filter-chip ${active === 'ALL' ? 'is-active' : ''}" onclick="window.PatternSearchV2.filterExplorerByTag('ALL')">Todas las filas</button>`,
            ...summary.map(s => {
                const pctText = s.fav_cover_pct !== null ? ` · Fav ${s.fav_cover_pct}%` : '';
                return `<button type="button" class="pv2-filter-chip ${active === s.code ? 'is-active' : ''}" onclick="window.PatternSearchV2.filterExplorerByTag('${esc(s.code)}')">${esc(s.label)} (${s.count})${pctText}</button>`;
            }),
        ].join('');

        bar.innerHTML = `
            <div class="pv2-explorer-bar">
                <div class="pv2-explorer-bar-head">
                    <div class="fw-bold" style="font-size:0.78rem; color:#312e81;">
                        <i class="fa-solid fa-tags me-1"></i>Etiquetado de Equipo (Explorador)
                        <span class="badge bg-dark ms-2">Visibles: ${stats.total}</span>
                        <span class="badge bg-success ms-1">Fav Cubre: ${stats.favPct}%</span>
                        <span class="badge bg-danger ms-1">Rival Cubre: ${stats.dogPct}%</span>
                    </div>
                    <button type="button" class="btn btn-sm btn-outline-secondary py-0 px-2" style="font-size:0.7rem;" onclick="window.PatternSearchV2.filterExplorerByTag('ALL')">Limpiar filtro etiqueta</button>
                </div>
                <div class="pv2-explorer-chips">${chipsHtml}</div>
            </div>
        `;
    }

    function filterExplorerByTag(tagCode) {
        State.explorerActiveTag = tagCode || 'ALL';
        const rows = Array.from(document.querySelectorAll('#table-body tr[data-match_id]'));
        rows.forEach(tr => {
            const mid = tr.dataset.match_id;
            const prof = State.explorerTagMap[mid];
            let visible = true;
            if (State.explorerActiveTag !== 'ALL') {
                if (!prof) {
                    visible = false;
                } else {
                    const codes = [
                        ...(prof.matchup_tags || []),
                        ...(prof.home_tags || []),
                        ...(prof.away_tags || []),
                    ].map(t => t.code);
                    visible = codes.includes(State.explorerActiveTag);
                }
            }
            tr.style.display = visible ? '' : 'none';
            const nextTr = tr.nextElementSibling;
            if (nextTr && !nextTr.hasAttribute('data-match_id') && nextTr.querySelector('.collapse')) {
                nextTr.style.display = visible ? '' : 'none';
            }
        });
        renderExplorerTagsBar();
    }

    async function tagExplorerRows(btnEl) {
        const rows = Array.from(document.querySelectorAll('#table-body tr[data-match_id]'));
        const ids = rows.map(tr => tr.dataset.match_id).filter(Boolean);
        if (!ids.length) {
            alert('No hay partidos cargados en la tabla actual del Explorador.');
            return;
        }
        const origHtml = btnEl ? btnEl.innerHTML : null;
        if (btnEl) {
            btnEl.disabled = true;
            btnEl.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Etiquetando...';
        }
        try {
            const resp = await fetch('/api/team_profile_tags', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ match_ids: ids }),
            });
            const data = await resp.json();
            if (!resp.ok || data.status !== 'success') {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }
            State.explorerTagMap = data.items || {};
            State.explorerTagSummary = data.tag_summary || [];
            State.explorerActiveTag = 'ALL';
            rows.forEach(tr => {
                const prof = State.explorerTagMap[tr.dataset.match_id];
                if (prof) injectRowTags(tr, prof);
            });
            renderExplorerTagsBar();
        } catch (err) {
            alert('Error etiquetando equipos en Explorador: ' + err.message);
        } finally {
            if (btnEl) {
                btnEl.disabled = false;
                if (origHtml !== null) btnEl.innerHTML = origHtml;
            }
        }
    }

    async function tagSingleExplorerRow(matchId, btnEl) {
        const tr = btnEl ? btnEl.closest('tr[data-match_id]') : document.querySelector(`#table-body tr[data-match_id="${CSS.escape(String(matchId))}"]`);
        const origHtml = btnEl ? btnEl.innerHTML : null;
        if (btnEl) btnEl.disabled = true;
        try {
            const resp = await fetch('/api/team_profile_tags', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ match_id: String(matchId) }),
            });
            const data = await resp.json();
            if (!resp.ok || data.status !== 'success') {
                throw new Error(data.error || `HTTP ${resp.status}`);
            }
            const prof = (data.items || {})[String(matchId)];
            if (prof) {
                State.explorerTagMap[String(matchId)] = prof;
                if (tr) injectRowTags(tr, prof);
            }
        } catch (err) {
            alert('No se pudo etiquetar esta fila: ' + err.message);
        } finally {
            if (btnEl) {
                btnEl.disabled = false;
                if (origHtml !== null) btnEl.innerHTML = origHtml;
            }
        }
    }

    // =========================================================================
    // 3. BOTÓN INDIVIDUAL EN CADA FICHA (.pre-context-moment)
    // =========================================================================
    async function toggleFichaTags(matchId, btnEl, ev) {
        if (ev) ev.stopPropagation();
        const momentEl = btnEl ? btnEl.closest('.pre-context-moment') : null;
        const slot = momentEl ? momentEl.querySelector('.pv2-ficha-tags-slot') : null;
        if (!slot) return;

        if (slot.dataset.loaded === '1') {
            const isHidden = slot.style.display === 'none';
            slot.style.display = isHidden ? 'block' : 'none';
            return;
        }

        slot.style.display = 'block';
        slot.innerHTML = '<div class="small text-muted"><span class="spinner-border spinner-border-sm me-1"></span>Analizando etiquetas de equipo y rivales comunes...</div>';

        const mid = String(matchId || '').trim();
        const inline = getInlineMatchPayload(mid);
        try {
            let prof = State.fichaTagCache[mid];
            if (!prof) {
                const resp = await fetch('/api/team_profile_tags', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        match_id: mid || undefined,
                        match_data: inline,
                    }),
                });
                const data = await resp.json();
                if (!resp.ok || data.status !== 'success') {
                    throw new Error(data.error || `HTTP ${resp.status}`);
                }
                const firstKey = Object.keys(data.items || {})[0];
                prof = (data.items || {})[mid] || (firstKey ? data.items[firstKey] : null);
                if (prof && mid) State.fichaTagCache[mid] = prof;
            }
            if (!prof) {
                slot.innerHTML = '<div class="small text-muted">Sin datos suficientes para etiquetar este partido.</div>';
                return;
            }
            const hTags = (prof.home_tags || []).map(t => renderTagPill(t, false)).join(' ');
            const aTags = (prof.away_tags || []).map(t => renderTagPill(t, false)).join(' ');
            const mTags = (prof.matchup_tags || []).map(t => renderTagPill(t, false)).join(' ');
            const cr = prof.common_rivals || {};

            slot.dataset.loaded = '1';
            slot.innerHTML = `
                <div class="pv2-ficha-tags-grid">
                    <div class="pv2-ficha-card">
                        <div class="pv2-ficha-card-title">
                            <span class="text-primary">🏠 ${esc(prof.home)}</span>
                            ${prof.home_rank ? `<span class="badge bg-light text-dark border">#${esc(prof.home_rank)}</span>` : ''}
                        </div>
                        <div class="pv2-ficha-metrics">${formatTeamSummaryLine(prof.home_stats, 'Casa')}</div>
                        <div class="pv2-ficha-tag-wrap">${hTags || '<span class="pv2-tag pv2-tag-neutral">Sin etiqueta extrema</span>'}</div>
                    </div>
                    <div class="pv2-ficha-card">
                        <div class="pv2-ficha-card-title">
                            <span>⚖️ Lectura del Hándicap (AH ${fmtAh(prof.ah)})</span>
                            ${mid ? `<button type="button" class="pv2-ficha-btn pv2-ficha-btn--v2" onclick="window.PatternSearchV2.openInNewTab('${esc(mid)}', this)"><i class="fa-solid fa-wand-magic-sparkles"></i>Patrón 2.0</button>` : ''}
                        </div>
                        <div class="pv2-ficha-metrics fw-bold text-dark">${esc(prof.headline_reason || '')}</div>
                        <div class="pv2-ficha-tag-wrap mb-1">${mTags || '<span class="pv2-tag pv2-tag-neutral">Cruce estándar</span>'}</div>
                        ${cr.count >= 2 ? `<div class="small text-muted" style="font-size:0.65rem;">Rivales comunes (${cr.count}): Local ${cr.home_pts} pts (${cr.home_gd >= 0 ? '+' : ''}${cr.home_gd}) vs Vis ${cr.away_pts} pts (${cr.away_gd >= 0 ? '+' : ''}${cr.away_gd})</div>` : ''}
                    </div>
                    <div class="pv2-ficha-card">
                        <div class="pv2-ficha-card-title">
                            <span class="text-danger">✈️ ${esc(prof.away)}</span>
                            ${prof.away_rank ? `<span class="badge bg-light text-dark border">#${esc(prof.away_rank)}</span>` : ''}
                        </div>
                        <div class="pv2-ficha-metrics">${formatTeamSummaryLine(prof.away_stats, 'Fuera')}</div>
                        <div class="pv2-ficha-tag-wrap">${aTags || '<span class="pv2-tag pv2-tag-neutral">Sin etiqueta extrema</span>'}</div>
                    </div>
                </div>
            `;
        } catch (err) {
            slot.innerHTML = `<div class="small text-danger">Error cargando etiquetas: ${esc(err.message)}</div>`;
        }
    }

    // =========================================================================
    // 4. NAVEGACIÓN EN PESTAÑA COMPLETA /explorador?patron_v2=1
    // =========================================================================
    function openInNewTab(matchId) {
        const cleanId = String(matchId || '').trim();
        if (!cleanId) return;
        const url = `/explorador?patron_v2=1&source_match_id=${encodeURIComponent(cleanId)}`;
        window.open(url, '_blank', 'noopener');
    }

    function isExplorerV2Active() {
        return Boolean(State.explorerV2 && State.explorerV2.active);
    }

    function deactivateExplorerV2() {
        State.explorerV2.active = false;
        State.explorerV2.bgRunToken += 1;
        State.explorerV2.bgRunning = false;
        State.explorerV2.bgPaused = false;
        State.explorerV2.hydratedCount = 0;
        State.explorerV2.pendingResortCount = 0;
        State.explorerV2.bgNeedsDownloadQueue = [];
        State.explorerV2.activeFilter = 'ALL';
        State.explorerV2.openDrawer = 'none';
        if (typeof document !== 'undefined' && document.body && document.body.classList) {
            document.body.classList.remove('pv2-explorer-active');
        }
        resetOuFiltersDom();
        const ws = typeof document !== 'undefined' ? document.getElementById('pv2-explorer-workspace') : null;
        if (ws) {
            ws.classList.add('d-none');
            ws.innerHTML = '';
        }
    }

    function readOuFiltersFromDom() {
        const next = emptyOuFilters();
        if (typeof document !== 'undefined') {
            OU_FILTER_DOM_MAP.forEach(({ key, id }) => {
                const el = document.getElementById(id);
                const val = el ? String(el.value || '').trim() : '';
                next[key] = val;
                if (el && el.classList) {
                    el.classList.toggle('is-active-filter', Boolean(val));
                }
            });
        }
        State.explorerV2.ouFilters = next;
        return next;
    }

    function resetOuFiltersDom() {
        State.explorerV2.ouFilters = emptyOuFilters();
        if (typeof document !== 'undefined') {
            OU_FILTER_DOM_MAP.forEach(({ id }) => {
                const el = document.getElementById(id);
                if (el) {
                    el.value = '';
                    if (el.classList) el.classList.remove('is-active-filter');
                }
            });
        }
    }

    function syncOuFilterTargetOptionLabels(targetOuSummary) {
        if (typeof document === 'undefined') return;
        const sideIds = [
            ['prev_home', 'filter-pv2-prev-home-ou-mov'],
            ['prev_away', 'filter-pv2-prev-away-ou-mov'],
            ['h2h_stadium', 'filter-pv2-h2h-stadium-ou-mov'],
            ['h2h_general', 'filter-pv2-h2h-general-ou-mov'],
        ];
        sideIds.forEach(([side, id]) => {
            const sel = document.getElementById(id);
            if (!sel || typeof sel.querySelector !== 'function') return;
            const opt = sel.querySelector('option[value="MATCH_TARGET"]');
            if (!opt) return;
            const tNode = targetOuSummary && targetOuSummary[side];
            const dir = tNode && tNode.ou_mov_dir ? String(tNode.ou_mov_dir) : '';
            if (!dir) {
                opt.textContent = '= Actual (N/D)';
                opt.disabled = true;
            } else {
                const dirLabel = dir === 'SUBE' ? 'Subió' : (dir === 'BAJA' ? 'Bajó' : 'Igual');
                const delta = typeof tNode.ou_delta === 'number' && !Number.isNaN(tNode.ou_delta)
                    ? ` ${tNode.ou_delta > 0 ? '+' : ''}${tNode.ou_delta}`
                    : '';
                opt.textContent = `= Actual (${dirLabel}${delta})`;
                opt.disabled = false;
            }
        });
    }

    function onOuFilterChange() {
        readOuFiltersFromDom();
        if (!isExplorerV2Active()) return;
        if (State.explorerV2.bridge && typeof State.explorerV2.bridge.applyFilters === 'function') {
            State.explorerV2.bridge.applyFilters();
        } else {
            renderExplorerWorkspace();
        }
    }

    function clearOuFilters(singleKey) {
        if (singleKey && Object.prototype.hasOwnProperty.call(State.explorerV2.ouFilters || {}, singleKey)) {
            State.explorerV2.ouFilters[singleKey] = '';
            const meta = OU_FILTER_DOM_MAP.find(f => f.key === singleKey);
            if (meta && typeof document !== 'undefined') {
                const el = document.getElementById(meta.id);
                if (el) {
                    el.value = '';
                    if (el.classList) el.classList.remove('is-active-filter');
                }
            }
        } else {
            resetOuFiltersDom();
        }
        if (!isExplorerV2Active()) return;
        if (State.explorerV2.bridge && typeof State.explorerV2.bridge.applyFilters === 'function') {
            State.explorerV2.bridge.applyFilters();
        } else {
            renderExplorerWorkspace();
        }
    }

    function matchesActiveExplorerFilter(item) {
        if (!isExplorerV2Active()) return true;
        const f = State.explorerV2.activeFilter || 'ALL';
        const ouOk = matchesActiveOuColumnFilters(
            item,
            State.explorerV2.ouFilters,
            State.explorerV2.payload?.target_ou_summary
        );
        if (!ouOk) return false;
        return matchesPatternFilter(item, f);
    }

    function setExplorerV2Filter(filterKey) {
        State.explorerV2.activeFilter = filterKey || 'ALL';
        if (State.explorerV2.bridge && typeof State.explorerV2.bridge.applyFilters === 'function') {
            State.explorerV2.bridge.applyFilters();
        }
        renderExplorerWorkspace();
    }

    function toggleExplorerV2Drawer(drawerName) {
        State.explorerV2.openDrawer = (State.explorerV2.openDrawer === drawerName) ? 'none' : drawerName;
        renderExplorerWorkspace();
    }

    function renderExplorerV2DrawerHtml(data) {
        const d = State.explorerV2.openDrawer;
        if (!data || d === 'none') return '';
        const active = State.explorerV2.activeFilter || 'ALL';

        if (d === 'ladder') {
            const ladderFilterKeys = ['TOP5', 'TOP10', 'TOP20', 'SIM75', 'SIM65', 'ALL'];
            const ladderRows = (data.similarity_ladder || []).map((row, idx) => {
                const fKey = ladderFilterKeys[idx] || 'ALL';
                const isSel = active === fKey;
                return `
                    <tr class="pv2-ladder-click-row ${isSel ? 'is-selected' : ''}" data-pv2-ws-filter="${esc(fKey)}" title="Clic para filtrar las fichas del Explorador a este escalón">
                        <td>${esc(row.label)}</td>
                        <td class="text-center fw-bold">${row.n}</td>
                        <td class="text-center fw-bold ${row.fav_cover_pct >= 60 ? 'text-success' : ''}">${row.fav_cover_pct}%</td>
                        <td class="text-center fw-bold ${row.dog_cover_pct >= 60 ? 'text-danger' : ''}">${row.dog_cover_pct}%</td>
                        <td class="text-center">${row.push_pct}%</td>
                        <td class="text-center">${row.over_pct}%</td>
                        <td class="text-center">${row.avg_goals}</td>
                    </tr>
                `;
            }).join('');

            return `
                <div class="pv2-ws-drawer">
                    <div class="pv2-panel">
                        <div class="pv2-panel-title">
                            <span><i class="fa-solid fa-stairs me-1 text-primary"></i>Escalera de Convergencia</span>
                            <span class="small text-muted fw-normal">Clic en una fila para filtrar fichas</span>
                        </div>
                        <table class="pv2-table">
                            <thead><tr><th>Escalón</th><th class="text-center">N</th><th class="text-center">Fav%</th><th class="text-center">Rival%</th><th class="text-center">Push</th><th class="text-center">Over%</th><th class="text-center">Goles</th></tr></thead>
                            <tbody>${ladderRows}</tbody>
                        </table>
                    </div>
                </div>
            `;
        }

        if (d === 'matrix') {
            const q = data.quadrant_matrix || {};
            const cellHtml = (title, stats, quadKey) => {
                const s = stats || { n: 0, fav_cover_pct: 0, dog_cover_pct: 0, over_pct: 0 };
                let heat = 'heat-even';
                if (s.n >= 3) {
                    if (s.fav_cover_pct >= 60) heat = 'heat-fav';
                    else if (s.dog_cover_pct >= 60) heat = 'heat-dog';
                }
                const fKey = `QUAD:${quadKey}`;
                const selCls = active === fKey ? 'is-selected' : '';
                return `
                    <div class="pv2-matrix-cell pv2-matrix-cell--clickable ${heat} ${selCls}" data-pv2-ws-filter="${esc(fKey)}" title="Clic para ver solo estas fichas en la tabla">
                        <div class="d-flex justify-content-between align-items-center mb-1">
                            <strong style="font-size:0.74rem;">${esc(title)}</strong>
                            <span class="badge bg-dark">N=${s.n}</span>
                        </div>
                        <div style="font-size:0.75rem;">
                            Fav: <strong class="text-success">${s.fav_cover_pct}%</strong> ·
                            Rival: <strong class="text-danger">${s.dog_cover_pct}%</strong> ·
                            Push: <strong>${s.push_pct ?? 0}%</strong>
                        </div>
                        <div class="small text-muted mt-1" style="font-size:0.67rem;">
                            1X2: ${s.home_pct ?? 0}% / ${s.draw_pct ?? 0}% / ${s.away_pct ?? 0}% · Over ${s.over_pct}% (${s.avg_goals ?? 0} g/p)
                        </div>
                    </div>
                `;
            };
            const cr = data.target?.common_rivals || {};
            const crSamples = (cr.samples || []).map(s => `
                <tr>
                    <td class="fw-bold">${esc(s.rival)}</td>
                    <td class="text-center">${esc(s.home_score)} <span class="text-muted">(AH ${esc(s.home_ah)})</span></td>
                    <td class="text-center">${esc(s.away_score)} <span class="text-muted">(AH ${esc(s.away_ah)})</span></td>
                </tr>
            `).join('');

            return `
                <div class="pv2-ws-drawer">
                    <div class="pv2-panel mb-2">
                        <div class="pv2-panel-title">
                            <span><i class="fa-solid fa-table-cells-large me-1 text-primary"></i>Matriz 2×2: Mercado × Perfil Equipos</span>
                            <span class="small text-muted fw-normal">Clic en un cuadrante para filtrar fichas</span>
                        </div>
                        <div class="pv2-matrix-grid">
                            <div></div>
                            <div class="pv2-matrix-hdr">Mercado Alto (≥65%)</div>
                            <div class="pv2-matrix-hdr">Mercado Parcial (&lt;65%)</div>
                            <div class="pv2-matrix-hdr">Perfil Equipos Alto (≥65%)</div>
                            ${cellHtml('Zona Oro: Mismo Mercado + Mismo Perfil', q.high_prof_high_mkt, 'high_prof_high_mkt')}
                            ${cellHtml('Mismo Perfil (Mercado Parcial)', q.high_prof_part_mkt, 'high_prof_part_mkt')}
                            <div class="pv2-matrix-hdr">Otro Perfil (&lt;65%)</div>
                            ${cellHtml('Mismo Mercado (Otro Perfil)', q.other_prof_high_mkt, 'other_prof_high_mkt')}
                            ${cellHtml('Similitud Baja', q.other_prof_part_mkt, 'other_prof_part_mkt')}
                        </div>
                    </div>
                    ${cr.count ? `
                    <div class="pv2-panel">
                        <div class="pv2-panel-title">
                            <span><i class="fa-solid fa-scale-balanced me-1 text-primary"></i>Rivales Comunes del Partido Actual (${cr.count})</span>
                            <span class="small text-muted">Local: ${cr.home_pts} pts (${cr.home_gd >= 0 ? '+' : ''}${cr.home_gd}) · Visitante: ${cr.away_pts} pts (${cr.away_gd >= 0 ? '+' : ''}${cr.away_gd})</span>
                        </div>
                        <table class="pv2-table">
                            <thead><tr><th>Rival Común</th><th class="text-center">Resultado ${esc(data.target?.home)}</th><th class="text-center">Resultado ${esc(data.target?.away)}</th></tr></thead>
                            <tbody>${crSamples}</tbody>
                        </table>
                    </div>` : ''}
                </div>
            `;
        }

        if (d === 'antitrap') {
            return `<div class="pv2-ws-drawer">${renderTabAntiTrap(data)}</div>`;
        }
        return '';
    }

    function getTargetExplorerCard() {
        if (!isExplorerV2Active()) return null;
        return State.explorerV2.payload?.target_explorer_card || null;
    }

    function getExplorerCellHitClass(item, colKey) {
        if (!isExplorerV2Active() || !item || item._isTargetRef || !colKey) return '';
        const hits = item._pv2 && item._pv2.col_hits;
        if (!hits) return '';
        const val = hits[colKey];
        if (val === 'exact') return 'pv2-col-exact';
        if (val === 'partial') return 'pv2-col-partial';
        return '';
    }

    function hasUserOpenedAnyExplorerFicha() {
        return Boolean(
            document.querySelector('#table-body .collapse.show') ||
            Array.from(document.querySelectorAll('#table-body .cell-inline-stats-slot'))
                .some(el => el.style && el.style.display !== 'none' && el.innerHTML.trim() !== '')
        );
    }

    function computeDualStatsClient(visibleCards, data) {
        const list = Array.isArray(visibleCards) ? visibleCards : [];
        const base = aggregateMatchesClient(list);
        let settledOu = 0, overOu = 0, underOu = 0, pushOu = 0;
        const sameMovList = [];

        list.forEach(item => {
            const r = (item && item._pv2) ? item._pv2 : (item || {});
            if (r.ou_res === 'OVER' || r.ou_res === 'UNDER' || r.ou_res === 'PUSH') {
                settledOu += 1;
                if (r.ou_res === 'OVER') overOu += 1;
                else if (r.ou_res === 'UNDER') underOu += 1;
                else pushOu += 1;
            }
            if (r.ou_mov_match) sameMovList.push(item);
        });

        const overPct = settledOu ? Number(((overOu / settledOu) * 100).toFixed(1)) : base.over_pct;
        const underPct = settledOu ? Number(((underOu / settledOu) * 100).toFixed(1)) : base.under_pct;
        const ouPushPct = settledOu ? Number(((pushOu / settledOu) * 100).toFixed(1)) : 0;
        const sameMovAgg = aggregateMatchesClient(sameMovList);

        const allCards = data?.explorer_results || [];
        const activeFilter = State.explorerV2.activeFilter || 'ALL';
        const hasVisibleDiff = activeFilter !== 'ALL' || hasActiveOuColumnFilters() || list.length !== allCards.length;
        const hydratedNow = State.explorerV2.hydratedCount > 0;

        let ahSignal = base.fav_cover_pct >= 62 ? 'SEÑAL FAV' : (base.dog_cover_pct >= 62 ? 'SEÑAL RIVAL/DOG' : 'ZONA MIXTA AH');
        let ahTone = base.fav_cover_pct >= 62 ? 'good' : (base.dog_cover_pct >= 62 ? 'warn' : 'neutral');
        let ahDesc = `Fav cubre ${base.fav_cover_pct}% · Rival cubre ${base.dog_cover_pct}% · Push ${base.push_pct}% · 1X2: ${base.home_pct}%/${base.draw_pct}%/${base.away_pct}%`;
        let ahConf = '';

        if (!hasVisibleDiff && !hydratedNow && data?.verdict) {
            ahSignal = data.verdict.signal || ahSignal;
            ahTone = data.verdict.tone || ahTone;
            ahDesc = data.verdict.summary || ahDesc;
            ahConf = data.verdict.confidence || '';
        } else {
            const favLabel = data?.target?.fav_side === 'AWAY' ? 'Visitante' : 'Local';
            const dogLabel = favLabel === 'Local' ? 'Visitante' : 'Local';
            if (base.n < 4) {
                ahSignal = 'MUESTRA AH INSUFICIENTE';
                ahTone = 'neutral';
                ahConf = 'BAJA';
            } else if (base.fav_cover_pct >= 62) {
                ahSignal = `SEÑAL FAV (${favLabel})`;
                ahConf = (base.n >= 10 && base.fav_cover_pct >= 68) ? 'ALTA' : 'MEDIA';
            } else if (base.dog_cover_pct >= 62) {
                ahSignal = `SEÑAL RIVAL/DOG (${dogLabel})`;
                ahConf = (base.n >= 10 && base.dog_cover_pct >= 68) ? 'ALTA' : 'MEDIA';
            } else {
                ahConf = 'BAJA';
            }
        }

        let ouSignal = 'ZONA NEUTRA O/U';
        let ouTone = 'neutral';
        let ouConf = 'BAJA';
        let ouDesc = `Over ${overPct}% · Under ${underPct}% · Push ${ouPushPct}% · Promedio ${base.avg_goals} g/p`;

        if (settledOu < 4) {
            ouSignal = 'MUESTRA O/U INSUFICIENTE';
        } else if (overPct >= 60) {
            ouSignal = 'SEÑAL OVER GOLES';
            ouTone = 'good';
            ouConf = (settledOu >= 10 && overPct >= 66) ? 'ALTA' : 'MEDIA';
            ouDesc = `El Over se cumple en el ${overPct}% (Under ${underPct}% · Push ${ouPushPct}%) con ${base.avg_goals} goles/p.`;
        } else if (underPct >= 60) {
            ouSignal = 'SEÑAL UNDER GOLES';
            ouTone = 'warn';
            ouConf = (settledOu >= 10 && underPct >= 66) ? 'ALTA' : 'MEDIA';
            ouDesc = `El Under domina en el ${underPct}% (Over ${overPct}% · Push ${ouPushPct}%) con ${base.avg_goals} goles/p.`;
        }

        return {
            base,
            settledOu,
            overPct,
            underPct,
            ouPushPct,
            sameMovAgg,
            ahSignal,
            ahTone,
            ahConf,
            ahDesc,
            ouSignal,
            ouTone,
            ouConf,
            ouDesc,
            hasVisibleDiff,
        };
    }

    function computeHydratedTagCohorts(allCards, target) {
        const hydrated = (allCards || []).filter(c => Boolean(c?._pv2?.ficha_hydrated));
        if (!hydrated.length) return [];

        const targetCrossCodes = (target?.matchup_tags || []).map(t => t.code);
        const buckets = new Map();

        if (targetCrossCodes.length) {
            const sameCross = hydrated.filter(c => {
                const rowCodes = new Set((c._pv2?.matchup_tags || []).map(t => t.code));
                return targetCrossCodes.every(code => rowCodes.has(code));
            });
            if (sameCross.length) {
                buckets.set('__SAME_CROSS__', {
                    code: '__SAME_CROSS__',
                    label: 'Mismo Cruce Exacto',
                    tone: 'strong',
                    rows: sameCross,
                });
            }
        }

        hydrated.forEach(c => {
            const seen = new Set();
            const allTags = [
                ...(c._pv2?.matchup_tags || []),
                ...(c._pv2?.home_tags || []),
                ...(c._pv2?.away_tags || []),
            ];
            allTags.forEach(t => {
                if (!t || !t.code || seen.has(t.code)) return;
                seen.add(t.code);
                if (!buckets.has(t.code)) {
                    buckets.set(t.code, {
                        code: t.code,
                        label: t.label,
                        tone: t.tone || 'neutral',
                        rows: [],
                    });
                }
                buckets.get(t.code).rows.push(c);
            });
        });

        const targetSet = new Set(targetCrossCodes);
        const out = [];
        buckets.forEach((b, code) => {
            if (code !== '__SAME_CROSS__' && b.rows.length < 2) return;
            const agg = aggregateMatchesClient(b.rows);
            out.push({
                code: b.code,
                label: b.label,
                tone: b.tone,
                is_target_tag: targetSet.has(code) || code === '__SAME_CROSS__',
                ...agg,
            });
        });

        out.sort((a, b) => {
            if (Boolean(a.is_target_tag) !== Boolean(b.is_target_tag)) return a.is_target_tag ? -1 : 1;
            if ((a.code === '__SAME_CROSS__') !== (b.code === '__SAME_CROSS__')) return a.code === '__SAME_CROSS__' ? -1 : 1;
            return (b.n || 0) - (a.n || 0);
        });
        return out;
    }

    function renderExplorerOuBadgeHtml(item, side) {
        if (!isExplorerV2Active() || !item) return '';
        if (side === 'h2h_general' && isSameH2HMatch(item)) return '';
        const node = item[side];
        if (!node || typeof node !== 'object') return '';

        const sideLabelMap = {
            prev_home: 'Prev Local',
            prev_away: 'Prev Visitante',
            h2h_stadium: 'H2H Estadio',
            h2h_general: 'H2H General',
        };
        const sideLabel = sideLabelMap[side] || side;

        const currOuRaw = item.candidate?.ou_line;
        const currOu = (currOuRaw !== undefined && currOuRaw !== null && currOuRaw !== '' && currOuRaw !== '-') ? String(currOuRaw).trim() : '';
        const prevOuRaw = node.ou || node.ou_line || node.over_under_line_raw || '';
        const prevOu = (prevOuRaw !== undefined && prevOuRaw !== null && prevOuRaw !== '' && prevOuRaw !== '-') ? String(prevOuRaw).trim() : '';
        const info = extractNodeOuInfo(item, side);
        const resChar = info.res ? ` · ${info.res === 'OVER' ? 'O' : (info.res === 'UNDER' ? 'U' : 'P')}` : '';

        if (prevOu || node.ou_movement) {
            const movTxt = node.ou_movement || (currOu ? `${prevOu} → ${currOu}` : prevOu);
            let deltaTxt = '';
            if (typeof info.delta === 'number' && !Number.isNaN(info.delta)) {
                deltaTxt = ` (${info.delta > 0 ? '+' : ''}${info.delta})`;
            }

            // Resaltar en verde si el movimiento O/U coincide con el del Partido Actual
            let isMatch = false;
            if (!item._isTargetRef) {
                const tNode = State.explorerV2.payload?.target_ou_summary?.[side];
                if (tNode && info.dir && tNode.ou_mov_dir && tNode.ou_mov_dir === info.dir) {
                    if (typeof info.delta === 'number' && typeof tNode.ou_delta === 'number') {
                        isMatch = Math.abs(info.delta - tNode.ou_delta) <= 0.26;
                    } else {
                        isMatch = true;
                    }
                }
            }

            const cls = [
                'pv2-ou-badge',
                isMatch ? 'pv2-ou-badge--match' : '',
                info.res === 'OVER' ? 'pv2-ou-badge--over' : (info.res === 'UNDER' ? 'pv2-ou-badge--under' : ''),
            ].filter(Boolean).join(' ');

            return `<div class="${cls}" title="Movimiento línea de goles (${esc(sideLabel)})">O/U ${esc(movTxt)}${esc(deltaTxt)}${esc(resChar)}${isMatch ? ' ✓' : ''}</div>`;
        }

        if ((side === 'h2h_stadium' || side === 'h2h_general') && info.res && currOu) {
            const cls = [
                'pv2-ou-badge',
                'pv2-ou-badge--fallback',
                info.res === 'OVER' ? 'pv2-ou-badge--over' : (info.res === 'UNDER' ? 'pv2-ou-badge--under' : ''),
            ].filter(Boolean).join(' ');
            return `<div class="${cls}" title="Resultado frente a línea O/U actual (${esc(sideLabel)})">vs ${esc(currOu)}${esc(resChar)}</div>`;
        }

        return '';
    }

    function renderExplorerPrevOuHtml(item, side) {
        return renderExplorerOuBadgeHtml(item, side);
    }

    function isExplorerFichaScraped(itemOrId) {
        const ctxCache = window.clientContextCache instanceof Map ? window.clientContextCache : null;
        if (itemOrId && typeof itemOrId === 'object') {
            const cid = itemOrId.match_id ? String(itemOrId.match_id) : '';
            return Boolean(itemOrId._pv2?.ficha_hydrated || (cid && ctxCache && ctxCache.has(cid)));
        }
        const cid = String(itemOrId || '').trim();
        return Boolean(cid && ctxCache && ctxCache.has(cid));
    }

    function patchExplorerRowInPlace(card) {
        if (!card || !card.match_id) return;
        const cid = String(card.match_id);
        const escCid = (window.CSS && typeof window.CSS.escape === 'function') ? window.CSS.escape(cid) : cid;
        const tr = document.querySelector(`#table-body tr[data-match_id="${escCid}"]`);
        if (!tr) return;

        const simSlot = tr.querySelector(`[data-pv2-sim-slot="${escCid}"]`);
        if (simSlot) simSlot.innerHTML = renderExplorerRowSimHtml(card);

        ['home', 'away', 'matchup'].forEach(side => {
            const tagSlot = tr.querySelector(`[data-pv2-tag-slot="${escCid}:${side}"]`);
            if (tagSlot) tagSlot.innerHTML = '';
        });

        ['prev_home', 'prev_away'].forEach(prevSide => {
            const ouPrevSlot = tr.querySelector(`[data-pv2-ou-prev-slot="${escCid}:${prevSide}"]`);
            if (ouPrevSlot) ouPrevSlot.innerHTML = renderExplorerOuBadgeHtml(card, prevSide);
        });

        ['h2h_stadium', 'h2h_general'].forEach(h2hSide => {
            const ouH2hSlot = tr.querySelector(`[data-pv2-ou-h2h-slot="${escCid}:${h2hSide}"]`);
            if (ouH2hSlot) ouH2hSlot.innerHTML = renderExplorerOuBadgeHtml(card, h2hSide);
        });

        const colHits = card._pv2?.col_hits || {};
        tr.querySelectorAll('[data-pv2-col]').forEach(td => {
            const colKey = td.getAttribute('data-pv2-col');
            td.classList.remove('pv2-col-exact', 'pv2-col-partial');
            const hitCls = getExplorerCellHitClass(card, colKey);
            if (hitCls) td.classList.add(hitCls);
        });

        const ouDot = tr.querySelector(`[data-pv2-ou-dot="${escCid}"]`);
        if (ouDot) {
            const ouHit = colHits.ou;
            ouDot.className = ouHit === 'exact'
                ? 'pv2-ou-hit-dot pv2-ou-hit-dot--exact'
                : (ouHit === 'partial' ? 'pv2-ou-hit-dot pv2-ou-hit-dot--partial' : 'd-none');
            ouDot.textContent = ouHit === 'exact' ? 'O/U ✓' : (ouHit === 'partial' ? 'O/U ~' : '');
        }

        const fichaBtn = tr.querySelector(`[data-pv2-ficha-btn="${escCid}"]`) || tr.querySelector(`[data-bs-target="#global-stats-${escCid}"]`);
        if (fichaBtn) {
            if (isExplorerFichaScraped(card)) {
                fichaBtn.classList.add('pv2-ficha-scraped-btn');
                fichaBtn.title = 'Ficha ya scrapeada y lista (abrir al instante)';
            } else {
                fichaBtn.classList.remove('pv2-ficha-scraped-btn');
            }
        }
    }

    function applyDeepRescoreSortAndRender() {
        const data = State.explorerV2.payload;
        if (!data || !Array.isArray(data.explorer_results)) return;
        const allCards = data.explorer_results;

        allCards.sort((a, b) => {
            const pa = a._pv2 || {};
            const pb = b._pv2 || {};
            const simDiff = (pb.similarity_pct || 0) - (pa.similarity_pct || 0);
            if (Math.abs(simDiff) > 0.05) return simDiff;
            const tierDiff = (pa.pattern_tier || 9) - (pb.pattern_tier || 9);
            if (tierDiff !== 0) return tierDiff;
            const fchDiff = (pb.pillars?.ficha_pct ?? -1) - (pa.pillars?.ficha_pct ?? -1);
            if (Math.abs(fchDiff) > 0.05) return fchDiff;
            const ouDiff = (pb.pillars?.ou_pct || 0) - (pa.pillars?.ou_pct || 0);
            if (Math.abs(ouDiff) > 0.05) return ouDiff;
            return (pb.pillars?.h2h_pct || 0) - (pa.pillars?.h2h_pct || 0);
        });

        let hydratedCnt = 0;
        const missingIds = [];
        allCards.forEach((c, idx) => {
            if (c._pv2) {
                c._pv2.rank = idx + 1;
                if (c._pv2.ficha_hydrated) hydratedCnt += 1;
                else missingIds.push(String(c.match_id || ''));
            }
        });
        State.explorerV2.hydratedCount = hydratedCnt;
        State.explorerV2.pendingResortCount = 0;
        data.hydrated_ficha_count = hydratedCnt;
        data.full_ficha_count = hydratedCnt;
        data.missing_ficha_ids = missingIds;
        data.tag_cohorts = [];

        data.similarity_ladder = [
            ['Top 5 Máxima Similitud', allCards.slice(0, 5)],
            ['Top 10 Más Similares', allCards.slice(0, 10)],
            ['Top 20 Más Similares', allCards.slice(0, 20)],
            ['Similitud >= 75%', allCards.filter(c => (c._pv2?.similarity_pct || 0) >= 75)],
            ['Similitud >= 65%', allCards.filter(c => (c._pv2?.similarity_pct || 0) >= 65)],
            [`Muestra Base (Top ${allCards.length})`, allCards],
        ].map(([label, sub]) => ({ label, ...aggregateMatchesClient(sub) }));

        const mCells = {
            high_prof_high_mkt: [],
            high_prof_part_mkt: [],
            other_prof_high_mkt: [],
            other_prof_part_mkt: [],
        };
        allCards.forEach(c => {
            const q = c._pv2?.quadrant;
            if (q && mCells[q]) mCells[q].push(c);
        });
        data.quadrant_matrix = Object.fromEntries(
            Object.entries(mCells).map(([k, rows]) => [k, aggregateMatchesClient(rows)])
        );

        if (State.explorerV2.bridge && typeof State.explorerV2.bridge.setResults === 'function') {
            State.explorerV2.bridge.setResults(allCards);
        } else {
            renderExplorerWorkspace();
        }
    }

    function _ingestUpdatedBatch(updMap, allCards) {
        const ctxCache = window.clientContextCache instanceof Map ? window.clientContextCache : null;
        let updatedInBatch = 0;
        const touchedCards = [];

        Object.keys(updMap || {}).forEach(cid => {
            const info = updMap[cid] || {};
            const card = allCards.find(c => String(c.match_id) === String(cid));
            if (card) {
                if (info.prev_home && typeof info.prev_home === 'object') {
                    card.prev_home = { ...(card.prev_home || {}), ...info.prev_home };
                }
                if (info.prev_away && typeof info.prev_away === 'object') {
                    card.prev_away = { ...(card.prev_away || {}), ...info.prev_away };
                }
                if (info.h2h_stadium && typeof info.h2h_stadium === 'object') {
                    card.h2h_stadium = { ...(card.h2h_stadium || {}), ...info.h2h_stadium };
                }
                if (info.h2h_general && typeof info.h2h_general === 'object') {
                    card.h2h_general = { ...(card.h2h_general || {}), ...info.h2h_general };
                }
                if (info._pv2) {
                    card._pv2 = { ...(card._pv2 || {}), ...info._pv2 };
                    updatedInBatch += 1;
                    touchedCards.push(card);
                }
            }
            if (info.pre_match_context && ctxCache) {
                ctxCache.set(String(cid), info.pre_match_context);
            }
        });

        State.explorerV2.hydratedCount = allCards.filter(c => Boolean(c._pv2?.ficha_hydrated)).length;
        if (State.explorerV2.payload) {
            State.explorerV2.payload.hydrated_ficha_count = State.explorerV2.hydratedCount;
            State.explorerV2.payload.full_ficha_count = State.explorerV2.hydratedCount;
            State.explorerV2.payload.tag_cohorts = [];
        }
        return { updatedInBatch, touchedCards };
    }

    async function startBackgroundFichaHydration(targetLimit = 20, resumeQueue = false) {
        if (!isExplorerV2Active() || !State.explorerV2.payload) return;
        const data = State.explorerV2.payload;
        const allCards = Array.isArray(data.explorer_results) ? data.explorer_results : [];
        const sourceMatchId = String(data.target?.match_id || '');
        if (!allCards.length || !sourceMatchId) return;

        const myToken = ++State.explorerV2.bgRunToken;
        State.explorerV2.bgRunning = true;
        State.explorerV2.bgPaused = false;
        const dp = State.explorerV2.deepProgress;

        try {
            let needsQueue = [];
            if (resumeQueue && Array.isArray(State.explorerV2.bgNeedsDownloadQueue) && State.explorerV2.bgNeedsDownloadQueue.length) {
                needsQueue = State.explorerV2.bgNeedsDownloadQueue.filter(cid => {
                    const card = allCards.find(c => String(c.match_id) === String(cid));
                    return card && !card._pv2?.ficha_hydrated;
                });
            } else {
                // Sub-fase 2A: Lectura instantánea de fichas completas ya existentes en SQLite (Top 35)
                const top35Ids = allCards
                    .slice(0, 35)
                    .filter(c => c && c.match_id && !c._pv2?.ficha_hydrated)
                    .map(c => String(c.match_id));

                dp.lastMsg = '2º plano: leyendo fichas completas en SQLite...';
                renderExplorerWorkspace();

                if (top35Ids.length) {
                    const respA = await fetch('/api/pattern_search_v2/deep_compare', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            source_match_id: sourceMatchId,
                            candidate_ids: top35Ids,
                            sqlite_only: true,
                        }),
                    });
                    if (myToken !== State.explorerV2.bgRunToken || !isExplorerV2Active()) return;

                    const resA = await respA.json();
                    if (respA.ok && resA.status === 'success') {
                        const { updatedInBatch, touchedCards } = _ingestUpdatedBatch(resA.updated || {}, allCards);
                        dp.sqlite += Number(resA.sqlite_count || 0);
                        dp.done = State.explorerV2.hydratedCount;
                        needsQueue = Array.isArray(resA.needs_download_ids) ? resA.needs_download_ids : [];
                        State.explorerV2.bgNeedsDownloadQueue = needsQueue.slice();

                        if (updatedInBatch > 0) {
                            if (!hasUserOpenedAnyExplorerFicha()) {
                                applyDeepRescoreSortAndRender();
                            } else {
                                touchedCards.forEach(c => patchExplorerRowInPlace(c));
                                State.explorerV2.pendingResortCount += updatedInBatch;
                            }
                        }
                    }
                }
            }

            const webCap = Math.max(4, Math.min(35, Number(targetLimit) || Number(State.explorerV2.bgTargetTotal) || 20));
            const remainingWeb = Math.max(0, webCap - State.explorerV2.hydratedCount);
            const webIds = remainingWeb > 0 ? needsQueue.slice(0, Math.min(needsQueue.length, remainingWeb)) : [];
            dp.total = State.explorerV2.hydratedCount + webIds.length;

            if (!webIds.length) {
                State.explorerV2.bgRunning = false;
                dp.done = State.explorerV2.hydratedCount;
                dp.lastMsg = `2º plano listo: ${State.explorerV2.hydratedCount} fichas completas listas (${dp.sqlite} SQLite · ${dp.downloaded} Web)`;
                renderExplorerWorkspace();
                return;
            }

            // Sub-fase 2B: Descarga asíncrona silenciosa por lotes adaptados al entorno
            const chunkSize = Math.max(1, Math.min(4, Number(State.explorerV2.bgChunkSize) || 4));
            for (let i = 0; i < webIds.length; i += chunkSize) {
                if (myToken !== State.explorerV2.bgRunToken || !isExplorerV2Active() || State.explorerV2.bgPaused) {
                    break;
                }
                const chunk = webIds.slice(i, i + chunkSize);
                dp.lastMsg = `2º plano: descargando fichas (${Math.min(i + chunk.length, webIds.length)}/${webIds.length}) sin interrumpir...`;
                renderExplorerWorkspace();

                const respB = await fetch('/api/pattern_search_v2/deep_compare', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        source_match_id: sourceMatchId,
                        candidate_ids: chunk,
                        sqlite_only: false,
                    }),
                });
                if (myToken !== State.explorerV2.bgRunToken || !isExplorerV2Active()) return;

                const resB = await respB.json();
                if (respB.ok && resB.status === 'success') {
                    const { updatedInBatch, touchedCards } = _ingestUpdatedBatch(resB.updated || {}, allCards);
                    touchedCards.forEach(c => patchExplorerRowInPlace(c));
                    State.explorerV2.pendingResortCount += updatedInBatch;
                    dp.downloaded += Number(resB.downloaded_count || 0);
                    dp.sqlite += Number(resB.sqlite_count || 0);
                    dp.failed += Array.isArray(resB.failed_ids) ? resB.failed_ids.length : 0;
                    dp.done = State.explorerV2.hydratedCount;
                    State.explorerV2.bgNeedsDownloadQueue = (State.explorerV2.bgNeedsDownloadQueue || [])
                        .filter(id => !chunk.includes(String(id)));
                    renderExplorerWorkspace();
                }
            }

            if (myToken !== State.explorerV2.bgRunToken || !isExplorerV2Active()) return;
            State.explorerV2.bgRunning = false;
            dp.done = State.explorerV2.hydratedCount;

            if (State.explorerV2.bgPaused) {
                dp.lastMsg = `2º plano en pausa (${State.explorerV2.hydratedCount} fichas listas · ${State.explorerV2.bgNeedsDownloadQueue.length} en cola)`;
                renderExplorerWorkspace();
            } else {
                dp.lastMsg = `2º plano completado: ${State.explorerV2.hydratedCount} fichas listas (SQLite: ${dp.sqlite} · Web: ${dp.downloaded})`;
                if (State.explorerV2.pendingResortCount > 0 && !hasUserOpenedAnyExplorerFicha()) {
                    applyDeepRescoreSortAndRender();
                } else {
                    renderExplorerWorkspace();
                }
            }
        } catch (err) {
            if (myToken !== State.explorerV2.bgRunToken) return;
            State.explorerV2.bgRunning = false;
            State.explorerV2.deepProgress.lastMsg = `Aviso en 2º plano: ${err.message || err}`;
            renderExplorerWorkspace();
        }
    }

    function pauseBackgroundHydration() {
        if (!isExplorerV2Active()) return;
        State.explorerV2.bgPaused = true;
        State.explorerV2.bgRunning = false;
        State.explorerV2.bgRunToken += 1;
        State.explorerV2.deepProgress.lastMsg = `2º plano en pausa (${State.explorerV2.hydratedCount} fichas completas listas)`;
        renderExplorerWorkspace();
    }

    function resumeBackgroundHydration() {
        if (!isExplorerV2Active()) return;
        startBackgroundFichaHydration(State.explorerV2.bgTargetTotal || 20, true);
    }

    function expandTopExplorerFichas(count = 5) {
        const maxN = Math.max(1, Number(count) || 5);
        const rows = Array.from(document.querySelectorAll('#table-body tr[data-match_id]'));
        let opened = 0;
        for (const tr of rows) {
            if (opened >= maxN) break;
            const mid = tr.getAttribute('data-match_id');
            if (!mid) continue;
            const collapseId = `global-stats-${mid}`;
            const collapseEl = document.getElementById(collapseId);
            if (!collapseEl) continue;
            if (!collapseEl.classList.contains('show')) {
                try {
                    if (window.bootstrap && window.bootstrap.Collapse) {
                        const inst = window.bootstrap.Collapse.getOrCreateInstance(collapseEl, { toggle: false });
                        inst.show();
                    } else {
                        collapseEl.classList.add('show');
                    }
                    if (typeof window.hydrateProgressionStats === 'function') {
                        window.hydrateProgressionStats(collapseId);
                    }
                } catch (_err) {
                    const btn = tr.querySelector(`[data-bs-target="#${collapseId}"]`);
                    if (btn) btn.click();
                }
            } else if (typeof window.hydrateProgressionStats === 'function') {
                window.hydrateProgressionStats(collapseId);
            }
            opened += 1;
        }
    }

    async function runDeepDownloadAndRescore(topLimit, forceRefresh = false) {
        if (State.explorerV2.deepBusy || !isExplorerV2Active() || !State.explorerV2.payload) return;
        const data = State.explorerV2.payload;
        const allCards = Array.isArray(data.explorer_results) ? data.explorer_results : [];
        if (!allCards.length) return;

        // Detener suavemente el bucle automático para no duplicar peticiones
        State.explorerV2.bgRunToken += 1;
        State.explorerV2.bgRunning = false;
        State.explorerV2.bgPaused = false;

        const limit = Math.max(1, Math.min(40, Number(topLimit) || Number(State.explorerV2.deepLimit) || 15));
        State.explorerV2.deepLimit = limit;

        const visible = Array.isArray(State.explorerV2.visibleCards) && State.explorerV2.visibleCards.length
            ? State.explorerV2.visibleCards
            : allCards;
        const pool = visible.slice(0, limit);
        const ctxCache = window.clientContextCache instanceof Map ? window.clientContextCache : null;
        const targetIds = pool
            .filter(c => c && c.match_id && (forceRefresh || !c._pv2?.ficha_hydrated || !(ctxCache && ctxCache.has(String(c.match_id)))))
            .map(c => String(c.match_id));

        if (!targetIds.length) {
            State.explorerV2.deepProgress.lastMsg = `Las Top ${pool.length} fichas visibles ya están hidratadas.`;
            if (State.explorerV2.pendingResortCount > 0) {
                applyDeepRescoreSortAndRender();
            } else {
                renderExplorerWorkspace();
            }
            return;
        }

        const sourceMatchId = String(data.target?.match_id || '');
        if (!sourceMatchId) return;

        State.explorerV2.deepBusy = true;
        const dp = State.explorerV2.deepProgress;
        dp.total = State.explorerV2.hydratedCount + targetIds.length;
        dp.lastMsg = `Descargando ${targetIds.length} fichas en lotes de 5...`;
        renderExplorerWorkspace();

        try {
            let processed = 0;
            for (let i = 0; i < targetIds.length; i += 5) {
                const chunk = targetIds.slice(i, i + 5);
                const resp = await fetch('/api/pattern_search_v2/deep_compare', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        source_match_id: sourceMatchId,
                        candidate_ids: chunk,
                        force_refresh: Boolean(forceRefresh),
                        sqlite_only: false,
                    }),
                });
                const res = await resp.json();
                if (!resp.ok || res.status !== 'success') {
                    throw new Error(res.error || `Error HTTP ${resp.status}`);
                }

                const { updatedInBatch, touchedCards } = _ingestUpdatedBatch(res.updated || {}, allCards);
                touchedCards.forEach(c => patchExplorerRowInPlace(c));
                State.explorerV2.pendingResortCount += updatedInBatch;
                processed += chunk.length;
                dp.done = State.explorerV2.hydratedCount;
                dp.downloaded += Number(res.downloaded_count || 0);
                dp.sqlite += Number(res.sqlite_count || 0);
                dp.failed += Array.isArray(res.failed_ids) ? res.failed_ids.length : 0;
                dp.lastMsg = `Hidratadas ${processed}/${targetIds.length} fichas nuevas (Total: ${State.explorerV2.hydratedCount})`;
                renderExplorerWorkspace();
            }

            State.explorerV2.deepBusy = false;
            dp.lastMsg = `Fichas listas (${State.explorerV2.hydratedCount} totales · Web: ${dp.downloaded} · SQLite: ${dp.sqlite})`;
            applyDeepRescoreSortAndRender();
        } catch (err) {
            State.explorerV2.deepBusy = false;
            State.explorerV2.deepProgress.lastMsg = `Error hidratando fichas: ${err.message || err}`;
            renderExplorerWorkspace();
        }
    }

    function renderExplorerWorkspace() {
        const ws = document.getElementById('pv2-explorer-workspace');
        const data = State.explorerV2.payload;
        if (!ws || !isExplorerV2Active() || !data) return;

        ws.classList.remove('d-none');
        const target = data.target || {};
        const allCards = data.explorer_results || [];
        const visibleCards = Array.isArray(State.explorerV2.visibleCards) ? State.explorerV2.visibleCards : allCards;
        const activeFilter = State.explorerV2.activeFilter || 'ALL';
        const dual = computeDualStatsClient(visibleCards, data);
        const stats = dual.base;

        const hydratedCount = State.explorerV2.hydratedCount || allCards.filter(c => Boolean(c._pv2?.ficha_hydrated)).length;
        const tier1Count = data.tier1_count ?? allCards.filter(c => Number(c._pv2?.pattern_tier) === 1).length;
        const ouMatchCount = allCards.filter(c => Boolean(c._pv2?.ou_match)).length;
        const ouMovCount = allCards.filter(c => Boolean(c._pv2?.ou_mov_match)).length;

        const allChips = [
            { id: 'ALL', label: `Todas (${allCards.length})` },
            { id: 'TIER1', label: `Patrón Base ✓ (${tier1Count})` },
            { id: 'OU_HIT', label: `Línea+Mov O/U ✓ (${ouMatchCount})` },
            { id: 'OU_MOV_MATCH', label: `Mismo Mov O/U ✓ (${ouMovCount})` },
            { id: 'H2H_HIT', label: 'H2H Mov+Res ✓' },
            { id: 'IND_HIT', label: 'Indirectas ✓' },
            { id: 'COL3_HIT', label: 'Col3 ✓' },
            ...(hydratedCount > 0 ? [{ id: 'FICHA_HIT', label: 'Ficha 10p ✓' }] : []),
            { id: 'WITH_FICHA', label: `Con Ficha (${hydratedCount})` },
            { id: 'TOP5', label: 'Top 5' },
            { id: 'TOP10', label: 'Top 10' },
            { id: 'TOP20', label: 'Top 20' },
            { id: 'SIM75', label: 'Sim ≥ 75%' },
            { id: 'SIM65', label: 'Sim ≥ 65%' },
            { id: 'EXACT_AH', label: 'AH Exacto' },
            ...(hydratedCount > 0 ? [{ id: 'QUAD:high_prof_high_mkt', label: 'Zona Oro' }] : []),
        ];
        const ouFiltersState = State.explorerV2.ouFilters || {};
        const activeOuPills = OU_FILTER_DOM_MAP
            .filter(f => Boolean(ouFiltersState[f.key]))
            .map(f => ({ key: f.key, label: `${f.label}: ${ouFiltersState[f.key]}` }));

        const d = State.explorerV2.openDrawer;
        const targetId = target.match_id || '';
        const dp = State.explorerV2.deepProgress || {};
        const deepBusy = Boolean(State.explorerV2.deepBusy);
        const bgRunning = Boolean(State.explorerV2.bgRunning);
        const bgPaused = Boolean(State.explorerV2.bgPaused);
        const pendingResort = Number(State.explorerV2.pendingResortCount || 0);
        const deepLimit = Number(State.explorerV2.deepLimit) || 15;
        const pctProg = dp.total > 0 ? Math.min(100, Math.round(((dp.done || hydratedCount) / dp.total) * 100)) : 0;
        const targetOuMovText = data.target_ou_summary?.text || data.ou_verdict?.target_ou_mov_text || '';
        const overBarW = dual.overPct;
        const ouPushBarW = dual.ouPushPct;
        const underBarW = dual.underPct;

        ws.innerHTML = `
            <div class="pv2-ws">
                <div class="pv2-ws-head">
                    <div>
                        <div class="fw-bold" style="font-size:0.96rem;">
                            <i class="fa-solid fa-wand-magic-sparkles me-2 text-warning"></i>Búsqueda Patrón 2.0 · Explorador de Similares
                        </div>
                        <div class="small opacity-75" style="font-size:0.74rem;">
                            ${esc(target.home || '')} vs ${esc(target.away || '')} · ${esc(target.league || '')} · AH ${fmtAh(target.ah)}${target.ou ? ` · O/U ${fmtAh(target.ou)}` : ''} · ${esc(data.elapsed_ms || 0)} ms
                        </div>
                    </div>
                    <div class="d-flex align-items-center gap-2 flex-wrap">
                        ${targetId ? `<a href="/estudio/${encodeURIComponent(targetId)}" target="_blank" rel="noopener" class="btn btn-sm btn-outline-light fw-bold" style="font-size:0.72rem;"><i class="fa-solid fa-external-link-alt me-1"></i>Ver Estudio</a>` : ''}
                        <button type="button" class="btn btn-sm btn-light fw-bold" id="pv2-ws-exit-btn" style="font-size:0.72rem;">
                            <i class="fa-solid fa-compass me-1"></i>Salir a Explorador Libre
                        </button>
                    </div>
                </div>
                <div class="pv2-ws-body">
                    ${renderTargetCard(target, { ouMovText: targetOuMovText })}
                    <div class="pv2-dual-verdict">
                        <div class="pv2-verdict-box tone-${esc(dual.ahTone)} mb-0" style="grid-template-columns:1fr;">
                            <div>
                                <div class="d-flex align-items-center gap-2 flex-wrap">
                                    <span class="badge bg-dark" style="font-size:0.62rem;">PRONÓSTICO AH</span>
                                    <span class="fw-bold" style="font-size:0.9rem;">${esc(dual.ahSignal)}</span>
                                    ${dual.ahConf ? `<span class="badge bg-light text-dark border" style="font-size:0.62rem;">Confianza ${esc(dual.ahConf)}</span>` : ''}
                                    <span class="badge bg-light text-dark border" style="font-size:0.62rem;">N=${stats.n} / ${esc(data.total_candidates_scanned || 0)} escaneados</span>
                                </div>
                                <div class="text-muted mt-1" style="font-size:0.73rem;">${esc(dual.ahDesc)}</div>
                                <div class="pv2-kpi-row mb-1">
                                    <span class="pv2-kpi-pill text-success">Fav Cubre: ${stats.fav_cover_pct}%</span>
                                    <span class="pv2-kpi-pill text-danger">Rival Cubre: ${stats.dog_cover_pct}%</span>
                                    <span class="pv2-kpi-pill text-warning">Push: ${stats.push_pct}%</span>
                                    <span class="pv2-kpi-pill">1X2: ${stats.home_pct}% / ${stats.draw_pct}% / ${stats.away_pct}%</span>
                                </div>
                                <div class="pv2-prob-bar">
                                    <div class="pv2-prob-seg-fav" style="width:${stats.fav_cover_pct}%"></div>
                                    <div class="pv2-prob-seg-push" style="width:${stats.push_pct}%"></div>
                                    <div class="pv2-prob-seg-dog" style="width:${stats.dog_cover_pct}%"></div>
                                </div>
                            </div>
                        </div>
                        <div class="pv2-ou-box tone-${esc(dual.ouTone)}">
                            <div>
                                <div class="d-flex align-items-center gap-2 flex-wrap">
                                    <span class="badge bg-success" style="font-size:0.62rem;">PRONÓSTICO GOLES O/U</span>
                                    <span class="fw-bold" style="font-size:0.9rem;">${esc(dual.ouSignal)}</span>
                                    <span class="badge bg-light text-dark border" style="font-size:0.62rem;">Confianza ${esc(dual.ouConf)}</span>
                                    <span class="badge bg-light text-dark border" style="font-size:0.62rem;">${stats.avg_goals} g/p (N=${dual.settledOu})</span>
                                </div>
                                <div class="text-muted mt-1" style="font-size:0.73rem;">${esc(dual.ouDesc)}</div>
                                <div class="pv2-kpi-row mb-1">
                                    <span class="pv2-kpi-pill text-success">Over: ${dual.overPct}%</span>
                                    <span class="pv2-kpi-pill text-warning">Under: ${dual.underPct}%</span>
                                    <span class="pv2-kpi-pill text-secondary">Push O/U: ${dual.ouPushPct}%</span>
                                    ${dual.sameMovAgg.n > 0 ? `<button type="button" class="pv2-filter-chip ${activeFilter === 'OU_MOV_MATCH' ? 'is-active' : ''}" data-pv2-ws-filter="OU_MOV_MATCH" title="Filtrar solo fichas con el mismo movimiento de línea de goles">Mismo Mov O/U (N=${dual.sameMovAgg.n}): Over ${dual.sameMovAgg.over_pct}% · Under ${dual.sameMovAgg.under_pct}%</button>` : ''}
                                </div>
                            </div>
                            <div class="pv2-ou-bar" title="Over ${overBarW}% · Push ${ouPushBarW}% · Under ${underBarW}%">
                                <div class="pv2-ou-seg-over" style="width:${overBarW}%"></div>
                                <div class="pv2-ou-seg-push" style="width:${ouPushBarW}%"></div>
                                <div class="pv2-ou-seg-under" style="width:${underBarW}%"></div>
                            </div>
                        </div>
                    </div>
                    <div class="pv2-deep-box">
                        <span class="pv2-bg-status-pill ${bgRunning && !bgPaused ? 'is-running' : ''}">
                            ${bgRunning && !bgPaused ? '⚡ 2º plano activo' : (bgPaused ? '⏸ 2º plano en pausa' : '✓ Fichas')}: ${hydratedCount}/${allCards.length}
                        </span>
                        <span class="badge bg-dark" style="font-size:0.67rem;">Patrón base: ${tier1Count}</span>
                        ${bgRunning && !bgPaused
                            ? '<button type="button" class="btn btn-sm btn-outline-secondary fw-bold" id="pv2-bg-pause-btn" style="font-size:0.7rem;"><i class="fa-solid fa-pause me-1"></i>Pausar 2º plano</button>'
                            : ((State.explorerV2.bgNeedsDownloadQueue || []).length > 0 && !deepBusy
                                ? '<button type="button" class="btn btn-sm btn-outline-primary fw-bold" id="pv2-bg-resume-btn" style="font-size:0.7rem;"><i class="fa-solid fa-play me-1"></i>Reanudar 2º plano</button>'
                                : '')}
                        ${pendingResort > 0 ? `<button type="button" class="btn btn-sm pv2-apply-resort-btn" id="pv2-apply-resort-btn"><i class="fa-solid fa-arrow-up-wide-short me-1"></i>Aplicar nuevo orden (${pendingResort} fichas)</button>` : ''}
                        <label class="small fw-bold text-muted mb-0 ms-1" for="pv2-deep-limit-sel" style="font-size:0.7rem;">Lote:</label>
                        <select id="pv2-deep-limit-sel" class="form-select form-select-sm" style="width:auto;font-size:0.72rem;padding:2px 24px 2px 8px;" ${deepBusy ? 'disabled' : ''}>
                            <option value="15" ${deepLimit === 15 ? 'selected' : ''}>Top 15</option>
                            <option value="25" ${deepLimit === 25 ? 'selected' : ''}>Top 25</option>
                            <option value="40" ${deepLimit === 40 ? 'selected' : ''}>Top 40</option>
                        </select>
                        <button type="button" class="btn btn-sm btn-primary fw-bold" id="pv2-deep-run-btn" style="font-size:0.72rem;" ${deepBusy ? 'disabled' : ''}>
                            ${deepBusy ? '<span class="spinner-border spinner-border-sm me-1"></span>Descargando...' : '<i class="fa-solid fa-cloud-arrow-down me-1"></i>Descargar más fichas'}
                        </button>
                        <span class="small fw-bold" style="font-size:0.69rem;color:#b91c1c;" title="El botón rojo en cada fila indica que su ficha ya está scrapeada y lista para abrir al instante">
                            <i class="fa-solid fa-table-cells me-1"></i>Rojo = ficha scrapeada
                        </span>
                        ${(dp.total > 0 || dp.lastMsg) ? `
                            ${dp.total > 0 ? `<div class="pv2-deep-progress" title="${esc(dp.lastMsg || '')}"><div class="pv2-deep-progress-bar" style="width:${pctProg}%"></div></div>` : ''}
                            <span class="small fw-bold text-dark" style="font-size:0.68rem;">${esc(dp.lastMsg || '')}</span>
                        ` : ''}
                    </div>
                    <div class="pv2-ws-tools">
                        <div class="d-flex flex-wrap gap-1 align-items-center">
                            <span class="small fw-bold text-muted me-1" style="font-size:0.7rem;">Filtrar fichas:</span>
                            ${allChips.map(ch => `<button type="button" class="pv2-filter-chip ${activeFilter === ch.id ? 'is-active' : ''}" data-pv2-ws-filter="${esc(ch.id)}">${esc(ch.label)}</button>`).join('')}
                        </div>
                        <div class="d-flex flex-wrap gap-1 align-items-center">
                            <button type="button" class="pv2-ws-drawer-btn ${d === 'ladder' ? 'is-active' : ''}" data-pv2-ws-drawer="ladder"><i class="fa-solid fa-stairs me-1"></i>Escalera</button>
                            <button type="button" class="pv2-ws-drawer-btn ${d === 'matrix' ? 'is-active' : ''}" data-pv2-ws-drawer="matrix"><i class="fa-solid fa-table-cells-large me-1"></i>Matriz 2×2 + Comunes</button>
                            <button type="button" class="pv2-ws-drawer-btn ${d === 'antitrap' ? 'is-active' : ''}" data-pv2-ws-drawer="antitrap"><i class="fa-solid fa-radar me-1"></i>Anti-Trampa</button>
                        </div>
                    </div>
                    ${activeOuPills.length ? `
                    <div class="pv2-ou-active-bar">
                        <span class="small fw-bold" style="font-size:0.69rem;color:#0369a1;"><i class="fa-solid fa-filter me-1"></i>Filtros O/U activos:</span>
                        ${activeOuPills.map(p => `<button type="button" class="pv2-ou-active-pill" data-pv2-clear-ou-key="${esc(p.key)}" title="Clic para quitar este filtro">${esc(p.label)} <i class="fa-solid fa-xmark"></i></button>`).join('')}
                        <button type="button" class="btn btn-sm btn-outline-info py-0 px-2 fw-bold" id="pv2-clear-all-ou-btn" style="font-size:0.68rem;">Limpiar O/U</button>
                    </div>` : ''}
                    ${renderExplorerV2DrawerHtml(data)}
                </div>
            </div>
        `;

        ws.querySelector('#pv2-ws-exit-btn')?.addEventListener('click', () => {
            if (State.explorerV2.bridge && typeof State.explorerV2.bridge.clearToNormalSearch === 'function') {
                State.explorerV2.bridge.clearToNormalSearch();
            } else {
                deactivateExplorerV2();
            }
        });
        ws.querySelector('#pv2-bg-pause-btn')?.addEventListener('click', pauseBackgroundHydration);
        ws.querySelector('#pv2-bg-resume-btn')?.addEventListener('click', resumeBackgroundHydration);
        ws.querySelector('#pv2-apply-resort-btn')?.addEventListener('click', applyDeepRescoreSortAndRender);
        ws.querySelectorAll('[data-pv2-clear-ou-key]').forEach(el => {
            el.addEventListener('click', () => {
                clearOuFilters(el.getAttribute('data-pv2-clear-ou-key'));
            });
        });
        ws.querySelector('#pv2-clear-all-ou-btn')?.addEventListener('click', () => clearOuFilters());
        ws.querySelector('#pv2-deep-limit-sel')?.addEventListener('change', (e) => {
            State.explorerV2.deepLimit = Number(e.target.value) || 15;
        });
        ws.querySelector('#pv2-deep-run-btn')?.addEventListener('click', () => {
            const selVal = ws.querySelector('#pv2-deep-limit-sel')?.value;
            runDeepDownloadAndRescore(selVal || State.explorerV2.deepLimit || 15, false);
        });
        ws.querySelectorAll('[data-pv2-ws-filter]').forEach(el => {
            el.addEventListener('click', () => {
                const next = el.getAttribute('data-pv2-ws-filter');
                setExplorerV2Filter(State.explorerV2.activeFilter === next && next !== 'ALL' ? 'ALL' : next);
            });
        });
        ws.querySelectorAll('[data-pv2-ws-drawer]').forEach(el => {
            el.addEventListener('click', () => {
                toggleExplorerV2Drawer(el.getAttribute('data-pv2-ws-drawer'));
            });
        });
    }

    function syncExplorerV2Stats(visibleCards) {
        if (!isExplorerV2Active()) return;
        State.explorerV2.visibleCards = Array.isArray(visibleCards) ? visibleCards : [];
        renderExplorerWorkspace();
    }

    async function initExplorerPatternV2Page(sourceMatchId, bridge) {
        const mid = String(sourceMatchId || '').trim();
        if (!mid || !bridge) return;

        State.explorerV2.active = true;
        State.explorerV2.bridge = bridge;
        State.explorerV2.activeFilter = 'ALL';
        State.explorerV2.openDrawer = 'none';
        State.explorerV2.payload = null;
        State.explorerV2.visibleCards = null;
        State.explorerV2.hydratedCount = 0;
        State.explorerV2.pendingResortCount = 0;
        State.explorerV2.bgRunning = false;
        State.explorerV2.bgPaused = false;
        State.explorerV2.bgRunToken += 1;
        State.explorerV2.bgNeedsDownloadQueue = [];
        State.explorerV2.deepBusy = false;
        State.explorerV2.deepProgress = { done: 0, total: 0, downloaded: 0, sqlite: 0, failed: 0, lastMsg: '' };
        if (typeof document !== 'undefined' && document.body && document.body.classList) {
            document.body.classList.add('pv2-explorer-active');
        }
        resetOuFiltersDom();

        const ws = document.getElementById('pv2-explorer-workspace');
        if (ws) {
            ws.classList.remove('d-none');
            ws.innerHTML = `
                <div class="pv2-ws">
                    <div class="pv2-ws-body text-center py-4">
                        <div class="spinner-border text-primary" role="status"></div>
                        <div class="fw-bold mt-2">Cargando Búsqueda Patrón 2.0 (AH + Líneas O/U + H2H + Indirectas + Col3)...</div>
                    </div>
                </div>
            `;
        }
        if (typeof bridge.showLoading === 'function') bridge.showLoading(true);

        try {
            const resp = await fetch('/api/pattern_search_v2', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ match_id: mid, top_n: 80 }),
            });
            const data = await resp.json();
            if (!resp.ok || data.status !== 'success') {
                throw new Error(data.error || `Error HTTP ${resp.status}`);
            }

            State.explorerV2.payload = data;
            syncOuFilterTargetOptionLabels(data.target_ou_summary);
            const rp = data.runtime_profile;
            if (rp && typeof rp === 'object') {
                if (Number.isFinite(Number(rp.bg_target_total)) && Number(rp.bg_target_total) > 0) {
                    State.explorerV2.bgTargetTotal = Number(rp.bg_target_total);
                }
                if (Number.isFinite(Number(rp.bg_chunk_size)) && Number(rp.bg_chunk_size) > 0) {
                    State.explorerV2.bgChunkSize = Number(rp.bg_chunk_size);
                }
            }
            const cards = Array.isArray(data.explorer_results) ? data.explorer_results : [];
            State.explorerV2.visibleCards = cards;

            if (data.target_pre_match_context && window.clientContextCache instanceof Map) {
                window.clientContextCache.set(String(mid), data.target_pre_match_context);
            }

            if (data.target?.home && data.target?.away) {
                document.title = `Patrón 2.0 · ${data.target.home} vs ${data.target.away}`;
            }

            renderExplorerWorkspace();
            if (typeof bridge.setResults === 'function') {
                bridge.setResults(cards);
            }

            // Arrancar hidratación silenciosa en segundo plano (SQLite inmediato + lotes web de 4)
            startBackgroundFichaHydration(State.explorerV2.bgTargetTotal || 20, false);
        } catch (err) {
            if (ws) {
                ws.innerHTML = `
                    <div class="alert alert-danger d-flex justify-content-between align-items-center mb-0">
                        <span>No se pudo cargar Patrón 2.0: ${esc(err.message)}</span>
                        <button type="button" class="btn btn-sm btn-outline-danger fw-bold" id="pv2-ws-err-exit">Ir al Explorador Normal</button>
                    </div>
                `;
                ws.querySelector('#pv2-ws-err-exit')?.addEventListener('click', () => {
                    if (typeof bridge.clearToNormalSearch === 'function') bridge.clearToNormalSearch();
                    else deactivateExplorerV2();
                });
            }
        } finally {
            if (typeof bridge.showLoading === 'function') bridge.showLoading(false);
        }
    }

    function _pillarChipHtml(label, val, titleTxt = '') {
        if (val === null || val === undefined || Number.isNaN(Number(val))) {
            return `<span class="pv2-pillar-chip pv2-pillar-chip--low"${titleTxt ? ` title="${esc(titleTxt)}"` : ''}>${esc(label)}:-</span>`;
        }
        const n = Math.round(Number(val));
        const cls = n >= 65 ? 'pv2-pillar-chip--good' : (n >= 45 ? 'pv2-pillar-chip--mid' : 'pv2-pillar-chip--low');
        return `<span class="pv2-pillar-chip ${cls}"${titleTxt ? ` title="${esc(titleTxt)}"` : ''}>${esc(label)}:${n}</span>`;
    }

    function renderExplorerRowSimHtml(item) {
        if (item && item._isTargetRef) {
            return '<div class="mt-1"><span class="pv2-target-ref-badge">PARTIDO ACTUAL</span></div>';
        }
        const p = item && item._pv2;
        if (!p) return '';
        const sim = Number(p.similarity_pct || 0);
        const pillCls = sim >= 75 ? 'pv2-sim-pill--high' : (sim >= 65 ? 'pv2-sim-pill--mid' : 'pv2-sim-pill--low');
        const pl = p.pillars || {};
        const tierBadge = Number(p.pattern_tier) === 1
            ? '<span class="pv2-pillar-chip pv2-pillar-chip--good">BASE ✓</span>'
            : (Number(p.pattern_tier) === 2 ? '<span class="pv2-pillar-chip pv2-pillar-chip--mid">Cercano</span>' : '<span class="pv2-pillar-chip pv2-pillar-chip--low">Bucket AH</span>');
        const isHydrated = Boolean(p.ficha_hydrated || isExplorerFichaScraped(item));
        const fichaBadge = isHydrated
            ? '<span class="pv2-pillar-chip pv2-pillar-chip--scraped">Scrapeada ✓</span>'
            : '<span class="pv2-pillar-chip pv2-pillar-chip--low">Pend. ficha</span>';

        const venTitle = [p.home_venue_summary, p.away_venue_summary].filter(Boolean).join(' · ');
        const pillarsList = [
            _pillarChipHtml('P', pl.core_pct),
            _pillarChipHtml('OU', pl.ou_pct),
            _pillarChipHtml('H2H', pl.h2h_pct),
            _pillarChipHtml('IND', pl.ind_pct),
            _pillarChipHtml('C3', pl.col3_pct),
        ];
        if (isHydrated) {
            pillarsList.push(_pillarChipHtml('FCH', pl.ficha_pct, venTitle));
        }
        const topReasons = (p.matched_reasons || []).slice(0, 2)
            .map(r => `<span class="pv2-sim-reason-chip">${esc(r)}</span>`).join('');
        const divHtml = (p.divergences && p.divergences.length)
            ? `<div class="text-warning fw-bold mt-1" style="font-size:0.55rem;" title="${esc(p.divergences.join(' | '))}">⚠ ${esc(p.divergences[0])}</div>`
            : '';

        return `
            <div class="pv2-sim-box" title="Núcleo ${pl.core_pct ?? '-'}% · O/U ${pl.ou_pct ?? '-'}% · H2H ${pl.h2h_pct ?? '-'}% · Indirectas ${pl.ind_pct ?? '-'}% · Col3 ${pl.col3_pct ?? '-'}%${isHydrated ? ` · Ficha 10p ${pl.ficha_pct ?? '-'}% (${venTitle})` : ''}">
                <span class="pv2-sim-pill ${pillCls}">#${p.rank} · ${sim}% SIM</span>
                <div class="pv2-sim-mini-bar"><span style="width:${Math.min(100, Math.max(0, sim))}%"></span></div>
                <div class="pv2-pillar-strip">${tierBadge}${fichaBadge}</div>
                <div class="pv2-pillar-strip">${pillarsList.join('')}</div>
                ${topReasons ? `<div class="pv2-sim-reasons">${topReasons}</div>` : ''}
                ${divHtml}
            </div>
        `;
    }

    function renderExplorerRowTeamTagsHtml() {
        return '';
    }

    window.PatternSearchV2 = {
        openInNewTab,
        openModal,
        closeModal,
        tagExplorerRows,
        tagSingleExplorerRow,
        toggleFichaTags,
        filterExplorerByTag,
        initExplorerPatternV2Page,
        isExplorerV2Active,
        deactivateExplorerV2,
        matchesActiveExplorerFilter,
        matchesOuNodeFilter,
        matchesActiveOuColumnFilters,
        onOuFilterChange,
        clearOuFilters,
        syncExplorerV2Stats,
        getTargetExplorerCard,
        getExplorerCellHitClass,
        isExplorerFichaScraped,
        renderExplorerOuBadgeHtml,
        renderExplorerPrevOuHtml,
        pauseBackgroundHydration,
        resumeBackgroundHydration,
        applyDeepRescoreSortAndRender,
        runDeepDownloadAndRescore,
        expandTopExplorerFichas,
        renderExplorerRowSimHtml,
        renderExplorerRowTeamTagsHtml,
    };
})();
