/**
 * PATTERN EXPLORER MODAL - Vista tipo /explorador
 * Módulo para explorar patrones históricos con filtros predefinidos
 * Versión 3.0 - Filtro de Hándicap Objetivo para visitante favorito
 */

(function () {
    'use strict';

    // ============ ESTADO DEL MODAL ============
    const ModalState = {
        allResults: [],
        filteredResults: [],
        matchInfo: {},
        multiSelectInstances: {},
        isInitialized: false,
        currentMatchId: null,
        // Auto H2H filter state
        autoH2H: {
            active: false,
            level: null,       // 'EXACT', 'FAMILY', null
            source: null,      // 'general', 'stadium', null
            startRaw: null,
            endRaw: null,
            results: []        // Pre-filtered results by autoFilterH2H
        }
    };

    // ============ CONFIGURACIÓN ============
    const CONFIG = {
        ahOptions: [
            { value: '-2.5', label: '-2.5-' },
            { value: '-2', label: '-2.0' },
            { value: '-1.5', label: '-1.5' },
            { value: '-1', label: '-1.0' },
            { value: '-0.5', label: '-0.5' },
            { value: '0', label: '0' },
            { value: '0.5', label: '+0.5' },
            { value: '1', label: '+1.0' },
            { value: '1.5', label: '+1.5' },
            { value: '2', label: '+2.0' },
            { value: '2.5', label: '+2.5+' }
        ],
        selectors: {
            overlay: 'explorer-modal-overlay',
            loading: 'explorer-modal-loading',
            tableContainer: 'explorer-modal-table-container',
            tbody: 'explorer-modal-tbody',
            matchInfo: 'explorer-modal-match-info',
            filters: {
                ah: 'modal-filter-ah',
                ou: 'modal-filter-ou',
                cover: 'modal-filter-cover',
                prevHomeAh: 'modal-filter-prev-home-ah',
                prevHomeRes: 'modal-filter-prev-home-res',
                prevAwayAh: 'modal-filter-prev-away-ah',
                prevAwayRes: 'modal-filter-prev-away-res',
                h2hStadiumMov: 'modal-filter-h2h-stadium-mov',
                h2hStadiumCover: 'modal-filter-h2h-stadium-cover',
                h2hStadiumStartAh: 'modal-filter-h2h-stadium-start-ah',
                h2hStadiumEndAh: 'modal-filter-h2h-stadium-end-ah',
                h2hGeneralMov: 'modal-filter-h2h-general-mov',

                h2hGeneralCover: 'modal-filter-h2h-general-cover',
                h2hGeneralStartAh: 'modal-filter-h2h-general-start-ah',
                h2hGeneralEndAh: 'modal-filter-h2h-general-end-ah',
                h2hCol3Espejo: 'modal-filter-h2h-col3-espejo',
                h2hCol3Ah: 'modal-filter-h2h-col3-ah',
                h2hCol3Cover: 'modal-filter-h2h-col3-cover',
                indLocalAh: 'modal-filter-ind-local-ah',
                indLocalRes: 'modal-filter-ind-local-res',
                indLocalLoc: 'modal-filter-ind-local-loc',
                indVisitanteAh: 'modal-filter-ind-visitante-ah',
                indVisitanteRes: 'modal-filter-ind-visitante-res',
                indVisitanteLoc: 'modal-filter-ind-visitante-loc'
            }
        }
    };

    // ============ UTILIDADES ============
    const Utils = {
        /**
         * Valida si el visitante cubrió el hándicap según las reglas específicas
         * @param {number} ahLine - Línea de hándicap (-0.25, -0.5, -0.75)
         * @param {string} score - Marcador en formato "H:A"
         * @returns {string} 'COVER', 'HALF_COVER', 'NO_COVER', o null
         */
        validateAwayHandicapCover(ahLine, score) {
            if (!score || !score.includes(':')) return null;

            const parts = score.split(':').map(s => parseInt(s.trim()));
            if (parts.length !== 2 || isNaN(parts[0]) || isNaN(parts[1])) return null;

            const [homeGoals, awayGoals] = parts;
            const goalDiff = awayGoals - homeGoals; // Perspectiva visitante

            const ah = parseFloat(ahLine);
            if (isNaN(ah)) return null;

            // Lógica específica para cada línea
            if (Math.abs(ah + 0.25) < 0.01) {
                // -0.25: Visitante debe GANAR (no empatar)
                return goalDiff > 0 ? 'COVER' : 'NO_COVER';
            }

            if (Math.abs(ah + 0.5) < 0.01) {
                // -0.5: Visitante debe GANAR por cualquier marcador
                return goalDiff > 0 ? 'COVER' : 'NO_COVER';
            }

            if (Math.abs(ah + 0.75) < 0.01) {
                // -0.75: Visitante gana por 2+ = COVER total, por 1 = HALF_COVER (aceptado)
                if (goalDiff >= 2) return 'COVER';
                if (goalDiff === 1) return 'HALF_COVER';
                return 'NO_COVER';
            }

            // Para otras líneas, aplicar lógica estándar
            const adjustedDiff = awayGoals - homeGoals - Math.abs(ah);
            if (adjustedDiff > 0.01) return 'COVER';
            if (adjustedDiff < -0.01) return 'NO_COVER';
            return 'PUSH';
        },

        /**
         * Obtiene datos H2H relevantes con jerarquía: General > Estadio
         */
        getRelevantH2HData(row) {
            // Prioridad A: H2H General
            if (row.h2h_general && row.h2h_general.movement && row.h2h_general.movement !== 'N/A') {
                return { source: 'H2H General', data: row.h2h_general };
            }

            // Prioridad B: H2H Estadio (si General está vacío)
            if (row.h2h_stadium && row.h2h_stadium.movement && row.h2h_stadium.movement !== 'N/A') {
                return { source: 'H2H Estadio', data: row.h2h_stadium };
            }

            return null;
        },

        /**
         * Normaliza un valor de handicap a su bucket
         * 0.25/0.5/0.75 → 0.5, 1.25/1.5/1.75 → 1.5, etc.
         * Enteros (.0) se quedan igual: 1.0 → 1.0, 2.0 → 2.0
         */
        normalizeToHandicapBucket(value) {
            if (value === 0) return 0;
            const sign = value >= 0 ? 1 : -1;
            const absVal = Math.abs(value);
            const intPart = Math.floor(absVal);
            const decPart = absVal - intPart;

            // Enteros se quedan igual
            if (decPart < 0.01) {
                return sign * intPart;
            }
            // Decimales .25, .5, .75 van al .5
            return sign * (intPart + 0.5);
        },

        /**
         * Devuelve la "familia" de un valor de handicap.
         * Familia 0.25: [0.25, 0.5, 0.75]
         * Familia 1.25: [1.25, 1.5, 1.75]
         * PK (0): [0]
         * Enteros (1.0, 2.0): [1.0] o [2.0] (solo ellos mismos)
         */
        getFamily(value) {
            const v = Math.abs(value);
            if (v < 0.01) return [0]; // PK
            const intPart = Math.floor(v);
            const dec = v - intPart;
            if (dec < 0.01) return [value]; // Enteros: solo ellos mismos
            // Familia: .25, .5, .75
            const sign = value >= 0 ? 1 : -1;
            return [sign * (intPart + 0.25), sign * (intPart + 0.5), sign * (intPart + 0.75)];
        },

        /**
         * Parsea el movimiento del H2H y devuelve los valores inicial y final normalizados
         * Formato: "X -> Y" o "X → Y"
         * Retorna: { startRaw, endRaw, startBucket, endBucket } o null
         */
        parseH2HMovement(movementStr) {
            if (!movementStr || movementStr === 'N/A') return null;

            // Normalizar separadores (→ a ->) y espacios
            let normalized = movementStr.replace(/\s+/g, '').replace('→', '->');

            // Manejar el formato europeo: reemplazar comas por puntos para decimales
            // Pero hacerlo de forma segura: solo reemplazar comas que están en contexto numérico
            // Ejemplo: "0,5->-0,5" → "0.5->-0.5"
            normalized = normalized.replace(/,/g, '.');

            const parts = normalized.split('->');
            if (parts.length !== 2) return null;

            const start = parseFloat(parts[0]);
            const end = parseFloat(parts[1]);
            if (isNaN(start) || isNaN(end)) return null;

            return {
                startRaw: start,
                endRaw: end,
                startBucket: this.normalizeToHandicapBucket(start),
                endBucket: this.normalizeToHandicapBucket(end)
            };
        },


        /**
         * Verifica si el movimiento es el patrón clave "-0.5 → 0" (subió de visitante favorito a línea 0)
         * Acepta variaciones: -0.25, -0.5, -0.75 hacia 0
         */
        isKeyMovement(movementStr) {
            const parsed = this.parseH2HMovement(movementStr);
            if (!parsed) return false;
            // El movimiento clave es: empezar en bucket -0.5 y terminar en 0
            return parsed.startBucket === -0.5 && parsed.endBucket === 0;
        },

        /**
         * Obtiene el movimiento H2H relevante (General primero, luego Estadio)
         */
        getRelevantH2HMovement(row) {
            // Prioridad A: H2H General
            if (row.h2h_general && row.h2h_general.movement && row.h2h_general.movement !== 'N/A') {
                return { movement: row.h2h_general.movement, source: 'general' };
            }
            // Prioridad B: H2H Estadio
            if (row.h2h_stadium && row.h2h_stadium.movement && row.h2h_stadium.movement !== 'N/A') {
                return { movement: row.h2h_stadium.movement, source: 'stadium' };
            }
            return null;
        },

        /**
         * Verifica si un handicap coincide con los valores seleccionados
         * Usa sistema de buckets
         */
        checkAhMatch(rowAhStr, selectedValues) {
            if (!selectedValues || selectedValues.length === 0) return true;
            const rowAh = parseFloat(rowAhStr);
            if (isNaN(rowAh)) return false;

            return selectedValues.some(valStr => {
                const fAh = parseFloat(valStr);
                const absFAh = Math.abs(fAh);

                // Bucket 2.5+ (incluye 2.25 y superiores)
                if (absFAh >= 2.49) {
                    if (fAh > 0) return rowAh >= 2.24;
                    else return rowAh <= -2.24;
                }

                // Bucket 2.0
                if (Math.abs(absFAh - 2.0) < 0.1) {
                    if (fAh > 0) return Math.abs(rowAh - 2) < 0.1;
                    else return Math.abs(rowAh + 2) < 0.1;
                }

                // Bucket 1.5
                if (Math.abs(absFAh - 1.5) < 0.1) {
                    if (fAh > 0) return rowAh >= 1.24 && rowAh <= 1.76;
                    else return rowAh <= -1.24 && rowAh >= -1.76;
                }

                // Bucket 1.0
                if (Math.abs(absFAh - 1.0) < 0.1) {
                    return Math.abs(rowAh - fAh) < 0.1;
                }

                // Bucket 0.5
                if (Math.abs(absFAh - 0.5) < 0.1) {
                    if (fAh > 0) return rowAh >= 0.24 && rowAh <= 0.76;
                    else return rowAh <= -0.24 && rowAh >= -0.76;
                }

                // Bucket 0
                if (absFAh < 0.1) {
                    return Math.abs(rowAh) < 0.1;
                }

                return Math.abs(rowAh - fAh) < 0.01;
            });
        },

        /**
         * Calcula WDL desde un score
         */
        calcWDL(score, isHome) {
            if (!score) return null;
            const parts = (score || '').replace(' ', '').replace('-', ':').split(':');
            if (parts.length !== 2) return null;
            const hg = parseInt(parts[0]);
            const ag = parseInt(parts[1]);
            if (isNaN(hg) || isNaN(ag)) return null;
            const diff = isHome ? (hg - ag) : (ag - hg);
            if (diff > 0) return 'W';
            if (diff < 0) return 'L';
            return 'D';
        },

        formatValue(val) {
            return val !== null && val !== undefined && val !== '' ? val : '-';
        },

        isValidValue(val) {
            return val !== undefined && val !== null && val !== 'undefined' && val !== '';
        },

        /**
         * Calcula si el favorito cubrió el hándicap
         * @param {string} scoreStr - Marcador "H:A"
         * @param {number} currentAh - Hándicap actual (positivo = local favorito, negativo/0 = visitante favorito)
         * @param {boolean} isInverted - Si es partido invertido (H2H General)
         * @returns {string} 'COVER', 'NO_COVER', 'PUSH'
         */
        calculateH2HResult(scoreStr, currentAh, isInverted = false) {
            if (!scoreStr || currentAh === null || currentAh === undefined) return null;
            const parts = scoreStr.replace('-', ':').split(':');
            if (parts.length !== 2) return null;
            const h = parseInt(parts[0]);
            const a = parseInt(parts[1]);

            if (isNaN(h) || isNaN(a) || isNaN(currentAh)) return null;

            // Regla: AH > 0 = LOCAL FAVORITO. AH <= 0 = VISITANTE FAVORITO
            const favIsLocal = currentAh > 0;
            const absAh = Math.abs(currentAh);

            let diff; // Diferencia goles desde la perspectiva del favorito
            if (isInverted) {
                diff = favIsLocal ? (a - h) : (h - a);
            } else {
                diff = favIsLocal ? (h - a) : (a - h);
            }

            // Aplicar handicap al favorito
            const finalDiff = diff - absAh;

            if (finalDiff > 0) return 'COVER';
            if (finalDiff < 0) return 'NO_COVER';
            return 'PUSH';
        }
    };

    // ============ RENDERIZADO ============
    const Renderer = {
        renderTable(results) {
            const tbody = document.getElementById(CONFIG.selectors.tbody);
            if (!tbody) return;

            if (!results || results.length === 0) {
                tbody.innerHTML = '<tr><td colspan="14" class="text-center py-4">No se encontraron patrones con los filtros actuales</td></tr>';
                return;
            }

            let html = '';
            results.forEach((r, idx) => {
                html += this.renderRow(r, idx);
                html += this.renderStatsRow(r, idx);
            });

            tbody.innerHTML = html;
        },

        renderRow(r, idx) {
            // Acceso a datos estructurados de la API (igual que explorer.html)
            const c = r.candidate || {};

            // Nombres de equipos - intentar múltiples campos
            const homeTeam = c.home || c.home_team || '';
            const awayTeam = c.away || c.away_team || '';
            const ahReal = c.ah_real;
            const score = c.score || '';
            const date = c.date || '';

            // Calcular cover usando la misma lógica que explorer.html
            const currentAh = parseFloat(ahReal);
            let coverStatus = 'UNKNOWN';
            if (score && score !== '?:?' && !isNaN(currentAh)) {
                coverStatus = Utils.calculateH2HResult(score, currentAh) || 'UNKNOWN';
            }

            // Badge de cover
            let coverBadge = '<span class="badge bg-secondary">-</span>';
            let ftColor = 'text-dark';
            if (coverStatus === 'COVER') {
                coverBadge = '<span class="badge bg-success">✓</span>';
                ftColor = 'text-success';
            } else if (coverStatus === 'NO_COVER') {
                coverBadge = '<span class="badge bg-danger">✗</span>';
                ftColor = 'text-danger';
            } else if (coverStatus === 'PUSH') {
                coverBadge = '<span class="badge bg-warning">P</span>';
                ftColor = 'text-warning';
            }

            const rowClass = coverStatus === 'COVER' ? 'row-cover' :
                (coverStatus === 'NO_COVER' ? 'row-nocover' : 'row-push');

            // H2H data
            const h2hStadium = r.h2h_stadium;
            const h2hGeneral = r.h2h_general;

            // Obtener movimiento con jerarquía General > Estadio para el filtro KEY
            const h2hData = Utils.getRelevantH2HMovement(r);
            const h2hMovement = h2hData ? h2hData.movement : '';
            const h2hSource = h2hData ? h2hData.source : '';

            return `
                <tr class="${rowClass}" data-row-id="${idx}"
                    data-ah="${ahReal || ''}"
                    data-covered="${coverStatus}"
                    data-h2h-movement="${h2hMovement}"
                    data-h2h-source="${h2hSource}"
                    data-h2h-stadium-mov="${h2hStadium?.mov_direction || ''}"
                    data-h2h-stadium-cover="${h2hStadium?.real_wdl || ''}"
                    data-h2h-general-mov="${h2hGeneral?.mov_direction || ''}"
                    data-h2h-general-cover="${h2hGeneral?.real_wdl || ''}"
                >
                    <td>${Utils.formatValue(date)}</td>
                    <td class="text-primary fw-bold">${Utils.formatValue(homeTeam)}</td>
                    <td class="text-danger fw-bold">${Utils.formatValue(awayTeam)}</td>
                    <td class="text-center fw-bold" style="background:#e8f5e9;">${Utils.formatValue(ahReal)}</td>
                    <td class="text-center ${ftColor} fw-bold">${Utils.formatValue(score)} ${coverBadge}</td>
                    <td style="background:#e8f5e9;">${this.renderPrevData(r.prev_home, true, c)}</td>
                    <td style="background:#ffebee;">${this.renderPrevData(r.prev_away, false, c)}</td>
                    <td style="background:#e3f2fd;" class="text-center">${this.renderH2HData(h2hStadium, currentAh, false)}</td>
                    <td style="background:#e3f2fd;" class="text-center">${this.renderH2HData(h2hGeneral, currentAh, true)}</td>
                    <td style="background:#ffe4ec;" class="text-center">${this.renderH2HCol3Data(r.h2h_col3)}</td>
                    <td style="background:#fff3e0;" class="text-center">${this.renderIndData(r.ind_local)}</td>
                    <td style="background:#fff3e0;" class="text-center">${this.renderIndData(r.ind_visitante)}</td>
                    <td class="text-center">
                        <button class="btn btn-outline-info btn-sm py-0 px-1" onclick="window.PatternExplorerModal.toggleStatsRow(${idx})" title="Ver estadísticas">
                            <i class="fa-solid fa-chart-bar"></i>
                        </button>
                    </td>
                </tr>
            `;
        },

        renderPrevData(prev, isHome, candidate) {
            if (!prev || !prev.score) return '<span class="text-muted">-</span>';

            const wdl = Utils.calcWDL(prev.score, isHome);
            const wdlClass = wdl === 'W' ? 'text-success' : (wdl === 'L' ? 'text-danger' : 'text-warning');

            // Obtener nombres de equipos del partido previo
            let rHome = prev.home_team || '';
            let rAway = prev.away_team || '';

            // Fallback si no hay equipos
            if (!rHome && !rAway && prev.rival) {
                const parts = (prev.rival || '').split(' vs ');
                if (parts.length > 1) {
                    rHome = parts[0];
                    rAway = parts[1];
                } else {
                    rHome = isHome ? (candidate?.home || candidate?.home_team || '') : (prev.rival || '');
                    rAway = isHome ? (prev.rival || '') : (candidate?.away || candidate?.away_team || '');
                }
            }

            return `
                <div class="d-flex flex-column" style="font-size: 0.75rem;">
                    ${prev.date ? `<div class="text-muted" style="font-size: 0.55rem;">${prev.date}</div>` : ''}
                    <div class="d-flex justify-content-between">
                        <span class="fw-bold small">${Utils.formatValue(prev.ah)}</span>
                        <span class="${wdlClass} fw-bold">${prev.score}</span>
                    </div>
                    ${(rHome || rAway) ? `
                        <div class="small" style="font-size: 0.65rem; line-height: 1.1;">
                            <span class="text-primary">${rHome}</span> <span class="text-muted" style="font-size:0.6rem">vs</span> <span class="text-danger">${rAway}</span>
                        </div>
                    ` : ''}
                </div>
            `;
        },

        renderH2HData(h2h, currentAh, isInverted = false) {
            if (!h2h) return '<span class="text-muted">-</span>';
            if (!h2h.movement && !h2h.score) return '<span class="text-muted">N/A</span>';

            // Calcular cover igual que explorer.html
            let coverStatus = null;
            let coverColor = 'text-dark';
            if (h2h.score && h2h.score !== '?:?' && h2h.score !== 'N/A' && !isNaN(currentAh)) {
                coverStatus = Utils.calculateH2HResult(h2h.score, currentAh, isInverted);
                coverColor = coverStatus === 'COVER' ? 'text-success' :
                    (coverStatus === 'NO_COVER' ? 'text-danger' : 'text-warning');
            }

            return `
                <div class="d-flex flex-column" style="font-size: 0.75rem;">
                    ${h2h.date ? `<div class="text-muted" style="font-size: 0.55rem;">${h2h.date}</div>` : ''}
                    <div class="fw-bold small ${coverColor}">${h2h.movement || 'N/A'}</div>
                    ${(h2h.score && h2h.score !== 'N/A' && h2h.score !== '?:?') ? `<div class="small ${coverColor}">${h2h.score}</div>` : ''}
                </div>
            `;
        },

        renderH2HCol3Data(col3) {
            if (!col3 || !col3.score) return '<span class="text-muted">-</span>';

            const statusBadge = col3.cover_status === 'MEJORA' ? '<span class="badge bg-success">MEJORA</span>' :
                (col3.cover_status === 'EMPEORA' ? '<span class="badge bg-danger">EMPEORA</span>' :
                    (col3.cover_status === 'IGUALA' ? '<span class="badge bg-warning">IGUALA</span>' : '<span class="badge bg-info">IGUALA+</span>'));

            return `
                <div style="font-size: 0.75rem;">
                    ${statusBadge}
                    ${col3.date ? `<div class="text-muted" style="font-size: 0.65rem; margin-top: 2px;">${col3.date}</div>` : ''}
                    ${col3.ah ? `<div class="fw-bold" style="margin-top: 2px;">${col3.ah}</div>` : ''}
                    <div class="fw-bold" style="margin-top: 2px;">${col3.score || '-'}</div>
                    ${col3.home_team ? `<div class="text-primary" style="font-size: 0.7rem;">${col3.home_team}</div>` : ''}
                    ${col3.away_team ? `<div class="text-danger" style="font-size: 0.7rem;">${col3.away_team}</div>` : ''}
                    <div style="margin-top: 2px;">${col3.espejo || '-'}</div>
                </div>
            `;
        },

        renderIndData(ind) {
            if (!ind || !ind.score) return '<span class="text-muted">-</span>';

            const coverBadge = ind.cover_status === 'COVER' ? '<span class="badge bg-success">✓</span>' :
                (ind.cover_status === 'NO_COVER' ? '<span class="badge bg-danger">✗</span>' : '<span class="badge bg-warning">P</span>');

            const locBadge = ind.locality === 'H' ? '<span class="badge bg-primary">H</span>' :
                (ind.locality === 'A' ? '<span class="badge bg-danger">A</span>' : '');

            return `
                <div style="font-size: 0.75rem;">
                    ${ind.date ? `<div class="text-muted" style="font-size: 0.65rem;">${ind.date}</div>` : ''}
                    <div class="fw-bold" style="margin-top: 2px;">${ind.ah || '-'}</div>
                    <div class="fw-bold" style="margin-top: 2px;">${ind.score || '-'}</div>
                    ${ind.home_team ? `<div class="text-primary" style="font-size: 0.7rem;">${ind.home_team}</div>` : ''}
                    ${ind.away_team ? `<div class="text-danger" style="font-size: 0.7rem;">${ind.away_team}</div>` : ''}
                    <div style="margin-top: 4px;">${coverBadge} ${locBadge}</div>
                </div>
            `;
        },

        renderStatsRow(r, idx) {
            return `
                <tr id="modal-stats-row-${idx}" class="d-none" style="background:#f8f9fa;">
                    <td colspan="13" style="padding:12px;">
                        <div class="d-flex gap-2 flex-wrap">
                            ${this.renderStatBox('Prev Home', r.prev_home, '#e8f5e9', 'text-success')}
                            ${this.renderStatBox('Prev Away', r.prev_away, '#ffebee', 'text-danger')}
                            ${this.renderH2HStatBox('H2H General', r.h2h_general, '#e3f2fd', 'text-primary')}
                            ${this.renderH2HStatBox('H2H Estadio', r.h2h_stadium, '#e3f2fd', 'text-primary')}
                            ${this.renderStatBox('H2H Col3', r.h2h_col3, '#fff8e1', 'text-warning')}
                            ${this.renderStatBox('Ind. Local', r.ind_local, '#e3f2fd', 'text-info')}
                            ${this.renderStatBox('Ind. Visitante', r.ind_visitante, '#e3f2fd', 'text-info')}
                        </div>
                    </td>
                </tr>
            `;
        },

        renderStatBox(title, data, bgColor, titleColor) {
            if (!data) {
                return `
                    <div style="flex:1;min-width:180px;background:${bgColor};border:1px solid rgba(0,0,0,0.15);border-radius:8px;padding:12px;min-height:100px;display:flex;flex-direction:column;justify-content:center;align-items:center;opacity:0.6;">
                        <div class="fw-bold ${titleColor} text-uppercase" style="font-size:0.75rem;margin-bottom:8px;">${title}</div>
                        <span class="text-muted" style="font-size:1rem;">-</span>
                    </div>
                `;
            }

            return `
                <div style="flex:1;min-width:180px;background:${bgColor};border:1px solid rgba(0,0,0,0.15);border-radius:8px;padding:12px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">
                    <div class="fw-bold ${titleColor} text-uppercase text-center" style="font-size:0.8rem;letter-spacing:0.5px;border-bottom:2px solid rgba(0,0,0,0.1);padding-bottom:6px;margin-bottom:8px;">${title}</div>
                    
                    ${data.home_team || data.away_team ? `
                        <div style="font-size:0.75rem;margin-bottom:6px;text-align:center;">
                            <span class="text-success fw-bold">${data.home_team || ''}</span>
                            <span class="fw-bold text-dark mx-2" style="font-size:0.9rem;background:rgba(255,255,255,0.5);padding:2px 6px;border-radius:4px;">${data.score || ''}</span>
                            <span class="text-danger fw-bold">${data.away_team || ''}</span>
                        </div>
                    ` : ''}
                    
                    ${data.date ? `<div class="text-muted text-center" style="font-size:0.65rem;margin-bottom:4px;">${data.date}</div>` : ''}
                    ${data.ah ? `<div class="text-center" style="font-size:0.7rem;margin-bottom:4px;">AH: <span class="fw-bold text-dark">${data.ah}</span></div>` : ''}

                    ${data.stats_rows && data.stats_rows.length > 0 ? `
                        <div style="margin-top:8px;border-top:1px solid rgba(0,0,0,0.1);padding-top:4px;">
                            ${data.stats_rows.map(stat => {
                // Determinar dominio (negrita para el mayor)
                const hVal = parseFloat(stat.home) || 0;
                const aVal = parseFloat(stat.away) || 0;
                const hClass = hVal > aVal ? 'fw-bold text-dark' : 'text-muted';
                const aClass = aVal > hVal ? 'fw-bold text-dark' : 'text-muted';

                return `
                                    <div class="d-flex justify-content-between align-items-center" style="font-size:0.7rem;margin-bottom:2px;">
                                        <span class="${hClass}">${stat.home}</span>
                                        <span class="text-muted" style="font-size:0.65rem;">${stat.label}</span>
                                        <span class="${aClass}">${stat.away}</span>
                                    </div>
                                `;
            }).join('')}
                        </div>
                    ` : ''}
                </div>
            `;
        },

        renderH2HStatBox(title, data, bgColor, titleColor) {
            if (!data) return this.renderStatBox(title, data, bgColor, titleColor);

            return `
                <div style="flex:1;min-width:180px;background:${bgColor};border:1px solid rgba(0,0,0,0.15);border-radius:8px;padding:12px;box-shadow:0 2px 4px rgba(0,0,0,0.1);">
                    <div class="fw-bold ${titleColor} text-uppercase text-center" style="font-size:0.8rem;letter-spacing:0.5px;border-bottom:2px solid rgba(0,0,0,0.1);padding-bottom:6px;margin-bottom:8px;">${title}</div>
                    ${data.movement ? `<div class="text-center fw-bold text-dark" style="font-size:0.85rem;margin-bottom:6px;">Mov: ${data.movement}</div>` : ''}
                    ${data.home_team || data.away_team ? `
                        <div style="font-size:0.75rem;margin-bottom:6px;text-align:center;">
                            <span class="text-primary fw-bold">${data.home_team || 'L'}</span>
                            <span class="fw-bold text-dark mx-2" style="font-size:0.9rem;background:rgba(255,255,255,0.5);padding:2px 6px;border-radius:4px;">${data.score || ''}</span>
                            <span class="text-danger fw-bold">${data.away_team || 'V'}</span>
                        </div>
                    ` : (data.score ? `<div class="text-center fw-bold" style="margin-bottom:6px;font-size:1.1rem;">${data.score}</div>` : '')}
                    ${data.date ? `<div class="text-muted text-center" style="font-size:0.65rem;margin-bottom:4px;">${data.date}</div>` : ''}

                    ${data.stats_rows && data.stats_rows.length > 0 ? `
                        <div style="margin-top:8px;border-top:1px solid rgba(0,0,0,0.1);padding-top:4px;">
                            ${data.stats_rows.map(stat => {
                // Determinar dominio (negrita para el mayor)
                const hVal = parseFloat(stat.home) || 0;
                const aVal = parseFloat(stat.away) || 0;
                const hClass = hVal > aVal ? 'fw-bold text-dark' : 'text-muted';
                const aClass = aVal > hVal ? 'fw-bold text-dark' : 'text-muted';

                return `
                                    <div class="d-flex justify-content-between align-items-center" style="font-size:0.7rem;margin-bottom:2px;">
                                        <span class="${hClass}">${stat.home}</span>
                                        <span class="text-muted" style="font-size:0.65rem;">${stat.label}</span>
                                        <span class="${aClass}">${stat.away}</span>
                                    </div>
                                `;
            }).join('')}
                        </div>
                    ` : ''}
                </div>
            `;
        },

        updateStats(results) {
            const total = results.length;

            // Calcular cover igual que en renderRow
            const getCoverStatus = (r) => {
                const c = r.candidate || {};
                const score = c.score;
                const currentAh = parseFloat(c.ah_real);
                if (score && score !== '?:?' && !isNaN(currentAh)) {
                    return Utils.calculateH2HResult(score, currentAh) || 'UNKNOWN';
                }
                return 'UNKNOWN';
            };

            const cover = results.filter(r => getCoverStatus(r) === 'COVER').length;
            const noCover = results.filter(r => getCoverStatus(r) === 'NO_COVER').length;
            const push = results.filter(r => getCoverStatus(r) === 'PUSH').length;
            const coverPct = total > 0 ? Math.round((cover / total) * 100) : 0;
            const noCoverPct = total > 0 ? Math.round((noCover / total) * 100) : 0;

            const updateEl = (id, value) => {
                const el = document.getElementById(id);
                if (el) el.textContent = value;
            };

            updateEl('modal-stat-total', total);
            updateEl('modal-stat-cover', cover);
            updateEl('modal-stat-cover-pct', coverPct);
            updateEl('modal-stat-nocover', noCover);
            updateEl('modal-stat-nocover-pct', noCoverPct);
        }
    };

    // ============ FILTROS ============
    const Filters = {
        /**
         * SISTEMA AUTOMÁTICO DE PATRONES H2H
         * Filtra resultados usando cascada: Exacto → Familia → Fallback Estadio
         * SOLO usa H2H General o H2H Estadio (nunca Prev, Ind, etc.)
         * Los valores de Ini/Fin son los RAW del partido actual (no buckets)
         */
        autoFilterH2H() {
            const matchId = ModalState.currentMatchId;
            if (!matchId) return;

            // Reset auto state
            ModalState.autoH2H = { active: false, level: null, source: null, startRaw: null, endRaw: null, results: [] };

            const precacheoData = window.precacheoData || {};
            const matchData = precacheoData[matchId];
            if (!matchData) {
                console.log('[AutoH2H] No se encontró matchData');
                return;
            }

            const h2hGeneral = matchData.market_analysis_data?.general;
            const h2hStadium = matchData.market_analysis_data?.stadium;

            // Determinar fuentes disponibles en orden de prioridad
            const sources = [];
            if (h2hGeneral && h2hGeneral.movement && h2hGeneral.movement !== 'N/A') {
                sources.push({ key: 'general', label: 'H2H General', data: h2hGeneral, isInverted: true });
            }
            if (h2hStadium && h2hStadium.movement && h2hStadium.movement !== 'N/A') {
                sources.push({ key: 'stadium', label: 'H2H Estadio', data: h2hStadium, isInverted: false });
            }

            if (sources.length === 0) {
                console.log('[AutoH2H] No hay movimiento H2H disponible');
                return;
            }

            const allData = ModalState.allResults;
            const MIN_RESULTS = 3; // Mínimo para considerar "suficientes"

            // --- CASCADA: Para cada fuente, intentar Exacto → Familia ---
            for (const src of sources) {
                const parsed = Utils.parseH2HMovement(src.data.movement);
                if (!parsed) continue;

                const startRaw = parsed.startRaw;
                const endRaw = parsed.endRaw;

                console.log(`[AutoH2H] Intentando ${src.label}: ${startRaw} → ${endRaw}`);

                // NIVEL 1: EXACTO (startRaw y endRaw deben coincidir exactamente)
                const exactResults = this._filterByH2HMovement(allData, startRaw, endRaw, src.key, false);
                console.log(`[AutoH2H] ${src.label} EXACTO: ${exactResults.length} resultados`);

                if (exactResults.length >= MIN_RESULTS) {
                    ModalState.autoH2H = {
                        active: true, level: 'EXACT', source: src.key,
                        startRaw, endRaw, results: exactResults
                    };
                    console.log(`[AutoH2H] ✅ Usando ${src.label} EXACTO (${exactResults.length} partidos)`);
                    return;
                }

                // NIVEL 2: FAMILIA (Ini y Fin dentro de su familia)
                const familyResults = this._filterByH2HMovement(allData, startRaw, endRaw, src.key, true);
                console.log(`[AutoH2H] ${src.label} FAMILIA: ${familyResults.length} resultados`);

                if (familyResults.length >= 1) {
                    ModalState.autoH2H = {
                        active: true, level: 'FAMILY', source: src.key,
                        startRaw, endRaw, results: familyResults
                    };
                    console.log(`[AutoH2H] ✅ Usando ${src.label} FAMILIA (${familyResults.length} partidos)`);
                    return;
                }

                // Si esta fuente (General) no tiene resultados, pasar a la siguiente (Estadio)
                console.log(`[AutoH2H] ${src.label} sin resultados suficientes, probando siguiente fuente...`);
            }

            console.log('[AutoH2H] No se encontraron resultados en ninguna fuente/nivel');
        },

        /**
         * Filtra resultados por movimiento H2H de una fuente específica.
         * @param {Array} data - Todos los resultados
         * @param {number} targetStart - Valor Ini RAW del partido actual
         * @param {number} targetEnd - Valor Fin RAW del partido actual
         * @param {string} sourceKey - 'general' o 'stadium'
         * @param {boolean} useFamily - Si true, usa familia para comparar
         */
        _filterByH2HMovement(data, targetStart, targetEnd, sourceKey, useFamily) {
            const startFamily = useFamily ? Utils.getFamily(targetStart) : [targetStart];
            const endFamily = useFamily ? Utils.getFamily(targetEnd) : [targetEnd];

            return data.filter(r => {
                // Obtener H2H de la fuente correcta (general o stadium)
                let h2h = null;
                if (sourceKey === 'general') {
                    h2h = r.h2h_general;
                    // Fallback a stadium si general no tiene datos
                    if (!h2h || !h2h.movement || h2h.movement === 'N/A') return false;
                } else {
                    h2h = r.h2h_stadium;
                    if (!h2h || !h2h.movement || h2h.movement === 'N/A') return false;
                }

                const parsed = Utils.parseH2HMovement(h2h.movement);
                if (!parsed) return false;

                // Comparar: el Ini del candidato debe estar en la familia del Ini target
                const startMatch = startFamily.some(v => Math.abs(parsed.startRaw - v) < 0.01);
                if (!startMatch) return false;

                // Comparar: el Fin del candidato debe estar en la familia del Fin target
                const endMatch = endFamily.some(v => Math.abs(parsed.endRaw - v) < 0.01);
                return endMatch;
            });
        },


        /**
         * Aplica todos los filtros
         */
        applyAllFilters() {
            // Si autoFilterH2H está activo, partir de sus resultados pre-filtrados
            // Si no, partir de todos los resultados
            let results = ModalState.autoH2H.active
                ? [...ModalState.autoH2H.results]
                : [...ModalState.allResults];

            // Filtro AH Principal - usar candidate.ah_real
            const ahValues = ModalState.multiSelectInstances.ah?.getValues() || [];
            if (ahValues.length > 0) {
                results = results.filter(r => {
                    const ah = r.candidate?.ah_real;
                    return Utils.checkAhMatch(ah, ahValues);
                });
            }

            // Filtro O/U - usar candidate.ou_line
            const ouFilter = document.getElementById(CONFIG.selectors.filters.ou)?.value;
            if (ouFilter) {
                const filterOU = parseFloat(ouFilter);
                results = results.filter(r => {
                    const ouLine = parseFloat(r.candidate?.ou_line);
                    if (isNaN(ouLine)) return false;
                    if (filterOU >= 4.0) return ouLine >= 4.0;
                    else return Math.abs(ouLine - filterOU) <= 0.26;
                });
            }

            // Filtro Cover Final - calcular usando calculateH2HResult
            const coverFilter = document.getElementById(CONFIG.selectors.filters.cover)?.value;
            if (coverFilter) {
                results = results.filter(r => {
                    const c = r.candidate || {};
                    const score = c.score;
                    const currentAh = parseFloat(c.ah_real);
                    if (score && score !== '?:?' && !isNaN(currentAh)) {
                        const coverResult = Utils.calculateH2HResult(score, currentAh);
                        return coverResult === coverFilter;
                    }
                    return false;
                });
            }

            // Filtros Prev Home
            const prevHomeAhValues = ModalState.multiSelectInstances.prevHomeAh?.getValues() || [];
            if (prevHomeAhValues.length > 0) {
                results = results.filter(r => r.prev_home && Utils.checkAhMatch(r.prev_home.ah, prevHomeAhValues));
            }

            const prevHomeRes = document.getElementById(CONFIG.selectors.filters.prevHomeRes)?.value;
            if (prevHomeRes) {
                results = results.filter(r => {
                    if (!r.prev_home || !r.prev_home.score) return false;
                    const wdl = Utils.calcWDL(r.prev_home.score, true);
                    return wdl === prevHomeRes;
                });
            }

            // Filtros Prev Away
            const prevAwayAhValues = ModalState.multiSelectInstances.prevAwayAh?.getValues() || [];
            if (prevAwayAhValues.length > 0) {
                results = results.filter(r => r.prev_away && Utils.checkAhMatch(r.prev_away.ah, prevAwayAhValues));
            }

            const prevAwayRes = document.getElementById(CONFIG.selectors.filters.prevAwayRes)?.value;
            if (prevAwayRes) {
                results = results.filter(r => {
                    if (!r.prev_away || !r.prev_away.score) return false;
                    const wdl = Utils.calcWDL(r.prev_away.score, false);
                    return wdl === prevAwayRes;
                });
            }

            // Filtros H2H Stadium - usar mov_direction y real_wdl
            const h2hStadiumMov = document.getElementById(CONFIG.selectors.filters.h2hStadiumMov)?.value;
            if (h2hStadiumMov) {
                results = results.filter(r => r.h2h_stadium?.mov_direction === h2hStadiumMov);
            }

            const h2hStadiumCover = document.getElementById(CONFIG.selectors.filters.h2hStadiumCover)?.value;
            if (h2hStadiumCover) {
                results = results.filter(r => r.h2h_stadium?.real_wdl === h2hStadiumCover);
            }

            // Filtros H2H Estadio por AH Inicial y AH Final
            const h2hStadiumStartValues = ModalState.multiSelectInstances.h2hStadiumStartAh?.getValues() || [];
            const h2hStadiumEndValues = ModalState.multiSelectInstances.h2hStadiumEndAh?.getValues() || [];

            if (h2hStadiumStartValues.length > 0 || h2hStadiumEndValues.length > 0) {
                results = results.filter(r => {
                    const h2h = r.h2h_stadium;
                    if (!h2h || !h2h.movement || h2h.movement === 'N/A') return false;

                    const parsed = Utils.parseH2HMovement(h2h.movement);
                    if (!parsed) return false;

                    // Verificar AH Inicial (bucket)
                    if (h2hStadiumStartValues.length > 0) {
                        const startMatch = h2hStadiumStartValues.some(v => {
                            const targetBucket = Utils.normalizeToHandicapBucket(parseFloat(v));
                            return parsed.startBucket === targetBucket;
                        });
                        if (!startMatch) return false;
                    }

                    // Verificar AH Final (bucket)
                    if (h2hStadiumEndValues.length > 0) {
                        const endMatch = h2hStadiumEndValues.some(v => {
                            const targetBucket = Utils.normalizeToHandicapBucket(parseFloat(v));
                            return parsed.endBucket === targetBucket;
                        });
                        if (!endMatch) return false;
                    }

                    return true;
                });
            }

            // Filtros H2H General - USAR JERARQUÍA: General primero, luego Estadio
            const h2hGeneralMov = document.getElementById(CONFIG.selectors.filters.h2hGeneralMov)?.value;
            if (h2hGeneralMov) {
                if (h2hGeneralMov === 'KEY') {
                    // Filtro especial: movimiento clave "-0.5 → 0" con jerarquía General > Estadio
                    results = results.filter(r => {
                        const h2hData = Utils.getRelevantH2HMovement(r);
                        return h2hData && Utils.isKeyMovement(h2hData.movement);
                    });
                } else {
                    // Filtro normal: usar jerarquía General > Estadio
                    results = results.filter(r => {
                        // Primero buscar en General
                        if (r.h2h_general && r.h2h_general.mov_direction) {
                            return r.h2h_general.mov_direction === h2hGeneralMov;
                        }
                        // Si no hay General, buscar en Estadio
                        if (r.h2h_stadium && r.h2h_stadium.mov_direction) {
                            return r.h2h_stadium.mov_direction === h2hGeneralMov;
                        }
                        return false;
                    });
                }
            }

            const h2hGeneralCover = document.getElementById(CONFIG.selectors.filters.h2hGeneralCover)?.value;
            if (h2hGeneralCover) {
                // Usar jerarquía General > Estadio para cover también
                results = results.filter(r => {
                    // Primero buscar en General
                    if (r.h2h_general && r.h2h_general.real_wdl) {
                        return r.h2h_general.real_wdl === h2hGeneralCover;
                    }
                    // Si no hay General, buscar en Estadio
                    if (r.h2h_stadium && r.h2h_stadium.real_wdl) {
                        return r.h2h_stadium.real_wdl === h2hGeneralCover;
                    }
                    return false;
                });
            }

            // Filtros H2H General por AH Inicial y AH Final
            const h2hStartValues = ModalState.multiSelectInstances.h2hGeneralStartAh?.getValues() || [];
            const h2hEndValues = ModalState.multiSelectInstances.h2hGeneralEndAh?.getValues() || [];

            if (h2hStartValues.length > 0 || h2hEndValues.length > 0) {
                results = results.filter(r => {
                    // Usar h2h_general primero, luego h2h_stadium
                    let h2h = r.h2h_general;
                    if (!h2h || !h2h.movement || h2h.movement === 'N/A') {
                        h2h = r.h2h_stadium;
                    }
                    if (!h2h || !h2h.movement || h2h.movement === 'N/A') return false;

                    const parsed = Utils.parseH2HMovement(h2h.movement);
                    if (!parsed) return false;

                    // Verificar AH Inicial (bucket)
                    if (h2hStartValues.length > 0) {
                        const startMatch = h2hStartValues.some(v => {
                            const targetBucket = Utils.normalizeToHandicapBucket(parseFloat(v));
                            return parsed.startBucket === targetBucket;
                        });
                        if (!startMatch) return false;
                    }

                    // Verificar AH Final (bucket)
                    if (h2hEndValues.length > 0) {
                        const endMatch = h2hEndValues.some(v => {
                            const targetBucket = Utils.normalizeToHandicapBucket(parseFloat(v));
                            return parsed.endBucket === targetBucket;
                        });
                        if (!endMatch) return false;
                    }

                    return true;
                });
            }

            // Filtros H2H Col3 - Espejo
            const h2hCol3Espejo = document.getElementById(CONFIG.selectors.filters.h2hCol3Espejo)?.value;
            if (h2hCol3Espejo) {
                results = results.filter(r => r.h2h_col3?.espejo === h2hCol3Espejo);
            }

            const h2hCol3AhValues = ModalState.multiSelectInstances.h2hCol3Ah?.getValues() || [];
            if (h2hCol3AhValues.length > 0) {
                results = results.filter(r => r.h2h_col3 && Utils.checkAhMatch(r.h2h_col3.ah, h2hCol3AhValues));
            }

            const h2hCol3Cover = document.getElementById(CONFIG.selectors.filters.h2hCol3Cover)?.value;
            if (h2hCol3Cover) {
                results = results.filter(r => r.h2h_col3?.cover_status === h2hCol3Cover);
            }

            // Filtros Indirectos Local
            const indLocalAhValues = ModalState.multiSelectInstances.indLocalAh?.getValues() || [];
            if (indLocalAhValues.length > 0) {
                results = results.filter(r => r.ind_local && Utils.checkAhMatch(r.ind_local.ah, indLocalAhValues));
            }

            const indLocalRes = document.getElementById(CONFIG.selectors.filters.indLocalRes)?.value;
            if (indLocalRes) {
                results = results.filter(r => r.ind_local?.cover_status === indLocalRes);
            }

            const indLocalLoc = document.getElementById(CONFIG.selectors.filters.indLocalLoc)?.value;
            if (indLocalLoc) {
                results = results.filter(r => {
                    if (!r.ind_local || !r.ind_local.localia) return false;
                    const loc = r.ind_local.localia;
                    const normLoc = (loc === 'L' || loc === 'H' || loc === 'Local' || loc === 'Home') ? 'H' : 'A';
                    return normLoc === indLocalLoc;
                });
            }

            // Filtros Indirectos Visitante
            const indVisitanteAhValues = ModalState.multiSelectInstances.indVisitanteAh?.getValues() || [];
            if (indVisitanteAhValues.length > 0) {
                results = results.filter(r => r.ind_visitante && Utils.checkAhMatch(r.ind_visitante.ah, indVisitanteAhValues));
            }

            const indVisitanteRes = document.getElementById(CONFIG.selectors.filters.indVisitanteRes)?.value;
            if (indVisitanteRes) {
                results = results.filter(r => r.ind_visitante?.cover_status === indVisitanteRes);
            }

            const indVisitanteLoc = document.getElementById(CONFIG.selectors.filters.indVisitanteLoc)?.value;
            if (indVisitanteLoc) {
                results = results.filter(r => {
                    if (!r.ind_visitante || !r.ind_visitante.localia) return false;
                    const loc = r.ind_visitante.localia;
                    const normLoc = (loc === 'L' || loc === 'H' || loc === 'Local' || loc === 'Home') ? 'H' : 'A';
                    return normLoc === indVisitanteLoc;
                });
            }

            ModalState.filteredResults = results;
            Renderer.renderTable(results);
            Renderer.updateStats(results);
        },

        /**
         * Limpia todos los filtros
         */
        clearAllFilters() {
            // Limpiar MultiSelects
            Object.values(ModalState.multiSelectInstances).forEach(ms => {
                if (ms && ms.selectedValues) {
                    ms.selectedValues = [];
                    ms.updateBtnText();
                    if (ms.renderOptions) ms.renderOptions();
                }
            });

            // Limpiar selects normales
            Object.values(CONFIG.selectors.filters).forEach(selector => {
                if (typeof selector === 'string') {
                    const el = document.getElementById(selector);
                    if (el && el.tagName === 'SELECT') {
                        el.value = '';
                    }
                }
            });

            this.applyAllFilters();
        }
    };

    // ============ INICIALIZACIÓN ============
    const Init = {
        initMultiSelects() {
            if (ModalState.isInitialized) return;

            if (typeof MultiSelect === 'undefined') {
                console.error('MultiSelect class not found. Make sure it\'s loaded before this module.');
                return;
            }

            const createMS = (id, placeholder) => {
                const container = document.getElementById(id);
                if (container && !ModalState.multiSelectInstances[id]) {
                    return new MultiSelect(id, CONFIG.ahOptions, placeholder, () => Filters.applyAllFilters());
                }
                return null;
            };

            ModalState.multiSelectInstances = {
                ah: createMS(CONFIG.selectors.filters.ah, 'AH...'),
                prevHomeAh: createMS(CONFIG.selectors.filters.prevHomeAh, 'AH...'),
                prevAwayAh: createMS(CONFIG.selectors.filters.prevAwayAh, 'AH...'),
                h2hStadiumStartAh: createMS(CONFIG.selectors.filters.h2hStadiumStartAh, 'Ini...'),
                h2hStadiumEndAh: createMS(CONFIG.selectors.filters.h2hStadiumEndAh, 'Fin...'),
                h2hGeneralStartAh: createMS(CONFIG.selectors.filters.h2hGeneralStartAh, 'Ini...'),
                h2hGeneralEndAh: createMS(CONFIG.selectors.filters.h2hGeneralEndAh, 'Fin...'),
                h2hCol3Ah: createMS(CONFIG.selectors.filters.h2hCol3Ah, 'AH...'),
                indLocalAh: createMS(CONFIG.selectors.filters.indLocalAh, 'AH...'),
                indVisitanteAh: createMS(CONFIG.selectors.filters.indVisitanteAh, 'AH...')
            };


            ModalState.isInitialized = true;
        }
    };

    // ============ API PÚBLICA ============
    window.PatternExplorerModal = {
        /**
         * Abre el modal con vista tipo explorador
         */
        async open(matchId) {
            ModalState.currentMatchId = matchId;
            const overlay = document.getElementById(CONFIG.selectors.overlay);
            const loading = document.getElementById(CONFIG.selectors.loading);
            const tableContainer = document.getElementById(CONFIG.selectors.tableContainer);
            const matchInfoEl = document.getElementById(CONFIG.selectors.matchInfo);

            // Mostrar modal con loading
            overlay.classList.remove('d-none');
            loading.classList.remove('d-none');
            tableContainer.classList.add('d-none');
            document.body.style.overflow = 'hidden';

            // Reset estado
            ModalState.isInitialized = false;
            ModalState.multiSelectInstances = {};

            // Inicializar MultiSelects
            Init.initMultiSelects();

            // ============ STEP 1: Extraer datos básicos del partido ============
            let homeTeam = '?';
            let awayTeam = '?';
            let ahActual = '?';

            const precacheoData = window.precacheoData || {};
            const matchData = precacheoData[matchId];

            if (matchData) {
                homeTeam = matchData.home_name || '?';
                awayTeam = matchData.away_name || '?';
                ahActual = matchData.main_match_odds?.ah_linea || '?';
                console.log(`[PatternExplorerModal] Datos del partido: ${homeTeam} vs ${awayTeam}, AH: ${ahActual}`);
            } else {
                // Fallback: extraer del DOM
                const matchRow = document.querySelector(`tr[data-match-id="${matchId}"]`);
                if (matchRow) {
                    const homeCell = matchRow.querySelector('td:nth-child(2)');
                    const awayCell = matchRow.querySelector('td:nth-child(3)');
                    const ahCell = matchRow.querySelector('td:nth-child(4)');
                    if (homeCell) homeTeam = homeCell.textContent.trim() || '?';
                    if (awayCell) awayTeam = awayCell.textContent.trim() || '?';
                    if (ahCell) {
                        const ahText = ahCell.querySelector('.fw-bold, strong') || ahCell;
                        ahActual = ahText.textContent.trim().split('\n')[0] || '?';
                    }
                }
            }

            // Normalizar AH Actual: reemplazar coma por punto si es necesario
            if (ahActual && typeof ahActual === 'string') {
                ahActual = ahActual.replace(',', '.');
            }

            try {
                // ============ STEP 3: Cargar datos del bucket por handicap (SIN filtros H2H) ============
                // El filtrado H2H se hace ENTERAMENTE en el frontend con autoFilterH2H()
                const fetchFilters = {
                    handicap: ahActual !== '?' ? ahActual : null,
                    exclude_empty: false,
                    only_with_history: false,
                    limit: 50000
                };

                console.log('[PatternExplorerModal] Filtros enviados al backend (solo bucket):', fetchFilters);

                const res = await fetch('/api/explorer_search', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ filters: fetchFilters })
                });

                const data = await res.json();

                if (data.error) {
                    loading.innerHTML = `<div class="text-danger"><i class="fa-solid fa-exclamation-circle"></i> ${data.error}</div>`;
                    return;
                }

                // Guardar TODOS los resultados del bucket
                ModalState.allResults = data.results || [];
                console.log(`[PatternExplorerModal] Total partidos en bucket: ${ModalState.allResults.length}`);

                ModalState.matchInfo = {
                    home: homeTeam,
                    away: awayTeam,
                    ah_actual: ahActual
                };

                // ============ INICIALIZAR MULTI-SELECTS ============
                Init.initMultiSelects();

                // ============ AUTO FILTER H2H (Cascada: Exacto → Familia → Fallback) ============
                Filters.autoFilterH2H();

                // Mostrar info del partido con badge del nivel de filtrado
                const autoState = ModalState.autoH2H;
                const totalBucket = ModalState.allResults.length;
                const totalFiltered = autoState.active ? autoState.results.length : totalBucket;
                const bucketInfo = ahActual !== '?' ? `Bucket: ${ahActual}` : 'Sin filtro';

                let filterBadge = '';
                if (autoState.active) {
                    const sourceLabel = autoState.source === 'general' ? 'H2H General' : 'H2H Estadio';
                    const levelLabel = autoState.level === 'EXACT' ? 'Exacto' : 'Familia';
                    const levelColor = autoState.level === 'EXACT' ? 'bg-success' : 'bg-warning text-dark';
                    filterBadge = `
                        <span class="border-start ps-2">
                            <span class="badge ${levelColor}">${levelLabel}</span>
                            <span class="small text-muted ms-1">${sourceLabel}: ${autoState.startRaw} → ${autoState.endRaw}</span>
                        </span>
                    `;
                } else {
                    filterBadge = '<span class="border-start ps-2 text-muted small"><i class="fa-solid fa-filter"></i> Sin filtro H2H automático</span>';
                }

                matchInfoEl.innerHTML = `
                    <div class="d-flex align-items-center flex-wrap gap-2">
                        <span><strong>Contexto:</strong> ${ModalState.matchInfo.home} vs ${ModalState.matchInfo.away}</span>
                        <span class="border-start ps-2"><strong>AH:</strong> ${ModalState.matchInfo.ah_actual}</span>
                        <span class="border-start ps-2 text-success">
                            <i class="fa-solid fa-bolt"></i> <strong>${totalFiltered}</strong> partidos (${bucketInfo}, total: ${totalBucket})
                        </span>
                        ${filterBadge}
                    </div>
                `;

                // Aplicar filtros visuales (los dropdowns manuales) sobre los resultados auto-filtrados
                Filters.applyAllFilters();

                // Mostrar tabla
                loading.classList.add('d-none');
                tableContainer.classList.remove('d-none');

            } catch (err) {
                loading.innerHTML = `<div class="text-danger"><i class="fa-solid fa-exclamation-circle"></i> Error: ${err.message}</div>`;
            }
        },

        close(event) {
            if (event && event.target.id !== CONFIG.selectors.overlay) return;
            const overlay = document.getElementById(CONFIG.selectors.overlay);
            overlay.classList.add('d-none');
            document.body.style.overflow = 'auto';
        },

        applyFilters() {
            Filters.applyAllFilters();
        },

        clearFilters() {
            Filters.clearAllFilters();
        },

        toggleStatsRow(idx) {
            const row = document.getElementById(`modal-stats-row-${idx}`);
            if (row) {
                row.classList.toggle('d-none');
            }
        }
    };

    // ESC para cerrar
    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') {
            const overlay = document.getElementById(CONFIG.selectors.overlay);
            if (overlay && !overlay.classList.contains('d-none')) {
                window.PatternExplorerModal.close();
            }
        }
    });

})();
