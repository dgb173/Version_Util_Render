/**
 * Pronóstico v6.1 RESEARCH desde el servidor. Las funciones v5 se conservan
 * para compatibilidad y regresión; no deciden las predicciones del modal.
 */
(() => {
    // --- UTILIDADES MATEMÁTICAS Y DE PARSEO ---
    const esc = str => String(str ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');

    const parseNum = (val, def = 0) => {
        if (val === null || val === undefined) return def;
        const cleaned = String(val).replace(/[^0-9.-]/g, '');
        const n = parseFloat(cleaned);
        return Number.isFinite(n) ? n : def;
    };

    /**
     * Parsea partidos del historial dividiendo en bloques CASA y FUERA.
     * En el bloque FUERA: invierte el signo del AH impreso para obtener la perspectiva real del Visitante.
     */
    function parseMatchesFromContextText(contextText) {
        const lines = String(contextText || '').split('\n');
        let currentSection = 'UNKNOWN';
        const homeMatches = [];
        const awayMatches = [];

        for (const rawLine of lines) {
            const line = rawLine.trim();
            if (!line) continue;

            if (/^CASA\s*—|^TODAS LAS LOCALÍAS\s*—\s*[^—]+$/i.test(line) && !line.includes('Visitante')) {
                currentSection = 'HOME';
                continue;
            } else if (/^FUERA\s*—/i.test(line) || (/^TODAS LAS LOCALÍAS\s*—/i.test(line) && currentSection === 'HOME')) {
                currentSection = 'AWAY';
                continue;
            } else if (/^ÚLTIMO ENFRENTAMIENTO/i.test(line)) {
                currentSection = 'H2H';
                continue;
            }

            // Formato de fila: YYYY-MM-DD | Liga ID ... | EquipoA 1:2 EquipoB | AH [signo]X.XX
            const m = line.match(/^(\d{4}-\d{2}-\d{2})\s*\|\s*[^|]+\|\s*([^0-9:]+)\s+(\d+)\s*:\s*(\d+)\s+([^|]+)\|\s*AH\s*([0-9.-]+)/i);
            if (m) {
                const date = m[1];
                const t1 = (m[2] || '').trim();
                const g1 = parseInt(m[3], 10);
                const g2 = parseInt(m[4], 10);
                const t2 = (m[5] || '').trim();
                const rawAhVal = parseFloat(m[6]);

                if (currentSection === 'HOME') {
                    homeMatches.push({
                        date,
                        homeTeam: t1,
                        awayTeam: t2,
                        gHome: g1,
                        gAway: g2,
                        rawAhPrinted: rawAhVal,
                        ahEvaluated: rawAhVal, // En CASA, el AH ya representa la perspectiva del local
                        opponent: t2
                    });
                } else if (currentSection === 'AWAY') {
                    // En FUERA: t1 es el RivalLocal y t2 es el Visitante analizado.
                    // El AH impreso es del RivalLocal; para el Visitante analizado se INVIERTE el signo:
                    const ahInvertido = -rawAhVal;
                    awayMatches.push({
                        date,
                        homeTeam: t1,
                        awayTeam: t2,
                        gHome: g1,
                        gAway: g2,
                        rawAhPrinted: rawAhVal,
                        ahEvaluated: ahInvertido, // ¡INVERSIÓN CRÍTICA DE PERSPECTIVA!
                        opponent: t1
                    });
                }
            }
        }

        return { homeMatches, awayMatches };
    }

    /**
     * Extrae partidos de referencia del prompt LLM y del contexto para auditar la compuerta g2 y el veto Anti-Under
     */
    function extractReferenceMatchesFromPrompt(promptText, rawContext, homeMatches, awayMatches) {
        const refs = [];
        const seen = new Set();
        const addScore = (g1, g2, tag) => {
            if (Number.isFinite(g1) && Number.isFinite(g2)) {
                const key = `${tag}_${g1}_${g2}`;
                if (!seen.has(key)) {
                    seen.add(key);
                    refs.push({ g1, g2, total: g1 + g2, tag });
                }
            }
        };

        if (promptText) {
            // 1. Previo Local
            const mPrevHome = promptText.match(/##\s*Partido Previo de[^\n]*\\(Local\\)[\s\\S]*?-\s*\\*\\*Resultado\\*\\*:\s*(\d+)\s*[:-]\s*(\d+)/i);
            if (mPrevHome) addScore(parseInt(mPrevHome[1], 10), parseInt(mPrevHome[2], 10), 'Previo Local');

            // 2. Previo Visitante
            const mPrevAway = promptText.match(/##\s*Partido Previo de[^\n]*\\(Visitante\\)[\s\\S]*?-\s*\\*\\*Resultado\\*\\*:\s*(\d+)\s*[:-]\s*(\d+)/i);
            if (mPrevAway) addScore(parseInt(mPrevAway[1], 10), parseInt(mPrevAway[2], 10), 'Previo Visita');

            // 3. H2H Estadio
            const mH2hEstadio = promptText.match(/##\s*Enfrentamiento Directo Estadio[\s\\S]*?-\s*\\*\\*Resultado\\*\\*:\s*(\d+)\s*[:-]\s*(\d+)/i);
            if (mH2hEstadio) addScore(parseInt(mH2hEstadio[1], 10), parseInt(mH2hEstadio[2], 10), 'H2H Estadio');

            // 4. H2H General
            const mH2hGen = promptText.match(/##\s*Enfrentamiento Directo General[\s\\S]*?-\s*\\*\\*Resultado\\*\\*:\s*(\d+)\s*[:-]\s*(\d+)/i);
            if (mH2hGen) addScore(parseInt(mH2hGen[1], 10), parseInt(mH2hGen[2], 10), 'H2H General');

            // 5. Espejo Col3
            const mEspejo = promptText.match(/##\s*Columna 3[^\n]*[\s\\S]*?-\s*\\*\\*Resultado\\*\\*:\s*(\d+)\s*[:-]\s*(\d+)/i);
            if (mEspejo) addScore(parseInt(mEspejo[1], 10), parseInt(mEspejo[2], 10), 'Espejo Col3');

            // 6. Indirecta Local
            const mIndHome = promptText.match(/(?:Indirecta Local|Indirectas[^\n]*Local)[^\n]*?(\d+)\s*[:\-]\s*(\d+)/i);
            if (mIndHome) addScore(parseInt(mIndHome[1], 10), parseInt(mIndHome[2], 10), 'Indirecta Local');

            // 7. Indirecta Visitante
            const mIndAway = promptText.match(/(?:Indirecta Visitante|Indirectas[^\n]*Visitante)[^\n]*?(\d+)\s*[:\-]\s*(\d+)/i);
            if (mIndAway) addScore(parseInt(mIndAway[1], 10), parseInt(mIndAway[2], 10), 'Indirecta Visita');
        }

        // Fallback con partidos recientes de contexto si faltan referencias
        if (refs.length < 3) {
            if (homeMatches && homeMatches.length > 0) {
                addScore(homeMatches[0].gHome, homeMatches[0].gAway, 'Historial Casa');
            }
            if (awayMatches && awayMatches.length > 0) {
                addScore(awayMatches[0].gHome, awayMatches[0].gAway, 'Historial Fuera');
            }
        }

        const totalPartidosRef = refs.length;
        const partidosCon3MasGoles = refs.filter(r => r.total >= 3).length;
        const sumGoals = refs.reduce((acc, r) => acc + r.total, 0);
        const mediaGolesRef = totalPartidosRef > 0 ? (sumGoals / totalPartidosRef) : 2.5;
        const ratio3Mas = totalPartidosRef > 0 ? (partidosCon3MasGoles / totalPartidosRef) : 0;

        return {
            refs,
            totalPartidosRef,
            partidosCon3MasGoles,
            mediaGolesRef,
            ratio3Mas,
            Mz: Math.round(mediaGolesRef * 4)
        };
    }

    /**
     * Paso 6: Comprueba si un equipo perdió su partido previo ante un rival común que el otro equipo venció
     */
    function checkCommonOpponentEdge(data, homeMatches, awayMatches) {
        const promptText = data.promptText || '';
        let homePrevOpp = null;
        let homeLostPrev = false;
        let awayPrevOpp = null;
        let awayLostPrev = false;

        // Extraer rival previo de Local
        const mPrevH = promptText.match(/##\s*Partido Previo de[^\n]*\\(Local\\)[\s\\S]*?-\s*\\*\\*Encuentro\\*\\*:\s*([^v\n]+)\s+vs\s+([^\n]+)[\s\\S]*?-\s*\\*\\*Resultado\\*\\*:\s*(\d+)\s*[:-]\s*(\d+)/i);
        if (mPrevH) {
            const t1 = (mPrevH[1] || '').trim();
            const t2 = (mPrevH[2] || '').trim();
            const g1 = parseInt(mPrevH[3], 10);
            const g2 = parseInt(mPrevH[4], 10);
            const isT1 = t1.toLowerCase().includes((data.homeName || '').toLowerCase());
            homeLostPrev = isT1 ? (g1 < g2) : (g2 < g1);
            homePrevOpp = isT1 ? t2 : t1;
        } else if (homeMatches && homeMatches.length > 0) {
            homeLostPrev = homeMatches[0].gHome < homeMatches[0].gAway;
            homePrevOpp = homeMatches[0].opponent;
        }

        // Extraer rival previo de Visitante
        const mPrevA = promptText.match(/##\s*Partido Previo de[^\n]*\\(Visitante\\)[\s\\S]*?-\s*\\*\\*Encuentro\\*\\*:\s*([^v\n]+)\s+vs\s+([^\n]+)[\s\\S]*?-\s*\\*\\*Resultado\\*\\*:\s*(\d+)\s*[:-]\s*(\d+)/i);
        if (mPrevA) {
            const t1 = (mPrevA[1] || '').trim();
            const t2 = (mPrevA[2] || '').trim();
            const g1 = parseInt(mPrevA[3], 10);
            const g2 = parseInt(mPrevA[4], 10);
            const isT1 = t1.toLowerCase().includes((data.awayName || '').toLowerCase());
            awayLostPrev = isT1 ? (g1 < g2) : (g2 < g1);
            awayPrevOpp = isT1 ? t2 : t1;
        } else if (awayMatches && awayMatches.length > 0) {
            awayLostPrev = awayMatches[0].gAway < awayMatches[0].gHome;
            awayPrevOpp = awayMatches[0].opponent;
        }

        // 1. Caso Anyang/Toyama: Local perdió su previo ante homePrevOpp, y Visitante lo venció o le marcó 3+ goles
        if (homeLostPrev && homePrevOpp) {
            const oppClean = homePrevOpp.toLowerCase().replace(/fc|sc|cf|united|city/g, '').trim();
            if (oppClean.length >= 3) {
                const awayMatchVsOpp = (awayMatches || []).find(m => m.opponent.toLowerCase().includes(oppClean));
                let awayWonOrScored3 = false;
                if (awayMatchVsOpp) {
                    const scAway = awayMatchVsOpp.gAway;
                    const scOpp = awayMatchVsOpp.gHome;
                    if (scAway > scOpp || scAway >= 3) awayWonOrScored3 = true;
                }
                if (!awayWonOrScored3 && promptText) {
                    const indM = promptText.match(new RegExp(`(?:Indirecta|${oppClean})[^\n]*?(\d+)\s*[:-]\s*(\d+)`, 'i'));
                    if (indM) {
                        const g1 = parseInt(indM[1], 10);
                        const g2 = parseInt(indM[2], 10);
                        if (g1 >= 3 || g2 >= 3 || Math.max(g1, g2) > Math.min(g1, g2)) awayWonOrScored3 = true;
                    }
                }
                if (awayWonOrScored3) {
                    return {
                        edgeTo: 'away',
                        opponent: homePrevOpp,
                        detail: `${data.homeName} perdió su previo contra ${homePrevOpp}, mientras que ${data.awayName} venció o marcó 3+ goles ante ese mismo rival común.`
                    };
                }
            }
        }

        // 2. Caso inverso: Visitante perdió su previo ante awayPrevOpp, y Local lo venció o le marcó 3+ goles
        if (awayLostPrev && awayPrevOpp) {
            const oppClean = awayPrevOpp.toLowerCase().replace(/fc|sc|cf|united|city/g, '').trim();
            if (oppClean.length >= 3) {
                const homeMatchVsOpp = (homeMatches || []).find(m => m.opponent.toLowerCase().includes(oppClean));
                let homeWonOrScored3 = false;
                if (homeMatchVsOpp) {
                    const scHome = homeMatchVsOpp.gHome;
                    const scOpp = homeMatchVsOpp.gAway;
                    if (scHome > scOpp || scHome >= 3) homeWonOrScored3 = true;
                }
                if (!homeWonOrScored3 && promptText) {
                    const indM = promptText.match(new RegExp(`(?:Indirecta|${oppClean})[^\n]*?(\d+)\s*[:-]\s*(\d+)`, 'i'));
                    if (indM) {
                        const g1 = parseInt(indM[1], 10);
                        const g2 = parseInt(indM[2], 10);
                        if (g1 >= 3 || g2 >= 3 || Math.max(g1, g2) > Math.min(g1, g2)) homeWonOrScored3 = true;
                    }
                }
                if (homeWonOrScored3) {
                    return {
                        edgeTo: 'home',
                        opponent: awayPrevOpp,
                        detail: `${data.awayName} perdió su previo contra ${awayPrevOpp}, mientras que ${data.homeName} venció o marcó 3+ goles ante ese mismo rival común.`
                    };
                }
            }
        }

        return { edgeTo: null, opponent: null, detail: null };
    }

    /**
     * Calcula la triangulación en rivales comunes e indirectas para la compuerta b3
     */
    function calculateTriangulationB3(homeMatches, awayMatches, favoritoRole, absHz) {
        const homeOpponents = new Map();
        homeMatches.forEach(m => {
            const oppKey = m.opponent.toLowerCase().replace(/fc|sc|cf|united|city/g, '').trim();
            if (oppKey.length >= 3) homeOpponents.set(oppKey, m);
        });

        const commonMatches = [];
        awayMatches.forEach(mAway => {
            const oppKey = mAway.opponent.toLowerCase().replace(/fc|sc|cf|united|city/g, '').trim();
            if (oppKey.length >= 3 && homeOpponents.has(oppKey)) {
                commonMatches.push({
                    homeMatch: homeOpponents.get(oppKey),
                    awayMatch: mAway,
                    opponentName: mAway.opponent
                });
            }
        });

        // Caso A: Hay rivales comunes directos
        if (commonMatches.length > 0) {
            let sumDeltaZ = 0;
            let favSuperiorCount = 0;

            commonMatches.forEach(pair => {
                const ahHome = pair.homeMatch.ahEvaluated; // perspectiva Local
                const ahAway = pair.awayMatch.ahEvaluated; // perspectiva Visitante (ya invertida)
                const dAh = (ahHome - ahAway);
                sumDeltaZ += Math.round(dAh * 4);

                const gdHome = pair.homeMatch.gHome - pair.homeMatch.gAway;
                const gdAway = pair.awayMatch.gAway - pair.awayMatch.gHome;
                if (favoritoRole === 'home' && gdHome >= gdAway) favSuperiorCount++;
                if (favoritoRole === 'away' && gdAway >= gdHome) favSuperiorCount++;
            });

            const avgDeltaZ = Math.round(sumDeltaZ / commonMatches.length);
            const favSuperior = favSuperiorCount >= Math.ceil(commonMatches.length / 2);

            return {
                deltaZ: avgDeltaZ,
                favSuperior,
                detail: `Triangulación directa con ${commonMatches.length} rival(es) común(es): brecha deltaZ = ${avgDeltaZ} cuartos (Rendimiento Fav: ${favSuperior ? 'Superior' : 'Inferior'}).`
            };
        }

        // Caso B: No hay rivales comunes directos -> Tasación media del mercado
        const avgAhHome = homeMatches.length ? (homeMatches.reduce((acc, m) => acc + m.ahEvaluated, 0) / homeMatches.length) : 0;
        const avgAhAway = awayMatches.length ? (awayMatches.reduce((acc, m) => acc + m.ahEvaluated, 0) / awayMatches.length) : 0;

        const deltaRaw = favoritoRole === 'home' ? (avgAhHome - avgAhAway) : (avgAhAway - avgAhHome);
        const deltaZ = Math.round(deltaRaw * 4);
        const favSuperior = deltaZ >= 0;

        return {
            deltaZ,
            favSuperior,
            detail: `Tasación media histórica de mercado (Sin rivales directos): Local AH ${avgAhHome.toFixed(2)} vs Visitante AH ${avgAhAway.toFixed(2)} (Perspectiva invertida). Brecha deltaZ = ${deltaZ} cuartos.`
        };
    }

    /**
     * Motor determinista evaluateMatchV4 (Actualizado a Especificación v5.0)
     */
    function evaluateMatchV4(data) {
        const homeName = data.homeName || 'Local';
        const awayName = data.awayName || 'Visitante';
        const leagueName = data.leagueName || 'Liga';
        const matchDate = data.date || '';

        // PASO 1: CUANTIZACIÓN ENTERA (Z)
        const rawAh = parseNum(data.handicap, 0);
        const rawOu = parseNum(data.goalLine, 2.5);

        const Hz = Math.round(rawAh * 4);
        const Gz = Math.round(rawOu * 4);
        const absHz = Math.abs(Hz);
        const lineDisplay = absHz > 0 ? `+${(absHz / 4).toFixed(2)}` : 'AH 0';

        // Identificación de roles según convención NowGoal de la plataforma:
        // rawAh > 0 (ej. +0.5, +1.25) -> El LOCAL es el Favorito (da el hándicap)
        // rawAh < 0 (ej. -0.5, -1.5)  -> El VISITANTE es el Favorito (el visitante da el hándicap)
        // rawAh == 0 -> Definir por posición en tabla (menor número de puesto) o correlación
        let favorito = homeName;
        let favoritoRole = 'home';
        let noFavorito = awayName;
        let noFavoritoRole = 'away';

        if (rawAh < 0) {
            favorito = awayName;
            favoritoRole = 'away';
            noFavorito = homeName;
            noFavoritoRole = 'home';
        } else if (rawAh === 0) {
            const homePos = data.homePos !== null ? data.homePos : 99;
            const awayPos = data.awayPos !== null ? data.awayPos : 99;
            if (awayPos < homePos) {
                favorito = awayName;
                favoritoRole = 'away';
                noFavorito = homeName;
                noFavoritoRole = 'home';
            }
        }

        // --- PASO 2: DETECTOR PSICOLÓGICO DE TRAMPAS DE LA CASA ---
        let trampaDetectada = 'NINGUNA_TRAMPA_EVIDENTE';
        let trampaDetalle = 'Las líneas de la casa no presentan anomalías o cebos extremos en este cruce.';

        const homePos = data.homePos !== null ? data.homePos : null;
        const awayPos = data.awayPos !== null ? data.awayPos : null;
        const homeWinRate = data.homeWinRate || 0;
        const favEmpatesPct = favoritoRole === 'home' ? (data.homeDrawRate || 0) : (data.awayDrawRate || 0);

        if (homePos !== null && awayPos !== null && (homePos - awayPos >= 5) && homeWinRate <= 0.20 && absHz <= 1) {
            trampaDetectada = 'TRAMPA_CEBO_TABLA';
            trampaDetalle = `El visitante ${awayName} va #${awayPos} en la tabla frente al #${homePos} de ${homeName} (diferencia de ${homePos - awayPos} puestos), pero la casa ofrece una línea cortísima (AH ${rawAh >= 0 ? '+' : ''}${rawAh}), induciendo masivamente al público a apostar al visitante mientras el local se refugia en su condición.`;
        } else if (data.localWinsStreak >= 2 && data.localAvgShots <= 6 && data.localShotEff >= 0.65) {
            trampaDetectada = 'TRAMPA_FALSA_RACHA_LOCAL';
            trampaDetalle = `${homeName} encadena victorias recientes engañosas promediando apenas ${data.localAvgShots.toFixed(1)} tiros totales y sosteniéndose por una efectividad insostenible (${Math.round(data.localShotEff * 100)}% de gol/tiro a puerta).`;
        } else if (data.bothHighScoringRecent && Gz >= 11 && (data.refGoalsAvg || 2.5) < rawOu) {
            trampaDetectada = 'TRAMPA_INFLACION_DOBLE_OVER';
            trampaDetalle = `Ambos equipos vienen de partidos con marcadores abultados y la casa infla la línea a Gz = ${Gz} (${rawOu} goles, obligando a 4 goles), cuando las referencias promedian ${(data.refGoalsAvg || 2.5).toFixed(2)} goles.`;
        } else if ((absHz === 1 || absHz === 2) && favEmpatesPct >= 0.45) {
            trampaDetectada = 'TRAMPA_EMPATE_LINEA_PAR';
            trampaDetalle = `El Favorito ${favorito} tiene una línea de ${rawAh} (${absHz} cuartos), pero registra un ${Math.round(favEmpatesPct * 100)}% de empates en su condición, haciendo que el empate castigue al apostador del favorito y premie al No-Favorito.`;
        }

        // --- PASO 3: PARSEO Y AUDITORÍA DE REFERENCIAS (Filtro Anti-Under Caso Omiya) ---
        const refAudit = extractReferenceMatchesFromPrompt(data.promptText || '', data.rawContext || '', data.homeMatches, data.awayMatches);
        const totalPartidosRef = refAudit.totalPartidosRef;
        const partidosCon3MasGoles = refAudit.partidosCon3MasGoles;
        const ratio3Mas = refAudit.ratio3Mas;
        const mediaGolesRef = refAudit.mediaGolesRef;
        const Mz = refAudit.Mz;

        // --- PASO 4: MÓDULO B — EVALUACIÓN DE LAS 6 COMPUERTAS DE GOLES (g1..g6) ---
        // (Se ejecuta primero para permitir el acoplamiento cruzado hacia Hándicap)
        const g = {};
        const gDet = {};

        // g1: Axioma Entero 1 + 2 = 3
        const noFavGolesMin1 = (data.noFavMarcaEnCondicionRate || 0) >= 0.75;
        const noFavMarcoH2H = !data.noFavSeQuedoEnCeroH2H;
        if (noFavGolesMin1 && noFavMarcoH2H && Gz <= 12) {
            g.g1 = 1;
            gDet.g1 = `Axioma 1+2=3 cumplido: ${noFavorito} marca en >=75% de partidos y no se quedó a cero en H2H directo. Piso mínimo de 3 goles.`;
        } else if ((data.favMarca2PlusRate || 0) >= 0.85 && (data.noFavMarcaRate || 0) >= 0.80) {
            g.g1 = 1;
            gDet.g1 = `Favorito marca 2+ goles en >=85% y visitante marca en >=80%.`;
        } else if (data.algunoPorteriaCero50Pct || data.algunoSinMarcarH2H) {
            g.g1 = 0;
            gDet.g1 = `Riesgo de portería a cero (>=50% de partidos o blanco en H2H directo).`;
        } else {
            g.g1 = 0;
            gDet.g1 = `No se garantiza el piso entero de gol del perdedor proyectado.`;
        }

        // g2: Media Entera en Referencias + LEY DE BLOQUEO ANTI-UNDER (Paso 4)
        let vetoAntiUnder = false;
        if (totalPartidosRef >= 3 && ratio3Mas >= 0.80 && Mz >= Gz + 3) {
            vetoAntiUnder = true;
            g.g2 = 1;
            gDet.g2 = `Veto Anti-Under Activado: ${partidosCon3MasGoles} de ${totalPartidosRef} (${Math.round(ratio3Mas * 100)}%) referencias con >= 3 goles. Mz = ${Mz} cuartos (${mediaGolesRef.toFixed(2)} goles) >= Gz + 3 (${Gz + 3}).`;
        } else if (Mz >= Gz) {
            g.g2 = 1;
            gDet.g2 = `Media entera de referencias Mz = ${Mz} cuartos (${mediaGolesRef.toFixed(2)} goles) >= Gz = ${Gz} cuartos (${rawOu}).`;
        } else {
            g.g2 = 0;
            gDet.g2 = `Media entera de referencias Mz = ${Mz} cuartos (${mediaGolesRef.toFixed(2)} goles) < Gz = ${Gz} cuartos (${rawOu}).`;
        }

        // g3: LEY DE FUSIÓN PONDERADA DE LOS 2 BLOQUES DE HÁNDICAPS SIMILARES (Paso 3)
        const N1_C = data.similaresActualHomeSamples || 0;
        const GF1_C = data.similaresGfHomeAvg;
        const GC1_C = data.similaresGcHomeAvg;
        const N2_C = data.similaresH2HHomeSamples || 0;
        const GF2_C = data.similaresH2HGfHomeAvg;
        const GC2_C = data.similaresH2HGcHomeAvg;

        const N1_F = data.similaresActualAwaySamples || 0;
        const GF1_F = data.similaresGfAwayAvg;
        const GC1_F = data.similaresGcAwayAvg;
        const N2_F = data.similaresH2HAwaySamples || 0;
        const GF2_F = data.similaresH2HGfAwayAvg;
        const GC2_F = data.similaresH2HGcAwayAvg;

        let sumGolesCasa = 0;
        let weightCasa = 0;
        if (N1_C > 0 && Number.isFinite(GF1_C) && Number.isFinite(GC1_C)) {
            sumGolesCasa += N1_C * (GF1_C + GC1_C);
            weightCasa += N1_C;
        }
        if (N2_C > 0 && Number.isFinite(GF2_C) && Number.isFinite(GC2_C)) {
            sumGolesCasa += N2_C * (GF2_C + GC2_C);
            weightCasa += N2_C;
        }
        const golesSimCasa = weightCasa > 0 ? (sumGolesCasa / weightCasa) : (Number.isFinite(GF1_C) && Number.isFinite(GC1_C) ? (GF1_C + GC1_C) : null);

        let sumGolesFuera = 0;
        let weightFuera = 0;
        if (N1_F > 0 && Number.isFinite(GF1_F) && Number.isFinite(GC1_F)) {
            sumGolesFuera += N1_F * (GF1_F + GC1_F);
            weightFuera += N1_F;
        }
        if (N2_F > 0 && Number.isFinite(GF2_F) && Number.isFinite(GC2_F)) {
            sumGolesFuera += N2_F * (GF2_F + GC2_F);
            weightFuera += N2_F;
        }
        const golesSimFuera = weightFuera > 0 ? (sumGolesFuera / weightFuera) : (Number.isFinite(GF1_F) && Number.isFinite(GC1_F) ? (GF1_F + GC1_F) : null);

        const Sz_Casa = golesSimCasa !== null ? Math.round(golesSimCasa * 4) : null;
        const Sz_Fuera = golesSimFuera !== null ? Math.round(golesSimFuera * 4) : null;

        if (Sz_Casa !== null && Sz_Fuera !== null) {
            const avgSz = (Sz_Casa + Sz_Fuera) / 2;
            if ((Sz_Casa >= Gz && Sz_Fuera >= Gz) || (avgSz >= Gz + 1)) {
                g.g3 = 1;
                gDet.g3 = `Fusión Ponderada Similares (${weightCasa + weightFuera} PJ): Sz Casa = ${Sz_Casa} (${golesSimCasa.toFixed(2)} g), Sz Fuera = ${Sz_Fuera} (${golesSimFuera.toFixed(2)} g), Promedio = ${avgSz.toFixed(1)} >= Gz (${Gz}).`;
            } else {
                g.g3 = 0;
                gDet.g3 = `Fusión Ponderada Similares (${weightCasa + weightFuera} PJ): Sz Casa = ${Sz_Casa}, Sz Fuera = ${Sz_Fuera}, Promedio = ${avgSz.toFixed(1)} < Gz (${Gz}).`;
            }
        } else if (Sz_Casa !== null || Sz_Fuera !== null) {
            const validSz = Sz_Casa !== null ? Sz_Casa : Sz_Fuera;
            if (validSz >= Gz) {
                g.g3 = 1;
                gDet.g3 = `Fusión Similares parcial: Lado válido Sz = ${validSz} >= Gz (${Gz}).`;
            } else {
                g.g3 = 0;
                gDet.g3 = `Fusión Similares parcial: Lado válido Sz = ${validSz} < Gz (${Gz}).`;
            }
        } else {
            g.g3 = g.g2;
            gDet.g3 = `Ambos bloques sin GF/GC numéricos (N/D); hereda de compuerta g2 (${g.g2}).`;
        }

        // g4: Compuerta Dual de Ruptura (Paliza Unilateral o Intercambio Bilateral)
        const triPre = data.triangulacionB3 || { deltaZ: 0 };
        const deltaZ = triPre.deltaZ || 0;
        const palizaUnilateral = Math.abs(deltaZ) >= Gz;
        const bttsRate = data.bttsRateRecent || 0;
        const hasCasaGoals = Number.isFinite(GF1_C) && Number.isFinite(GC1_C);
        const hasAwayGoals = Number.isFinite(GF1_F) && Number.isFinite(GC1_F);
        const intercambioBilateral = (hasCasaGoals && hasAwayGoals) &&
            (((GC1_C >= 1.8 && GF1_F >= 1.8) || (GC1_F >= 1.8 && GF1_C >= 1.8)) && bttsRate >= 0.70);

        if (palizaUnilateral) {
            g.g4 = 1;
            gDet.g4 = `Ruptura Unilateral: brecha |deltaZ| (${Math.abs(deltaZ)}) >= Gz (${Gz}). El favorito cubre el Over él solo.`;
        } else if (intercambioBilateral) {
            g.g4 = 1;
            gDet.g4 = `Intercambio Bilateral: ambos conceden >= 1.8 goles en similares y BTTS >= 70%.`;
        } else {
            g.g4 = 0;
            gDet.g4 = `Sin ruptura proyectada (|deltaZ|=${Math.abs(deltaZ)} < Gz=${Gz}).`;
        }

        // g5: Ritmo Acumulado de Ataques Peligrosos y Tiros a Puerta
        const apCombined = (data.statsHomeAp || 0) + (data.statsAwayAp || 0);
        const stCombined = (data.statsHomeTirosPuerta || 0) + (data.statsAwayTirosPuerta || 0);

        if (data.hasDetailedStats && (apCombined > 0 || stCombined > 0)) {
            if (apCombined >= 85 || stCombined >= 8.5) {
                g.g5 = 1;
                gDet.g5 = `Alto volumen ofensivo combinado (${Math.round(apCombined)} AP, ${stCombined.toFixed(1)} Tiros a puerta).`;
            } else {
                g.g5 = 0;
                gDet.g5 = `Volumen de peligro insuficiente (${Math.round(apCombined)} AP, ${stCombined.toFixed(1)} TP < 8.5).`;
            }
        } else {
            // Proxy ofensivo si stats están cerradas
            const proxyGoalsSum = (golesSimCasa || 2.5) + (golesSimFuera || 2.5);
            if (proxyGoalsSum >= 5.0) {
                g.g5 = 1;
                gDet.g5 = `Proxy ofensivo: Ritmo goleador combinado en similares (${(proxyGoalsSum / 2).toFixed(2)} goles/partido).`;
            } else {
                g.g5 = 0;
                gDet.g5 = `Proxy ofensivo: Ritmo goleador combinado moderado (${(proxyGoalsSum / 2).toFixed(2)} goles/partido).`;
            }
        }

        // g6: Densidad de Tiros y Efectividad de Conversión
        const shCombined = (data.statsHomeTiros || 0) + (data.statsAwayTiros || 0);
        if (data.hasDetailedStats && shCombined > 0) {
            if (shCombined >= 20 || (stCombined >= 8 && (data.statsConversionRate || 0.32) >= 0.30)) {
                g.g6 = 1;
                gDet.g6 = `Densidad de disparos (${shCombined} Tiros) y alta conversión (${Math.round((data.statsConversionRate || 0.32) * 100)}%).`;
            } else {
                g.g6 = 0;
                gDet.g6 = `Baja densidad de disparos (${shCombined} Tiros totales < 20).`;
            }
        } else {
            if (data.bothHighScoringRecent || (golesSimCasa && golesSimCasa >= 3.0) || (golesSimFuera && golesSimFuera >= 3.0)) {
                g.g6 = 1;
                gDet.g6 = `Proxy de efectividad: Precedentes y similares con alta frecuencia de marcadores over.`;
            } else {
                g.g6 = 0;
                gDet.g6 = `Sin densidad ofensiva comprobada.`;
            }
        }

        // REGLAS DETERMINISTAS DE DECISIÓN DE GOLES
        let scoreOu = g.g1 + g.g2 + g.g3 + g.g4 + g.g5 + g.g6;
        if (vetoAntiUnder) {
            scoreOu = Math.min(6, scoreOu + 2);
        }

        let veredictoOu = '';
        if (vetoAntiUnder) {
            veredictoOu = `VETO ANTI-UNDER -> ENTRADA OVER ${rawOu} (Pleno de Referencias >= 80% con 3+ goles)`;
        } else if (g.g1 === 1) {
            veredictoOu = `AXIOMA 1+2=3 CUMPLIDO -> ENTRADA DIRECTA OVER ${rawOu}`;
        } else if (scoreOu >= 5) {
            veredictoOu = `SCORE OU ${scoreOu}/6 -> OVER ${rawOu} LÍNEA PRINCIPAL`;
        } else if (scoreOu >= 3) {
            veredictoOu = `SCORE OU ${scoreOu}/6 -> OVER EN VIVO (Min 15-20 con descuento de línea)`;
        } else {
            veredictoOu = `SCORE OU ${scoreOu}/6 -> UNDER ${rawOu} (o NO OPERAR)`;
        }

        // --- PASO 5: MÓDULO A — LAS 6 COMPUERTAS BINARIAS DE HÁNDICAP (b1..b6) ---
        const b = {};
        const bDet = {};

        // b1: Correlación Actual en Hándicaps Similares
        const favSamplesActual = favoritoRole === 'home' ? (data.similaresActualHomeSamples || 0) : (data.similaresActualAwaySamples || 0);
        const noFavSamplesActual = favoritoRole === 'home' ? (data.similaresActualAwaySamples || 0) : (data.similaresActualHomeSamples || 0);
        const favHasCoverActual = favoritoRole === 'home' ? !!data.similaresActualHomeHasCover : !!data.similaresActualAwayHasCover;
        const noFavHasCoverActual = favoritoRole === 'home' ? !!data.similaresActualAwayHasCover : !!data.similaresActualHomeHasCover;

        const coverFavActual = favHasCoverActual ? (favoritoRole === 'home' ? data.similaresActualHomeCover : data.similaresActualAwayCover) : null;
        const coverNoFavActual = noFavHasCoverActual ? (favoritoRole === 'home' ? data.similaresActualAwayCover : data.similaresActualHomeCover) : null;

        const ambosSinMuestraActual = !favHasCoverActual && !noFavHasCoverActual;
        const corrActualTexto = (data.correlacionActualTexto || '').toLowerCase();
        const corrActualFavoreceLocal = corrActualTexto.includes('favorable al local');
        const corrActualFavoreceVis = corrActualTexto.includes('favorable al visitante');
        const corrActualInsuficiente = corrActualTexto.includes('insuficiente');

        const corrActualFavoreceFav = (favoritoRole === 'home' && corrActualFavoreceLocal) || (favoritoRole === 'away' && corrActualFavoreceVis);
        const corrActualFavoreceNoFav = (favoritoRole === 'home' && corrActualFavoreceVis) || (favoritoRole === 'away' && corrActualFavoreceLocal);

        if (corrActualFavoreceFav) {
            b.b1 = 1;
            bDet.b1 = `Correlación oficial favorable al Favorito [${favorito}] (${coverFavActual !== null ? coverFavActual + '%' : 'ventaja histórica'}).`;
        } else if (corrActualFavoreceNoFav) {
            b.b1 = 0;
            bDet.b1 = `Correlación oficial favorable al No-Favorito [${noFavorito}] (${coverNoFavActual !== null ? coverNoFavActual + '%' : 'ventaja histórica'}).`;
        } else if (ambosSinMuestraActual || corrActualInsuficiente) {
            b.b1 = 0;
            bDet.b1 = `Muestra insuficiente (< 2 partidos en ambos lados). No aplica bloqueo ni sesgo.`;
        } else if (favHasCoverActual && !noFavHasCoverActual) {
            b.b1 = coverFavActual >= 50 ? 1 : 0;
            bDet.b1 = `Cover Favorito (${coverFavActual}%) en ${favSamplesActual} PJ (No-Fav sin muestra).`;
        } else if (!favHasCoverActual && noFavHasCoverActual) {
            b.b1 = coverNoFavActual <= 50 ? 1 : 0;
            bDet.b1 = `Cover No-Fav (${coverNoFavActual}%) en ${noFavSamplesActual} PJ (Fav sin muestra).`;
        } else {
            if (coverFavActual >= coverNoFavActual) {
                b.b1 = 1;
                bDet.b1 = `Cover Favorito (${coverFavActual}%) supera o iguala a No-Favorito (${coverNoFavActual}%).`;
            } else {
                b.b1 = 0;
                bDet.b1 = `Cover No-Favorito (${coverNoFavActual}%) supera a Favorito (${coverFavActual}%).`;
            }
        }

        // b2: Correlación Histórica H2H en Hándicaps Similares
        const favSamplesH2H = favoritoRole === 'home' ? (data.similaresH2HHomeSamples || 0) : (data.similaresH2HAwaySamples || 0);
        const noFavSamplesH2H = favoritoRole === 'home' ? (data.similaresH2HAwaySamples || 0) : (data.similaresH2HHomeSamples || 0);
        const favHasCoverH2H = favoritoRole === 'home' ? !!data.similaresH2HHomeHasCover : !!data.similaresH2HAwayHasCover;
        const noFavHasCoverH2H = favoritoRole === 'home' ? !!data.similaresH2HAwayHasCover : !!data.similaresH2HHomeHasCover;

        const coverFavH2H = favHasCoverH2H ? (favoritoRole === 'home' ? data.similaresH2HHomeCover : data.similaresH2HAwayCover) : null;
        const coverNoFavH2H = noFavHasCoverH2H ? (favoritoRole === 'home' ? data.similaresH2HAwayCover : data.similaresH2HHomeCover) : null;
        const ambosSinMuestraH2H = !favHasCoverH2H && !noFavHasCoverH2H;

        const corrH2HTexto = (data.correlacionH2HTexto || '').toLowerCase();
        const corrH2HFavoreceLocal = corrH2HTexto.includes('favorable al local');
        const corrH2HFavoreceVis = corrH2HTexto.includes('favorable al visitante');
        const corrH2HInsuficiente = corrH2HTexto.includes('insuficiente');

        const corrH2HFavoreceFav = (favoritoRole === 'home' && corrH2HFavoreceLocal) || (favoritoRole === 'away' && corrH2HFavoreceVis);
        const corrH2HFavoreceNoFav = (favoritoRole === 'home' && corrH2HFavoreceVis) || (favoritoRole === 'away' && corrH2HFavoreceLocal);

        if (corrH2HFavoreceFav) {
            b.b2 = 1;
            bDet.b2 = `Correlación H2H favorable al Favorito [${favorito}] (${coverFavH2H !== null ? coverFavH2H + '%' : 'ventaja H2H'}).`;
        } else if (corrH2HFavoreceNoFav) {
            b.b2 = 0;
            bDet.b2 = `Correlación H2H favorable al No-Favorito [${noFavorito}] (${coverNoFavH2H !== null ? coverNoFavH2H + '%' : 'ventaja H2H'}).`;
        } else if (ambosSinMuestraH2H || corrH2HInsuficiente) {
            b.b2 = 0;
            bDet.b2 = `Muestra H2H previa insuficiente (< 2 PJ). No aplica bloqueo.`;
        } else if (favHasCoverH2H && !noFavHasCoverH2H) {
            b.b2 = coverFavH2H >= 50 ? 1 : 0;
            bDet.b2 = `Cover H2H Fav (${coverFavH2H}%) en ${favSamplesH2H} PJ.`;
        } else if (!favHasCoverH2H && noFavHasCoverH2H) {
            b.b2 = coverNoFavH2H <= 50 ? 1 : 0;
            bDet.b2 = `Cover H2H No-Fav (${coverNoFavH2H}%) en ${noFavSamplesH2H} PJ.`;
        } else {
            if (coverFavH2H >= coverNoFavH2H) {
                b.b2 = 1;
                bDet.b2 = `Cover H2H Fav (${coverFavH2H}%) supera o iguala a No-Fav (${coverNoFavH2H}%).`;
            } else {
                b.b2 = 0;
                bDet.b2 = `Cover H2H No-Fav (${coverNoFavH2H}%) supera a Fav (${coverFavH2H}%).`;
            }
        }

        // b3 y b4: PASO 6 — PENALIZACIÓN/BONIFICACIÓN POR RIVAL COMÚN (Caso Anyang / Toyama)
        const commonEdge = checkCommonOpponentEdge(data, data.homeMatches, data.awayMatches);
        const tri = data.triangulacionB3 || { deltaZ: absHz + 1, favSuperior: true, detail: 'Sin triangulación específica.' };

        if (commonEdge.edgeTo !== null) {
            const favHasEdge = commonEdge.edgeTo === favoritoRole;
            b.b3 = favHasEdge ? 1 : 0;
            bDet.b3 = favHasEdge
                ? `Bonificación por rival común (${commonEdge.opponent}): ${commonEdge.detail}`
                : `Penalización por rival común (${commonEdge.opponent}): ${commonEdge.detail}`;

            b.b4 = favHasEdge ? 1 : 0;
            bDet.b4 = favHasEdge
                ? `Momento favorable: ${favorito} superó al rival común ${commonEdge.opponent} ante el que tropezó ${noFavorito}.`
                : `Momento desfavorable: ${favorito} tropezó ante el rival común ${commonEdge.opponent} que ${noFavorito} superó.`;
        } else {
            // Triangulación matemática estándar b3
            if (tri.deltaZ >= absHz && tri.favSuperior) {
                b.b3 = 1;
                bDet.b3 = `Brecha deltaZ (${tri.deltaZ} cuartos) >= |Hz| (${absHz}) con tasación/rendimiento favorable. [${tri.detail}]`;
            } else {
                b.b3 = 0;
                bDet.b3 = `Brecha deltaZ (${tri.deltaZ} cuartos) < |Hz| (${absHz}) o No-Favorito más eficiente. [${tri.detail}]`;
            }

            // Memoria H2H y Tabla estándar b4
            const favGanoH2H = !!data.lastH2HFavWonAndCovered;
            const favNoInferiorEnTabla = (homePos !== null && awayPos !== null)
                ? (favoritoRole === 'home' ? (homePos - awayPos <= 2) : (awayPos - homePos <= 2))
                : true;
            if (favGanoH2H && favNoInferiorEnTabla) {
                b.b4 = 1;
                bDet.b4 = `Favorito cubrió el último H2H directo y mantiene coherencia clasificatoria.`;
            } else {
                b.b4 = 0;
                bDet.b4 = `Favorito no cubrió el último H2H o está por debajo en tabla frente al rival.`;
            }
        }

        // b5: Volumen Ofensivo Filtrado por Rol y Filiales (Con Proxy de Goles si Stats están cerradas)
        const favTirosPuerta = favoritoRole === 'home' ? (data.statsHomeTirosPuerta || 0) : (data.statsAwayTirosPuerta || 0);
        const noFavTirosPuerta = favoritoRole === 'home' ? (data.statsAwayTirosPuerta || 0) : (data.statsHomeTirosPuerta || 0);
        const favAp = favoritoRole === 'home' ? (data.statsHomeAp || 0) : (data.statsAwayAp || 0);
        const noFavAp = favoritoRole === 'home' ? (data.statsAwayAp || 0) : (data.statsHomeAp || 0);
        const favTirosTotales = favoritoRole === 'home' ? (data.statsHomeTiros || 0) : (data.statsAwayTiros || 0);

        if (data.hasDetailedStats && (favTirosTotales > 0 || favAp > 0)) {
            if ((favTirosPuerta - noFavTirosPuerta >= 1.0) || (favAp - noFavAp >= 5) || favTirosTotales >= 9) {
                b.b5 = 1;
                bDet.b5 = `Favorito supera en generación ofensiva (+${(favTirosPuerta - noFavTirosPuerta).toFixed(1)} TP, +${Math.round(favAp - noFavAp)} AP, ${favTirosTotales} Tiros).`;
            } else {
                b.b5 = 0;
                bDet.b5 = `No-Favorito equipara o supera volumen (Fav ${favTirosPuerta.toFixed(1)} TP vs No-Fav ${noFavTirosPuerta.toFixed(1)} TP).`;
            }
        } else {
            const favGf = favoritoRole === 'home' ? (data.similaresGfHomeAvg || 1.3) : (data.similaresGfAwayAvg || 1.3);
            const favGc = favoritoRole === 'home' ? (data.similaresGcHomeAvg || 1.1) : (data.similaresGcAwayAvg || 1.1);
            if (favGf >= favGc) {
                b.b5 = 1;
                bDet.b5 = `Proxy ofensivo: Favorito con balance positivo de goles (${favGf.toFixed(2)} GF >= ${favGc.toFixed(2)} GC). Despliega Stats para auditar tiros.`;
            } else {
                b.b5 = 0;
                bDet.b5 = `Proxy ofensivo: Favorito encaja más de lo que marca (${favGf.toFixed(2)} GF < ${favGc.toFixed(2)} GC).`;
            }
        }

        // b6: Solidez de Victoria Dinámica vs Fricción de Empate
        const favWinRate = favoritoRole === 'home' ? (data.homeWinRate || 0) : (data.awayWinRate || 0);
        if (favWinRate >= 0.38 && favEmpatesPct <= 0.40) {
            b.b6 = 1;
            bDet.b6 = `Favorito sólido (${Math.round(favWinRate * 100)}% victorias) con baja tasa de empates (${Math.round(favEmpatesPct * 100)}%).`;
        } else if (favWinRate >= 0.50) {
            b.b6 = 1;
            bDet.b6 = `Alta efectividad de victoria del Favorito (${Math.round(favWinRate * 100)}%).`;
        } else {
            b.b6 = 0;
            bDet.b6 = `Fricción de empate alta (${Math.round(favEmpatesPct * 100)}%) o victorias insuficientes (${Math.round(favWinRate * 100)}%).`;
        }

        // REGLAS DETERMINISTAS DE DECISIÓN DE HÁNDICAP
        let scoreAh = b.b1 + b.b2 + b.b3 + b.b4 + b.b5 + b.b6;
        let bloqueoAh = null;
        let veredictoAh = '';
        const muestraValidaParaBloqueo = (favSamplesActual >= 2 || noFavSamplesActual >= 2);

        // Bloqueos de correlación estricta
        if (b.b1 === 0 && b.b2 === 0 && corrActualFavoreceNoFav && corrH2HFavoreceNoFav && muestraValidaParaBloqueo) {
            bloqueoAh = 'BLOQUEO POR DOBLE CORRELACIÓN CONTRA FAVORITO';
            veredictoAh = `TRAMPA EN EL FAVORITO -> ENTRADA FUERTE AL NO-FAVORITO (+AH) [${noFavorito}] (${bloqueoAh})`;
        } else if (noFavHasCoverActual && coverNoFavActual === 100 && noFavSamplesActual >= 4 && b.b3 === 0) {
            bloqueoAh = 'BLOQUEO POR NO-FAVORITO 100% COVER EN HÁNDICAPS SIMILARES';
            veredictoAh = `ENTRADA FUERTE AL NO-FAVORITO (+AH) [${noFavorito}] (${bloqueoAh})`;
        } else if (b.b1 === 1 && b.b2 === 1 && noFavHasCoverActual && coverNoFavActual <= 33) {
            bloqueoAh = 'BLOQUEO INVERSO PRO-FAVORITO (PROHIBIDO NO-FAVORITO)';
            veredictoAh = `ENTRADA FUERTE AL FAVORITO [${favorito} ${lineDisplay}] (${bloqueoAh})`;
        }

        // --- PASO 6: LEY DE ACOPLAMIENTO CRUZADO GOLES -> HÁNDICAP (Filtro Anti-Gimcheon) ---
        let rupturaEmpatador = false;
        let acoplamientoCruzadoEstado = 'Normal';

        const tasaEmpatesNoFav = noFavoritoRole === 'home' ? (data.homeDrawRate || 0) : (data.awayDrawRate || 0);
        const gfSimFav = favoritoRole === 'home' ? (data.similaresGfHomeAvg || 0) : (data.similaresGfAwayAvg || 0);
        const gfSimNoFav = noFavoritoRole === 'home' ? (data.similaresGfHomeAvg || 0) : (data.similaresGfAwayAvg || 0);

        // Verificar si No-Favorito perdió su partido previo o tiene Cover 0% en Bloque 2 H2H
        const noFavPerdioPrevio = commonEdge.edgeTo === favoritoRole;
        const noFavCoverH2HCero = (coverNoFavH2H !== null && coverNoFavH2H === 0 && noFavSamplesH2H >= 2);

        // Condición de Ruptura del Empatador (Caso Gimcheon 1:2 Daejeon)
        if (scoreOu >= 5 && gfSimFav >= 2.0 && gfSimNoFav <= 1.35 && (noFavPerdioPrevio || noFavCoverH2HCero || tasaEmpatesNoFav >= 0.45)) {
            rupturaEmpatador = true;
            acoplamientoCruzadoEstado = 'Ruptura de Empatador por Techo Goleador';
            bloqueoAh = null; // Anula veto al favorito
            scoreAh = Math.min(6, scoreAh + 3); // Bonificación de +3 puntos al favorito
            veredictoAh = `FAVORITO -0.25 / -0.5 [${favorito} ${lineDisplay}] (Ruptura por Techo Goleador: Rival promedia GF >= 2.0 en partido Over)`;
        } else if (vetoAntiUnder) {
            acoplamientoCruzadoEstado = 'Veto Anti-Under por Pleno Ref';
        }

        if (!rupturaEmpatador && !bloqueoAh) {
            if (b.b1 === 1 || scoreAh >= 3) {
                veredictoAh = `ENTRADA AL FAVORITO [${favorito} ${lineDisplay}] en línea principal.`;
            } else {
                veredictoAh = `VALOR EN EL NO-FAVORITO [${noFavorito} ${lineDisplay}] (+AH).`;
            }
        }

        // --- PREDICCIONES OFICIALES Y APUESTAS RECOMENDADAS ---
        const pred = {
            ah: { pick: '', accion: '', confianza: 'ALTA', confClass: 'conf-alta', razon: '' },
            ou: { pick: '', accion: '', confianza: 'ALTA', confClass: 'conf-alta', razon: '' }
        };

        // 1. Predicción de Hándicap Asiático
        if (rupturaEmpatador) {
            pred.ah.pick = `${favorito} ${lineDisplay} (Favorito)`;
            pred.ah.accion = `Entrada Directa al Favorito [${favorito}]`;
            pred.ah.confianza = 'RUPTURA POR TECHO GOLEADOR (ALTA)';
            pred.ah.confClass = 'conf-bloqueo';
            pred.ah.razon = `Score OU ${scoreOu}/6. Ruptura de Empatador: ${noFavorito} (${Math.round(tasaEmpatesNoFav * 100)}% empates, GF ${gfSimNoFav.toFixed(2)}) no puede sostener el partido ante ${favorito} (GF ${gfSimFav.toFixed(2)}). Marcador proyectado: 1:2 o 1:3 a favor de ${favorito}.`;
        } else if (bloqueoAh) {
            if (bloqueoAh.includes('PRO-FAVORITO')) {
                pred.ah.pick = `${favorito} ${lineDisplay} (Favorito)`;
                pred.ah.accion = `Entrada Fuerte al Favorito [${favorito}]`;
                pred.ah.confianza = 'BLOQUEO PRO-FAVORITO (ALTA)';
                pred.ah.confClass = 'conf-bloqueo';
                pred.ah.razon = bloqueoAh;
            } else {
                pred.ah.pick = `${noFavorito} ${lineDisplay} (No-Favorito)`;
                pred.ah.accion = `Entrada Fuerte al No-Favorito (+AH) [${noFavorito}]`;
                pred.ah.confianza = 'BLOQUEO ANTI-SESGO (ALTA)';
                pred.ah.confClass = 'conf-bloqueo';
                pred.ah.razon = `${bloqueoAh}. Todo el valor está en ${noFavorito}.`;
            }
        } else if (b.b1 === 1 || scoreAh >= 3) {
            pred.ah.pick = `${favorito} ${lineDisplay} (Favorito)`;
            pred.ah.accion = `Apostar al Favorito [${favorito}]`;
            pred.ah.confianza = scoreAh >= 4 ? 'ALTA' : 'MEDIA-ALTA';
            pred.ah.confClass = 'conf-alta';
            pred.ah.razon = `Score AH ${scoreAh}/6. ${b.b1 === 1 ? 'Correlación favorable al favorito' : 'Mayoría de compuertas a favor'}.`;
        } else {
            pred.ah.pick = `${noFavorito} ${lineDisplay} (No-Favorito)`;
            pred.ah.accion = `Apostar al No-Favorito (+AH) [${noFavorito}]`;
            pred.ah.confianza = 'ALTA';
            pred.ah.confClass = 'conf-alta';
            pred.ah.razon = `Score AH ${scoreAh}/6. La correlación y las métricas otorgan el valor al no-favorito.`;
        }

        // 2. Predicción de Goles
        if (vetoAntiUnder) {
            pred.ou.pick = `Over ${rawOu} Goles (o Over ${(rawOu - 0.25).toFixed(2)} Live min 15)`;
            pred.ou.accion = `Entrada Over ${rawOu} / Live min 15`;
            pred.ou.confianza = 'VETO ANTI-UNDER (ALTA)';
            pred.ou.confClass = 'conf-bloqueo';
            pred.ou.razon = `Veto Anti-Under: ${partidosCon3MasGoles} de ${totalPartidosRef} (${Math.round(ratio3Mas * 100)}%) referencias con >= 3 goles. Mz = ${Mz} cuartos (${mediaGolesRef.toFixed(2)} goles) >= Gz + 3 (${Gz + 3}). Prohibido Under.`;
        } else if (g.g1 === 1) {
            pred.ou.pick = `Over ${rawOu} Goles`;
            pred.ou.accion = `Entrada Over ${rawOu} Pre o Live min 15`;
            pred.ou.confianza = 'AXIOMA 1+2=3 (OBLIGATORIO)';
            pred.ou.confClass = 'conf-bloqueo';
            pred.ou.razon = 'Piso matemático de 3 goles garantizado por gol proyectado del no-favorito.';
        } else if (scoreOu >= 5) {
            pred.ou.pick = `Over ${rawOu} Goles`;
            pred.ou.accion = `Entrada Directa a Over ${rawOu}`;
            pred.ou.confianza = 'ALTA';
            pred.ou.confClass = 'conf-alta';
            pred.ou.razon = `Score de goles ${scoreOu}/6 con alta densidad ofensiva y ruptura.`;
        } else if (scoreOu >= 3) {
            const lineDesc = Math.max(1.5, rawOu - 0.5);
            pred.ou.pick = `Over ${lineDesc} Goles (Live min 15-20)`;
            pred.ou.accion = `Esperar descuento en vivo si va 0:0`;
            pred.ou.confianza = 'MEDIA (Estrategia Live)';
            pred.ou.confClass = 'conf-media';
            pred.ou.razon = 'Línea de apertura algo inflada por la casa; aprovechar descuento en vivo.';
        } else {
            pred.ou.pick = `Under ${rawOu} Goles (o No Bet)`;
            pred.ou.accion = `Evitar Over pre-partido`;
            pred.ou.confianza = 'MEDIA';
            pred.ou.confClass = 'conf-media';
            pred.ou.razon = 'Bajo ritmo esperado y fricción defensiva en hándicaps similares.';
        }

        return {
            homeName,
            awayName,
            leagueName,
            date: matchDate,
            rawAh,
            rawOu,
            Hz,
            Gz,
            absHz,
            favorito,
            favoritoRole,
            noFavorito,
            noFavoritoRole,
            trampaDetectada,
            trampaDetalle,
            b,
            bDet,
            scoreAh,
            bloqueoAh,
            veredictoAh,
            g,
            gDet,
            scoreOu,
            veredictoOu,
            pred,
            acoplamientoCruzadoEstado,
            vetoAntiUnder,
            rupturaEmpatador,
            refAudit,
            hasDetailedStats: data.hasDetailedStats,
            openStatsCount: data.openStatsCount || 0
        };
    }

    /**
     * Extrae datos del DOM y del texto de contexto para armar matchData en caliente
     */
    function extractMatchDataFromDOM(panel, matchId, promptText = '') {
        const copyBtn = panel.querySelector('.pre-context-copy');
        const standingsBtn = panel.querySelector('.league-table-trigger');

        const homeName = standingsBtn?.dataset?.homeName || panel.querySelector('.highlight-home')?.innerText?.trim() || 'Local';
        const awayName = standingsBtn?.dataset?.awayName || panel.querySelector('.highlight-away')?.innerText?.trim() || 'Visitante';
        const leagueName = standingsBtn?.dataset?.leagueName || panel.dataset?.currentLeague || 'Liga';
        const date = standingsBtn?.dataset?.matchDate || '';
        let handicap = standingsBtn?.dataset?.handicap || '0';
        const hcPromptMatch = promptText.match(/(?:H[aá]ndicap\s+Inicial|\*\*H[aá]ndicap\s+Inicial\*\*)[^\n:]*:\s*([0-9.-]+)/i);
        if (hcPromptMatch) {
            handicap = (hcPromptMatch[1] || '').trim();
        }
        const goalLine = standingsBtn?.dataset?.goalLine || '2.5';

        const rawContext = decodeURIComponent(copyBtn?.dataset?.context || '');
        const fullText = `${rawContext}\n${promptText}`;

        // 1. Extraer hándicaps similares con validación estricta de muestra y detección de N/D
        const extractSimilares = (block) => {
            const res = {
                homeCover: null,
                awayCover: null,
                homeLosses: 0,
                awayLosses: 0,
                homeSamples: 0,
                awaySamples: 0,
                homeGf: null,
                homeGc: null,
                awayGf: null,
                awayGc: null,
                hasValidHomeCover: false,
                hasValidAwayCover: false,
                label: ''
            };
            if (!block) return res;

            const sampleMatches = [...block.matchAll(/(?:CASA|FUERA|EQUIPO\s*\d+)[^:]*:\s*(\d+)\s+partidos/gi)];
            if (sampleMatches.length >= 2) {
                res.homeSamples = parseNum(sampleMatches[0][1], 0);
                res.awaySamples = parseNum(sampleMatches[1][1], 0);
            }

            const coverMatches = [...block.matchAll(/Cover\s+(\d+)%/g)];
            if (coverMatches.length >= 2) {
                if (res.homeSamples >= 2) {
                    res.homeCover = parseNum(coverMatches[0][1], 50);
                    res.hasValidHomeCover = true;
                }
                if (res.awaySamples >= 2) {
                    res.awayCover = parseNum(coverMatches[1][1], 50);
                    res.hasValidAwayCover = true;
                }
            }

            const vedMatches = [...block.matchAll(/V\s+(\d+)\s+E\s+(\d+)\s+D\s+(\d+)/g)];
            if (vedMatches.length >= 2) {
                res.homeLosses = parseNum(vedMatches[0][3], 0);
                res.awayLosses = parseNum(vedMatches[1][3], 0);
            }

            // Parsear GF y GC (cuidando N/D)
            const gfgcMatches = [...block.matchAll(/GF\s+([0-9.]+|N\/D)\s+GC\s+([0-9.]+|N\/D)/gi)];
            if (gfgcMatches.length >= 2) {
                const hGf = parseFloat(gfgcMatches[0][1]);
                const hGc = parseFloat(gfgcMatches[0][2]);
                const aGf = parseFloat(gfgcMatches[1][1]);
                const aGc = parseFloat(gfgcMatches[1][2]);

                if (Number.isFinite(hGf) && Number.isFinite(hGc)) {
                    res.homeGf = hGf;
                    res.homeGc = hGc;
                }
                if (Number.isFinite(aGf) && Number.isFinite(aGc)) {
                    res.awayGf = aGf;
                    res.awayGc = aGc;
                }
            }

            const corrMatch = block.match(/CORRELACI[OÓ]N:\s*([^·\n]+)/i);
            if (corrMatch && corrMatch[1]) res.label = corrMatch[1].trim();
            return res;
        };

        const contextSplit = rawContext.split('ÚLTIMO ENFRENTAMIENTO');
        const currentBlock = contextSplit[0] || '';
        const h2hBlock = contextSplit[1] || '';

        const simActual = extractSimilares(currentBlock);
        const simH2h = extractSimilares(h2hBlock);

        // 2. Parseo de partidos del historial con inversión de signo en FUERA
        const { homeMatches, awayMatches } = parseMatchesFromContextText(rawContext);
        const rawAh = parseNum(handicap, 0);
        const favoritoRole = rawAh < 0 ? 'away' : 'home';
        const absHz = Math.abs(Math.round(rawAh * 4));
        const triangulacionB3 = calculateTriangulationB3(homeMatches, awayMatches, favoritoRole, absHz);

        // 3. Detección en vivo de fichas de estadísticas abiertas (.match-stats-slot)
        const openStatsSlots = panel.querySelectorAll('.match-stats-slot, .pre-card-stats-container');
        let openStatsCount = 0;
        let shotsHome = 0, shotsAway = 0, stHome = 0, stAway = 0, apHome = 0, apAway = 0;
        let statsRowsFound = 0;

        openStatsSlots.forEach(slot => {
            if (slot.style.display !== 'none' && slot.innerHTML.trim().length > 30) {
                openStatsCount++;
                slot.querySelectorAll('table tbody tr, .modern-match-stats-card div').forEach(row => {
                    const cells = row.querySelectorAll('td, span');
                    if (cells.length >= 3) {
                        const v1 = parseNum(cells[0].innerText);
                        const label = cells[1].innerText.toLowerCase();
                        const v2 = parseNum(cells[cells.length - 1].innerText);

                        if (label.includes('tiros a puerta') || label.includes('shots on target')) {
                            stHome += v1; stAway += v2; statsRowsFound++;
                        } else if (label.includes('ataques peligrosos') || label.includes('dangerous')) {
                            apHome += v1; apAway += v2; statsRowsFound++;
                        } else if (label.includes('tiros') || label.includes('shots')) {
                            shotsHome += v1; shotsAway += v2;
                        }
                    }
                });
            }
        });

        // 4. Si no hay en DOM, intentar parsear desde el prompt exportado
        if (statsRowsFound === 0) {
            const parsePromptStats = (txt) => {
                const stM = [...txt.matchAll(/Tiros\s+a\s+puerta:\s*(\d+)\s*-\s*(\d+)/gi)];
                stM.forEach(m => { stHome += parseNum(m[1]); stAway += parseNum(m[2]); statsRowsFound++; });
                const apM = [...txt.matchAll(/Ataques\s+peligrosos:\s*(\d+)\s*-\s*(\d+)/gi)];
                apM.forEach(m => { apHome += parseNum(m[1]); apAway += parseNum(m[2]); statsRowsFound++; });
                const shM = [...txt.matchAll(/Tiros\s+totales:\s*(\d+)\s*-\s*(\d+)/gi)];
                shM.forEach(m => { shotsHome += parseNum(m[1]); shotsAway += parseNum(m[2]); });
            };
            parsePromptStats(fullText);
        }

        const hasDetailedStats = openStatsCount > 0 || statsRowsFound > 0;
        const nStats = Math.max(1, Math.ceil(statsRowsFound / 2));

        // 5. Parsear puestos en la tabla desde prompt
        let homePos = null;
        let awayPos = null;
        const posHomeMatch = promptText.match(new RegExp(`${homeName}[^:]*:\s*Puesto\s*(\d+)`, 'i'));
        if (posHomeMatch) homePos = parseNum(posHomeMatch[1]);
        const posAwayMatch = promptText.match(new RegExp(`${awayName}[^:]*:\s*Puesto\s*(\d+)`, 'i'));
        if (posAwayMatch) awayPos = parseNum(posAwayMatch[1]);

        // 6. Extracción dinámica real de Victorias y Empates
        let homeWinRate = 0.50;
        let awayWinRate = 0.50;
        let homeDrawRate = 0.25;
        let awayDrawRate = 0.25;
        let localWinsStreak = 0;

        const resumenMatches = [...rawContext.matchAll(/Resumen:\s*V\s*(\d+)\s*\|\s*E\s*(\d+)\s*\|\s*D\s*(\d+)/gi)];
        if (resumenMatches.length >= 1) {
            const v1 = parseNum(resumenMatches[0][1], 0);
            const e1 = parseNum(resumenMatches[0][2], 0);
            const d1 = parseNum(resumenMatches[0][3], 0);
            const t1 = v1 + e1 + d1;
            if (t1 > 0) {
                homeWinRate = v1 / t1;
                homeDrawRate = e1 / t1;
            }
        }
        if (resumenMatches.length >= 2) {
            const v2 = parseNum(resumenMatches[1][1], 0);
            const e2 = parseNum(resumenMatches[1][2], 0);
            const d2 = parseNum(resumenMatches[1][3], 0);
            const t2 = v2 + e2 + d2;
            if (t2 > 0) {
                awayWinRate = v2 / t2;
                awayDrawRate = e2 / t2;
            }
        }

        const stdRegex = /\((?:Stats general|Stats):\s*(\d+)-(\d+)-(\d+)\)/gi;
        const stdMatches = [...promptText.matchAll(stdRegex)];
        if (stdMatches.length >= 1 && resumenMatches.length === 0) {
            const v = parseNum(stdMatches[0][1], 0);
            const e = parseNum(stdMatches[0][2], 0);
            const d = parseNum(stdMatches[0][3], 0);
            const t = v + e + d;
            if (t > 0) {
                homeWinRate = v / t;
                homeDrawRate = e / t;
            }
        }
        if (stdMatches.length >= 2 && resumenMatches.length < 2) {
            const v = parseNum(stdMatches[1][1], 0);
            const e = parseNum(stdMatches[1][2], 0);
            const d = parseNum(stdMatches[1][3], 0);
            const t = v + e + d;
            if (t > 0) {
                awayWinRate = v / t;
                awayDrawRate = e / t;
            }
        }

        return {
            homeName,
            awayName,
            leagueName,
            date,
            handicap,
            goalLine,
            homePos,
            awayPos,
            homeWinRate,
            awayWinRate,
            homeDrawRate,
            awayDrawRate,
            localWinsStreak,
            localAvgShots: shotsHome / nStats || 8,
            localShotEff: 0.40,
            bothHighScoringRecent: /3:3|3:0|2:4|4:1|4:2/.test(rawContext),
            refGoalsAvg: 2.6,
            similaresActualHomeCover: simActual.homeCover,
            similaresActualAwayCover: simActual.awayCover,
            similaresActualHomeHasCover: simActual.hasValidHomeCover,
            similaresActualAwayHasCover: simActual.hasValidAwayCover,
            similaresActualHomeSamples: simActual.homeSamples,
            similaresActualAwaySamples: simActual.awaySamples,
            similaresActualAwayLosses: simActual.awayLosses,
            similaresGfHomeAvg: simActual.homeGf,
            similaresGcHomeAvg: simActual.homeGc,
            similaresGfAwayAvg: simActual.awayGf,
            similaresGcAwayAvg: simActual.awayGc,
            correlacionActualTexto: simActual.label,
            similaresH2HHomeCover: simH2h.homeCover,
            similaresH2HAwayCover: simH2h.awayCover,
            similaresH2HHomeHasCover: simH2h.hasValidHomeCover,
            similaresH2HAwayHasCover: simH2h.hasValidAwayCover,
            similaresH2HHomeSamples: simH2h.homeSamples,
            similaresH2HAwaySamples: simH2h.awaySamples,
            similaresH2HGfHomeAvg: simH2h.homeGf,
            similaresH2HGcHomeAvg: simH2h.homeGc,
            similaresH2HGfAwayAvg: simH2h.awayGf,
            similaresH2HGcAwayAvg: simH2h.awayGc,
            correlacionH2HTexto: simH2h.label,
            triangulacionB3,
            homeMatches,
            awayMatches,
            promptText,
            rawContext,
            lastH2HFavWonAndCovered: !rawContext.includes('NO CUBRIÓ'),
            hasDetailedStats,
            openStatsCount,
            statsHomeTirosPuerta: stHome / nStats,
            statsAwayTirosPuerta: stAway / nStats,
            statsHomeAp: apHome / nStats,
            statsAwayAp: apAway / nStats,
            statsHomeTiros: shotsHome / nStats,
            statsAwayTiros: shotsAway / nStats,
            efectoPresaFav: (stHome / nStats >= 8 && apHome / nStats >= 50),
            favMargin2GolesRate: 0.35,
            noFavMarcaEnCondicionRate: 0.80,
            noFavSeQuedoEnCeroH2H: false,
            favMarca2PlusRate: 0.80,
            noFavMarcaRate: 0.75,
            bttsRateRecent: 0.70,
            statsConversionRate: 0.32,
            highGoals4PlusRate: 0.45,
        };
    }

    /**
     * Construye la plantilla de inyección en el prompt LLM con auditoría completa v5.0
     */
    function buildPromptInjectionTemplate(res) {
        return `
========================================================================
PREDICCIONES OFICIALES DEL MOTOR v5.0 (APUESTAS RECOMENDADAS)
========================================================================
🎯 PICK HÁNDICAP ASIÁTICO: [${res.pred.ah.pick}]
   * Acción: ${res.pred.ah.accion}
   * Nivel de Confianza: ${res.pred.ah.confianza}
   * Fundamento Cuantitativo: ${res.pred.ah.razon}

⚽ PICK LÍNEA DE GOLES (O/U): [${res.pred.ou.pick}]
   * Acción: ${res.pred.ou.accion}
   * Nivel de Confianza: ${res.pred.ou.confianza}
   * Fundamento Cuantitativo: ${res.pred.ou.razon}

========================================================================
AUDITORÍA MATEMÁTICA DETALLADA — MOTOR v5.0 (INVIOLABLE)
========================================================================
- Conversión Entera: Hándicap (Hz) = ${res.Hz} enteros | Línea de Goles (Gz) = ${res.Gz} enteros
- Favorito de la Línea (NowGoal): ${res.favorito} | No-Favorito (+AH): ${res.noFavorito}
- Trampa Psicológica Detectada: ${res.trampaDetectada} (${res.trampaDetalle})
- Acoplamiento Cruzado (AH <-> OU): [${res.acoplamientoCruzadoEstado}]
${!res.hasDetailedStats ? '- AVISO: Fichas Stats no abiertas en la tarjeta; compuertas b5, g5 y g6 operan con proxy/resguardo.' : ''}

[MÓDULO A: HÁNDICAP ASIÁTICO — Score_AH: ${res.scoreAh}/6]
- b1 (Correlación Actual Similares): [${res.b.b1}] -> ${res.bDet.b1}
- b2 (Correlación Histórica H2H):    [${res.b.b2}] -> ${res.bDet.b2}
- b3 (Triangulación Rivales Comunes):[${res.b.b3}] -> ${res.bDet.b3}
- b4 (Memoria H2H + Tabla):          [${res.b.b4}] -> ${res.bDet.b4}
- b5 (Volumen Filtrado Mismo Rol):   [${res.b.b5}] -> ${res.bDet.b5}
- b6 (Filtro Empate / Margen):       [${res.b.b6}] -> ${res.bDet.b6}
>>> SEÑAL OBLIGATORIA HÁNDICAP: ${res.veredictoAh}

[MÓDULO B: LÍNEA DE GOLES O/U — Score_OU: ${res.scoreOu}/6]
- g1 (Axioma Piso 1+2=3):            [${res.g.g1}] -> ${res.gDet.g1}
- g2 (Media Entera Referencias Mz):  [${res.g.g2}] -> ${res.gDet.g2}
- g3 (Fusión Ponderada Similares):   [${res.g.g3}] -> ${res.gDet.g3}
- g4 (Ruptura Unilateral/Bilateral): [${res.g.g4}] -> ${res.gDet.g4}
- g5 (Ritmo Ataques Peligrosos>=100):[${res.g.g5}] -> ${res.gDet.g5}
- g6 (Tiros>=22 o Conversión>=35%):  [${res.g.g6}] -> ${res.gDet.g6}
>>> SEÑAL OBLIGATORIA GOLES: ${res.veredictoOu}

INSTRUCCIÓN ESTRICTA PARA EL LLM:
Redacta el análisis táctico y matemático de este partido obedeciendo al 100% los resultados de los vectores [b1..b6] y [g1..g6] precalculados arriba por el Motor v5.0. Tienes ESTRICTAMENTE PROHIBIDO contradecir la SEÑAL OBLIGATORIA de Hándicap o de Goles, inventar narrativas que ignoren la Doble Correlación de Hándicaps Similares, o recomendar un Under si la compuerta g1 vale 1 o si rige el Veto Anti-Under por Pleno de Referencias. Explica con precisión quirúrgica dónde está el engaño de la casa de apuestas y entrega la orden operativa final (Pre-partido y Live).`;
    }

    /**
     * Renderiza el Modal / Pop-Up del Motor Cuantitativo v5.0
     */
    function showMotorV4Modal(res, combinedPromptText, matchId) {
        let modalEl = document.getElementById('motorV4ModalContainer');
        if (!modalEl) {
            modalEl = document.createElement('div');
            modalEl.id = 'motorV4ModalContainer';
            document.body.appendChild(modalEl);
        }

        const renderGate = (code, val, name, det) => `
            <div class="motor-gate-item ${val === 1 ? 'gate-pass' : 'gate-fail'}">
                <span class="gate-badge ${val === 1 ? 'pass' : 'fail'}">${code} = ${val}</span>
                <div class="gate-info">
                    <div class="gate-top-row">
                        <span class="gate-id">${code}</span>
                        <span class="gate-name">${esc(name)}</span>
                    </div>
                    <div class="gate-detail">${esc(det)}</div>
                </div>
            </div>
        `;

        modalEl.innerHTML = `
            <div class="motor-v4-overlay" id="motorV4Overlay" onclick="if(event.target===this) window.closeMotorV4Modal()">
                <div class="motor-v4-card" role="dialog" aria-modal="true">
                    <!-- HEADER -->
                    <div class="motor-v4-header">
                        <div class="d-flex align-items-center gap-3">
                            <span class="motor-icon-badge"><i class="fa-solid fa-brain"></i></span>
                            <div class="motor-v4-title">
                                <div class="motor-v4-title-row">
                                    <h5 class="motor-v4-match-name">Motor Cuantitativo Dual Entero-Binario v5.0</h5>
                                    <span class="motor-v4-badge">Acoplamiento Cruzado Anti-Sesgos</span>
                                </div>
                                <div class="motor-v4-meta-sub">
                                    <span><i class="fa-solid fa-futbol me-1 text-info"></i>${esc(res.homeName)} vs ${esc(res.awayName)}</span>
                                    <span><i class="fa-solid fa-trophy me-1 text-warning"></i>${esc(res.leagueName)}</span>
                                    <span class="badge bg-secondary">Hz: ${res.Hz} (${res.absHz}/4)</span>
                                    <span class="badge bg-secondary">Gz: ${res.Gz} (${res.Gz}/4)</span>
                                    <span class="badge ${res.favoritoRole === 'home' ? 'bg-primary' : 'bg-info'} text-white">Fav: ${esc(res.favorito)}</span>
                                    <span class="badge bg-success text-white">Acoplamiento: ${esc(res.acoplamientoCruzadoEstado)}</span>
                                </div>
                            </div>
                        </div>
                        <button type="button" class="motor-v4-close" onclick="window.closeMotorV4Modal()" aria-label="Cerrar"><i class="fa-solid fa-xmark"></i></button>
                    </div>

                    <!-- BODY SCROLLABLE -->
                    <div class="motor-v4-body">
                        <!-- ALERTA FICHAS STATS CERRADAS (SI APLICA) -->
                        ${!res.hasDetailedStats ? `
                            <div class="motor-stats-warning-banner">
                                <div class="d-flex align-items-center justify-content-between flex-wrap gap-2">
                                    <div>
                                        <i class="fa-solid fa-triangle-exclamation text-warning me-2 fs-5"></i>
                                        <strong>⚠️ Abre primero las fichas verdes de Stats del partido y vuelve a pulsar el botón para incluir Tiros y Ataques Peligrosos</strong>
                                        <div class="small text-secondary mt-1">Las compuertas b5, g5 y g6 operan actualmente con proxy/resguardo de goles en hándicaps similares.</div>
                                    </div>
                                    <button type="button" class="btn btn-sm btn-warning fw-bold text-dark px-3" onclick="window.autoOpenStatsAndRecalculate(this, '${matchId}')">
                                        <i class="fa-solid fa-bolt me-1"></i> Abrir Stats y Recalcular
                                    </button>
                                </div>
                            </div>
                        ` : ''}

                        <!-- ================================================================= -->
                        <!-- PREDICCIONES OFICIALES Y APUESTAS RECOMENDADAS (HERO PRINCIPAL) -->
                        <!-- ================================================================= -->
                        <div class="motor-predictions-hero">
                            <div class="predictions-hero-header">
                                <span class="predictions-hero-title">
                                    <i class="fa-solid fa-crosshairs text-warning fs-5"></i>
                                    PREDICCIONES DEL MOTOR v5.0 (APUESTAS RECOMENDADAS)
                                </span>
                                <span class="predictions-hero-badge">Recomendación Oficial</span>
                            </div>

                            <div class="predictions-cards-grid">
                                <!-- TARJETA HÁNDICAP -->
                                <div class="prediction-card card-ah">
                                    <div class="pred-card-header">
                                        <span class="pred-type-label"><i class="fa-solid fa-shield-halved me-1 text-primary"></i>MERCADO HÁNDICAP ASIÁTICO</span>
                                        <span class="pred-conf-badge ${res.pred.ah.confClass}">${esc(res.pred.ah.confianza)}</span>
                                    </div>
                                    <div class="pred-main-pick">${esc(res.pred.ah.pick)}</div>
                                    <div class="pred-action-box">
                                        <span class="action-tag">Orden:</span>
                                        <strong class="action-text">${esc(res.pred.ah.accion)}</strong>
                                    </div>
                                    <div class="pred-reason-box">
                                        <i class="fa-solid fa-brain me-1 text-secondary"></i>
                                        <span>${esc(res.pred.ah.razon)}</span>
                                    </div>
                                </div>

                                <!-- TARJETA GOLES -->
                                <div class="prediction-card card-ou">
                                    <div class="pred-card-header">
                                        <span class="pred-type-label"><i class="fa-solid fa-futbol me-1 text-warning"></i>MERCADO LÍNEA DE GOLES (O/U)</span>
                                        <span class="pred-conf-badge ${res.pred.ou.confClass}">${esc(res.pred.ou.confianza)}</span>
                                    </div>
                                    <div class="pred-main-pick">${esc(res.pred.ou.pick)}</div>
                                    <div class="pred-action-box">
                                        <span class="action-tag">Orden:</span>
                                        <strong class="action-text">${esc(res.pred.ou.accion)}</strong>
                                    </div>
                                    <div class="pred-reason-box">
                                        <i class="fa-solid fa-chart-line me-1 text-secondary"></i>
                                        <span>${esc(res.pred.ou.razon)}</span>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- BANNER TRAMPA DE LA CASA -->
                        <div class="motor-trap-banner ${res.trampaDetectada !== 'NINGUNA_TRAMPA_EVIDENTE' ? 'trap-active' : 'trap-none'}">
                            <div class="trap-icon"><i class="fa-solid fa-triangle-exclamation"></i></div>
                            <div class="trap-content">
                                <div class="trap-title">Trampa Detectada: <span>${esc(res.trampaDetectada)}</span></div>
                                <div class="trap-desc">${esc(res.trampaDetalle)}</div>
                            </div>
                        </div>

                        <!-- LÍNEA DE ACOPLAMIENTO CRUZADO -->
                        <div class="motor-summary-banner" style="margin-bottom: 1rem; padding: 0.75rem 1rem; background: #0f172a; border-left: 4px solid #10b981; border-radius: 6px;">
                            <div class="d-flex align-items-center justify-content-between flex-wrap gap-2">
                                <div>
                                    <strong style="color: #34d399;"><i class="fa-solid fa-link me-2"></i>Acoplamiento Cruzado (AH <-> OU):</strong>
                                    <span style="color: #f1f5f9; font-weight: 600; margin-left: 0.5rem;">[${esc(res.acoplamientoCruzadoEstado)}]</span>
                                </div>
                                <div class="small text-secondary">
                                    Referencias 3+ goles: <strong>${res.refAudit?.partidosCon3MasGoles || 0}/${res.refAudit?.totalPartidosRef || 0}</strong> | Mz = ${res.refAudit?.Mz || 0} vs Gz = ${res.Gz}
                                </div>
                            </div>
                        </div>

                        <!-- SECCIÓN COMPUERTAS HÁNDICAP -->
                        <div class="motor-section">
                            <div class="motor-section-header">
                                <div class="d-flex align-items-center gap-2">
                                    <i class="fa-solid fa-shield-halved text-primary"></i>
                                    <h6 class="mb-0">Módulo A: Compuertas de Hándicap Asiático</h6>
                                </div>
                                <div class="d-flex align-items-center gap-2">
                                    <span class="motor-score-pill">Score AH: ${res.scoreAh} / 6</span>
                                    ${res.bloqueoAh ? `<span class="badge bg-danger text-white">${esc(res.bloqueoAh)}</span>` : ''}
                                    ${res.rupturaEmpatador ? `<span class="badge bg-warning text-dark">Ruptura Empatador</span>` : ''}
                                </div>
                            </div>
                            <div class="motor-gates-grid">
                                ${renderGate('b1', res.b.b1, 'Correlación Actual Similares', res.bDet.b1)}
                                ${renderGate('b2', res.b.b2, 'Correlación Histórica H2H', res.bDet.b2)}
                                ${renderGate('b3', res.b.b3, 'Triangulación Rivales Comunes', res.bDet.b3)}
                                ${renderGate('b4', res.b.b4, 'Memoria H2H + Tabla', res.bDet.b4)}
                                ${renderGate('b5', res.b.b5, 'Volumen Filtrado Mismo Rol', res.bDet.b5)}
                                ${renderGate('b6', res.b.b6, 'Filtro Empate / Margen', res.bDet.b6)}
                            </div>
                            <div class="motor-verdict-box">
                                <span class="verdict-label">Dictamen Operativo Hándicap:</span>
                                <span class="verdict-val">${esc(res.veredictoAh)}</span>
                            </div>
                        </div>

                        <!-- SECCIÓN COMPUERTAS GOLES -->
                        <div class="motor-section">
                            <div class="motor-section-header">
                                <div class="d-flex align-items-center gap-2">
                                    <i class="fa-solid fa-futbol text-warning"></i>
                                    <h6 class="mb-0">Módulo B: Compuertas de Línea de Goles (O/U)</h6>
                                </div>
                                <div class="d-flex align-items-center gap-2">
                                    <span class="motor-score-pill">Score O/U: ${res.scoreOu} / 6</span>
                                    ${res.vetoAntiUnder ? `<span class="badge bg-danger text-white">Veto Anti-Under</span>` : ''}
                                </div>
                            </div>
                            <div class="motor-gates-grid">
                                ${renderGate('g1', res.g.g1, 'Axioma Piso 1+2=3', res.gDet.g1)}
                                ${renderGate('g2', res.g.g2, 'Media Entera Referencias Mz', res.gDet.g2)}
                                ${renderGate('g3', res.g.g3, 'Fusión Ponderada Similares', res.gDet.g3)}
                                ${renderGate('g4', res.g.g4, 'Ruptura Unilateral/Bilateral', res.gDet.g4)}
                                ${renderGate('g5', res.g.g5, 'Ritmo Peligro Combinado', res.gDet.g5)}
                                ${renderGate('g6', res.g.g6, 'Densidad Disparos / Conversión', res.gDet.g6)}
                            </div>
                            <div class="motor-verdict-box">
                                <span class="verdict-label">Dictamen Operativo Goles:</span>
                                <span class="verdict-val">${esc(res.veredictoOu)}</span>
                            </div>
                        </div>
                    </div>

                    <!-- FOOTER ACTIONS -->
                    <div class="motor-v4-footer">
                        <div class="small text-secondary">
                            <i class="fa-solid fa-circle-check text-success me-1"></i> Auditoría calculada determinísticamente sin sesgos.
                        </div>
                        <div class="d-flex gap-2">
                            <button type="button" class="btn btn-outline-secondary btn-sm fw-bold px-3" onclick="window.closeMotorV4Modal()">
                                Cerrar
                            </button>
                            <button type="button" class="btn btn-primary btn-sm fw-bold px-3" id="btnCopyMotorV4Prompt" onclick="window.copyMotorV4AuditedPrompt('${matchId}')">
                                <i class="fa-regular fa-copy me-1"></i> Copiar Prompt Auditado v5.0
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        `;

        window._lastMotorV4Prompt = combinedPromptText;
        window._lastMotorV4MatchId = matchId;
    }

    function showResearchModal(result, match, promptText) {
        let modal = document.getElementById('motorV4ModalContainer');
        if (!modal) {
            modal = document.createElement('div');
            modal.id = 'motorV4ModalContainer';
            document.body.appendChild(modal);
        }

        const esc = s => String(s ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&#039;');
        const dec = (v, d = 2) => Number.isFinite(v) ? Number(v).toFixed(d) : 'N/D';
        const pct = v => Number.isFinite(v) ? (100 * v).toFixed(1) + '%' : 'N/D';

        const matchId = match?.id || window._lastMotorV4MatchId || '';
        const homeName = match?.home || 'Local';
        const awayName = match?.away || 'Visitante';
        const leagueName = match?.league || '';

        // Picks y puntuaciones v6.2
        const ahPick = result.ah_pick || {};
        const ouPick = result.ou_pick || {};
        const scoreAh = result.score_ah || {};
        const scoreOu = result.score_ou || {};
        const bGates = result.b || [];
        const gGates = result.g || [];
        const patterns = result.patterns || [];
        const policies = result.policy_trace || [];
        const shotProj = result.shot_projection || {};
        const sha256 = result.input_sha256 || '';

        // Formateo del Pick AH
        let ahTeamText = 'N/D';
        if (ahPick.market === 'AH_HOME') {
            ahTeamText = `Local (${homeName})`;
        } else if (ahPick.market === 'AH_AWAY') {
            ahTeamText = `Visitante (${awayName})`;
        }
        const ahLineText = ahPick.line !== undefined ? (ahPick.line > 0 ? '+' + ahPick.line : ahPick.line) : 'N/D';
        const ahOddsText = ahPick.quote ? `Cuota @${dec(ahPick.quote.odds, 2)}` : (ahPick.quote_status === 'QUOTED' ? 'Cotizado' : 'Línea objetivo por cotizar');

        // Formateo del Pick O/U
        const ouSideText = ouPick.market === 'OVER' ? 'OVER (Más de)' : (ouPick.market === 'UNDER' ? 'UNDER (Menos de)' : 'N/D');
        const ouLineText = ouPick.line !== undefined ? ouPick.line : 'N/D';
        const ouOddsText = ouPick.quote ? `Cuota @${dec(ouPick.quote.odds, 2)}` : (ouPick.quote_status === 'QUOTED' ? 'Cotizado' : 'Línea objetivo por cotizar');

        // Renderizado de compuertas
        const gateDescMap = {
            'b1': 'Apoyo estructural del no favorito',
            'b2': 'Correlación histórica o favorito invertido',
            'b3': 'Brecha de mercado y rendimiento en cruces',
            'b4': 'Último H2H ganado y cubierto',
            'b5': 'Dominio SOT/AP o efecto presa',
            'b6': 'Veto por empates o margen de un gol',
            'g1': 'Escenario 1+2=3 con favorito proyectado',
            'g2': 'Media de referencias únicas contra la línea',
            'g3': 'Fusión de bloques o desplazamiento de línea',
            'g4': 'Ruptura unilateral o bilateral',
            'g5': 'Suma combinada de ataques peligrosos (AP >= 100)',
            'g6': '22 tiros combinados o conversión >= 35%'
        };

        const renderGateList = (gates) => {
            return gates.map(g => {
                const bitClass = g.bit === 1 ? 'badge bg-success' : (g.available ? 'badge bg-secondary' : 'badge bg-dark text-muted');
                const bitLabel = g.bit === 1 ? '1 (Cumple)' : (g.available ? '0 (No cumple)' : '0 (Sin datos)');
                const desc = gateDescMap[g.id] || g.id;
                return `
                    <div style="background:#1e293b; border:1px solid #334155; border-radius:6px; padding:8px 10px; margin-bottom:6px;">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px;">
                            <strong style="color:#e2e8f0; font-size:.82rem;">${g.id.toUpperCase()}: ${esc(desc)}</strong>
                            <span class="${bitClass}" style="font-size:.72rem;">${bitLabel}</span>
                        </div>
                        <div style="color:#94a3b8; font-size:.75rem;">${esc(g.reason)}</div>
                    </div>
                `;
            }).join('');
        };

        // Telemetría de remates (proyección)
        let projectionHtml = '';
        if (shotProj.favorite && shotProj.underdog) {
            projectionHtml = `
                <div style="background:#0f172a; border:1px solid #334155; border-radius:8px; padding:12px; margin-top:12px;">
                    <h6 style="color:#38bdf8; font-size:.85rem; margin-bottom:8px;"><i class="fa-solid fa-calculator me-1"></i> Proyección de Remates y Goles (Regla de Tres)</h6>
                    <div class="row g-2 text-center" style="font-size:.78rem;">
                        <div class="col-4">
                            <div style="color:#94a3b8;">Favorito Proyectado</div>
                            <div style="font-weight:700; color:#f8fafc; font-size:.9rem;">${dec(shotProj.favorite.goals_proxy, 2)} goles</div>
                            <div style="color:#64748b; font-size:.7rem;">(${dec(shotProj.favorite.sot, 1)} TP · ${pct(shotProj.favorite.conversion)})</div>
                        </div>
                        <div class="col-4">
                            <div style="color:#94a3b8;">No Favorito Proyectado</div>
                            <div style="font-weight:700; color:#f8fafc; font-size:.9rem;">${dec(shotProj.underdog.goals_proxy, 2)} goles</div>
                            <div style="color:#64748b; font-size:.7rem;">(${dec(shotProj.underdog.sot, 1)} TP · ${pct(shotProj.underdog.conversion)})</div>
                        </div>
                        <div class="col-4">
                            <div style="color:#94a3b8;">Total Proyectado / Margen</div>
                            <div style="font-weight:700; color:#a855f7; font-size:.9rem;">${dec(shotProj.total_proxy, 2)} goles</div>
                            <div style="color:#64748b; font-size:.7rem;">Margen: ${dec(shotProj.margin_proxy, 2)}</div>
                        </div>
                    </div>
                </div>
            `;
        }

        // Patrones y Políticas Chips
        const patternChips = patterns.length ? patterns.map(p => `<span class="badge bg-warning text-dark me-1 mb-1" style="font-size:.72rem;">⚠️ ${esc(p)}</span>`).join('') : '<span style="color:#64748b; font-size:.75rem;">Ningún sesgo de fricción activo</span>';
        const policyChips = policies.length ? policies.map(p => `<span class="badge bg-info text-dark me-1 mb-1" style="font-size:.72rem;">⚖️ ${esc(p)}</span>`).join('') : '<span style="color:#64748b; font-size:.75rem;">Resolución estándar por compuertas</span>';

        modal.innerHTML = `
            <div class="motor-v4-overlay" onclick="if(event.target===this) window.closeMotorV4Modal()">
                <div class="motor-v4-card" role="dialog" aria-modal="true" style="max-width:880px; width:95%; background:#0b1120; color:#e2e8f0; border:1px solid #1e293b; border-radius:12px; box-shadow:0 25px 50px -12px rgba(0,0,0,0.5);">
                    <div class="motor-v4-header" style="border-bottom:1px solid #1e293b; padding:1rem 1.25rem; display:flex; justify-content:space-between; align-items:flex-start;">
                        <div>
                            <div style="display:flex; align-items:center; gap:8px;">
                                <h5 style="margin:0; font-weight:800; color:#f8fafc; font-size:1.1rem;"><i class="fa-solid fa-brain" style="color:#7c3aed;"></i> MOTOR DUAL ENTERO-BINARIO v6.2 OPERATIVO</h5>
                                <span class="badge" style="background:#4338ca; color:#e0e7ff; font-size:.7rem;">12 Compuertas</span>
                            </div>
                            <div style="color:#94a3b8; font-size:.82rem; margin-top:4px;">
                                ${esc(homeName)} vs ${esc(awayName)} · <span style="color:#cbd5e1;">${esc(leagueName)}</span>
                            </div>
                        </div>
                        <button type="button" class="btn-close btn-close-white" onclick="window.closeMotorV4Modal()" aria-label="Cerrar"></button>
                    </div>

                    <div class="motor-v4-body" style="padding:1.25rem; max-height:calc(85vh - 120px); overflow-y:auto;">
                        <!-- TARJETAS DE PICKS -->
                        <div class="row g-3 mb-3">
                            <!-- PICK AH -->
                            <div class="col-md-6">
                                <div style="background:#131c31; border:2px solid #3b82f6; border-radius:10px; padding:14px; position:relative;">
                                    <div style="display:flex; justify-content:space-between; align-items:center;">
                                        <span style="font-weight:700; color:#93c5fd; font-size:.75rem; text-transform:uppercase; letter-spacing:.5px;">PICK HÁNDICAP ASIÁTICO</span>
                                        <span class="badge bg-primary" style="font-size:.72rem;">Score: ${dec(scoreAh.of_six, 1)} / 6</span>
                                    </div>
                                    <div style="font-size:1.35rem; font-weight:800; color:#ffffff; margin:6px 0 2px 0;">
                                        ${esc(ahTeamText)} <span style="color:#60a5fa;">${esc(ahLineText)}</span>
                                    </div>
                                    <div style="font-size:.78rem; color:#cbd5e1;">
                                        ${esc(ahOddsText)} · Votos válidos: <strong>${scoreAh.votes || 0}/${scoreAh.valid || 0}</strong>
                                    </div>
                                </div>
                            </div>
                            <!-- PICK O/U -->
                            <div class="col-md-6">
                                <div style="background:#131c31; border:2px solid #10b981; border-radius:10px; padding:14px; position:relative;">
                                    <div style="display:flex; justify-content:space-between; align-items:center;">
                                        <span style="font-weight:700; color:#6ee7b7; font-size:.75rem; text-transform:uppercase; letter-spacing:.5px;">PICK TOTALES (GOLES)</span>
                                        <span class="badge bg-success" style="font-size:.72rem;">Score: ${dec(scoreOu.of_six, 1)} / 6</span>
                                    </div>
                                    <div style="font-size:1.35rem; font-weight:800; color:#ffffff; margin:6px 0 2px 0;">
                                        ${esc(ouSideText)} <span style="color:#34d399;">${esc(ouLineText)}</span>
                                    </div>
                                    <div style="font-size:.78rem; color:#cbd5e1;">
                                        ${esc(ouOddsText)} · Votos válidos: <strong>${scoreOu.votes || 0}/${scoreOu.valid || 0}</strong>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- PROYECCIÓN DE REMATES -->
                        ${projectionHtml}

                        <!-- COMPUERTAS BINARIAS (12 COMPUERTAS) -->
                        <div class="row g-3 mt-1">
                            <div class="col-md-6">
                                <h6 style="color:#60a5fa; font-size:.85rem; font-weight:700; margin-bottom:8px;"><i class="fa-solid fa-list-check me-1"></i> Compuertas AH (b1 … b6)</h6>
                                ${renderGateList(bGates)}
                            </div>
                            <div class="col-md-6">
                                <h6 style="color:#34d399; font-size:.85rem; font-weight:700; margin-bottom:8px;"><i class="fa-solid fa-futbol me-1"></i> Compuertas Totales (g1 … g6)</h6>
                                ${renderGateList(gGates)}
                            </div>
                        </div>

                        <!-- PATRONES Y POLÍTICAS -->
                        <div class="row g-3 mt-2">
                            <div class="col-md-6">
                                <div style="background:#1e293b; border-radius:8px; padding:10px;">
                                    <div style="color:#cbd5e1; font-size:.78rem; font-weight:700; margin-bottom:6px;">Patrones de Mercado:</div>
                                    <div>${patternChips}</div>
                                </div>
                            </div>
                            <div class="col-md-6">
                                <div style="background:#1e293b; border-radius:8px; padding:10px;">
                                    <div style="color:#cbd5e1; font-size:.78rem; font-weight:700; margin-bottom:6px;">Políticas y Ajustes:</div>
                                    <div>${policyChips}</div>
                                </div>
                            </div>
                        </div>

                        <!-- AUDITORÍA Y GEMINI REVIEW -->
                        <div style="margin-top:16px; background:#0f172a; border:1px solid #1e293b; border-radius:8px; padding:12px;">
                            <div style="display:flex; justify-content:space-between; align-items:center;">
                                <div style="font-size:.75rem; color:#94a3b8;">
                                    SHA-256 Inmutable: <code style="color:#a855f7;">${esc(sha256)}</code>
                                </div>
                                <button type="button" class="btn btn-outline-info btn-sm py-1" id="btnRequestGeminiAudit" onclick="window.requestGeminiAudit('${esc(matchId)}')">
                                    <i class="fa-solid fa-wand-magic-sparkles me-1"></i> Auditoría Gemini AI
                                </button>
                            </div>
                            <div id="geminiAuditContainer" style="display:none; margin-top:10px; font-size:.78rem;"></div>
                        </div>
                    </div>

                    <div class="motor-v4-footer" style="border-top:1px solid #1e293b; padding:.75rem 1.25rem; display:flex; justify-content:space-between; align-items:center;">
                        <span class="small" style="color:#64748b;">Motor v6.2 Operativo · Determinista FT90</span>
                        <div style="display:flex; gap:8px;">
                            <button class="btn btn-primary btn-sm" id="btnCopyMotorV4Prompt" onclick="window.copyMotorV4AuditedPrompt('${esc(matchId)}')">
                                <i class="fa-regular fa-copy me-1"></i> Copiar Análisis v6.2
                            </button>
                            <button type="button" class="btn btn-secondary btn-sm" onclick="window.closeMotorV4Modal()">Cerrar</button>
                        </div>
                    </div>
                </div>
            </div>
        `;

        window._lastMotorV4Prompt = promptText;
        window._lastMotorV4MatchId = matchId;
        window._lastMotorV4Result = result;
    }
    window.closeMotorV4Modal = function () {
        const modalEl = document.getElementById('motorV4ModalContainer');
        if (modalEl) modalEl.innerHTML = '';
    };

    /**
     * Auto-apertura de fichas de estadísticas en caliente y recálculo
     */
    window.autoOpenStatsAndRecalculate = async function (triggerBtn, matchId) {
        if (!matchId) return;
        const panel = document.querySelector(`.pre-context-panel[data-match-id="${matchId}"]`) ||
                      triggerBtn?.closest('.pre-context-panel') ||
                      document.querySelector(`button[onclick*="${matchId}"]`)?.closest('.pre-context-panel') ||
                      document;

        if (triggerBtn) {
            triggerBtn.disabled = true;
            triggerBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin me-1"></i> Abriendo Stats…';
        }

        const statsButtons = panel.querySelectorAll('.pre-context-stats-btn, .mm-btn-stats');
        statsButtons.forEach(btn => {
            if (!btn.classList.contains('active') && !btn.classList.contains('btn-success')) {
                btn.click();
            }
        });

        await new Promise(resolve => setTimeout(resolve, 800));

        const motorBtn = panel.querySelector('.pre-context-motor-v4-btn') || triggerBtn;
        if (window.openMotorV4Modal) {
            await window.openMotorV4Modal(motorBtn, matchId);
        }
    };

    /**
     * Copia al portapapeles el prompt completo auditado
     */
    window.copyMotorV4AuditedPrompt = async function (matchId) {
        const mid = matchId || window._lastMotorV4MatchId;
        let textToCopy = window._lastMotorV4Prompt || '';
        const btn = document.getElementById('btnCopyMotorV4Prompt');

        if (!textToCopy && mid) {
            const panel = document.querySelector(`.pre-context-panel[data-match-id="${mid}"]`) || document;
            const copyBtn = panel.querySelector('.pre-context-copy');
            const encoded = copyBtn?.dataset?.context || '';
            const contextText = decodeURIComponent(encoded);
            const openCardsText = typeof window.extractOpenContextCardsText === 'function' ? window.extractOpenContextCardsText(panel) : '';
            const promptText = window._lastBasePromptText || '';
            const divider = '='.repeat(72);
            textToCopy = `${contextText}${openCardsText}\n\n${divider}\nPROMPT LLM COMPLETO\n${divider}\n\n${promptText}`;
            if (panel._motorV4AuditTemplate) {
                textToCopy += `\n\n${panel._motorV4AuditTemplate}`;
            }
            window._lastMotorV4Prompt = textToCopy;
        }

        if (!textToCopy) {
            alert('No se encontró el texto del prompt para copiar.');
            return;
        }

        try {
            await navigator.clipboard.writeText(textToCopy);
            if (btn) {
                const prev = btn.innerHTML;
                btn.innerHTML = '<i class="fa-solid fa-check me-1"></i> ¡Prompt Copiado!';
                btn.classList.replace('btn-primary', 'btn-success');
                setTimeout(() => {
                    btn.innerHTML = prev;
                    btn.classList.replace('btn-success', 'btn-primary');
                }, 2000);
            }
        } catch (err) {
            console.error('Error al copiar al portapapeles:', err);
            prompt('Copia manualmente el prompt auditado:', textToCopy);
        }
    };

    /**
     * Controlador principal al pulsar el botón "🧠 Motor v5.0"
     */
    window.openMotorV4Modal = async function (button, matchId) {
        try {
            const panel = button?.closest('.pre-context-panel') ||
                          document.querySelector(`.pre-context-panel[data-match-id="${matchId}"]`) ||
                          document;

            let promptText = '';
            try {
                const res = await fetch(`/api/export_prompt/${encodeURIComponent(matchId)}`);
                const json = await res.json();
                if (json.success && json.prompt) {
                    promptText = json.prompt;
                    window._lastBasePromptText = promptText;
                }
            } catch (e) {
                console.warn('No se pudo cargar /api/export_prompt, continuando con DOM:', e);
            }

            const response = await fetch(`/api/quant/audit/${encodeURIComponent(matchId)}`, { method: 'POST' });
            const data = await response.json();
            if (!response.ok || !data.success) throw new Error(data.error || 'No se pudo consultar el modelo');
            const result = data.prediction;
            const auditedText = JSON.stringify(result, null, 2);
            const combinedPromptText = promptText + '\n\nMODELO v6.1 RESEARCH (revisión crítica permitida; sin garantía de acierto):\n' + auditedText;
            showResearchModal(result, data.match, combinedPromptText);
        } catch (err) {
            console.error('Error ejecutando pronóstico v6.1:', err);
            alert('Error en pronóstico v6.1: ' + err.message);
        }
    };

    // Alias para v5

    window.requestGeminiAudit = async function (matchId) {
        const btn = document.getElementById('btnRequestGeminiAudit');
        const container = document.getElementById('geminiAuditContainer');
        if (!container) return;

        container.style.display = 'block';
        container.innerHTML = '<div style="color:#38bdf8;"><i class="fa-solid fa-spinner fa-spin me-1"></i> Consultando auditoría estructurada con Gemini...</div>';
        if (btn) btn.disabled = true;

        try {
            const res = await fetch(`/api/quant/gemini/${encodeURIComponent(matchId)}`, { method: 'POST' });
            const data = await res.json();
            if (!data.success) throw new Error(data.error || 'Error en auditoría');

            if (data.live) {
                const r = data.review;
                const verdictClass = r.verdict === 'ready_for_validation' ? 'badge bg-success' : 'badge bg-warning text-dark';
                container.innerHTML = `
                    <div style="background:#131c31; border:1px solid #3b82f6; border-radius:8px; padding:12px;">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                            <strong style="color:#60a5fa;"><i class="fa-solid fa-check-double me-1"></i> Veredicto Gemini Live</strong>
                            <span class="${verdictClass}">${r.verdict}</span>
                        </div>
                        <p style="margin:0 0 8px 0; color:#e2e8f0;">${r.summary}</p>
                        ${r.issues?.length ? '<div style="font-weight:700; color:#fbbf24; margin-bottom:4px;">Puntos a revisar:</div><ul>' + r.issues.map(i => `<li><strong>${i.category}:</strong> ${i.statement} <em>(${i.suggested_test})</em></li>`).join('') + '</ul>' : '<div style="color:#34d399;">Sin observaciones bloqueantes.</div>'}
                    </div>
                `;
            } else {
                container.innerHTML = `
                    <div style="background:#131c31; border:1px solid #475569; border-radius:8px; padding:10px;">
                        <div style="color:#94a3b8; margin-bottom:4px;"><i class="fa-solid fa-circle-info me-1"></i> ${data.message}</div>
                        <div style="font-size:.72rem; color:#cbd5e1;">Esquema de solicitud preparado y validado contra el hash <code style="color:#a855f7;">${data.packet_sha256}</code>. Para revisión real, configure GEMINI_API_KEY en su entorno.</div>
                    </div>
                `;
            }
        } catch (e) {
            container.innerHTML = `<div style="color:#f87171;"><i class="fa-solid fa-triangle-exclamation me-1"></i> ${e.message}</div>`;
        } finally {
            if (btn) btn.disabled = false;
        }
    };

    window.openMotorV5Modal = window.openMotorV4Modal;
    window.evaluateMatchV4 = evaluateMatchV4;
    window.evaluateMatchV5 = evaluateMatchV4;

})();
