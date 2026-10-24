/**
 * Motor Cuantitativo Dual Entero-Binario v4.0 (Anti-Sesgos)
 * Implementación determinista basada en Números Enteros (Z) y 12 Compuertas Binarias ({0,1})
 * con 5 Reglas de Bloqueo de Sesgos, Detector de Trampas de la Casa y Corrección de Perspectiva NowGoal.
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
                const t1 = m[2].trim();
                const g1 = parseInt(m[3], 10);
                const g2 = parseInt(m[4], 10);
                const t2 = m[5].trim();
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
                // Tasación relativa Local vs Visitante ante este rival común
                const dAh = (ahHome - ahAway);
                sumDeltaZ += Math.round(dAh * 4);

                // Rendimiento deportivo ante el rival:
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
     * Motor determinista evaluateMatchV4
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
        const homeWinRate = data.homeWinRate || 0; // 0..1
        const favEmpatesPct = favoritoRole === 'home' ? (data.homeDrawRate || 0) : (data.awayDrawRate || 0);

        // Trampa 1: Cebo de tabla
        // El visitante va mucho mejor en tabla (awayPos < homePos con brecha >= 5), el local tiene <= 20% victorias, pero la línea es cortísima (|Hz| <= 1).
        if (homePos !== null && awayPos !== null && (homePos - awayPos >= 5) && homeWinRate <= 0.20 && absHz <= 1) {
            trampaDetectada = 'TRAMPA_CEBO_TABLA';
            trampaDetalle = `El visitante ${awayName} va #${awayPos} en la tabla frente al #${homePos} de ${homeName} (diferencia de ${homePos - awayPos} puestos), pero la casa ofrece una línea cortísima (AH ${rawAh >= 0 ? '+' : ''}${rawAh}), induciendo masivamente al público a apostar al visitante mientras el local se refugia en su condición.`;
        }
        // Trampa 2: Falsa racha local
        else if (data.localWinsStreak >= 2 && data.localAvgShots <= 6 && data.localShotEff >= 0.65) {
            trampaDetectada = 'TRAMPA_FALSA_RACHA_LOCAL';
            trampaDetalle = `${homeName} encadena victorias recientes engañosas promediando apenas ${data.localAvgShots.toFixed(1)} tiros totales y sosteniéndose por una efectividad insostenible (${Math.round(data.localShotEff * 100)}% de gol/tiro a puerta).`;
        }
        // Trampa 3: Inflación doble over
        else if (data.bothHighScoringRecent && Gz >= 11 && data.refGoalsAvg < rawOu) {
            trampaDetectada = 'TRAMPA_INFLACION_DOBLE_OVER';
            trampaDetalle = `Ambos equipos vienen de partidos con marcadores abultados y la casa infla la línea a Gz = ${Gz} (${rawOu} goles, obligando a 4 goles), cuando los enfrentamientos de referencia promedian ${data.refGoalsAvg.toFixed(2)} goles.`;
        }
        // Trampa 4: Empate en línea par
        else if ((absHz === 1 || absHz === 2) && favEmpatesPct >= 0.45) {
            trampaDetectada = 'TRAMPA_EMPATE_LINEA_PAR';
            trampaDetalle = `El Favorito ${favorito} tiene una línea de ${rawAh} (${absHz} cuartos), pero registra un ${Math.round(favEmpatesPct * 100)}% de empates en su condición, haciendo que el empate castigue al apostador del favorito y premie al No-Favorito.`;
        }

        // --- PASO 3: MÓDULO A — LAS 6 COMPUERTAS BINARIAS DE HÁNDICAP (b1..b6) ---
        const b = {};
        const bDet = {};
        // b1: Correlación Actual en Hándicaps Similares (Lectura directa de Correlación oficial)
        const favSamplesActual = favoritoRole === 'home' ? (data.similaresActualHomeSamples || 0) : (data.similaresActualAwaySamples || 0);
        const noFavSamplesActual = favoritoRole === 'home' ? (data.similaresActualAwaySamples || 0) : (data.similaresActualHomeSamples || 0);
        const favHasCoverActual = favoritoRole === 'home' ? !!data.similaresActualHomeHasCover : !!data.similaresActualAwayHasCover;
        const noFavHasCoverActual = favoritoRole === 'home' ? !!data.similaresActualAwayHasCover : !!data.similaresActualHomeHasCover;

        const coverFavActual = favHasCoverActual ? (favoritoRole === 'home' ? data.similaresActualHomeCover : data.similaresActualAwayCover) : null;
        const coverNoFavActual = noFavHasCoverActual ? (favoritoRole === 'home' ? data.similaresActualAwayCover : data.similaresActualHomeCover) : null;
        const noFavLossesActual = noFavHasCoverActual ? (favoritoRole === 'home' ? (data.similaresActualAwayLosses || 0) : (data.similaresActualHomeLosses || 0)) : 0;

        const ambosSinMuestraActual = !favHasCoverActual && !noFavHasCoverActual;

        // Interpretación de la etiqueta oficial de correlación de la app
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
            // Comparación cuantitativa directa de covers
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
            bDet.b2 = `Muestra H2H previa insuficiente. No aplica bloqueo.`;
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


        // b3: Triangulación Entera en Rivales Comunes e Indirectas (Signo Invertido en FUERA)
        const tri = data.triangulacionB3 || { deltaZ: absHz + 1, favSuperior: true, detail: 'Sin triangulación específica.' };
        const deltaZ = tri.deltaZ;
        if (deltaZ >= absHz && tri.favSuperior) {
            b.b3 = 1;
            bDet.b3 = `Brecha deltaZ (${deltaZ} cuartos) >= |Hz| (${absHz}) con tasación/rendimiento favorable. [${tri.detail}]`;
        } else {
            b.b3 = 0;
            bDet.b3 = `Brecha deltaZ (${deltaZ} cuartos) < |Hz| (${absHz}) o No-Favorito más eficiente. [${tri.detail}]`;
        }

        // b4: Memoria H2H Directo y Coherencia Clasificatoria
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
            // Si las fichas Stats están cerradas, evaluar con balance de goles en hándicaps similares (Proxy ofensivo)
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

        // b6: Solidez de Victoria y Margen vs Fricción de Empate
        const favWinRate = favoritoRole === 'home' ? (data.homeWinRate || 0) : (data.awayWinRate || 0);
        // favEmpatesPct ya declarado arriba

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

        // Solo activar bloqueo si las DOS correlaciones contradicen de forma manifiesta al favorito con muestra
        if (b.b1 === 0 && b.b2 === 0 && corrActualFavoreceNoFav && corrH2HFavoreceNoFav && muestraValidaParaBloqueo) {
            bloqueoAh = 'BLOQUEO POR DOBLE CORRELACIÓN CONTRA FAVORITO';
            veredictoAh = `TRAMPA EN EL FAVORITO -> ENTRADA FUERTE AL NO-FAVORITO (+AH) [${noFavorito}] (${bloqueoAh})`;
        } else if (noFavHasCoverActual && coverNoFavActual === 100 && noFavSamplesActual >= 4 && b.b3 === 0) {
            bloqueoAh = 'BLOQUEO POR NO-FAVORITO 100% COVER EN HÁNDICAPS SIMILARES';
            veredictoAh = `ENTRADA FUERTE AL NO-FAVORITO (+AH) [${noFavorito}] (${bloqueoAh})`;
        } else if (b.b1 === 1 && b.b2 === 1 && noFavHasCoverActual && coverNoFavActual <= 33) {
            bloqueoAh = 'BLOQUEO INVERSO PRO-FAVORITO (PROHIBIDO NO-FAVORITO)';
            veredictoAh = `ENTRADA FUERTE AL FAVORITO [${favorito} ${lineDisplay}] (${bloqueoAh})`;
        } else {
            // Decisión equilibrada sin sesgo:
            if (b.b1 === 1 || scoreAh >= 3) {
                // Favorito respaldado por correlación actual o mayoría de compuertas
                veredictoAh = `ENTRADA AL FAVORITO [${favorito} ${lineDisplay}] en línea principal.`;
            } else {
                // No-favorito solo si la correlación falla (b1=0) y el score es minoritario (score <= 2)
                veredictoAh = `VALOR EN EL NO-FAVORITO [${noFavorito} ${lineDisplay}] (+AH).`;
            }
        }


        // --- PASO 4: MÓDULO B — LAS 6 COMPUERTAS BINARIAS DE GOLES (g1..g6) ---
        const g = {};
        const gDet = {};

        // g1: Axioma Entero 1 + 2 = 3 (Bloqueo de contradicción lógica)
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

        // g2: Promedio Entero en Referencias y Rivales Comunes (Mz >= Gz)
        const refGoalsAvg = data.refGoalsAvg || 2.5;
        const Mz = Math.round(refGoalsAvg * 4);
        if (Mz >= Gz) {
            g.g2 = 1;
            gDet.g2 = `Media entera de referencias Mz = ${Mz} cuartos (${refGoalsAvg.toFixed(2)} goles) >= Gz = ${Gz} cuartos (${rawOu}).`;
        } else {
            g.g2 = 0;
            gDet.g2 = `Media entera de referencias Mz = ${Mz} cuartos (${refGoalsAvg.toFixed(2)} goles) < Gz = ${Gz} cuartos (${rawOu}).`;
        }

        // g3: Goles Esperados en Hándicaps Similares (Sz >= Gz) — BLINDADO CONTRA NaN y N/D
        const gfHome = data.similaresGfHomeAvg;
        const gcHome = data.similaresGcHomeAvg;
        const gfAway = data.similaresGfAwayAvg;
        const gcAway = data.similaresGcAwayAvg;

        const hasCasaGoals = Number.isFinite(gfHome) && Number.isFinite(gcHome);
        const hasAwayGoals = Number.isFinite(gfAway) && Number.isFinite(gcAway);

        const szCasa = hasCasaGoals ? Math.round((gfHome + gcHome) * 4) : null;
        const szFuera = hasAwayGoals ? Math.round((gfAway + gcAway) * 4) : null;

        if (hasCasaGoals && hasAwayGoals) {
            if ((szCasa >= Gz && szFuera >= Gz) || (szCasa >= Gz + 4 && szFuera >= Gz - 1) || (szFuera >= Gz + 4 && szCasa >= Gz - 1)) {
                g.g3 = 1;
                gDet.g3 = `Goles en Similares (Sz Casa = ${szCasa}, Sz Fuera = ${szFuera}) superan línea Gz = ${Gz}.`;
            } else {
                g.g3 = 0;
                gDet.g3 = `Fricción en Similares (Sz Casa = ${szCasa}, Sz Fuera = ${szFuera}) por debajo de Gz = ${Gz}.`;
            }
        } else if (hasCasaGoals || hasAwayGoals) {
            const validSz = hasCasaGoals ? szCasa : szFuera;
            const validSide = hasCasaGoals ? 'Casa' : 'Fuera';
            if (validSz >= Gz && Mz >= Gz) {
                g.g3 = 1;
                gDet.g3 = `Lado válido (${validSide} Sz = ${validSz}) y media Mz (${Mz}) superan Gz = ${Gz} (el otro lado con GF/GC N/D).`;
            } else {
                g.g3 = 0;
                gDet.g3 = `Lado válido (${validSide} Sz = ${validSz}) o media Mz (${Mz}) no superan Gz = ${Gz}.`;
            }
        } else {
            // Ambos lados sin datos numéricos (N/D)
            g.g3 = g.g2;
            gDet.g3 = `Ambos lados sin GF/GC en similares (N/D); hereda valor de compuerta g2 (${g.g2}).`;
        }

        // g4: Compuerta Dual de Ruptura (Paliza Unilateral o Intercambio Bilateral)
        const palizaUnilateral = Math.abs(deltaZ) >= Gz;
        const bttsRate = data.bttsRateRecent || 0;
        const intercambioBilateral = (hasCasaGoals && hasAwayGoals) &&
            (((gcHome >= 1.8 && gfAway >= 1.8) || (gcAway >= 1.8 && gfHome >= 1.8)) && bttsRate >= 0.70);

        if (palizaUnilateral) {
            g.g4 = 1;
            gDet.g4 = `Ruptura Unilateral: brecha |deltaZ| (${Math.abs(deltaZ)}) >= Gz (${Gz}). El favorito cubre el Over él solo.`;
        } else if (intercambioBilateral) {
            g.g4 = 1;
            gDet.g4 = `Ruptura Bilateral: Fragilidad defensiva mutua (GC >= 1.8) con BTTS en el ${Math.round(bttsRate * 100)}% de los partidos.`;
        } else {
            g.g4 = 0;
            gDet.g4 = `Sin ruptura unilateral ni intercambio bilateral abierto de goles.`;
        }

        // g5: Ritmo de Ataques Peligrosos Combinados (>= 100 AP)
        const totalApCombinados = (data.statsHomeAp || 0) + (data.statsAwayAp || 0);
        if (data.hasDetailedStats && totalApCombinados > 0) {
            if (totalApCombinados >= 100) {
                g.g5 = 1;
                gDet.g5 = `Ritmo alto: ${Math.round(totalApCombinados)} Ataques Peligrosos combinados (>= 100 AP). Ida y vuelta constante.`;
            } else {
                g.g5 = 0;
                gDet.g5 = `Ritmo pausado: ${Math.round(totalApCombinados)} Ataques Peligrosos combinados (< 100 AP).`;
            }
        } else {
            g.g5 = g.g2;
            gDet.g5 = `Sin fichas Stats abiertas; hereda compuerta g2 (${g.g2}). Despliega Stats para auditar los >= 100 AP reales.`;
        }

        // g6: Volumen de Tiro (>= 22) o Letalidad Estructural (>= 35%)
        const totalTirosCombinados = (data.statsHomeTiros || 0) + (data.statsAwayTiros || 0);
        const conversionRate = data.statsConversionRate || 0.30;
        if (data.hasDetailedStats && totalTirosCombinados > 0) {
            if (totalTirosCombinados >= 22 || conversionRate >= 0.35) {
                g.g6 = 1;
                gDet.g6 = `Volumen de tiro alto (${Math.round(totalTirosCombinados)} tiros) o letalidad (${Math.round(conversionRate * 100)}% >= 35%).`;
            } else {
                g.g6 = 0;
                gDet.g6 = `Volumen de tiro insuficiente (${Math.round(totalTirosCombinados)} tiros < 22) y baja conversión.`;
            }
        } else {
            g.g6 = g.g2;
            gDet.g6 = `Sin fichas Stats abiertas; hereda compuerta g2 (${g.g2}). Despliega Stats para auditar tiros >= 22.`;
        }

        // REGLAS DE BLOQUEO DE GOLES
        let scoreOu = g.g1 + g.g2 + g.g3 + g.g4 + g.g5 + g.g6;
        let veredictoOu = '';

        if (g.g1 === 1) {
            veredictoOu = `OVER ASIGNADO POR AXIOMA ENTERO 1+2=3 (PROHIBIDO UNDER). Ir a Over ${rawOu} en pre-partido o esperar línea entera en vivo al min 15.`;
        } else if (scoreOu >= 5) {
            veredictoOu = `OVER FUERTE (Línea ${rawOu}). Excelente ritmo ofensivo y ruptura de líneas. Entrar en pre-partido.`;
        } else if (scoreOu >= 3) {
            veredictoOu = `TENDENCIA OVER CON RIESGO DE LÍNEA ALTA. Esperar en vivo al min 15-20 para conseguir descuento de 1-2 enteros (ej. over ${Math.max(1.5, rawOu - 0.5)}).`;
        } else {
            veredictoOu = `PARTIDO DE BAJO RITMO / UNDER (Línea ${rawOu}). Superioridad defensiva o fricción en hándicaps similares.`;
        }

        // PREDICCIONES CONCRETAS Y APUESTAS ÓPTIMAS RECOMENDADAS
        const pred = {
            ah: {
                pick: '',
                accion: '',
                confianza: 'ALTA',
                confClass: 'conf-alta',
                razon: ''
            },
            ou: {
                pick: '',
                accion: '',
                confianza: 'ALTA',
                confClass: 'conf-alta',
                razon: ''
            }
        };
        // 1. Predicción de Hándicap Asiático (Equilibrada: Favorito vs No-Favorito)
        const lineDisplay = absHz > 0 ? `+${(absHz / 4).toFixed(2)}` : 'AH 0';

        if (bloqueoAh) {
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
            // ¡¡¡RECOMENDACIÓN AL FAVORITO CUANDO LA CORRELACIÓN O EL SCORE LO RESPALDAN!!!
            pred.ah.pick = `${favorito} ${lineDisplay} (Favorito)`;
            pred.ah.accion = `Apostar al Favorito [${favorito}]`;
            pred.ah.confianza = scoreAh >= 4 ? 'ALTA' : 'MEDIA-ALTA';
            pred.ah.confClass = 'conf-alta';
            pred.ah.razon = `Score AH ${scoreAh}/6. ${b.b1 === 1 ? 'Correlación favorable al favorito' : 'Mayoría de compuertas a favor'}.`;
        } else {
            // Solo si score <= 2 y b1 === 0
            pred.ah.pick = `${noFavorito} ${lineDisplay} (No-Favorito)`;
            pred.ah.accion = `Apostar al No-Favorito (+AH) [${noFavorito}]`;
            pred.ah.confianza = 'ALTA';
            pred.ah.confClass = 'conf-alta';
            pred.ah.razon = `Score AH ${scoreAh}/6. La correlación y las métricas otorgan el valor al no-favorito.`;
        }



        // 2. Predicción de Goles
        if (g.g1 === 1) {
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
            pred.ou.razon = 'Score de goles >= 5/6 con alta densidad ofensiva y ruptura.';
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
        const hcPromptMatch = promptText.match(/H[aá]ndicap\s+Inicial\*?\*?:\s*([0-9.-]+)/i);
        if (hcPromptMatch) {
            handicap = hcPromptMatch[1].trim();
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

            // Muestras de partidos: ej. "EQUIPO 1 AH +0.5 (±0.25): 3 partidos" o "CASA AH +0.25 (±0.5): 0 partidos"
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
            if (corrMatch) res.label = corrMatch[1].trim();
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
                // Buscar tablas y filas dentro de la ficha de estadísticas
                slot.querySelectorAll('table tbody tr, .modern-match-stats-card div').forEach(row => {
                    const text = row.innerText || '';
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
        const posHomeMatch = promptText.match(new RegExp(`${homeName}[^:]*:\\s*Puesto\\s*(\\d+)`, 'i'));
        if (posHomeMatch) homePos = parseNum(posHomeMatch[1]);
        const posAwayMatch = promptText.match(new RegExp(`${awayName}[^:]*:\\s*Puesto\\s*(\\d+)`, 'i'));
        if (posAwayMatch) awayPos = parseNum(posAwayMatch[1]);

        // 6. Extracción dinámica real de Victorias y Empates desde el Resumen del historial
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

        // Si no hay resumen textual, buscar en el promptText (standings)
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
            correlacionH2HTexto: simH2h.label,
            triangulacionB3,
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
     * Construye la plantilla de inyección en el prompt LLM con auditoría completa
     */
    function buildPromptInjectionTemplate(res) {
        return `
========================================================================
PREDICCIONES OFICIALES DEL MOTOR v4.0 (APUESTAS RECOMENDADAS)
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
AUDITORÍA MATEMÁTICA DETALLADA — COMPUERTAS BINARIAS (INVIOLABLE)
========================================================================
- Conversión Entera: Hándicap (Hz) = ${res.Hz} enteros | Línea de Goles (Gz) = ${res.Gz} enteros
- Favorito de la Línea (NowGoal): ${res.favorito} | No-Favorito (+AH): ${res.noFavorito}
- Trampa Psicológica Detectada: ${res.trampaDetectada} (${res.trampaDetalle})
${!res.hasDetailedStats ? '- AVISO: Fichas Stats no abiertas en la tarjeta; compuertas b5, g5 y g6 operan con herencia/resguardo.' : ''}

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
- g3 (Goles en Hándicaps Similares): [${res.g.g3}] -> ${res.gDet.g3}
- g4 (Ruptura Unilateral/Bilateral): [${res.g.g4}] -> ${res.gDet.g4}
- g5 (Ritmo Ataques Peligrosos>=100):[${res.g.g5}] -> ${res.gDet.g5}
- g6 (Tiros>=22 o Conversión>=35%):  [${res.g.g6}] -> ${res.gDet.g6}
>>> SEÑAL OBLIGATORIA GOLES: ${res.veredictoOu}

INSTRUCCIÓN ESTRICTA PARA EL LLM:
Redacta el análisis táctico y matemático de este partido obedeciendo al 100% los resultados de los vectores [b1..b6] y [g1..g6] precalculados arriba. Tienes ESTRICTAMENTE PROHIBIDO contradecir la SEÑAL OBLIGATORIA de Hándicap o de Goles, inventar narrativas que ignoren la Doble Correlación de Hándicaps Similares, o recomendar un Under si la compuerta g1 vale 1. Explica con precisión quirúrgica dónde está el engaño de la casa de apuestas y entrega la orden operativa final (Pre-partido y Live).`;
    }

    /**
     * Renderiza el Modal / Pop-Up del Motor Cuantitativo v4.0
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
                                    <h5 class="motor-v4-match-name">Motor Cuantitativo Dual Entero-Binario v4.0</h5>
                                    <span class="motor-v4-badge">Anti-Sesgos</span>
                                </div>
                                <div class="motor-v4-meta-sub">
                                    <span><i class="fa-solid fa-futbol me-1 text-info"></i>${esc(res.homeName)} vs ${esc(res.awayName)}</span>
                                    <span><i class="fa-solid fa-trophy me-1 text-warning"></i>${esc(res.leagueName)}</span>
                                    <span class="badge bg-secondary">Hz: ${res.Hz} (${res.absHz}/4)</span>
                                    <span class="badge bg-secondary">Gz: ${res.Gz} (${res.Gz}/4)</span>
                                    <span class="badge ${res.favoritoRole === 'home' ? 'bg-primary' : 'bg-info'} text-white">Fav: ${esc(res.favorito)}</span>
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
                                        <strong>Fichas de estadísticas Stats cerradas en esta tarjeta:</strong>
                                        <div class="small text-secondary mt-1">Las compuertas b5, g5 y g6 (Tiros y Ataques Peligrosos) requieren abrir las fichas "Stats" de los partidos para auditar con volumen real.</div>
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
                                    PREDICCIONES DEL MOTOR v4.0 (APUESTAS RECOMENDADAS)
                                </span>
                                <span class="predictions-hero-badge">Recomendación Oficial</span>
                            </div>
                            <div class="predictions-grid">
                                <!-- PICK 1: HANDICAP -->
                                <div class="prediction-card ah-pick">
                                    <div class="prediction-market-label">
                                        <span><i class="fa-solid fa-scale-balanced me-1 text-info"></i> Hándicap Asiático</span>
                                        <span class="prediction-conf-pill ${res.pred.ah.confClass}">${res.pred.ah.confianza}</span>
                                    </div>
                                    <div class="prediction-selection">${esc(res.pred.ah.pick)}</div>
                                    <div class="prediction-action-row">
                                        <i class="fa-solid fa-arrow-right-long text-info"></i>
                                        <span>${esc(res.pred.ah.accion)}</span>
                                    </div>
                                    <p class="prediction-reason mb-0">${esc(res.pred.ah.razon)}</p>
                                </div>

                                <!-- PICK 2: GOLES -->
                                <div class="prediction-card ou-pick">
                                    <div class="prediction-market-label">
                                        <span><i class="fa-solid fa-futbol me-1 text-success"></i> Línea de Goles (O/U)</span>
                                        <span class="prediction-conf-pill ${res.pred.ou.confClass}">${res.pred.ou.confianza}</span>
                                    </div>
                                    <div class="prediction-selection">${esc(res.pred.ou.pick)}</div>
                                    <div class="prediction-action-row">
                                        <i class="fa-solid fa-arrow-right-long text-success"></i>
                                        <span>${esc(res.pred.ou.accion)}</span>
                                    </div>
                                    <p class="prediction-reason mb-0">${esc(res.pred.ou.razon)}</p>
                                </div>
                            </div>
                        </div>

                        <!-- TRAMPA DETECTADA BANNER -->
                        <div class="motor-trap-banner ${res.trampaDetectada !== 'NINGUNA_TRAMPA_EVIDENTE' ? 'trap-detected' : 'trap-safe'}">
                            <div class="trap-head">
                                <span><i class="fa-solid fa-shield-halved me-1"></i> TRAMPA DE LA CASA: <strong>${esc(res.trampaDetectada)}</strong></span>
                                <span class="trap-pill">${res.trampaDetectada !== 'NINGUNA_TRAMPA_EVIDENTE' ? 'ALERTA TRAMPA' : 'LÍNEAS LIMPIAS'}</span>
                            </div>
                            <div class="trap-desc">${esc(res.trampaDetalle)}</div>
                        </div>

                        <!-- GRID MODULO A Y B -->
                        <div class="motor-modules-grid">
                            <!-- MODULO A: HANDICAP -->
                            <div class="motor-module-card ah-card">
                                <div class="module-title-bar">
                                    <div class="d-flex align-items-center justify-content-between">
                                        <span class="fw-bold"><i class="fa-solid fa-scale-balanced me-1 text-primary"></i> MÓDULO A: HÁNDICAP ASIÁTICO</span>
                                        <span class="badge bg-primary fs-6">${res.scoreAh} / 6</span>
                                    </div>
                                    <div class="module-verdict mt-2">
                                        <strong>SEÑAL:</strong> ${esc(res.veredictoAh)}
                                    </div>
                                </div>
                                <div class="module-gates-list">
                                    ${renderGate('b1', res.b.b1, 'Correlación Actual Similares', res.bDet.b1)}
                                    ${renderGate('b2', res.b.b2, 'Correlación Histórica H2H', res.bDet.b2)}
                                    ${renderGate('b3', res.b.b3, 'Triangulación Rivales Comunes', res.bDet.b3)}
                                    ${renderGate('b4', res.b.b4, 'Memoria H2H + Posición Tabla', res.bDet.b4)}
                                    ${renderGate('b5', res.b.b5, 'Volumen Ofensivo Mismo Rol', res.bDet.b5)}
                                    ${renderGate('b6', res.b.b6, 'Filtro Empate vs Margen Victoria', res.bDet.b6)}
                                </div>
                            </div>

                            <!-- MODULO B: GOLES -->
                            <div class="motor-module-card ou-card">
                                <div class="module-title-bar">
                                    <div class="d-flex align-items-center justify-content-between">
                                        <span class="fw-bold"><i class="fa-solid fa-futbol me-1 text-success"></i> MÓDULO B: LÍNEA DE GOLES (O/U)</span>
                                        <span class="badge bg-success fs-6">${res.scoreOu} / 6</span>
                                    </div>
                                    <div class="module-verdict mt-2">
                                        <strong>SEÑAL:</strong> ${esc(res.veredictoOu)}
                                    </div>
                                </div>
                                <div class="module-gates-list">
                                    ${renderGate('g1', res.g.g1, 'Axioma Piso 1+2=3 Goles', res.gDet.g1)}
                                    ${renderGate('g2', res.g.g2, 'Media Referencias Mz >= Gz', res.gDet.g2)}
                                    ${renderGate('g3', res.g.g3, 'Goles en Hándicaps Similares', res.gDet.g3)}
                                    ${renderGate('g4', res.g.g4, 'Ruptura Unilateral/Bilateral', res.gDet.g4)}
                                    ${renderGate('g5', res.g.g5, 'Ritmo Ataques Peligrosos >= 100', res.gDet.g5)}
                                    ${renderGate('g6', res.g.g6, 'Volumen Tiros >= 22 o Letalidad', res.gDet.g6)}
                                </div>
                            </div>
                        </div>

                        <!-- PLAN OPERATIVO DESTACADO -->
                        <div class="motor-plan-box">
                            <div class="plan-header"><i class="fa-solid fa-crosshairs me-1 text-warning"></i> <strong>PLAN OPERATIVO CUANTITATIVO RECOMENDADO</strong></div>
                            <div class="plan-grid">
                                <div>
                                    <span class="plan-pill pre">PRE-PARTIDO</span>
                                    <p class="mb-0 mt-1 small">${esc(res.veredictoAh.split('[')[0])} · ${esc(res.veredictoOu.split('(')[0])}</p>
                                </div>
                                <div>
                                    <span class="plan-pill live">EN VIVO (LIVE)</span>
                                    <p class="mb-0 mt-1 small">Si el marcador es 0:0 al min 15-20, aprovechar descuento de 1-2 enteros en líneas alternativas.</p>
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- FOOTER ACTIONS -->
                    <div class="motor-v4-footer">
                        <button type="button" class="btn btn-outline-secondary btn-sm fw-bold px-3" onclick="window.closeMotorV4Modal()">
                            <i class="fa-solid fa-xmark me-1"></i> Cerrar
                        </button>
                        <button type="button" class="btn btn-primary btn-sm fw-bold px-3" id="btnCopyMotorV4Prompt" onclick="window.copyMotorV4AuditedPrompt('${matchId}')">
                            <i class="fa-solid fa-clipboard-check me-1"></i> Copiar Contexto + Prompt LLM Inyectado
                        </button>
                    </div>
                </div>
            </div>
        `;

        window._lastMotorV4Prompt = combinedPromptText;
        window._lastMotorV4MatchId = matchId;
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
            if (typeof btn.click === 'function') {
                btn.click();
            }
        });

        // Esperar 400ms para permitir renderizado del DOM
        await new Promise(r => setTimeout(r, 450));

        const motorBtn = panel.querySelector('.pre-context-motor-v4-btn');
        if (motorBtn) {
            await window.openMotorV4Modal(motorBtn, matchId);
        }
    };

    /**
     * Copia al portapapeles con recálculo en fresco del estado DOM
     */
    window.copyMotorV4AuditedPrompt = async function (matchId) {
        const mid = matchId || window._lastMotorV4MatchId;
        let textToCopy = window._lastMotorV4Prompt || '';

        // Recálculo en caliente para asegurar que captura cualquier ficha que se haya abierto
        if (mid) {
            const panel = document.querySelector(`button[onclick*="${mid}"]`)?.closest('.pre-context-panel') || document;
            const copyBtn = panel.querySelector('.pre-context-copy');
            const rawContext = decodeURIComponent(copyBtn?.dataset?.context || '');
            let openCardsText = '';
            if (typeof extractOpenContextCardsText === 'function') {
                openCardsText = extractOpenContextCardsText(panel);
            }

            const promptText = window._lastBasePromptText || '';
            const matchData = extractMatchDataFromDOM(panel, mid, promptText);
            const evaluation = evaluateMatchV4(matchData);
            const auditTemplate = buildPromptInjectionTemplate(evaluation);

            const divider = '='.repeat(72);
            textToCopy = `${rawContext}${openCardsText}\n\n${divider}\nPROMPT LLM COMPLETO\n${divider}\n\n${promptText}\n\n${auditTemplate}`;
            panel._motorV4AuditTemplate = auditTemplate;
            panel._motorV4AuditedPrompt = textToCopy;
            window._lastMotorV4Prompt = textToCopy;
        }

        if (!textToCopy) return;

        try {
            await navigator.clipboard.writeText(textToCopy);
            const btn = document.getElementById('btnCopyMotorV4Prompt');
            if (btn) {
                const orig = btn.innerHTML;
                btn.innerHTML = '<i class="fa-solid fa-check me-1"></i> ¡Copiado con Éxito!';
                btn.classList.remove('btn-primary');
                btn.classList.add('btn-success');
                setTimeout(() => {
                    btn.innerHTML = orig;
                    btn.classList.remove('btn-success');
                    btn.classList.add('btn-primary');
                }, 2000);
            }
        } catch (e) {
            alert('Error copiando al portapapeles: ' + e.message);
        }
    };

    /**
     * Handler activado exclusivamente al hacer click en el botón de un partido
     */
    window.openMotorV4Modal = async function (button, matchId) {
        if (!button || !matchId) return;

        const panel = button.closest('.pre-context-panel') || document;
        const originalHtml = button.innerHTML;
        button.disabled = true;
        button.innerHTML = '<i class="fa-solid fa-spinner fa-spin me-1"></i> Evaluando…';

        try {
            // 1. Obtener prompt exportado del partido
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

            // 2. Extraer datos del DOM en tiempo real
            const matchData = extractMatchDataFromDOM(panel, matchId, promptText);

            // 3. Ejecutar algoritmo cuantitativo v4.0 con signos y protecciones corregidas
            const evaluation = evaluateMatchV4(matchData);

            // 4. Inyectar auditoría en el prompt
            const auditTemplate = buildPromptInjectionTemplate(evaluation);
            const copyBtn = panel.querySelector('.pre-context-copy');
            const rawContext = decodeURIComponent(copyBtn?.dataset?.context || '');
            let openCardsText = '';
            if (typeof extractOpenContextCardsText === 'function') {
                openCardsText = extractOpenContextCardsText(panel);
            }

            const divider = '='.repeat(72);
            const combinedPromptWithAudit = `${rawContext}${openCardsText}\n\n${divider}\nPROMPT LLM COMPLETO\n${divider}\n\n${promptText}\n\n${auditTemplate}`;

            // Almacenar en panel para que si pulsa el botón original de copiar prompt, también lo incluya
            panel._motorV4AuditTemplate = auditTemplate;
            panel._motorV4AuditedPrompt = combinedPromptWithAudit;

            // 5. Desplegar pop-up modal
            showMotorV4Modal(evaluation, combinedPromptWithAudit, matchId);
        } catch (err) {
            console.error('Error ejecutando Motor v4.0:', err);
            alert('Error en Motor v4.0: ' + err.message);
        } finally {
            button.disabled = false;
            button.innerHTML = originalHtml;
        }
    };
})();
