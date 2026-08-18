function normalizeLeagueExtractionMatch(payload, extraction) {
    const source = payload && payload.source && typeof payload.source === 'object' ? payload.source : {};
    const raw = payload && payload.data && typeof payload.data === 'object' ? payload.data : {};
    const id = String(source.id || raw.match_id || raw.id || '');
    const odds = raw.main_match_odds && typeof raw.main_match_odds === 'object' ? raw.main_match_odds : {};
    const sourceDate = String(source.date || '');
    const sourceDateMatch = sourceDate.match(/^(\d{4}-\d{2}-\d{2})(?:[ T](\d{1,2}:\d{2}))?/);
    let handicap = raw.handicap;
    if (handicap === undefined || handicap === null || handicap === '') handicap = odds.ah_linea;
    if (handicap === undefined || handicap === null || handicap === '') handicap = source.visible_ah;
    const normalized = {
        ...raw,
        id,
        match_id: id,
        home_team: raw.home_team || raw.home_name || source.home || '',
        away_team: raw.away_team || raw.away_name || source.away || '',
        home_name: raw.home_name || raw.home_team || source.home || '',
        away_name: raw.away_name || raw.away_team || source.away || '',
        league: raw.league || raw.league_name || extraction.league_name || '',
        league_name: raw.league_name || raw.league || extraction.league_name || '',
        handicap,
        match_date: raw.match_date || raw.date || (sourceDateMatch ? sourceDateMatch[1] : sourceDate),
        time: raw.time || (sourceDateMatch && sourceDateMatch[2] ? sourceDateMatch[2] : ''),
        round: source.round || '',
        league_round_label: source.round || '',
        source_registry_status: source.status || '',
        _date_format_hint: 'YMD'
    };
    const schedule = getMatchScheduleParts(normalized);
    if (schedule) normalized._schedule_parts = schedule;
    return normalized;
}

function updateLeagueRoundControls(currentRound) {
    const select = document.getElementById('league-round-select');
    if (!select) return;
    select.innerHTML = activeLeagueRounds.map(round =>
        `<option value="${String(round.key).replaceAll('&', '&amp;').replaceAll('"', '&quot;')}" ${round.key === currentRound ? 'selected' : ''}>${round.label} (${round.count})</option>`
    ).join('');
    const index = activeLeagueRounds.findIndex(round => round.key === currentRound);
    document.getElementById('league-round-prev').disabled = index <= 0;
    document.getElementById('league-round-next').disabled = index < 0 || index >= activeLeagueRounds.length - 1;
    const active = activeLeagueRounds[index];
    document.getElementById('league-round-summary').textContent = active
        ? `${active.available}/${active.count} análisis completos · posiciones y forma general/casa/fuera incluidas`
        : 'Sin jornadas registradas';
}

async function loadLeagueExtractionRound(roundKey = activeLeagueRoundKey) {
    const params = new URLSearchParams();
    if (roundKey) params.set('round', roundKey);
    const suffix = params.toString() ? `?${params.toString()}` : '';
    const response = await fetch(`/api/league-extractions/${encodeURIComponent(LEAGUE_EXTRACTION_ID)}${suffix}`, {cache: 'no-store'});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = await response.json();
    if (data.error) throw new Error(data.error);

    activeLeagueRounds = Array.isArray(data.rounds) ? data.rounds : [];
    activeLeagueRoundKey = data.current_round || '';
    updateLeagueRoundControls(activeLeagueRoundKey);
    const extraction = data.extraction || {};
    const rows = (Array.isArray(data.matches) ? data.matches : [])
        .map(payload => normalizeLeagueExtractionMatch(payload, extraction))
        .filter(row => row.id);
    precacheoData = {};
    rows.forEach(row => { precacheoData[row.id] = row; });
    totalFilteredMatches = rows;
    upcomingMatches = rows;
    currentPage = 1;
    populateLeagueFilters(rows);
    renderTableWithPagination();

    const url = new URL(window.location.href);
    url.searchParams.set('league_extraction', LEAGUE_EXTRACTION_ID);
    if (activeLeagueRoundKey) url.searchParams.set('round', activeLeagueRoundKey);
    window.history.replaceState({}, '', url);
}

async function copyAllLeagueSeasons(button) {
    if (!LEAGUE_EXTRACTION_ID) {
        alert('No hay una liga extraída activa.');
        return;
    }

    const originalHtml = button.innerHTML;
    button.disabled = true;
    button.innerHTML = '<i class="fa-solid fa-spinner fa-spin me-1"></i> Preparando TODO…';
    try {
        const response = await fetch(
            `/api/league-extractions/${encodeURIComponent(LEAGUE_EXTRACTION_ID)}/export-all-seasons`,
            {cache: 'no-store'}
        );
        if (!response.ok) {
            let errorMessage = `HTTP ${response.status}`;
            try {
                const payload = await response.json();
                errorMessage = payload.error || errorMessage;
            } catch (_) {
                // La respuesta puede ser texto plano si falla durante la exportación.
            }
            throw new Error(errorMessage);
        }

        const exportText = await response.text();
        if (!exportText.trim()) throw new Error('La exportación está vacía');
        await copyToClipboard(exportText);

        const seasons = response.headers.get('X-League-Seasons') || '0';
        const rounds = response.headers.get('X-League-Rounds') || '0';
        const matches = response.headers.get('X-League-Matches') || '0';
        button.classList.remove('btn-success');
        button.classList.add('btn-dark');
        button.innerHTML = `<i class="fa-solid fa-check me-1"></i> ${seasons} temp. · ${rounds} jorn. · ${matches} partidos`;
    } catch (error) {
        alert(`Error al copiar todas las temporadas: ${error.message}`);
    } finally {
        window.setTimeout(() => {
            button.innerHTML = originalHtml;
            button.classList.remove('btn-dark');
            button.classList.add('btn-success');
            button.disabled = false;
        }, 3500);
    }
}

let leagueMissingPollActive = false;

function leagueMissingStorageKey() {
    return `league-missing-job:${LEAGUE_EXTRACTION_ID}`;
}

function rememberLeagueMissingJob(jobId) {
    try {
        if (jobId) localStorage.setItem(leagueMissingStorageKey(), jobId);
        else localStorage.removeItem(leagueMissingStorageKey());
    } catch (_) {
        // El proceso del servidor continúa aunque el navegador bloquee localStorage.
    }
}

function waitLeagueMissingPoll(milliseconds) {
    return new Promise(resolve => window.setTimeout(resolve, milliseconds));
}

async function monitorLeagueMissingJob(button, jobId) {
    if (!jobId || leagueMissingPollActive) return;
    leagueMissingPollActive = true;
    button.disabled = true;
    rememberLeagueMissingJob(jobId);

    try {
        while (true) {
            const response = await fetch(
                `/api/league_handicap/status/${encodeURIComponent(jobId)}`,
                {cache: 'no-store'}
            );
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const job = await response.json();
            const processed = Number(job.processed || 0);
            const total = Number(job.total || 0);
            button.innerHTML = `<i class="fa-solid fa-spinner fa-spin me-1"></i> ${processed}/${total} · lote en curso`;

            if (job.status === 'completed') {
                const remaining = Number(job.remaining || 0);
                rememberLeagueMissingJob('');
                await loadLeagueExtractionRound(activeLeagueRoundKey);
                button.disabled = false;
                button.innerHTML = remaining > 0
                    ? `<i class="fa-solid fa-forward me-1"></i> Continuar faltantes <span class="badge bg-dark ms-1">${remaining}</span>`
                    : '<i class="fa-solid fa-check me-1"></i> Liga completa';
                alert(
                    remaining > 0
                        ? `Lote terminado. Quedan ${remaining} partidos. Pulsa de nuevo para procesar los siguientes 500.`
                        : 'Lote terminado. Ya no quedan partidos sin scrapear en esta extracción.'
                );
                return;
            }
            if (job.status === 'failed') {
                throw new Error(job.error || 'El lote ha fallado');
            }
            await waitLeagueMissingPoll(2000);
        }
    } catch (error) {
        rememberLeagueMissingJob('');
        button.disabled = false;
        button.innerHTML = '<i class="fa-solid fa-rotate me-1"></i> Reintentar faltantes <span class="badge bg-dark ms-1">500/lote</span>';
        alert(`Error siguiendo el lote: ${error.message}`);
    } finally {
        leagueMissingPollActive = false;
    }
}

async function scrapeMissingLeagueBatch(button) {
    if (!LEAGUE_EXTRACTION_ID) {
        alert('No hay una liga extraída activa.');
        return;
    }
    if (!confirm('Se scrapearán como máximo 500 partidos que todavía falten. Al terminar, el proceso se detendrá y podrás continuar con el siguiente lote.')) {
        return;
    }

    button.disabled = true;
    button.innerHTML = '<i class="fa-solid fa-spinner fa-spin me-1"></i> Preparando lote…';
    try {
        const response = await fetch(
            `/api/league-extractions/${encodeURIComponent(LEAGUE_EXTRACTION_ID)}/scrape-missing`,
            {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({limit: 500, workers: 4})
            }
        );
        const data = await response.json();
        if (!response.ok && response.status !== 202) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }
        if (data.status === 'completed' && !data.job_id) {
            button.disabled = false;
            button.innerHTML = '<i class="fa-solid fa-check me-1"></i> Liga completa';
            alert(data.message || 'No quedan partidos pendientes.');
            return;
        }
        await monitorLeagueMissingJob(button, data.job_id);
    } catch (error) {
        button.disabled = false;
        button.innerHTML = '<i class="fa-solid fa-rotate me-1"></i> Reintentar faltantes <span class="badge bg-dark ms-1">500/lote</span>';
        alert(`No se pudo iniciar el lote: ${error.message}`);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const button = document.getElementById('btn-scrape-league-missing');
    if (!button) return;
    try {
        const rememberedJob = localStorage.getItem(leagueMissingStorageKey());
        if (rememberedJob) monitorLeagueMissingJob(button, rememberedJob);
    } catch (_) {
        // Sin restauración automática; el botón sigue siendo utilizable.
    }
});
