/**
 * Vista móvil de Pre-Cacheo.
 *
 * La tabla sigue siendo la fuente de verdad para filtros, paginación y
 * exportación. En móvil se refleja como tarjetas y se actualiza cuando la
 * tabla cambia o cuando un filtro oculta/muestra filas.
 */
class MobileViewHandler {
    constructor() {
        this.mediaQuery = window.matchMedia('(max-width: 768px)');
        this.tableContainer = document.querySelector('.main-content > .table-responsive');
        this.tableBody = document.getElementById('table-body');
        this.mobileContainer = null;
        this.refreshFrame = null;
        this.observer = null;
        this.hasRenderedMatches = false;
        this.emptyStateTimer = null;
        this.init();
    }

    init() {
        if (!this.tableContainer || !this.tableBody) return;

        this.createMobileContainer();
        this.observeTable();

        const onBreakpointChange = () => this.handleViewChange();
        if (this.mediaQuery.addEventListener) {
            this.mediaQuery.addEventListener('change', onBreakpointChange);
        } else {
            this.mediaQuery.addListener(onBreakpointChange);
        }

        this.handleViewChange();
    }

    createMobileContainer() {
        this.mobileContainer = document.getElementById('mobile-cards-container');
        if (this.mobileContainer) return;

        this.mobileContainer = document.createElement('section');
        this.mobileContainer.id = 'mobile-cards-container';
        this.mobileContainer.className = 'mobile-cards-container';
        this.mobileContainer.setAttribute('aria-label', 'Partidos');
        this.mobileContainer.hidden = true;
        this.tableContainer.insertAdjacentElement('afterend', this.mobileContainer);
    }

    observeTable() {
        this.observer = new MutationObserver(() => this.scheduleRefresh());
        this.observer.observe(this.tableBody, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ['style', 'class']
        });
    }

    handleViewChange() {
        const isMobile = this.mediaQuery.matches;
        this.tableContainer.classList.toggle('mobile-table-hidden', isMobile);
        this.mobileContainer.hidden = !isMobile;

        if (isMobile) this.scheduleRefresh();
    }

    scheduleRefresh() {
        if (!this.mediaQuery.matches || this.refreshFrame !== null) return;

        this.refreshFrame = window.requestAnimationFrame(() => {
            this.refreshFrame = null;
            this.renderMobileView();
        });
    }

    renderMobileView() {
        const matches = this.extractMatchesFromTable();

        if (!matches.length) {
            const isInitialLoad = !this.hasRenderedMatches;
            this.mobileContainer.innerHTML = `
                <div class="mobile-empty-state">
                    <i class="fa-solid ${isInitialLoad ? 'fa-spinner fa-spin' : 'fa-inbox'}"></i>
                    <p>${isInitialLoad ? 'Cargando partidos…' : 'No hay partidos que mostrar con estos filtros.'}</p>
                </div>`;

            if (isInitialLoad && this.emptyStateTimer === null) {
                this.emptyStateTimer = window.setTimeout(() => {
                    this.emptyStateTimer = null;
                    this.hasRenderedMatches = true;
                    this.renderMobileView();
                }, 8000);
            }
            return;
        }

        this.hasRenderedMatches = true;
        if (this.emptyStateTimer !== null) {
            window.clearTimeout(this.emptyStateTimer);
            this.emptyStateTimer = null;
        }
        this.mobileContainer.innerHTML = matches
            .map((match, index) => this.createMatchCard(match, index + 1, matches.length))
            .join('');
    }

    extractMatchesFromTable() {
        return Array.from(this.tableBody.children)
            .filter(row => row.matches('tr[data-match-id]'))
            .filter(row => !row.hidden && row.style.display !== 'none')
            .map(row => {
                const cells = row.querySelectorAll(':scope > td');
                if (cells.length < 14) return null;

                return {
                    id: String(row.dataset.matchId || ''),
                    date: this.cleanText(cells[0]?.textContent),
                    homeTeam: cells[1]?.innerHTML || '',
                    awayTeam: cells[2]?.innerHTML || '',
                    handicap: this.cleanText(cells[3]?.textContent),
                    result: this.cleanText(cells[4]?.textContent),
                    prevHome: cells[5]?.innerHTML || '',
                    prevAway: cells[6]?.innerHTML || '',
                    h2hStadium: cells[7]?.innerHTML || '',
                    h2hGeneral: cells[8]?.innerHTML || '',
                    h2hCol3: cells[9]?.innerHTML || '',
                    indLocal: cells[10]?.innerHTML || '',
                    indVisitante: cells[11]?.innerHTML || '',
                    pick: cells[12]?.innerHTML || '',
                    actions: this.cleanActions(cells[13]?.innerHTML || '')
                };
            })
            .filter(Boolean);
    }

    createMatchCard(match, position, total) {
        const safeId = this.escapeHtml(match.id);
        const hasPick = this.htmlHasContent(match.pick);
        const details = [
            ['Prev. local', match.prevHome],
            ['Prev. visitante', match.prevAway],
            ['H2H estadio', match.h2hStadium],
            ['H2H general', match.h2hGeneral],
            ['H2H espejo', match.h2hCol3],
            ['Indirecta local', match.indLocal],
            ['Indirecta visitante', match.indVisitante]
        ].map(([label, value]) => this.renderDetailRow(label, value)).join('');

        return `
            <article class="match-card" data-mobile-match-id="${safeId}">
                <header class="match-card-header mobile-match-meta">
                    <span class="match-date">${this.escapeHtml(match.date)}</span>
                    <span class="mobile-match-index">${position}/${total}</span>
                </header>

                <div class="mobile-team-grid">
                    <section class="mobile-team-panel">
                        <span class="mobile-team-label">Local</span>
                        <div class="mobile-team-body">${match.homeTeam}</div>
                    </section>
                    <section class="mobile-team-panel">
                        <span class="mobile-team-label">Visitante</span>
                        <div class="mobile-team-body">${match.awayTeam}</div>
                    </section>
                </div>

                <div class="mobile-market-grid">
                    <div class="mobile-market-item">HA / O-U<br>${this.escapeHtml(match.handicap || 'N/D')}</div>
                    <div class="mobile-market-item is-result">FT<br>${this.escapeHtml(match.result || '?:?')}</div>
                </div>

                ${hasPick ? `<div class="match-pick-display">${match.pick}</div>` : ''}

                <details class="mobile-analysis">
                    <summary>Ver análisis completo</summary>
                    <div class="mobile-analysis-content">${details}</div>
                </details>

                ${match.actions ? `<div class="match-actions">${match.actions}</div>` : ''}
            </article>`;
    }

    renderDetailRow(label, value) {
        if (!this.htmlHasContent(value)) return '';
        return `
            <div class="detail-row">
                <span class="detail-label">${this.escapeHtml(label)}</span>
                <div class="detail-value">${value}</div>
            </div>`;
    }

    cleanActions(html) {
        if (!html) return '';
        const template = document.createElement('template');
        template.innerHTML = html;

        // El botón de estadísticas controla una fila collapse de la tabla
        // de escritorio. En móvil esa información ya vive en "análisis".
        template.content.querySelectorAll('[data-bs-toggle="collapse"]').forEach(node => node.remove());
        return template.innerHTML.trim();
    }

    htmlHasContent(html) {
        if (!html) return false;
        const template = document.createElement('template');
        template.innerHTML = html;
        const text = this.cleanText(template.content.textContent).toUpperCase();
        return Boolean(text && text !== 'N/A' && text !== 'N/D' && text !== '-');
    }

    cleanText(value) {
        return String(value || '').replace(/\s+/g, ' ').trim();
    }

    escapeHtml(value) {
        return String(value || '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#039;');
    }

    refresh() {
        this.scheduleRefresh();
    }
}

let mobileViewHandler;

function initMobileView() {
    mobileViewHandler = new MobileViewHandler();
}

if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initMobileView, { once: true });
} else {
    initMobileView();
}

function refreshMobileView() {
    if (mobileViewHandler) mobileViewHandler.refresh();
}
