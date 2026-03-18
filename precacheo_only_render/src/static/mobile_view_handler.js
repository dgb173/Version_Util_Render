/**
 * Mobile View Handler for Pre-Cacheo
 * Este archivo maneja la renderización de la vista móvil con tarjetas
 */

class MobileViewHandler {
    constructor() {
        this.isMobile = window.innerWidth <= 768;
        this.mobileContainer = null;
        this.init();
    }

    init() {
        // Crear contenedor de tarjetas móviles si no existe
        this.createMobileContainer();

        // Escuchar cambios de tamaño de ventana
        window.addEventListener('resize', () => {
            const wasMobile = this.isMobile;
            this.isMobile = window.innerWidth <= 768;

            if (wasMobile !== this.isMobile) {
                this.handleViewChange();
            }
        });

        // Renderizar vista inicial
        this.handleViewChange();
    }

    createMobileContainer() {
        // Buscar si ya existe
        this.mobileContainer = document.getElementById('mobile-cards-container');

        if (!this.mobileContainer) {
            // Crear contenedor nuevo
            this.mobileContainer = document.createElement('div');
            this.mobileContainer.id = 'mobile-cards-container';
            this.mobileContainer.className = 'mobile-cards-container';
            this.mobileContainer.style.display = 'none';

            // Insertar después de la tabla
            const tableContainer = document.querySelector('.table-responsive, #results-table')?.parentElement;
            if (tableContainer) {
                tableContainer.insertAdjacentElement('afterend', this.mobileContainer);
            }
        }
    }

    handleViewChange() {
        const table = document.querySelector('.table-responsive, #results-table')?.parentElement;

        if (this.isMobile) {
            // Mostrar vista móvil
            if (table) table.style.display = 'none';
            this.mobileContainer.style.display = 'block';
            this.renderMobileView();
        } else {
            // Mostrar vista desktop
            if (table) table.style.display = 'block';
            this.mobileContainer.style.display = 'none';
        }
    }

    renderMobileView() {
        // Obtener datos actuales de la tabla
        const matches = this.extractMatchesFromTable();

        if (matches.length === 0) {
            this.mobileContainer.innerHTML = `
                <div class="mobile-empty-state">
                    <i class="fa-solid fa-inbox"></i>
                    <p>No hay partidos que mostrar</p>
                </div>
            `;
            return;
        }

        // Renderizar tarjetas
        this.mobileContainer.innerHTML = matches.map(match => this.createMatchCard(match)).join('');

        // Agregar event listeners a las tarjetas
        this.attachCardEventListeners();
    }

    extractMatchesFromTable() {
        const matches = [];
        const tbody = document.querySelector('#table-body, tbody');

        if (!tbody) return matches;

        const rows = tbody.querySelectorAll('tr');

        rows.forEach(row => {
            if (row.style.display === 'none') return; // Skip hidden rows

            const cells = row.querySelectorAll('td');
            if (cells.length < 10) return;

            const match = {
                id: row.dataset.matchId || `match-${Math.random()}`,
                date: cells[0]?.textContent.trim() || '',
                homeTeam: cells[1]?.textContent.trim() || '',
                awayTeam: cells[2]?.textContent.trim() || '',
                handicap: cells[3]?.textContent.trim() || '',
                result: cells[4]?.textContent.trim() || '',
                prevHome: cells[5]?.innerHTML || '',
                prevAway: cells[6]?.innerHTML || '',
                h2hStadium: cells[7]?.innerHTML || '',
                h2hGeneral: cells[8]?.innerHTML || '',
                h2hCol3: cells[9]?.innerHTML || '',
                indLocal: cells[10]?.innerHTML || '',
                indVisitante: cells[11]?.innerHTML || '',
                pick: cells[12]?.innerHTML || '',
                actions: cells[13]?.innerHTML || '',
                row: row
            };

            matches.push(match);
        });

        return matches;
    }

    createMatchCard(match) {
        const handicapChip = match.handicap ? `<span class="chip chip-handicap">📊 ${match.handicap}</span>` : '';
        const resultChip = match.result && match.result !== '-' ? `<span class="chip chip-ou">⚽ ${match.result}</span>` : '';
        const pickChip = match.pick && match.pick.trim() ? `<span class="chip chip-pick">🎯 Pick</span>` : '';

        return `
            <div class="match-card" data-match-id="${match.id}">
                <div class="match-card-header">
                    <span class="match-date">${this.formatDate(match.date)}</span>
                </div>

                <div class="match-teams">
                    <div class="match-team">
                        <span class="match-team-icon">🏠</span>
                        <span>${match.homeTeam}</span>
                    </div>
                    <div class="match-vs">VS</div>
                    <div class="match-team">
                        <span class="match-team-icon">✈️</span>
                        <span>${match.awayTeam}</span>
                    </div>
                </div>

                <div class="match-chips">
                    ${handicapChip}
                    ${resultChip}
                    ${pickChip}
                </div>

                ${match.pick && match.pick.trim() ? `
                    <div class="match-pick-display">
                        ${match.pick}
                    </div>
                ` : ''}

                <button class="match-expand-toggle" onclick="mobileViewHandler.toggleCardExpansion('${match.id}')">
                    <span>Ver Análisis Completo</span>
                    <i class="fa-solid fa-chevron-down"></i>
                </button>

                <div class="match-details" id="details-${match.id}">
                    <div class="match-details-content">
                        ${this.renderDetailRow('Prev Home', match.prevHome)}
                        ${this.renderDetailRow('Prev Away', match.prevAway)}
                        ${this.renderDetailRow('H2H Estadio', match.h2hStadium)}
                        ${this.renderDetailRow('H2H General', match.h2hGeneral)}
                        ${this.renderDetailRow('H2H Col3', match.h2hCol3)}
                        ${this.renderDetailRow('Ind. Local', match.indLocal)}
                        ${this.renderDetailRow('Ind. Visitante', match.indVisitante)}
                    </div>
                </div>

                <div class="match-actions">
                    ${match.actions}
                </div>
            </div>
        `;
    }

    renderDetailRow(label, value) {
        if (!value || value.trim() === '' || value === 'N/A' || value === '-') {
            return '';
        }

        return `
            <div class="detail-row">
                <span class="detail-label">${label}</span>
                <span class="detail-value">${value}</span>
            </div>
        `;
    }

    formatDate(dateStr) {
        // Intentar hacer la fecha más legible
        const parts = dateStr.split(' ');
        if (parts.length >= 2) {
            return `${parts[0]} • ${parts[1]}`;
        }
        return dateStr;
    }

    toggleCardExpansion(matchId) {
        const details = document.getElementById(`details-${matchId}`);
        const toggle = details?.previousElementSibling;

        if (details && toggle) {
            const isExpanded = details.classList.contains('show');

            if (isExpanded) {
                details.classList.remove('show');
                toggle.classList.remove('expanded');
            } else {
                details.classList.add('show');
                toggle.classList.add('expanded');
            }
        }
    }

    attachCardEventListeners() {
        // Los event listeners para botones de acción ya deberían funcionar
        // ya que estamos copiando el HTML de las acciones directamente

        // Asegurar que los botones de acciones funcionen
        this.mobileContainer.querySelectorAll('.match-actions button').forEach(btn => {
            const onclick = btn.getAttribute('onclick');
            if (onclick) {
                btn.addEventListener('click', function (e) {
                    eval(onclick);
                });
            }
        });
    }

    refresh() {
        if (this.isMobile) {
            this.renderMobileView();
        }
    }
}

// Instancia global
let mobileViewHandler;

// Inicializar cuando el DOM esté listo
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
        mobileViewHandler = new MobileViewHandler();
    });
} else {
    mobileViewHandler = new MobileViewHandler();
}

// Hook para refrescar la vista móvil cuando se actualice la tabla
// Esto debe ser llamado después de renderizar/filtrar la tabla
function refreshMobileView() {
    if (mobileViewHandler) {
        mobileViewHandler.refresh();
    }
}
