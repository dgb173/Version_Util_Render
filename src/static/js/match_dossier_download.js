async function downloadMatchDossierPdf(mode, button) {
    const table = document.getElementById('table-body');
    const checked = Array.from(table?.querySelectorAll('.match-card-checkbox:checked') || []);
    const rows = checked.length ? checked : Array.from(table?.querySelectorAll('tr[data-match-id], tr[data-match_id]') || [])
        .filter(row => row.style.display !== 'none');
    const ids = Array.from(new Set(rows.map(row => String(
        row.dataset.matchId || row.dataset.match_id || row.getAttribute('data-match_id') || ''
    ).trim()).filter(id => /^\d+$/.test(id))));
    const status = document.getElementById('download-cards-status');
    if (!ids.length) {
        if (status) status.textContent = 'No hay partidos seleccionados o visibles.';
        return;
    }
    const original = button.innerHTML;
    button.disabled = true;
    button.innerHTML = '<i class="fa-solid fa-spinner fa-spin me-1"></i> Generando PDF...';
    if (status) {
        status.className = 'small text-primary';
        status.textContent = `Preparando ${Math.min(ids.length, 100)} partidos y sus estadísticas...`;
    }
    try {
        const response = await fetch('/api/export_gemini_dossier_pdf', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({match_ids: ids.slice(0, 100), mode})
        });
        if (!response.ok) {
            const error = await response.json().catch(() => ({}));
            throw new Error(error.error || `Error HTTP ${response.status}`);
        }
        const blob = await response.blob();
        if (blob.type !== 'application/pdf' || !blob.size) throw new Error('El servidor no devolvió un PDF válido');
        const disposition = response.headers.get('Content-Disposition') || '';
        const filename = disposition.match(/filename="?([^";]+)"?/i)?.[1] || 'dossier_partidos.pdf';
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        setTimeout(() => URL.revokeObjectURL(url), 60000);
        if (status) {
            status.className = 'small text-success';
            status.textContent = `PDF descargado: ${response.headers.get('X-Exported-Matches') || ids.length} partidos`;
        }
    } catch (error) {
        if (status) {
            status.className = 'small text-danger';
            status.textContent = `No se pudo descargar el PDF: ${error.message}`;
        }
    } finally {
        button.disabled = false;
        button.innerHTML = original;
    }
}
