(() => {
  let key = '';
  let timer;
  let requestedAt = 0;
  const label = () => document.getElementById('cloud-bot-status');
  async function poll(kind) {
    try {
      const response = await fetch('/api/cloud_bots/run_status', {method: 'POST',
        headers: {'Content-Type': 'application/json', 'X-Trigger-Key': key}, body: JSON.stringify({kind})});
      const data = await response.json();
      if (!response.ok) throw new Error(data.error || 'No se pudo consultar el estado.');
      const run = data.run || {};
      if (new Date(run.created_at || 0).getTime() < requestedAt) return;
      if (run.status === 'completed') {
        label().textContent = run.conclusion === 'success'
          ? 'Actualización completada. Los datos aparecerán cuando termine el despliegue.'
          : 'La actualización tiene incidencias. Consulta el detalle de los bots.';
        clearInterval(timer);
      } else label().textContent = 'Actualización en curso…';
    } catch (error) { label().textContent = error.message; clearInterval(timer); }
  }
  window.launchCloudCache = async kind => {
    if (!key) {
      const dialog = document.getElementById('cloud-bot-key-dialog');
      dialog.dataset.kind = kind;
      dialog.showModal();
      return;
    }
    const buttons = document.querySelectorAll('[data-cloud-bot]');
    buttons.forEach(button => button.disabled = true);
    try {
      const response = await fetch('/api/cloud_bots/trigger', {method: 'POST',
        headers: {'Content-Type': 'application/json', 'X-Trigger-Key': key}, body: JSON.stringify({kind})});
      const data = await response.json();
      if (!response.ok) { if (response.status === 401) key = ''; throw new Error(data.error); }
      label().textContent = data.message;
      requestedAt = data.status === 'running' ? 0 : Date.now() - 5000;
      clearInterval(timer);
      timer = setInterval(() => poll(kind), 30000);
    } catch (error) { label().textContent = error.message || 'No se pudo activar la actualización.'; }
    finally { buttons.forEach(button => button.disabled = false); }
  };
  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('cloud-bot-key-form').addEventListener('submit', event => {
      event.preventDefault();
      key = document.getElementById('cloud-bot-key').value;
      document.getElementById('cloud-bot-key').value = '';
      const dialog = document.getElementById('cloud-bot-key-dialog');
      dialog.close();
      launchCloudCache(dialog.dataset.kind);
    });
    fetch('/api/cloud_bots/status').then(response => response.json()).then(data => {
      const last = data.published.upcoming;
      label().textContent = last ? `Último precacheo: ${last.saved} guardados, ${last.failed} pendientes de reintento.` : 'Actualiza la lista o descarga las fichas completas.';
      document.getElementById('cloud-bot-actions').href = data.actions_url;
    }).catch(() => {});
  });
})();
