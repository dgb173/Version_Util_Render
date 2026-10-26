// Adaptation point: exportSnapshot must return examples/snapshot.schema.json.
// No DOM selectors are invented for precacheo.html or explorer.html.
export function mountMotorButton(card, exportSnapshot, renderAnalysis) {
  const button = document.createElement('button');
  button.type = 'button';
  button.textContent = 'Motor binario v6.2';
  button.addEventListener('click', async () => {
    button.disabled = true;
    try {
      const snapshot = await exportSnapshot(card);
      const response = await fetch('http://127.0.0.1:8765/analyze', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(snapshot)
      });
      const result = await response.json();
      if (!response.ok) throw new Error(JSON.stringify(result));
      renderAnalysis(result); // Render data with textContent, not untrusted innerHTML.
    } catch (error) {
      renderAnalysis({error: String(error)});
    } finally {
      button.disabled = false;
    }
  });
  card.appendChild(button);
  return button;
}
