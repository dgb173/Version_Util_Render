/* One shared, on-demand analysis viewer for Precacheo and Explorador. */
(() => {
    'use strict';
    const esc = v => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    const fmt = v => v === null || v === undefined ? 'N/D' : String(v);
    const signed = v => v === null || v === undefined ? 'N/D' : `${v > 0 ? '+' : ''}${v}`;
    const categories = {APOYO_AL_FAVORITO:'Apoyo al favorito', LINEA_NO_RESPALDADA:'Línea no respaldada', EVIDENCIA_ENFRENTADA:'Evidencia enfrentada', DATOS_INSUFICIENTES:'Datos insuficientes', MARGEN_INCIERTO:'Margen incierto'};
    const terms = {FAVOR:'A favor', CONTRA:'En contra', MIXTO:'Mixto', ND:'Sin evidencia', OVER:'Predominio over', UNDER:'Predominio under'};
    const labels = {'1':'Gana completa','0.5':'Media ganancia','0':'Devolución','-0.5':'Media pérdida','-1':'Pierde completa'};
    const result = v => v == null ? 'N/D' : labels[String(v)];
    const pill = v => `<span class="ua-pill ${esc(v)}">${esc(categories[v] || terms[v] || v)}</span>`;
    const summary = c => !c?.n ? 'Sin líneas verificadas' : `${c.win} G · ${c.half_win} ½G · ${c.push} Dev · ${c.half_loss} ½P · ${c.loss} P (n=${c.n})`;
    const cache = new Map();
    let dialog, current, requestId = 0, controller;
    const rows = () => [...document.querySelectorAll('#table-body tr[data-match-id],#table-body tr[data-match_id]')];
    const rowId = r => r.dataset.matchId || r.dataset.match_id;
    async function json(url, options = {}) {
        const response = await fetch(url, {...options, signal:options.signal || AbortSignal.timeout(60000)});
        const raw = await response.text();
        let data;
        try { data = JSON.parse(raw); }
        catch (_) { throw new Error(response.status === 404
            ? 'La ruta de análisis no está disponible en este servidor. Hay que reiniciar el servidor con la versión actual.'
            : `El servidor devolvió una respuesta no válida (HTTP ${response.status}). Vuelve a intentarlo.`); }
        if (!response.ok || !data.success) throw new Error(data.error || `Error HTTP ${response.status}`);
        return data;
    }
    function ensureDialog() {
        if (dialog) return;
        dialog = document.createElement('dialog');
        dialog.className = 'ua-dialog';
        dialog.setAttribute('aria-labelledby','ua-title');
        document.body.appendChild(dialog);
        dialog.addEventListener('click', e => {
            if (e.target === dialog || e.target.closest('[data-ua-close]')) dialog.close();
            const tab = e.target.closest('[data-ua-tab]');
            if (tab) {
                dialog.querySelectorAll('[data-ua-tab]').forEach(b => b.setAttribute('aria-selected', String(b === tab)));
                dialog.querySelectorAll('[data-ua-panel]').forEach(p => { p.hidden = p.dataset.uaPanel !== tab.dataset.uaTab; });
            }
            const action = e.target.closest('[data-ua-action]');
            if (action) runAction(action);
        });
        dialog.addEventListener('close', () => { controller?.abort(); requestId++; });
    }
    const header = a => `<header class="ua-head"><div><div class="ua-eyebrow">Análisis universal · AH + O/U</div><h2 id="ua-title">${a ? `${esc(a.home)} — ${esc(a.away)}` : 'Analizando partido'}</h2><div class="ua-muted" style="color:#c0cde1">${a ? `${esc(a.date || 'Fecha N/D')} · ${a.favorite ? `Favorito: ${esc(a.favorite)} ${signed(a.line)}` : 'Sin favorito determinado'} · O/U ${fmt(a.ou)}` : 'Reuniendo antecedentes guardados'}</div></div><button class="ua-close" data-ua-close aria-label="Cerrar análisis">×</button></header>`;
    function historyTable(list) {
        if (!list?.length) return '<p class="ua-muted">No hay antecedentes válidos para este bloque.</p>';
        return `<div class="ua-scroll"><table class="ua-table"><thead><tr><th>Fecha / sede</th><th>Equipo · rival</th><th>GF:GC · V/E/D</th><th>AH propio</th><th>Liquidación real</th><th>Con AH actual</th><th>O/U previo</th><th>Over previo / actual</th><th>Contexto</th></tr></thead><tbody>${list.map(r => `<tr><td>${esc(r.date)}<br>${esc(r.venue)}</td><td class="wrap">${esc(r.team)}<br><small>vs ${esc(r.opponent)}</small></td><td>${r.gf}:${r.ga} · ${r.wdl}</td><td>${signed(r.own_line)}</td><td>${result(r.actual_ah)}</td><td>${result(r.target_ah)}</td><td>${fmt(r.ou)}</td><td>${result(r.over_actual)}<br>${result(r.over_target)}</td><td>${r.reds?.some(x => x > 0) ? 'Expulsión · ' : ''}${r.different_league ? 'Otra competición · ' : ''}${r.age_days} días<br>${esc(r.id || 'Sin ID')}</td></tr>`).join('')}</tbody></table></div>`;
    }
    function profileCard(p) {
        const s = p.telemetry.sot;
        return `<section class="ua-card"><h3>${esc(p.team)}</h3><div class="ua-kpis"><div class="ua-kpi"><strong>${p.wins}/${p.n}</strong><span>Victorias</span></div><div class="ua-kpi"><strong>${p.wide_wins}/${p.n}</strong><span>Victorias por 2+</span></div><div class="ua-kpi"><strong>${p.wide_losses}/${p.n}</strong><span>Derrotas por 2+</span></div></div><p>${p.wins}V · ${p.draws}E · ${p.losses}D en los últimos ${p.n} disponibles.</p><p><b>AH histórico:</b> ${summary(p.actual)}</p><p><b>Aplicando la línea actual:</b> ${summary(p.target)}</p><p><b>AH habitual en esta sede:</b> ${signed(p.habitual)} · ${p.habitual_n} líneas. Cambio del AH propio: ${signed(p.line_change)} (más negativo = mayor exigencia).</p><p><b>Tiros a puerta:</b> ${fmt(s.for)} a favor / ${fmt(s.against)} en contra · n=${s.n}.</p></section>`;
    }
    function referenceCard(title, r, line) {
        if (!r) return `<section class="ua-card"><h3>${esc(title)}</h3><p>Sin antecedente verificable.</p></section>`;
        return `<section class="ua-card"><h3>${esc(title)}</h3><p><b>${esc(r.home)} ${esc(r.score)} ${esc(r.away)}</b> · ${esc(r.date)}</p><p>${esc(r.team)}: ${r.wdl === 'V' ? 'ganó' : r.wdl === 'E' ? 'empató' : 'perdió'} · margen ${signed(r.margin)}.</p><p>AH propio entonces ${signed(r.own_line)} → ahora ${signed(line)}. ${r.requirement_change == null ? 'Cambio no verificable.' : r.requirement_change > 0 ? 'La exigencia aumenta.' : r.requirement_change < 0 ? 'La exigencia disminuye.' : 'Misma exigencia.'}</p><p>Liquidación entonces: <b>${result(r.actual_ah)}</b> · con la línea actual: <b>${result(r.target_ah)}</b>.</p><p>Goles: ${r.gf+r.ga} · O/U entonces ${fmt(r.ou)} · Over actual: ${result(r.over_target)}.</p><p class="ua-muted">Antigüedad: ${r.age_days} días. ${r.reds?.some(x=>x>0) ? 'Hubo expulsiones.' : ''}</p></section>`;
    }
    function telemetry(p) {
        const names = {sot:'Tiros a puerta', shots:'Tiros', danger:'Ataques peligrosos', attacks:'Ataques', corners:'Córners'};
        return `<section class="ua-card"><h3>${esc(p.team)} · producción</h3><table class="ua-table"><thead><tr><th>Variable</th><th>A favor</th><th>En contra</th><th>Muestra</th></tr></thead><tbody>${Object.entries(p.telemetry).map(([k,v])=>`<tr><td>${names[k]}</td><td>${fmt(v.for)}</td><td>${fmt(v.against)}</td><td>${v.n}/${p.n}</td></tr>`).join('')}</tbody></table><p class="ua-muted">Medias de los antecedentes disponibles. Los ataques no sustituyen a las ocasiones ni al margen necesario.</p></section>`;
    }
    function contextCards(a) {
        return `<div class="ua-grid">${['home','away'].map(side=>{
            const c=a.context?.[side] || {}, s={total_v:'N/D',total_e:'N/D',total_d:'N/D',...(c.standings || {})};
            const ou=x=>x?.total ? `${fmt(x.over)} over · ${fmt(x.under)} under · ${fmt(x.push)} devoluciones / ${x.total} (${esc(x.source || 'fuente sin indicar')})` : 'Sin resumen guardado';
            return `<section class="ua-card"><h3>${esc(a[side])} · contexto guardado</h3><p>Posición: ${esc(s.ranking || 'N/D')} · Liga: ${esc(s.total_v || '0')}V / ${esc(s.total_e || '0')}E / ${esc(s.total_d || '0')}D, ${esc(s.total_pj || 'N/D')} partidos.</p><p>Goles de liga: ${esc(s.total_gf || 'N/D')} a favor / ${esc(s.total_gc || 'N/D')} en contra.</p><p>Intervalo desde el último antecedente disponible: ${fmt(c.rest_days)} días. Puede haber partidos intermedios sin guardar.</p><p>O/U general: ${ou(c.ou_general)}</p><p>O/U en esta sede: ${ou(c.ou_venue)}</p><p class="ua-muted">Resumen contextual; no se suma de nuevo a los antecedentes ni acredita una clasificación histórica tomada antes del encuentro.</p></section>`;
        }).join('')}</div>`;
    }
    function render(a) {
        const f = a.favorite_side ? a.profiles[a.favorite_side] : null;
        const c = a.col3;
        const groups = ['Síntesis','Forma y líneas','H2H y movimiento','Col3 e indirectas','Over / Under','Datos y criterio'];
        return header(a)+`<div class="ua-body"><div class="ua-hero">${pill(a.category)} ${pill(a.ou_category)}<h3>${a.favorite ? `${esc(a.favorite)} ${signed(a.line)} · O/U ${fmt(a.ou)}` : 'El mercado no identifica un favorito por hándicap'}</h3><p>${f ? `El favorito ganó ${f.wins}/${f.n}; al aplicar la línea actual a esos marcadores: ${summary(f.target)}.` : 'Revisa la línea y el contexto de ambos equipos.'}</p><p class="ua-muted">Clasificación descriptiva de antecedentes; los recuentos no son probabilidades del partido.</p></div>
        <nav class="ua-tabs" aria-label="Secciones del análisis">${groups.map((t,i)=>`<button data-ua-tab="${i}" aria-selected="${i===0}">${t}</button>`).join('')}</nav>
        <section data-ua-panel="0"><div class="ua-grid">${profileCard(a.profiles.home)}${profileCard(a.profiles.away)}</div><section class="ua-card"><h3>Qué diferencia a este partido</h3>${a.signals.map(s=>`<div class="ua-proof"><strong>${esc(s.label)}</strong> ${pill(s.value)}<p class="ua-muted">${esc(s.reason)}</p></div>`).join('')}</section><section class="ua-card"><h3>Huella para buscar patrones</h3><code class="ua-fingerprint">${esc(a.fingerprint)}</code><p class="ua-muted">Conserva línea exacta, sede del favorito, forma, resistencia, H2H, Col3, indirectas y goles. «N/D» significa desconocido; dos huecos iguales no confirman un patrón.</p><div class="ua-tools"><button class="ua-button primary" data-ua-action="same">Buscar mismo tipo AH + O/U</button><button class="ua-button" data-ua-action="exact">Buscar misma huella</button><button class="ua-button" data-ua-action="classify">Categorizar filas visibles</button><button class="ua-button" data-ua-action="copy">Copiar análisis</button></div><p class="ua-status" id="ua-status" role="status"></p></section></section>
        <section data-ua-panel="1" hidden>${Object.values(a.profiles).map(p=>`<section class="ua-card"><h3>${esc(p.team)} · últimos cinco disponibles</h3>${historyTable(p.rows)}<details><summary>Misma sede (${p.same_venue.length})</summary>${historyTable(p.same_venue)}</details><details><summary>Hándicaps comparables (${p.comparable.length})</summary><p>Misma competición conocida, sede y papel, AH a ±0,25 de la línea actual.</p>${historyTable(p.comparable)}</details></section>`).join('')}<div class="ua-grid">${telemetry(a.profiles.home)}${telemetry(a.profiles.away)}</div></section>
        <section data-ua-panel="2" hidden><div class="ua-grid">${referenceCard('Último H2H general',a.h2h[0],a.line)}${referenceCard('Último H2H en este estadio',a.h2h_stadium,a.line)}</div><section class="ua-card"><h3>Cambios de línea</h3><p>${esc(a.market.note)}</p><p>Etapa de la cuota guardada: ${esc(a.market.stage || 'N/D')} · captura ${esc(a.market.captured_at || 'N/D')}.</p><p>Una mayor exigencia tras perder el último H2H se conserva en la huella como una contradicción a revisar; no implica por sí sola una victoria.</p><details><summary>Otros H2H (${a.h2h.length}; duplicados excluidos)</summary>${historyTable(a.h2h)}</details></section></section>
        <section data-ua-panel="3" hidden><section class="ua-card"><h3>Col3 · enlace por rivales previos</h3>${pill(c.signal)}<p>${esc(c.reason)}</p>${c.row ? `<p><b>Rival espejo:</b> ${esc(c.mirror)} · <b>oponente común:</b> ${esc(c.common_opponent)} · ${esc(c.orientation)}.</p><p>Diferencia de margen ajustado (favorito menos espejo): <b>${signed(c.residual_delta)}</b>. Cambio de AH propio espejo → favorito: ${signed(c.line_gap)}.</p>${historyTable([c.favorite_previous,c.row])}` : '<p>No se fuerza una orientación cuando faltan equipos o antecedentes.</p>'}</section>${a.indirect.map(r=>`<section class="ua-card"><h3>Indirecta ${r.side==='left'?'local':'visitante'}</h3>${pill(r.signal)}<p>${r.verified?'Rival común verificado.':'No se ha podido verificar un rival común.'} Diferencia de margen ajustado a favor del favorito: ${signed(r.residual_delta)}.</p>${historyTable([r.row,r.reference].filter(Boolean))}</section>`).join('')}</section>
        <section data-ua-panel="4" hidden><section class="ua-card"><h3>Lectura de goles · línea ${fmt(a.ou)}</h3>${pill(a.ou_category)}<p>El tipo OVER/UNDER requiere que ambos equipos tengan al menos tres antecedentes y coincidan en el predominio de resultados frente a la línea actual. Se conserva como mixto cuando discrepan.</p></section><div class="ua-grid">${Object.values(a.profiles).map(p=>`<section class="ua-card"><h3>${esc(p.team)}</h3><p><b>Over en sus líneas originales:</b><br>${summary(p.over_actual)}</p><p><b>Over con la línea actual:</b><br>${summary(p.over_target)}</p><p>Under tiene la liquidación opuesta. Las medias ganancias y pérdidas se conservan.</p></section>`).join('')}</div><section class="ua-card"><h3>Confirmación H2H / Col3</h3><p>Último H2H · Over actual: ${result(a.h2h[0]?.over_target)}.</p><p>Col3 · Over actual: ${result(c.row?.over_target)}.</p><p class="ua-muted">Estos antecedentes se muestran por separado: no aumentan artificialmente la muestra de forma. La combinación AH + O/U describe el partido, sin multiplicar frecuencias como si fueran independientes.</p></section></section>
        <section data-ua-panel="5" hidden><section class="ua-card"><h3>Datos que faltan o requieren revisión</h3><ul class="ua-issues">${a.issues.map(x=>`<li>${esc(x)}</li>`).join('') || '<li>No se detectaron huecos en los bloques básicos.</li>'}</ul></section><section class="ua-card"><h3>Criterio universal · versión ${esc(a.version)}</h3><p>${esc(a.rules)}</p><p>Apoyo al favorito o línea no respaldada requieren coincidencia en forma, resistencia y hándicaps comparables, sin señales opuestas en los otros bloques. Si falta alguno de los tres bloques básicos, se indica información insuficiente.</p><p>Margen ajustado = goles propios − goles del rival + AH propio. En este panel el AH propio negativo concede ventaja y el positivo la recibe.</p><p>Las frecuencias se calculan sobre registros disponibles; no acreditan que la muestra esté completa. H2H, Col3 e indirectas no son observaciones independientes. La clasificación necesita validación cronológica antes de usarse como modelo predictivo.</p></section></section></div>`;
    }
    function tagRows() {
        rows().forEach(r => {
            const a = cache.get(String(rowId(r)));
            const button = r.querySelector('.ua-trigger');
            if (!a || !button) return;
            let tag = button.parentElement.querySelector('.ua-row-tag');
            if (!tag) { tag=document.createElement('span'); tag.className='ua-row-tag'; button.parentElement.appendChild(tag); }
            const text = `${categories[a.category]} · ${terms[a.ou_category] || a.ou_category}`;
            if (tag.textContent !== text) tag.textContent=text;
            button.title = `Análisis universal: ${text}`;
        });
    }
    function clearFilter() {
        document.querySelectorAll('.ua-hidden').forEach(r=>r.classList.remove('ua-hidden'));
        document.getElementById('ua-filterbar')?.remove();
    }
    function filterVisible(ids, mode, ref) {
        clearFilter();
        let shown = 0;
        rows().forEach(r => {
            const id = String(rowId(r));
            if (!ids.includes(id)) return;
            const a = cache.get(id);
            const keep = a && (mode==='exact' ? a.fingerprint===ref.fingerprint : a.line===ref.line && a.ou===ref.ou && a.category===ref.category && a.ou_category===ref.ou_category);
            if (keep) shown++;
            else {
                r.classList.add('ua-hidden');
                let sibling = r.nextElementSibling;
                while (sibling && !sibling.matches('[data-match-id],[data-match_id]')) { sibling.classList.add('ua-hidden'); sibling=sibling.nextElementSibling; }
            }
        });
        const bar=document.createElement('div'); bar.id='ua-filterbar'; bar.className='ua-filterbar';
        const label=document.createElement('span');label.textContent=`${shown}/${ids.length} filas cargadas coinciden · ${mode==='exact'?'misma huella':'mismo tipo AH + O/U'}. Los datos ausentes no confirman un patrón.`;
        const reset=document.createElement('button');reset.className='ua-button';reset.textContent='Quitar filtro universal';reset.onclick=clearFilter;
        bar.append(label,reset);document.querySelector('#table-body')?.closest('table')?.before(bar);
    }
    async function runAction(button) {
        const status=dialog.querySelector('#ua-status');
        const ref=current;
        button.disabled=true;
        try {
            const action=button.dataset.uaAction;
            if (action==='copy') { await navigator.clipboard.writeText(JSON.stringify(ref,null,2)); status.textContent='Análisis y huella copiados.'; return; }
            const ids=[...new Set(rows().filter(r=>r.getClientRects().length>0).map(r=>String(rowId(r))))];
            status.textContent=`Categorizando ${ids.length} filas cargadas…`;
            for (let i=0;i<ids.length;i+=60) {
                const data=await json('/api/quant/universal-bulk',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({match_ids:ids.slice(i,i+60)})});
                Object.entries(data.results).forEach(([id,a])=>{if (!a.error) cache.set(id,a);});
            }
            tagRows();
            const missing=ids.filter(id=>!cache.has(id)).length;
            status.textContent=`${ids.length-missing} filas categorizadas; ${missing} sin datos guardados.`;
            if (action==='same' || action==='exact') { filterVisible(ids,action,ref); dialog.close(); }
        } catch(e) { status.textContent=`No se pudo completar: ${e.message}`; }
        finally { button.disabled=false; }
    }
    async function open(id) {
        ensureDialog(); controller?.abort(); const token=++requestId; controller=new AbortController();
        current=null; dialog.innerHTML=header()+`<div class="ua-body" role="status">Calculando líneas, márgenes, H2H, Col3 y goles…</div>`;
        if (!dialog.open) dialog.showModal();
        const activeController=controller;
        const timer=setTimeout(()=>activeController.abort(),60000);
        try {
            const data=await json('/api/quant/universal/'+encodeURIComponent(id),{signal:controller.signal});
            if (token !== requestId) return;
            current=data.analysis;cache.set(String(id),current);dialog.innerHTML=render(current);
            dialog.querySelector('[data-ua-panel="5"]').insertAdjacentHTML('afterbegin',contextCards(current));
            tagRows();dialog.querySelector('[data-ua-close]').focus();
        } catch(e) {
            if(token!==requestId) return;
            dialog.innerHTML=header()+`<div class="ua-body"><p role="alert">No se pudo cargar el análisis: ${esc(e.name==='AbortError'?'Se agotó el tiempo de espera. Inténtalo de nuevo.':e.message)}</p><button class="ua-button" data-ua-close>Cerrar</button></div>`;
        } finally { clearTimeout(timer); }
    }
    document.addEventListener('click',e=>{const b=e.target.closest('[data-universal-id]');if(b){e.preventDefault();e.stopPropagation();open(b.dataset.universalId);}});
    const tbody=document.getElementById('table-body');
    if (tbody) new MutationObserver(changes=>{
        if (changes.some(c=>[...c.addedNodes,...c.removedNodes].some(n=>n.nodeName==='TR'))) {
            clearFilter(); tagRows();
        }
    }).observe(tbody,{childList:true});
    window.UniversalAnalysis={open,clearFilter,render};
})();
